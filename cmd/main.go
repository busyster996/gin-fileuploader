package main

import (
	"context"
	_ "embed"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pires/go-proxyproto"
	"github.com/tus/tusd/v2/pkg/filelocker"
	"github.com/tus/tusd/v2/pkg/filestore"
	tusd "github.com/tus/tusd/v2/pkg/handler"
	"golang.org/x/exp/slog"

	"github.com/busyster996/gin-fileuploader/internal/redis/locker"
	"github.com/busyster996/gin-fileuploader/internal/redis/store"
)

//go:embed index.html
var indexHtml []byte

var (
	host      string
	port      int
	uploadDir string
	redisUri  string
)

func main() {
	flag.StringVar(&host, "host", "0.0.0.0", "listen host addr")
	flag.IntVar(&port, "port", 8080, "listen port")
	flag.StringVar(&uploadDir, "upload-dir", "./uploads", "upload dir")
	flag.StringVar(&redisUri, "redis-uri", "", "redis uri")
	flag.Parse()

	serverCtx, cancelServerCtx := context.WithCancelCause(context.Background())
	_ = os.MkdirAll(uploadDir, os.ModeDir)
	composer := tusd.NewStoreComposer()
	if redisUri != "" {
		_store, err := store.New(uploadDir, redisUri)
		if err != nil {
			slog.Error(fmt.Sprintf("failed to create store: %v", err))
			os.Exit(255)
		}
		_locker, err := locker.New(redisUri)
		if err != nil {
			slog.Error(fmt.Sprintf("failed to create locker: %v", err))
			os.Exit(255)
		}
		_store.UseIn(composer)
		_locker.UseIn(composer)
	} else {
		_store := filestore.New(uploadDir)
		_locker := filelocker.New(uploadDir)
		_store.UseIn(composer)
		_locker.UseIn(composer)
	}
	tusdHandler, err := tusd.NewHandler(tusd.Config{
		Logger:        slog.Default(),
		BasePath:      "/api/v1/files/",
		StoreComposer: composer,
	})
	if err != nil {
		slog.Error(fmt.Sprintf("failed to create tusd handler: %v", err))
		os.Exit(255)
	}

	gin.SetMode(gin.ReleaseMode)
	gin.DisableConsoleColor()
	router := gin.New()
	router.Any("/api/v1/files", gin.WrapH(http.StripPrefix("/api/v1/files", tusdHandler)))
	router.Any("/api/v1/files/*any", gin.WrapH(http.StripPrefix("/api/v1/files/", tusdHandler)))
	router.Any("/", func(c *gin.Context) {
		c.Header("Content-Type", "text/html")
		_, _ = c.Writer.Write(indexHtml)
	})

	ln, err := net.Listen("tcp", net.JoinHostPort(host, fmt.Sprintf("%d", port)))
	if err != nil {
		log.Fatalln("failed to listen", err)
	}
	log.Println("listen on", ln.Addr().String())

	server := &http.Server{
		Handler:           router,
		ReadHeaderTimeout: 60 * time.Second,
		IdleTimeout:       60 * time.Second,
		ReadTimeout:       0,
		WriteTimeout:      0,
		MaxHeaderBytes:    15 << 20, // 15MB
		BaseContext: func(_ net.Listener) context.Context {
			return serverCtx
		},
	}
	shutdownComplete := setupSignalHandler(server, cancelServerCtx)
	err = server.Serve(&proxyproto.Listener{Listener: ln})
	if errors.Is(err, http.ErrServerClosed) {
		<-shutdownComplete
	} else if err != nil {
		log.Fatalln("failed to serve", err)
	}
}

func setupSignalHandler(server *http.Server, cancelServerCtx context.CancelCauseFunc) <-chan struct{} {
	shutdownComplete := make(chan struct{})

	// We read up to two signals, so use a capacity of 2 here to not miss any signal
	c := make(chan os.Signal, 2)

	// os.Interrupt is mapped to SIGINT on Unix and to the termination instructions on Windows.
	// On Unix we also listen to SIGTERM.
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	// When closing the server, cancel its context so all open requests shut down as well.
	// See context.go for the logic.
	server.RegisterOnShutdown(func() {
		cancelServerCtx(http.ErrServerClosed)
	})

	go func() {
		// First interrupt signal
		<-c
		log.Println("Received interrupt signal. Shutting down tusd...")

		// Wait for second interrupt signal, while also shutting down the existing server
		go func() {
			<-c
			log.Println("Received second interrupt signal. Exiting immediately!")
			os.Exit(1)
		}()

		// Shutdown the server, but with a user-specified timeout
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err := server.Shutdown(ctx)

		if err == nil {
			log.Println("Shutdown completed. Goodbye!")
		} else if errors.Is(err, context.DeadlineExceeded) {
			log.Println("Shutdown timeout exceeded. Exiting immediately!")
		} else {
			log.Println("Failed to shutdown gracefully: ", "err", err)
		}

		close(shutdownComplete)
	}()

	return shutdownComplete
}

func apiLogger(c *gin.Context) {
	start := time.Now()
	c.Next()
	latency := time.Since(start)

	status := c.Writer.Status()
	clientIP := c.ClientIP()
	method := c.Request.Method
	proto := c.Request.Proto
	path := c.Request.URL.String()
	userAgent := c.Request.UserAgent()

	if len(c.Errors) > 0 {
		for _, err := range c.Errors.Errors() {
			log.Println(clientIP, method, proto, status, path, latency, err)
		}
		c.AbortWithStatus(http.StatusInternalServerError)
	} else {
		log.Println(clientIP, method, proto, status, path, latency, userAgent)
	}
}

func apiRecovery(c *gin.Context) {
	defer func() {
		if err := recover(); err != nil {
			handlePanic(c, err)
		}
	}()
	c.Next()
}

func handlePanic(c *gin.Context, err interface{}) {
	if isBrokenPipeError(err) {
		httpRequest, _ := httputil.DumpRequest(c.Request, false)
		log.Println("Broken pipe:", c.Request.URL.Path, string(httpRequest), err)
		c.Abort() // Avoid returning InternalServerError for broken pipes
		return
	}

	// Log panic details and return 500
	httpRequest, _ := httputil.DumpRequest(c.Request, false)
	log.Println("[Recovery from panic]",
		time.Now().Format(time.RFC3339),
		string(httpRequest),
		string(debug.Stack()),
		err,
	)
	c.AbortWithStatus(http.StatusInternalServerError)
}

func isBrokenPipeError(err interface{}) bool {
	ne, ok := err.(*net.OpError)
	if !ok {
		return false
	}
	se, ok := ne.Err.(*os.SyscallError)
	if !ok {
		return false
	}

	errMsg := strings.ToLower(se.Error())
	return strings.Contains(errMsg, "broken pipe") || strings.Contains(errMsg, "connection reset by peer")
}
