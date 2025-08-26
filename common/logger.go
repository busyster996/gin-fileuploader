package common

type ILogger interface {
	Printf(format string, args ...interface{})
}
