package example

import (
	"context"
	"sync"
)

var (
	queues map[string][]string
	lock   sync.RWMutex
)

func init() {
	queues = make(map[string][]string)
}

type LocalQueue struct{}

func (q *LocalQueue) Enqueue(ctx context.Context, key string, message string, args ...interface{}) (ok bool, err error) {
	lock.Lock()
	defer lock.Unlock()

	if _, ok = queues[key]; !ok {
		queues[key] = make([]string, 0)
	}

	queues[key] = append(queues[key], message)
	return true, nil
}

func (q *LocalQueue) BatchEnqueue(ctx context.Context, key string, messages []string, args ...interface{}) (ok bool, err error) {
	lock.Lock()
	defer lock.Unlock()

	if _, ok = queues[key]; !ok {
		queues[key] = make([]string, 0)
	}

	queues[key] = append(queues[key], messages...)
	return true, nil
}

func (q *LocalQueue) Dequeue(ctx context.Context, key string, args ...interface{}) (message string, tag string, token string, dequeueCount int64, err error) {
	lock.Lock()
	defer lock.Unlock()

	if len(queues[key]) > 0 {
		message = queues[key][0]
		queues[key] = queues[key][1:]
	}

	return
}

func (q *LocalQueue) AckMsg(ctx context.Context, key string, token string, args ...interface{}) (bool, error) {
	return true, nil
}

type MyLogger struct{}

func (logger *MyLogger) Trace(v ...interface{}) {

}

func (logger *MyLogger) Tracef(format string, args ...interface{}) {

}

func (logger *MyLogger) Debug(v ...interface{}) {

}

func (logger *MyLogger) Debugf(format string, args ...interface{}) {

}

func (logger *MyLogger) Info(v ...interface{}) {

}

func (logger *MyLogger) Infof(format string, args ...interface{}) {

}

func (logger *MyLogger) Warn(v ...interface{}) {

}

func (logger *MyLogger) Warnf(format string, args ...interface{}) {

}

func (logger *MyLogger) Error(v ...interface{}) {

}

func (logger *MyLogger) Errorf(format string, args ...interface{}) {

}
