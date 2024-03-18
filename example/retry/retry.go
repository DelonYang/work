package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/qit-team/work"
	"github.com/qit-team/work/example"
)

const (
	TopicRetry = "topic:retry"
)

func main() {
	job := work.New()
	job.AddFuncWithRetry(TopicRetry, test, 3, 3)
	job.AddQueue(&example.LocalQueue{})
	job.SetConsoleLevel(work.Info)
	job.SetLogger(new(example.MyLogger))
	pushQueueData(job, TopicRetry, 1, 1)
	job.Start()
	select {}
}

func test(task work.Task) work.TaskResult {
	fmt.Println("task", task)
	time.Sleep(time.Second)
	return work.TaskResult{State: work.StateFailed}
}

func pushQueueData(job *work.Job, topic string, args ...int) {
	ctx := context.Background()
	start := 1
	length := 1
	if len(args) > 0 {
		length = args[0]
		if len(args) > 1 {
			start = args[1]
		}
	}

	strs := make([]string, 0)
	for i := start; i < start+length; i++ {
		strs = append(strs, strconv.Itoa(i))
	}
	if len(args) > 0 {
		job.BatchEnqueue(ctx, topic, strs)
	}
}
