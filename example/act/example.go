package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/qit-team/work"
	"github.com/qit-team/work/example"
)

func main() {
	stop := make(chan int, 0)

	testJobFeature()
	q := new(example.LocalQueue)
	q2 := new(example.LocalQueue)

	job := work.New()
	job.AddQueue(q)
	job.AddQueue(q2, "kxy1")
	job.SetLogger(new(example.MyLogger))
	job.SetConsoleLevel(work.Info)
	job.SetEnableTopics("kxy1", "hts2")
	//task任务panic的回调函数
	job.RegisterTaskPanicCallback(func(task work.Task, e ...interface{}) {
		fmt.Println("task_panic_callback", task.Message)
		if len(e) > 0 {
			fmt.Println("task_panic_error", e[0])
		}
	})

	bench(job)

	//termStop(job);
	<-stop
}

// 测试job新的轮训策略
func testJobFeature() {
	q := new(example.LocalQueue)
	job := work.New()
	go addData(job)
	job.AddQueue(q)
	job.SetSleepy(time.Second, time.Second*10)
	job.AddFunc("kxy1", Me, 1, "instanceId", "groupId")
	job.Start()
}

func addData(job *work.Job) {
	time.Sleep(time.Second * 60)
	pushQueueData(job, "kxy1", 1, 1)
}

// 压测
func bench(job *work.Job) {
	RegisterWorkerBench(job)
	// 验证参数透传
	job.AddFunc("hts1", Me, 5, "instanceId", "groupId")
	pushQueueData(job, "kxy1", 1000000, 10000)
	//启动服务
	job.Start()
	//启动统计脚本
	go jobStats(job)
}

// 验证平滑关闭
func termStop(job *work.Job) {
	RegisterWorker2(job)
	//预先生成数据到本地内存队列
	pushQueueData(job, "hts1", 10000)
	pushQueueData(job, "hts2", 10000, 10000)
	pushQueueData(job, "kxy1", 10000, 20000)

	//启动服务
	job.Start()

	//启动统计脚本
	go jobStats(job)

	//结束服务
	time.Sleep(time.Millisecond * 3000)
	job.Stop()
	err := job.WaitStop(time.Second * 10)
	fmt.Println("wait stop return", err)

	//统计数据，查看是否有漏处理的任务
	stat := job.Stats()
	fmt.Println(stat)
	// count := len(queues["hts1"]) + len(queues["hts2"]) + len(queues["kxy1"])
	// fmt.Println("remain count:", count)
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

func jobStats(job *work.Job) {
	var stat map[string]int64
	var count int64
	var str string

	lastStat := job.Stats()
	keys := make([]string, 0)
	for k, _ := range lastStat {
		keys = append(keys, k)
	}

	for {
		stat = job.Stats()

		str = ""
		for _, k := range keys {
			count = stat[k] - lastStat[k]
			if count > 0 {
				str += k + ":" + strconv.FormatInt(count, 10) + "   "
			}
		}
		fmt.Println(time.Now().Format("2006-01-02 15:04:05"), str)

		lastStat = stat
		time.Sleep(time.Second)
	}
}

/**
 * 配置队列任务
 */
func RegisterWorkerBench(job *work.Job) {
	job.AddWorker("kxy1", &work.Worker{Call: work.MyWorkerFunc(Mock), MaxConcurrency: 100})
}

func Mock(task work.Task) work.TaskResult {
	time.Sleep(time.Millisecond * 5)
	return work.TaskResult{Id: task.Id}
}

/**
 * 配置队列任务
 */
func RegisterWorker2(job *work.Job) {

	job.AddFunc("hts1", Me, 5)
	job.AddFunc("hts2", Me, 3)
	job.AddWorker("kxy1", &work.Worker{Call: work.MyWorkerFunc(Me), MaxConcurrency: 5})
}

func Me(task work.Task) work.TaskResult {
	time.Sleep(time.Millisecond * 50)
	i, _ := strconv.Atoi(task.Message)
	if i%10 == 0 {
		panic("wo cuo le " + task.Message + " (" + task.Topic + ")")
	}
	s, _ := work.JsonEncode(task)
	fmt.Println("do task", s)
	return work.TaskResult{Id: task.Id}
}
