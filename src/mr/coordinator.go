package mr

import (
	"log"
	"os"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

var mu sync.Mutex

type MasterTaskStatus int

const (
	Idle MasterTaskStatus = iota
	Inprogress
	Completed
)

type Taskinfo struct {
	Taskstatus    MasterTaskStatus //任务状态
	Starttime     time.Time        //任务开始时间
	TaskReference *Task            //任务指针
}

type Coordinator struct {
	// Your definitions here.
	TaskQueue     chan *Task        //任务管道
	TaskMeta      map[int]*Taskinfo //记录所有任务信息
	Masterstatus  State             //master状态
	Nreduce       int               //每次map映射reduce任务数量
	Inputfiles    []string          ///输入文件
	Intermediates [][]string        //map产生的中间文件
}

type Task struct {
	Input         string //文件名
	TaskState     State  //任务种类
	NReducer      int
	TaskNumber    int      //任务编号
	Intermediates []string //map产生的NReducer个中间文件名
	Output        string   //reduce输出文件名
}

type State int

const (
	Map State = iota
	Reduce
	Exit
	Wait
)

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	ret := c.Masterstatus == Exit

	return ret
}
func (c *Coordinator) catchtimeout() { //超时处理
	for {
		time.Sleep(5 * time.Second)
		mu.Lock()
		if c.Masterstatus == Exit {
			mu.Unlock()
			return
		}
		for _, taskinfo := range c.TaskMeta {
			if taskinfo.Taskstatus == Inprogress && time.Now().Sub(taskinfo.Starttime) > 10*time.Second {
				c.TaskQueue <- taskinfo.TaskReference
				taskinfo.Taskstatus = Idle
			}
		}
		mu.Unlock()
	}
}
func (c *Coordinator) createMapTask() { //创建map任务
	for i, filename := range c.Inputfiles {
		taskinfo := Task{
			Input:      filename,
			TaskState:  Map,
			NReducer:   c.Nreduce,
			TaskNumber: i,
		}
		c.TaskQueue <- &taskinfo
		c.TaskMeta[i] = &Taskinfo{
			Taskstatus:    Idle,
			TaskReference: &taskinfo,
		}
	}
}
func (c *Coordinator) createReduceTask() { //创建reduce任务
	c.TaskMeta = make(map[int]*Taskinfo)
	for i, filename := range c.Intermediates {
		taskinfo := Task{
			TaskState:     Reduce,
			NReducer:      c.Nreduce,
			TaskNumber:    i,
			Intermediates: filename,
		}
		c.TaskQueue <- &taskinfo
		c.TaskMeta[i] = &Taskinfo{
			Taskstatus:    Idle,
			TaskReference: &taskinfo,
		}
	}
}
func (c *Coordinator) Assigntask(args *Args, reply *Task) error { //分配任务，rpc应答
	mu.Lock()
	defer mu.Unlock()
	if len(c.TaskQueue) > 0 {
		*reply = *<-c.TaskQueue
		c.TaskMeta[reply.TaskNumber].Taskstatus = Inprogress
		c.TaskMeta[reply.TaskNumber].Starttime = time.Now()
	} else if c.Masterstatus == Exit {
		*reply = Task{TaskState: Exit}
	} else {
		*reply = Task{TaskState: Wait}
	}
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator { //创建master
	c := Coordinator{
		TaskQueue:     make(chan *Task, max(nReduce, len(files))),
		TaskMeta:      make(map[int]*Taskinfo),
		Masterstatus:  Map,
		Nreduce:       nReduce,
		Inputfiles:    files,
		Intermediates: make([][]string, nReduce),
	}

	c.createMapTask()
	// Your code here.
	c.server()
	go c.catchtimeout()
	return &c
}
func (c *Coordinator) Taskcomplete(task *Task, reply *Reply) error { //任务完成后rpc应答
	mu.Lock()
	defer mu.Unlock()
	if task.TaskState != c.Masterstatus || c.TaskMeta[task.TaskNumber].Taskstatus == Completed {
		return nil
	}
	c.TaskMeta[task.TaskNumber].Taskstatus = Completed
	go c.processTaskres(task)
	return nil
}
func (c *Coordinator) processTaskres(task *Task) { //对完成的任务的处理
	mu.Lock()
	defer mu.Unlock()
	switch task.TaskState {
	case Map:
		for reduceid, file := range task.Intermediates {
			c.Intermediates[reduceid] = append(c.Intermediates[reduceid], file)
		}
		if c.alltaskdone() {
			c.createReduceTask()
			c.Masterstatus = Reduce
		}
	case Reduce:
		if c.alltaskdone() {
			c.Masterstatus = Exit
		}
	}
}
func (c *Coordinator) alltaskdone() bool {
	for _, task := range c.TaskMeta {
		if task.Taskstatus != Completed {
			return false
		}
	}
	return true
}
func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
