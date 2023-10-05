package mr

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapF func(string, string) []KeyValue,
	reduceF func(string, []string) string) {
	for {
		task := getTask()
		switch task.TaskState {
		case Map:
			mapper(&task, mapF)
		case Reduce:
			reducer(&task, reduceF)
		case Wait:
			time.Sleep(5 * time.Second)
		case Exit:
			return
		}
	}
}
func mapper(task *Task, mapF func(string, string) []KeyValue) {
	content, err := os.ReadFile(task.Input)
	if err != nil {
		log.Fatal("fail to read file: "+task.Input, err)
	}
	intermediates := mapF(task.Input, string(content))

	buf := make([][]KeyValue, task.NReducer)
	for _, kv := range intermediates {
		id := ihash(kv.Key) % task.NReducer
		buf[id] = append(buf[id], kv)
	}
	mapoutput := make([]string, 0)
	for i := 0; i < task.NReducer; i++ {
		mapoutput = append(mapoutput, writeToLocalFile(task.TaskNumber, i, &buf[i]))
	}
	task.Intermediates = mapoutput
	TaskComplete(task)
}
func TaskComplete(task *Task) {
	reply := Reply{}
	call("Coordinator.Taskcomplete", task, &reply)
}
func writeToLocalFile(tasknum int, id int, buf *[]KeyValue) string {
	dir, _ := os.Getwd()
	tempfile := fmt.Sprintf("tem-%v-%v", tasknum, id)
	reducefile := fmt.Sprintf("mr-%v-%v", tasknum, id)
	tf, err := os.CreateTemp(dir, tempfile)
	if err != nil {
		log.Fatal("fail to create temp file ", err)
	}
	enc := json.NewEncoder(tf)
	for _, kv := range *buf {
		if err := enc.Encode(&kv); err != nil {
			log.Fatal("fail to write kv pair ", err)
		}
	}
	tf.Close()
	os.Rename(tf.Name(), reducefile)
	return filepath.Join(dir, reducefile)
}
func readFromLocalFile(files []string) *[]KeyValue {
	kva := []KeyValue{}
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			log.Fatal("fail to open file ", err)
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		f.Close()
	}
	return &kva
}
func reducer(task *Task, reduceF func(string, []string) string) {
	kvs := *readFromLocalFile(task.Intermediates)
	sort.Sort(ByKey(kvs))

	dir, _ := os.Getwd()
	tempFile, err := os.CreateTemp(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Fail to create temp file", err)
	}
	// 这部分代码修改自mrsequential.go
	i := 0
	for i < len(kvs) {
		//将相同的key放在一起分组合并
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		//交给reducef，拿到结果
		output := reduceF(kvs[i].Key, values)
		//写到对应的output文件
		fmt.Fprintf(tempFile, "%v %v\n", kvs[i].Key, output)
		i = j
	}
	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", task.TaskNumber)
	os.Rename(tempFile.Name(), oname)
	task.Output = oname
	TaskComplete(task)
}
func getTask() Task {
	// worker从master获取任务
	args := ExampleArgs{}
	reply := Task{}
	call("Coordinator.Assigntask", &args, &reply)
	return reply
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()

	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
