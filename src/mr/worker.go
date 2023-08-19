package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "io/ioutil"
import "encoding/json"
import "sort"
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// main/mrworker.go calls this function.
// 1 send request to coordinator to ask for task
// 2 execute ask
// 3 send reply to coordinator to notify 
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	log_file, err := os.OpenFile("worker.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("open log file failed")
		os.Exit(-1)
	}
	defer log_file.Close()
	log.SetOutput(log_file)

	for {
		request := TaskArgs{}
		reply := TaskArgs{}
		log.Println("[Worker] begin ask task, i am",os.Getpid())
		AskTask(&request, &reply)		// ask a task to coordinator
		if reply.Type == WORKER_EXIT {
			break
		}
		switch reply.Type {
			case WORKER_MAP : 			// receive a map task
				DoMapTask(mapf, reply)
				DoMapTaskDone(reply)
			case WORKER_REDUCE :
				log.Println("[Worker] receive reduce task, task id :", reply.TaskId)
				DoReduceTask(reducef, reply)
				DoReduceTaskDone(reply)
				log.Println("[Worker] reduce task done, task id :", reply.TaskId)
			case WORKER_WAIT : 			// coordinator has no available tasks, wait
				log.Println("[Worker] no available task, wait")
				time.Sleep(5*time.Second)
			default :
				fmt.Println("[Worker] receive undefined task type")
				os.Exit(1)
		}
	}
}

// worker ask for a task to coordinator 
func AskTask(request *TaskArgs, reply *TaskArgs) {
	request.Type = WORKER_REQUEST
	ok := call("Coordinator.AskTask", request, reply)
	if !ok {
		fmt.Println("call failed!")
		os.Exit(-1)
	}
}

// worker do map task
// 1 read corresponding file and invoke mapf to compute intermediate
// 2 save intermediate to temp file
func DoMapTask(mapf func(string, string) []KeyValue, task TaskArgs) {
	// 1 compute intermediate with mapf
	intermediate := []KeyValue{}
	filename := task.FileName
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("cannot open", filename)
		os.Exit(-1)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Println("cannot read", filename)
		os.Exit(-1)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)		// accumulate the intermediate Map output

	// 2 save mapf output to temp file
	reduce_num := task.ReduceNum
	hash_kv := make([][]KeyValue, reduce_num)
	for _, kv := range intermediate {				// write every key to corresponding temp
		pos := ihash(kv.Key) % reduce_num
		hash_kv[pos]=append(hash_kv[pos], kv)
	}
	for i:=0; i<reduce_num; i++ {		// mr-out-[reduce_task_id]-[map_task_id]
		oname := "mr-tmp-" + fmt.Sprintf("%d", i) + "-" + fmt.Sprintf("%d",task.TaskId)
		ofile,_ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range hash_kv[i] {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Println("Encode error")
				os.Exit(-1)
			}
		} 
		ofile.Close()
	}
}

// worker do map task done, notify coodinator
func DoMapTaskDone(task TaskArgs) {
	task_done_reply := TaskArgs{}
	task_done_reply.Type = WORKER_FINISH
	task_done_reply.TaskId = task.TaskId
	ok := call("Coordinator.DoMapTaskDone",&task_done_reply, nil)
	if !ok {
		fmt.Println("call failed")
		os.Exit(-1)
	}
}

func DoReduceTask(reducef func(string, []string) string, task TaskArgs) {
	task_id := task.TaskId

	// 1 read from tmp file
	intermediate := []KeyValue{}
	for i:=0;i<task.MapNum;i++ {			// mr-out-[reduce_task_id]-[map_task_id]
		tmp_filename := "mr-tmp-" + fmt.Sprintf("%d", task_id) + "-" + fmt.Sprintf("%d", i)
		tmp_file, err := os.Open(tmp_filename)
		if err != nil {
			fmt.Println("cannot open", tmp_filename)
			os.Exit(-1)
		}
		dec := json.NewDecoder(tmp_file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err!=nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		tmp_file.Close()
	}

	// 2 do reduce task, write result to mr-out-X
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + fmt.Sprintf("%d", task_id)
	ofile,err := os.Create(oname)
	if err!=nil {
		fmt.Println("cannot create", oname)
		os.Exit(-1)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		log.Println("[Worker] DoReduceTask : begin reducef, i am",os.Getpid(), ", task id :", task_id)
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
	log.Println("[Worker] DoReduceTask done, task id : ", task_id)
}

func DoReduceTaskDone(task TaskArgs) {
	task_done_reply := TaskArgs{}
	task_done_reply.Type = WORKER_FINISH
	task_done_reply.TaskId = task.TaskId
	ok := call("Coordinator.DoReduceTaskDone",&task_done_reply, nil)
	if !ok {
		fmt.Println("call failed")
		os.Exit(-1)
	}
}
//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
