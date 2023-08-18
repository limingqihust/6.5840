package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "sync"
import "time"
var mutex sync.Mutex

// coordinator state
const (
	PHASE_MAP 		= 1
	PHASE_REDUCE 	= 2
	PHASE_FINISH   	= 3
)

// task state
const (
	TASK_FREE 		= 1
	TASK_MAPPING 	= 2
	TASK_REDUCING	= 3
	TASK_FINISH 	= 4
)

type Task struct {
	TaskId 		int							// task id, unique identifier
	Type		int							// task state
	FileName 	string						// file the task responsible for 
	StartTime	int64						// when the task begin
}

type Coordinator struct {
	MapTasks			[]Task
	ReduceTasks			[]Task
	Phase  				int					// coordinator state, MAP or REDUCE
	ReduceNum 			int					// num of reduce task
	MapNum 				int
}


// init map and reduce tasks
func (c *Coordinator) InitTask(files []string) {
	mutex.Lock()
	defer mutex.Unlock()

	fmt.Println("[Coordinator] init task begin")
	task_id := 0
	// init map tasks
	for i, filename := range files {
		c.MapTasks[i] = Task {
			TaskId 		: task_id,
			Type 		: TASK_FREE,
			FileName	: filename,
		}
		task_id += 1
	}
	// init reduce tasks
	task_id = 0
	for i:=0; i<c.ReduceNum; i++ {
		c.ReduceTasks[i] = Task {
			TaskId 		: task_id,
			Type		: TASK_FREE,
		}
		task_id += 1
	}
	fmt.Println("[Coordinator] init task done, map task", len(c.MapTasks), ", reduce task", len(c.ReduceTasks))
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// find a available map task for worker
// return false stands for no available map task, notify workers to wait 
func (c *Coordinator) AskMapTask(reply *TaskArgs) bool {
	for i, map_task := range c.MapTasks {
		if map_task.Type == TASK_FREE {
			reply.Type = WORKER_MAP
			reply.FileName = map_task.FileName
			reply.TaskId = map_task.TaskId
			reply.ReduceNum = c.ReduceNum
			c.MapTasks[i].Type = TASK_MAPPING
			c.MapTasks[i].StartTime = time.Now().Unix()
			fmt.Println("[Coordinator] coordinator return a map task, TaskId :",map_task.TaskId)
			return true
		} 
	}
	fmt.Println("[Coordinator] coordinator notify worker wait")
	return false
}

// find a available reduce task for worker
// return false stands for no available reduce task, notify workers to wait 
func (c *Coordinator) AskReduceTask(reply *TaskArgs) bool {
	for i, reduce_task := range c.ReduceTasks {
		if reduce_task.Type == TASK_FREE {
			reply.Type = WORKER_REDUCE
			reply.TaskId = reduce_task.TaskId
			reply.ReduceNum = c.ReduceNum
			reply.MapNum = c.MapNum
			c.ReduceTasks[i].Type = TASK_REDUCING
			c.ReduceTasks[i].StartTime = time.Now().Unix()
			fmt.Println("[Coordinator] coordinator return a reduce task, TaskId :",reduce_task.TaskId)
			return true
		} 
	}
	fmt.Println("[Coordinator] coordinator notify worker wait")
	return false
}

// worker ask for a task, coordinator alloc a map/reduce task
func (c *Coordinator) AskTask(request *TaskArgs, reply *TaskArgs) error {
	mutex.Lock()
	defer mutex.Unlock()
	switch c.Phase {
		case PHASE_MAP :						// try to find a map task
			if !c.AskMapTask(reply) {			
				reply.Type = WORKER_WAIT		// have no available map task for the worker, send wait reply
			}
		case PHASE_REDUCE : 					// try to find a reduce task
			if !c.AskReduceTask(reply) {	
				reply.Type = WORKER_WAIT		// have no available reduce task for the worker, send wait reply
			}
		case PHASE_FINISH : 							// notify worker to exit
			reply.Type = WORKER_EXIT		
	}
	return nil
}

// modify coordinator phase, MAP_PHASE -> REDUCE_PHASE or REDUCE_PHASE -> FINISH
func (c *Coordinator) PromotePhase() {
	if c.Phase == PHASE_MAP {
		for _, map_task := range c.MapTasks {
			if map_task.Type != TASK_FINISH {
				return 
			}
		}
		fmt.Println("[Coordinator] Map done, begin to Reduce")
		c.Phase = PHASE_REDUCE
	} else if c.Phase == PHASE_REDUCE {
		for _, reduce_task := range c.ReduceTasks {
			if reduce_task.Type != TASK_FINISH {
				return 
			}
		}
		fmt.Println("[Coordinator] Reduce done, Finish")
		c.Phase = PHASE_FINISH
	} 
}

// worker do map task done
func (c *Coordinator) DoMapTaskDone(reply *TaskArgs, _ *TaskArgs) error {
	mutex.Lock()
	defer mutex.Unlock()
	task_id := reply.TaskId
	c.MapTasks[task_id].Type = TASK_FINISH
	fmt.Println("[Coordinator] map task",task_id, "done")
	c.PromotePhase()
	return nil
}

// worker do reduce task done
func (c *Coordinator) DoReduceTaskDone(task *TaskArgs, _ *TaskArgs) error {
	mutex.Lock()
	defer mutex.Unlock()
	task_id := task.TaskId
	c.ReduceTasks[task_id].Type = TASK_FINISH
	c.PromotePhase()
	fmt.Println("[Coordinator] reduce task",task_id, "done")
	return nil
}


// check whether has a worker crash
// has been locked by main
func (c *Coordinator) CheckCrash() {
	cur_time := time.Now().Unix()
	if c.Phase == PHASE_MAP {
		for i,map_task := range c.MapTasks {
			if cur_time - map_task.StartTime > 20 {
				c.MapTasks[i].Type = TASK_FREE
			}
		}
	} else {
		for i,reduce_task := range c.ReduceTasks {
			if cur_time - reduce_task.StartTime > 20 {
				c.ReduceTasks[i].Type = TASK_FREE
			}
		}
	}
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	mutex.Lock()
	defer mutex.Unlock()
	if c.Phase == PHASE_FINISH {
		return true
	}
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator {
		MapTasks     		: make([]Task, len(files)),
		ReduceTasks			: make([]Task, nReduce),
		Phase 				: PHASE_MAP,
		ReduceNum			: nReduce,
		MapNum				:len(files),
	}
	c.InitTask(files)
	c.server()
	// for {
	// 	mutex.Lock()
	// 	if c.Phase == PHASE_FINISH {		// all task done
	// 		mutex.Unlock()
	// 		break
	// 	}
	// 	c.CheckCrash()						// check whether has worker crash
	// 	mutex.Unlock()
	// }
	return &c
}
