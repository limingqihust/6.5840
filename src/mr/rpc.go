package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const (
	// worker向coordinator发送请求的类型
	WORKER_REQUEST 	= 1		// worker向coordinator请求任务
	WORKER_FINISH 	= 2	  	// worker通知coordinator任务完成
	
	// coordinator向worker发送响应的任务类型
	WORKER_EXIT 	= 3
	WORKER_MAP 		= 4
	WORKER_REDUCE 	= 5
	WORKER_WAIT 	= 6
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type TaskArgs struct {	
	Type 		int 			// 请求类型
	FileName 	string			// 该worker需要负责的文件名数组
	TaskId 		int				// 任务id
	ReduceNum 	int				// reduce任务数量
	MapNum		int 			// map任务数量
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
