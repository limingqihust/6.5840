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

// worker向coordinator发送请求的类型
const (
	REQUEST_ASK_TASK = 0		// worker向coordinator请求任务
	REQUEST_FINISH_TASK = 1	  	// worker通知coordinator任务完成
)

// coordinator的任务类型
const (
	TASK_FREE = 0
	TASK_MAP = 1
	TASK_REDUCE = 2
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type SocketArgs struct {	
	Type int 				// 请求类型
	Filenames []string		// 该worker需要负责的文件名数组
	WorkerID int
	
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
