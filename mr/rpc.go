package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskReply struct {
    TaskId     int
    TypeOfTask int
    Input      string
    NReduce    int
}

type TaskArg struct {
    MachineId     int
    TaskId        int
    CompletedType int
}
