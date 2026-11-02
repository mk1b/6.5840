package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
import "errors"
import "strconv" 


type Coordinator struct {
	Mu sync.Mutex
	Remaining []int
	InProg map[int]int
	TimeAssign map[int]time.Time
	NReduce int
	NMap int
	Task int
	Files []string
}

func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (master *Coordinator) Done() bool {
	master.Mu.Lock()
	defer master.Mu.Unlock()
	return master.Task == 2
}

func MakeCoordinator(sockname string, Files []string, NReduce int) *Coordinator {	// right okay.
	master := Coordinator{}	// Its gonna be concentrated here.
	master.NReduce = NReduce
	master.NMap = len(Files)
	master.Remaining = []int{}
	for i := 0; i < len(Files); i++ {
		master.Remaining = append(master.Remaining, i)
	}
	master.InProg = make(map[int]int)
	master.TimeAssign = make(map[int]time.Time)
	master.Files = Files
	go func(){
		for{
			time.Sleep(500 * time.Millisecond)
			master.Mu.Lock()
			if master.Task == 2 {
				master.Mu.Unlock()
				return
			}
			now := time.Now()
			for MachineId, assignedTime := range master.TimeAssign{
				if now.Sub(assignedTime) >= 10 * time.Second {
					if TaskId, ok := master.InProg[MachineId]; ok {	//delete only if it is in progress. if not in progress(assignwork has deleted), then np, ignore.
						master.Remaining = append(master.Remaining, TaskId)
						delete(master.InProg, MachineId)
					}
					delete(master.TimeAssign, MachineId)
				}
			}
			master.Mu.Unlock()
		}
	
	}()
	master.server(sockname)
	return &master
}

func (c *Coordinator) AssignWork(args *TaskArg, reply *TaskReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	MachineId := args.MachineId
	TaskId := args.TaskId
	CompletedType := args.CompletedType
	if TaskId != -1 && CompletedType == c.Task {
		if assigned, ok := c.InProg[MachineId]; ok && assigned == TaskId {
        		delete(c.InProg, MachineId)
        		delete(c.TimeAssign, MachineId)
    		}
	}
	if c.Task == 0 {
		if len(c.Remaining) + len(c.InProg) == 0 {	// every task is done. This makes sure that even the last thread signs off by ejecting that inProg. Okay!
			c.Task = 1
			for i := 0; i < c.NReduce; i++ {
				c.Remaining = append(c.Remaining, i)
			}
			return errors.New("Wait for new task")
		}
		remnLen := len(c.Remaining)
		if remnLen == 0 {	// no tasks to give
			return errors.New("Wait for new task")
		}
		reply.TaskId = c.Remaining[remnLen - 1]	
		
		c.InProg[MachineId] = reply.TaskId
		c.TimeAssign[MachineId] = time.Now()
		c.Remaining = c.Remaining[:remnLen - 1]	//give task
		
		reply.TypeOfTask = 0
		reply.Input = c.Files[reply.TaskId]	// here the input needs to be the file name. Taskid is the index.
		reply.NReduce = c.NReduce
		
		return nil
	}
	if c.Task == 1 {
		if len(c.Remaining) + len(c.InProg) == 0 {
			c.Task = 2
			return errors.New("Computation Finished")
		}
		remnLen := len(c.Remaining)
		if remnLen == 0 {
			return errors.New("Wait for new task")
		}
		reply.TaskId = c.Remaining[remnLen - 1]	//[3,4,7]
		
		c.InProg[MachineId] = reply.TaskId
		c.TimeAssign[MachineId] = time.Now()
		c.Remaining = c.Remaining[:remnLen - 1]
		
		reply.TypeOfTask = 1
		reply.Input = strconv.Itoa(reply.TaskId)	// Expects the partition as the input and as string.
		reply.NReduce = c.NReduce
		
		return nil
	}
	
	if c.Task == 2 {
		reply.TypeOfTask = 2
		return errors.New("Computation Finished")
	}
	return nil	
	
}
