package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "strconv"
import "time"
import "path/filepath"
import "slices"
import "io"
import "encoding/json"


// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


const (
	MapTask = iota
	ReduceTask
	WaitTask
	ExitTask
)

var coordSockName string // socket for coordinator


// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	
	conn := establishConnection(sockname)
	defer conn.Close()

	// Your worker implementation here.

	var MachineId int = os.Getpid()
	var prevTaskId int = -1
	var CompletedType = -1

	for {	
		arg := TaskArg{MachineId: MachineId, TaskId: prevTaskId, CompletedType: CompletedType}
		reply, err := requestWork(conn, &arg)
		if err != nil {
    			switch err.Error() {
    			case "Wait for new task":
		        	prevTaskId = -1
		        	CompletedType = -1
				time.Sleep(time.Second)
        			continue

    			case "Computation Finished":
        			return

    			default:
        			time.Sleep(time.Second)
        			continue
    			}
		}

		NReduce := reply.NReduce	
		Input := reply.Input
		TaskId := reply.TaskId

		if reply.TypeOfTask == 0 {	// Mapper
			
			contents, err := os.ReadFile(Input)
			if err != nil {
				// The file isn't opening so maybe retry? Or just crash? You can retry a fixed number of times and then beam a failure message.
				log.Fatal("File not opening", err)
			}

			emittedKeyValues := mapf(Input, string(contents)) // Does this need to be sorted? Lets sort this: its []KeyValue, and we sort ascending w/key and tiebreak with val

			enc := make([]*json.Encoder, NReduce)
			intermediateFiles := make([]*os.File, NReduce)

			for i := 0; i < NReduce; i++ {
				file, err := os.CreateTemp(".", "mr-tmp-*")
				if err != nil {
					log.Fatal(err)
				}
				enc[i] = json.NewEncoder(file)
				intermediateFiles[i] = file
			}
			
			for _, kv := range emittedKeyValues {
				bucket := ihash(kv.Key) % NReduce
				err := enc[bucket].Encode(&kv)	// So this has multiple JSON objects written back to back.
				if err != nil {
					log.Fatal("Failed to encode JSON", err)
				}
			}

			for i, file := range intermediateFiles {
				fileName := "mr-int-"+strconv.Itoa(TaskId)+ "-" + strconv.Itoa(i)
				file.Close()
				err := os.Rename(file.Name(), fileName)	// This thing is atomic.
				if err != nil {
					log.Fatal("Failed to rename JSON", err)
				}
			}
		}
		
		if reply.TypeOfTask == 1 {	// Reducer
			partitionId := TaskId
			partitionFiles, err := filepath.Glob(fmt.Sprintf("mr-int-*-%d", partitionId))
			
			var intermediateKVs []KeyValue
			for _, filePath := range partitionFiles {
				intermediateFile, err := os.Open(filePath)
				if err != nil {
					log.Fatal(err)
				}
				dec := json.NewDecoder(intermediateFile)
				for {
					var kv KeyValue
				    	err := dec.Decode(&kv)
				    	if err == io.EOF {
				        	break
				    	}
				    	if err != nil {
				        	log.Fatal("JSON decoding failed: ", err)
				    	}
				    	intermediateKVs = append(intermediateKVs, kv)
				}
				intermediateFile.Close()
			}
			// Expects key(string), values([]string)
			finalFile, err := os.CreateTemp(".", "mr-out-tmp-*")
			if err != nil {
				log.Fatal("Failed to create tmp reduce file", err)
			}
			finalKV := make(map[string][]string)
			for _, KV := range intermediateKVs {
				finalKV[KV.Key] = append(finalKV[KV.Key], KV.Value)
			}
			keys := make([]string, 0, len(finalKV))
			for key, _ := range finalKV {
				keys = append(keys, key)
			}
			slices.Sort(keys)
			for _, key := range keys {
				valueList := finalKV[key]
				slices.Sort(valueList)
				finalResult := reducef(key, valueList)
				_, _ = fmt.Fprintf(finalFile, "%v %v\n", key, finalResult)
			}

			finalFileName := "mr-out-"+strconv.Itoa(partitionId)
			finalFile.Close()
			err = os.Rename(finalFile.Name(), finalFileName)
			if err != nil {
				log.Fatal("Failed to rename Final File", err)
			}
		}
		prevTaskId = reply.TaskId
		CompletedType = reply.TypeOfTask	
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}
func establishConnection(sockname string) *rpc.Client {
	conn, err := rpc.DialHTTP("unix", sockname)
	if(err != nil){
		log.Fatal("Connection Failed", err)
		return nil
	}
	return conn
}

func requestWork(conn *rpc.Client, arg *TaskArg) (*TaskReply, error) {
    reply := new(TaskReply)
    err := conn.Call("Coordinator.AssignWork", arg, reply)
    if err != nil {
        return nil, err
    }
    return reply, nil
}	// so it must beam the machine id. will fill in other as you need.
