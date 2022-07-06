package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	fail := 0
	done := false
	// ask for task until all mapreduce tasks are done.
	for done == false {
		// sleeping with time.Sleep() between each request.
		reply := CallTask()
		if reply == nil {
			time.Sleep(time.Second)
			fail += 1
			if fail == 2 {
				done = true
			}
			continue
		}
		fail = 0
		//fmt.Println(reply.ReplyJob.JobType)
		switch reply.ReplyJob.JobType {
		case "exit":
			done = true
		case "map":
			MapWorker(mapf, &reply.ReplyJob)
		case "reduce":
			ReduceWorker(reducef, &reply.ReplyJob)
		case "wait":
		}
		time.Sleep(time.Second)
	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
}

func MapWorker(mapf func(string, string) []KeyValue, job *Job) {
	intermediate := make([][]KeyValue, job.NReduce)
	// store the intermedia results.
	fileLoc := make([]string, 0)
	for _, JobFile := range job.JobFile {
		file, err := os.Open(JobFile)
		if err != nil {
			log.Fatalf("cannot open %v", JobFile)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", JobFile)
		}
		file.Close()
		kva := mapf(JobFile, string(content))
		for _, key := range kva {
			id := ihash(key.Key) % job.NReduce
			intermediate[id] = append(intermediate[id], key)
		}
	}
	for id, inter := range intermediate {
		fname := "mr-" + strconv.Itoa(job.JobID) + "-" + strconv.Itoa(id)
		fileLoc = append(fileLoc, fname)
		outfile, err := os.Create(fname)
		if err != nil {
			log.Fatal(err)
		}
		enc := json.NewEncoder(outfile)
		for _, kv := range inter {
			if err := enc.Encode(&kv); err != nil {
				log.Fatal(err)
			}
		}
		outfile.Close()
	}

	SendBack(fileLoc, job)
}

func ReduceWorker(reducef func(string, []string) string, job *Job) {
	if len(job.JobFile) == 0 {
		return
	}

	reduceId := strings.Split(job.JobFile[0], "-")[2]
	kva := make([]KeyValue, 0)

	for _, JobFile := range job.JobFile {
		file, err := os.Open(JobFile)
		if err != nil {
			log.Fatalf("cannot open %v", JobFile)
		}
		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))
	oname := "mr-out-" + reduceId
	ofile, _ := ioutil.TempFile(".", oname)
	defer os.Remove(ofile.Name())

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-X.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
	_, err := os.Stat(oname)
	if os.IsNotExist(err) {
		fmt.Println(ofile.Name())
		fmt.Println(oname)
		os.Rename(ofile.Name(), oname)
	}
	ReduceDone(job)
}

func CallTask() *Reply {
	args := Args{}

	reply := Reply{}

	ok := call("Coordinator.TaskRequest", &args, &reply)
	if ok {
		return &reply
	}
	fmt.Printf("call failed!\n")
	return nil
}

func SendBack(files []string, job *Job) {
	args := Args{FileLoc: files, WorkingJob: job}
	reply := Reply{}

	ok := call("Coordinator.ReceiveTask", &args, &reply)
	if !ok {
		fmt.Println("call failed!")
	}
}

func ReduceDone(job *Job) {
	args := Args{WorkingJob: job}
	reply := Reply{}

	ok := call("Coordinator.ReduceDone", &args, &reply)
	if !ok {
		fmt.Println("call failed!")
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
