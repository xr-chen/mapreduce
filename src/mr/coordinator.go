package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Job struct {
	JobType string
	JobFile []string
	NReduce int
	JobID   int
}

type Coordinator struct {
	MMap       int
	NReduce    int
	Idle       []Job
	InProgress map[int]*Job
	Intermedia [][]string
	TaskId     int
	mu         sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) TaskRequest(args *Args, reply *Reply) error {
	// held a lock when worker call RPC and coordinator needs to modify its state.
	var jobID int
	c.mu.Lock()
	if len(c.Idle) == 0 && len(c.InProgress) == 0 {
		reply.ReplyJob = Job{"exit", nil, c.NReduce, -1}
	} else if len(c.Idle) == 0 {
		reply.ReplyJob = Job{"wait", nil, c.NReduce, -1}
	} else {
		reply.ReplyJob = c.Idle[0]
		c.InProgress[c.Idle[0].JobID] = &c.Idle[0]
		jobID = c.Idle[0].JobID
		c.Idle = c.Idle[1:]
	}
	c.mu.Unlock()
	// if the worker didn't finish its work within 10s, we will add this task to 'idle' again
	go func(id int) {
		time.Sleep(time.Second * 10)
		c.mu.Lock()
		defer c.mu.Unlock()
		if _, ok := c.InProgress[id]; ok {
			unFinishied := c.InProgress[id]
			delete(c.InProgress, id)
			unFinishied.JobID = c.TaskId
			c.Idle = append(c.Idle, *unFinishied)
			c.TaskId += 1
		}
	}(jobID)
	return nil
}

func (c *Coordinator) ReceiveTask(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.InProgress[args.WorkingJob.JobID]; !ok {
		return nil
	}
	delete(c.InProgress, args.WorkingJob.JobID)
	for _, file := range args.FileLoc {
		reduceId, _ := strconv.Atoi(strings.Split(file, "-")[2])
		c.Intermedia[reduceId] = append(c.Intermedia[reduceId], file)
		if len(c.Intermedia[reduceId]) == c.MMap {
			c.Idle = append(c.Idle, Job{JobType: "reduce", JobFile: c.Intermedia[reduceId], NReduce: c.NReduce, JobID: c.TaskId})
			c.TaskId += 1
		}
	}
	return nil
}

func (c *Coordinator) ReduceDone(args *Args, reply *Reply) error {
	c.mu.Lock()
	delete(c.InProgress, args.WorkingJob.JobID)
	c.mu.Unlock()
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	c.mu.Lock()
	ret := len(c.InProgress) == 0 && len(c.Idle) == 0
	//fmt.Printf("%d jobs are in progress, %d jobs are idle\n", len(c.InProgress), len(c.Idle))
	c.mu.Unlock()
	if ret {
		time.Sleep(time.Second * 3)
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// each file corresponds to one "split", and is the input to one Map task
	c.NReduce = nReduce
	c.MMap = len(files)
	c.Idle = make([]Job, 0)
	for _, file := range files {
		c.Idle = append(c.Idle, Job{JobType: "map", JobFile: []string{file}, NReduce: nReduce, JobID: c.TaskId})
		c.TaskId += 1
	}
	c.InProgress = make(map[int]*Job)
	c.Intermedia = make([][]string, nReduce)
	c.server()
	return &c
}
