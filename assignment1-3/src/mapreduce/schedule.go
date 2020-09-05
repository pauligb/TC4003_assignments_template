package mapreduce

import (
	"sync"
)

func waitForTasksToBeCompleted(mr *Master, avail chan string) {
	for server := range avail {
		mr.registerChannel <- server
	}
}

func createWorker(mr *Master, avail chan string, server string,
	args DoTaskArgs, i int, waitGroups sync.WaitGroup) {
	for {
		ok := call(server, "Worker.DoTask", args, new(struct{}))
		if ok {
			avail <- server
			debug("Schedule: task %d Done\n", i)
			break
		}
		debug("Schedule: task %v failed on %s\n", args, server)
	}
	waitGroups.Done()
}

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	var waitGroups sync.WaitGroup
	waitGroups.Add(ntasks)
	avail := make(chan string, ntasks)

	for i := 0; i < ntasks; i++ {
		var file string

		switch phase {
			case mapPhase:
				file = mr.files[i]
		}

		var server string
		select {
			case server = <-mr.registerChannel:
			case server = <-avail:
		}
		args := DoTaskArgs{
			JobName:       mr.jobName,
			File:          file,
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: nios,
		}

		go createWorker(mr, avail, server, args, i, waitGroups)
	}
	waitGroups.Wait()

	go waitForTasksToBeCompleted(mr, avail)




	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	debug("Schedule: %v phase done\n", phase)
}
