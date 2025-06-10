package main

import (
	"fmt"
	"thanhldt060802/badgerqueue"
	"time"
)

var QueueDiskInstance *badgerqueue.QueueDisk
var BatchQueueDiskInstance *badgerqueue.BatchQueueDisk

func main() {

	QueueDiskInstance = badgerqueue.NewQueueDisk("queue_disk_storage")
	BatchQueueDiskInstance = badgerqueue.NewBatchQueueDisk("batch_queue_disk_storage")

	fmt.Println("Start demo ...")

	// enqueueDemo(5000)
	// panic("hello")
	dequeDemo(5000)

	// batchEnqueueDemo(5000)
	// batchDequeDemo(2000)

	fmt.Println("End demo ...")

	QueueDiskInstance.Close()
	BatchQueueDiskInstance.Close()

}

func enqueueDemo(n int) {
	count := 0
	startTime := time.Now()
	for i := 1; i <= n; i++ {
		// time.Sleep(10 * time.Millisecond)
		if err := QueueDiskInstance.Enqueue(fmt.Sprintf("message %v", i)); err != nil {
			fmt.Println(err.Error())
			break
		}
		count++
	}
	endTime := time.Now()
	fmt.Printf("Total time for enqueue %v elements: %v\n", count, endTime.Sub(startTime))
}

func dequeDemo(n int) {
	count := 0
	startTime := time.Now()
	for i := 1; i <= n; i++ {
		// time.Sleep(10 * time.Millisecond)
		value, err := QueueDiskInstance.Dequeue()
		if err != nil {
			fmt.Println(err.Error())
			break
		}
		fmt.Println(value)
		count++
	}
	endTime := time.Now()
	fmt.Printf("Total time for deque %v elements: %v\n", count, endTime.Sub(startTime))
}

func batchEnqueueDemo(n int) {
	fmt.Println("Preparing data ...")
	values := []string{}
	for i := 1; i <= n; i++ {
		values = append(values, fmt.Sprintf("message %v", i))
	}
	fmt.Println("Prepare successful")

	startTime := time.Now()
	if err := BatchQueueDiskInstance.Enqueue(values); err != nil {
		fmt.Println(err.Error())
		return
	}
	endTime := time.Now()
	fmt.Printf("Total time for enqueue %v elements: %v\n", n, endTime.Sub(startTime))
}

func batchDequeDemo(batchSize int) {
	startTime := time.Now()
	values, err := BatchQueueDiskInstance.Dequeue(batchSize)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	endTime := time.Now()

	for _, value := range values {
		fmt.Println(value)
	}
	fmt.Printf("Total time for deque %v elements: %v\n", len(values), endTime.Sub(startTime))
}
