package main

import (
	"fmt"
	"thanhldt060802/queue"
	"time"
)

var QueueDiskInstance *queue.QueueDisk

func main() {

	QueueDiskInstance = queue.NewQueueDisk("queue_disk_storage")

	fmt.Println("Start demo ...")

	EnqueueDemo(10000)
	DequeDemo(10000)

	fmt.Println("End demo ...")

	QueueDiskInstance.Close()

}

func EnqueueDemo(n int) {
	count := 0
	startTime := time.Now()
	for i := 1; i <= n; i++ {
		if err := QueueDiskInstance.Enqueue(fmt.Sprintf("message %v", i)); err != nil {
			fmt.Println(err.Error())
			break
		}
		count++
	}
	endTime := time.Now()
	fmt.Printf("Total time for enqueue %v elements: %v\n", count, endTime.Sub(startTime))
}

func DequeDemo(n int) {
	count := 0
	startTime := time.Now()
	for i := 1; i <= n; i++ {
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
