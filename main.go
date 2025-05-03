package main

import (
	"fmt"
	"runtime"
)

func main() {
	noOfWorkers, maxPrime := 4, 2000

	workerPool := NewWorkerPool(noOfWorkers, maxPrime)
	workerPool.Init()
	numGoroutines := runtime.NumGoroutine()
	fmt.Printf("Number of Running Goroutines: %d\n", numGoroutines)
	fmt.Printf("Progress Bar:\n")
	for i := 1; i <= maxPrime; i++ {
		//ascii := strconv.Itoa(i)
		newInfo := Info{
			//Name:        ascii + ascii + ascii,
			//PhNumber:    []int{i, i},
			//Email:       []string{ascii, ascii},
			PrimeNumber: i,
		}
		workerPool.inCh <- newInfo
	}
	fmt.Printf("Channel utilization: %+v", workerPool.chUtilization)
}
