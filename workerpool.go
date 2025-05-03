package main

import (
	"fmt"
	"sync"
)

type Info struct {
	//Name        string
	//PhNumber    []int
	//Email       []string
	PrimeNumber int
}

type WorkerPool struct {
	// Worker threads
	noOfWorkers int
	inCh        chan Info
	//outChWorkers  []chan Info			// This list can be used if you want to manually load balance the Info to separate workers
	commonOutChForWorker chan Info // This common channel is useful if you don't want to manually load balance and let all the workers listen to the same channel for Info
	curWorker            int
	chUtilization        map[int]int

	// Progress Bar
	curProgressInterval int
	progressBarPercent  int
	maxPrimes           int

	// Locks for synchronization
	mapRWMutex sync.RWMutex
	logMutex   sync.Mutex
}

func NewWorkerPool(noOfWorkers, maxPrimes int) *WorkerPool {
	w := &WorkerPool{
		noOfWorkers: noOfWorkers,
		inCh:        make(chan Info),
		//outChWorkers:         []chan Info{},
		commonOutChForWorker: make(chan Info),
		chUtilization:        make(map[int]int),
		curProgressInterval:  0,
		progressBarPercent:   10,
		maxPrimes:            maxPrimes,
	}
	go w.startWorkerQueue()
	return w
}

func (w *WorkerPool) Init() {
	for i := 0; i < w.noOfWorkers; i++ {
		w.chUtilization[i] = 0
		// Uncomment these lines if you want separate channels for each worker
		//outChForWorker := make(chan Info)
		//w.outChWorkers = append(w.outChWorkers, outChForWorker)
		//go w.startWorker(outChForWorker, i)
		go w.startWorker(w.commonOutChForWorker, i)
	}
}

func (w *WorkerPool) startWorkerQueue() {
	for info := range w.inCh {
		//fmt.Printf("Got info in the worker queue. Need to redirect it to %d worker\n", w.curWorker)
		// Uncomment these if you want to manually load balance the Info to all the workers
		//w.outChWorkers[w.curWorker] <- info
		//w.curWorker = (w.curWorker + 1) % w.noOfWorkers
		w.commonOutChForWorker <- info
	}
}

func (w *WorkerPool) startWorker(inChWorker chan Info, chNo int) {
	for info := range inChWorker {
		findPrime(info.PrimeNumber)
		go w.logProgressBar(info.PrimeNumber)
		w.updateChannelUtilization(chNo)
	}
}

func (w *WorkerPool) updateChannelUtilization(chNo int) {
	w.mapRWMutex.Lock()
	defer w.mapRWMutex.Unlock()
	w.chUtilization[chNo] += 1
}

// Brute force way to find the ith prime number. Since this method will consume more resources to find big primes,
// it will be helpful to check how workers are working in real-time
func findPrime(i int) int {
	st := 2
	for {
		factors := findFactors(st)
		if factors == 0 {
			i -= 1
			if i == 0 {
				return st
			}
		}
		st += 1
	}
}

func findFactors(num int) int {
	factors := 0
	for i := 2; i < num; i++ {
		if num%i == 0 {
			factors += 1
		}
	}
	return factors
}

func (w *WorkerPool) logProgressBar(curPrime int) {
	progressBarInterval := w.maxPrimes / w.progressBarPercent
	primeLiesInInterval := (curPrime + progressBarInterval - 1) / progressBarInterval
	w.logMutex.Lock()
	if primeLiesInInterval == w.curProgressInterval+2 {
		printProgressDone, printProgressRem := "", ""
		for i := 1; i <= 100/w.progressBarPercent; i++ {
			if progressBarInterval*i <= curPrime {
				printProgressDone += "===="
			} else {
				printProgressRem += "...."
			}
		}
		fmt.Printf("%s>%s\n", printProgressDone, printProgressRem)
		w.curProgressInterval += 1
	}
	w.logMutex.Unlock()
}
