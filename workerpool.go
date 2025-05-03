package main

import (
	"fmt"
	"sync"
)

type Info struct {
	Name        string
	PhNumber    []int
	Email       []string
	PrimeNumber int
}

type WorkerPool struct {
	noOfWorkers         int
	inCh                chan Info
	outChWorkers        []chan Info
	curCh               int
	chUtilization       map[int]int
	curProgressInterval int
	progressBarPercent  int
	maxPrimes           int
	mapRWMutex          sync.RWMutex
	logMutex            sync.Mutex
}

func NewWorkerPool(noOfWorkers, maxPrimes int) *WorkerPool {
	chMap := make(map[int]int)
	for i := 0; i < noOfWorkers; i++ {
		chMap[i] = 0
	}
	w := &WorkerPool{
		noOfWorkers:         noOfWorkers,
		inCh:                make(chan Info),
		outChWorkers:        []chan Info{},
		chUtilization:       chMap,
		curProgressInterval: 0,
		progressBarPercent:  10,
		maxPrimes:           maxPrimes,
	}
	go w.startWorkerQueue()
	return w
}

func (w *WorkerPool) Init() {
	//commonOutChForWorker := make(chan Info)
	for i := 0; i < w.noOfWorkers; i++ {
		outChForWorker := make(chan Info)
		w.outChWorkers = append(w.outChWorkers, outChForWorker)
		go w.startWorker(outChForWorker, i)
	}
}

func (w *WorkerPool) startWorkerQueue() {
	for info := range w.inCh {
		//fmt.Printf("Got info in the worker queue. Need to redirect it to %d worker\n", w.curCh)
		w.outChWorkers[w.curCh] <- info
		w.curCh = (w.curCh + 1) % w.noOfWorkers
	}
}

func (w *WorkerPool) startWorker(inChWorker chan Info, chNo int) {
	for info := range inChWorker {
		findPrime(info.PrimeNumber)
		go w.logProgressBar(info.PrimeNumber)
		//if info.PrimeNumber == 1 {
		//	fmt.Printf("Got info from channel number %d. %dst prime number is %d\n", chNo, info.PrimeNumber, prime)
		//} else if info.PrimeNumber == 2 {
		//	fmt.Printf("Got info from channel number %d. %dnd prime number is %d\n", chNo, info.PrimeNumber, prime)
		//} else {
		//	fmt.Printf("Got info from channel number %d. %dth prime number is %d\n", chNo, info.PrimeNumber, prime)
		//}
		w.updateChannelUtilization(chNo)
	}
}

func (w *WorkerPool) updateChannelUtilization(chNo int) {
	w.mapRWMutex.Lock()
	defer w.mapRWMutex.Unlock()
	w.chUtilization[chNo] += 1
}

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
	if primeLiesInInterval == w.curProgressInterval+2 {
		w.logMutex.Lock()
		printProgressDone, printProgressRem := "", ""
		for i := 1; i <= 100/w.progressBarPercent; i++ {
			if progressBarInterval*i <= curPrime {
				printProgressDone += "===="
			} else {
				printProgressRem += "...."
			}
		}
		// Additional check in cases where two separate goroutines might have got the lock at same time.
		if primeLiesInInterval == w.curProgressInterval+2 {
			fmt.Printf("%s>%s\n", printProgressDone, printProgressRem)
		}
		w.curProgressInterval += 1
		w.logMutex.Unlock()
	}
}
