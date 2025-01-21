package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

func filler(b []byte, ifzero, ifnot byte) {
	for i := range b {
		if rand.Intn(2) == 0 {
			b[i] = ifzero
		} else {
			b[i] = ifnot
		}
	}
}

func main() {
	const size = 100
	b := make([]byte, size)
	var muLeft, muRight sync.RWMutex

	go func() {
		for {
			muLeft.Lock()
			filler(b[:size/2], '0', '1')
			muLeft.Unlock()
			time.Sleep(time.Second)
		}
	}()
	go func() {
		for {
			muRight.Lock()
			filler(b[size/2:], 'X', 'Y')
			muRight.Unlock()
			time.Sleep(time.Second)
		}
	}()
	go func() {
		for {
			muLeft.RLock()
			muRight.RLock()
			fmt.Println(string(b))
			muLeft.RUnlock()
			muRight.RUnlock()
			time.Sleep(time.Second)
		}
	}()
	for {
	}
}
