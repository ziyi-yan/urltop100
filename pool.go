package main

import "sync"

var mapPool = sync.Pool{
	New: poolNew,
}

func poolNew() interface{} {
	return make(map[string]int)
}

func poolGet() map[string]int {
	return mapPool.Get().(map[string]int)
}

func poolPut(m map[string]int) {
	for k := range m {
		m[k] = 0
	}
	mapPool.Put(m)
}
