package main

import (
	"encoding/json"
	"sync"
)

type node struct {
	Data     json.RawMessage `json:"data"`
	DateTime string          `json:"date_time"`
}

type getData struct {
	Event  string `json:"event"`
	Status string `json:"status"`
	App    string `json:"app"`
}

type queue struct {
	nodes []*node
	mutex *sync.Mutex
}

func (queue *queue) Push(node *node) {
	queue.mutex.Lock()
	defer queue.mutex.Unlock()
	queue.nodes = append(queue.nodes, node)
}

func (queue *queue) Pop() *node {
	if len(queue.nodes) == 0 {
		return nil
	}

	queue.mutex.Lock()
	defer queue.mutex.Unlock()

	node := queue.nodes[0]
	queue.nodes = (queue.nodes)[1:]

	return node
}

func (queue *queue) Length() int {
	return len(queue.nodes)
}

func newQueue(size int) *queue {
	return &queue{
		nodes: make([]*node, 0, size),
		mutex: &sync.Mutex{},
	}
}
