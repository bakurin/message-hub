package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"encoding/json"
	"io/ioutil"
	"time"
)

type Node struct {
	Data     json.RawMessage `json:"data"`
	DateTime string          `json:"date_time"`
}

type Queue struct {
	nodes []*Node
	size  int
	head  int
	tail  int
	count int
}

func (q *Queue) Push(n *Node) {
	if q.head == q.tail && q.count > 0 {
		nodes := make([]*Node, len(q.nodes)+q.size)
		copy(nodes, q.nodes[q.head:])
		copy(nodes[len(q.nodes)-q.head:], q.nodes[:q.head])
		q.head = 0
		q.tail = len(q.nodes)
		q.nodes = nodes
	}
	q.nodes[q.tail] = n
	q.tail = (q.tail + 1) % len(q.nodes)
	q.count++
}

func (q *Queue) Pop() *Node {
	if q.count == 0 {
		return nil
	}
	node := q.nodes[q.head]
	q.head = (q.head + 1) % len(q.nodes)
	q.count--
	return node
}

func NewQueue(size int) *Queue {
	return &Queue{
		nodes: make([]*Node, size),
		size:  size,
	}
}

var (
	host         string
	port         int
	key          string
	messageQueue *Queue
)

func main() {
	messageQueue = NewQueue(100)
	authMiddleware := createAuthMiddleware(key)

	http.Handle("/push", authMiddleware(pushHandler(messageQueue)))
	http.Handle("/pop", authMiddleware(popHandler(messageQueue)))

	err := http.ListenAndServe(fmt.Sprintf("%s:%d", host, port), nil)
	if err != nil {
		log.Fatal("listen and serve: ", err)
	}
}

func pushHandler(queue *Queue) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.Write([]byte("error"))
			return
		}

		data := json.RawMessage{}
		err = json.Unmarshal(body, &data)
		if err != nil {
			w.Write([]byte("error"))
			return
		}

		queue.Push(&Node{Data:data, DateTime: time.Now().Format(time.UnixDate)})
		w.Write([]byte("ok"))
	})
}

func popHandler(queue *Queue) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := json.Marshal(queue.Pop())
		if err != nil {
			w.Write([]byte("error"))
			return
		}

		w.Write(data)
	})
}

func createAuthMiddleware(key string) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("key") != key {
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func init() {
	flag.StringVar(&key, "host", "", "API key")
	flag.IntVar(&port, "port", 3030, "HTTP server port")
	flag.StringVar(&key, "key", "", "API key")

	flag.Parse()

	if key == "" {
		panic("api key must be set")
	}
}
