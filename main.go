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

type GetData struct {
	Event  string `json:"event"`
	Status string `json:"status"`
	App    string `json:"app"`
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

func (q *Queue) Count() int {
	return q.count
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

	http.Handle("/push", authMiddleware(responseHeaderMiddleware(pushHandler(messageQueue))))
	http.Handle("/pop", authMiddleware(responseHeaderMiddleware(popHandler(messageQueue))))
	http.Handle("/stat", authMiddleware(responseHeaderMiddleware(statHandler(messageQueue))))

	err := http.ListenAndServe(fmt.Sprintf("%s:%d", host, port), nil)
	if err != nil {
		log.Fatal("listen and serve: ", err)
	}
}

func init() {
	flag.StringVar(&host, "host", "127.0.0.1", "API key")
	flag.IntVar(&port, "port", 7001, "HTTP server port")
	flag.StringVar(&key, "key", "", "API key")

	flag.Parse()

	if key == "" {
		panic("api key must be set")
	}
}

func pushHandler(queue *Queue) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var (
			err  error
			data json.RawMessage
		)

		switch r.Method {
		case http.MethodPost:
			data, err = pushFromPost(r)
			break
		case http.MethodGet:
			data, err = pushFromGet(r)
			break
		default:
			response := newErrorResponse(http.StatusMethodNotAllowed, "not allowed")
			response.Write(w)
			return
		}

		if err != nil {
			response := newErrorResponse(http.StatusInternalServerError, err.Error())
			response.Write(w)
			return
		}

		queue.Push(&Node{Data:data, DateTime: time.Now().Format(time.RFC3339)})
		response := newResponse(http.StatusCreated, Payload{Ok: true})
		response.Write(w)
	})
}

func pushFromPost(r *http.Request) (json.RawMessage, error) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	data := json.RawMessage{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func pushFromGet(r *http.Request) (json.RawMessage, error) {
	query := r.URL.Query()
	data := GetData{
		Event:  query.Get("event"),
		Status: query.Get("status"),
		App:    query.Get("app"),
	}

	content, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	return content, nil
}

func popHandler(queue *Queue) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		node := queue.Pop()
		if node == nil {
			response := newErrorResponse(http.StatusNoContent, "queue is empty")
			response.Write(w)
			return
		}

		response := newResponse(http.StatusOK, node)
		response.Write(w)
	})
}

func statHandler(queue *Queue) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := newResponse(http.StatusOK, queue.Count())
		response.Write(w)
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

func responseHeaderMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		next.ServeHTTP(w, r)
	})
}

type Payload struct {
	Ok bool `json:"ok"`
}

type ErrorPayload struct {
	Ok      bool   `json:"ok"`
	Message string `json:"message"`
}

type Response struct {
	StatusCode int
	Payload    interface{}
}

func newResponse(status int, payload interface{}) *Response {
	return &Response{
		StatusCode: status,
		Payload:    payload,
	}
}

func newErrorResponse(status int, message string) *Response {
	return &Response{
		StatusCode: status,
		Payload: ErrorPayload{
			Ok:      false,
			Message: message,
		},
	}
}

func (response Response) Write(w http.ResponseWriter) {
	data, err := json.Marshal(response.Payload)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("{\"ok\": false}"))
		return
	}

	w.WriteHeader(response.StatusCode)
	w.Write(data)
}
