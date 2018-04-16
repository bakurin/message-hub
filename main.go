package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"
)

const lockDefaultDuration = 60

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
	size  int
	head  int
	tail  int
	count int
	mutex *sync.Mutex
}

func (q *queue) Push(n *node) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.head == q.tail && q.count > 0 {
		nodes := make([]*node, len(q.nodes)+q.size)
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

func (q *queue) Pop() *node {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.count == 0 {
		return nil
	}
	node := q.nodes[q.head]
	q.head = (q.head + 1) % len(q.nodes)
	q.count--
	return node
}

func (q *queue) Length() int {
	return q.count
}

func newQueue(size int) *queue {
	return &queue{
		nodes: make([]*node, size),
		size:  size,
		mutex: &sync.Mutex{},
	}
}

type lock struct {
	lockedUntil time.Time
	mutex       *sync.Mutex
}

func (lock *lock) IsLocked() bool {
	if lock.lockedUntil.After(time.Now()) {
		return true
	}

	return false
}

func (lock *lock) Lock(lockFor time.Duration) {
	lock.mutex.Lock()
	defer lock.mutex.Unlock()

	if lock.IsLocked() {
		return
	}

	lock.lockedUntil = time.Now().Add(lockFor)
}

func newLock() *lock {
	return &lock{
		mutex: &sync.Mutex{},
	}
}

type lockPayload struct {
	Seconds int `json:"seconds,omitempty"`
}

var (
	host         string
	port         int
	key          string
	messageQueue *queue
)

func main() {
	messageQueue = newQueue(100)
	lock := newLock()
	authMiddleware := createAuthMiddleware(key)
	lockMiddleware := createLockMiddleware(lock)

	http.Handle("/push", authMiddleware(responseHeaderMiddleware(lockMiddleware(pushHandler(messageQueue)))))
	http.Handle("/pop", authMiddleware(responseHeaderMiddleware(popHandler(messageQueue))))
	http.Handle("/stat", authMiddleware(responseHeaderMiddleware(statHandler(messageQueue))))
	http.Handle("/lock", authMiddleware(responseHeaderMiddleware(lockHandler(lock))))

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

func pushHandler(queue *queue) http.Handler {
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

		queue.Push(&node{Data: data, DateTime: time.Now().Format(time.RFC3339)})
		response := newResponse(http.StatusCreated, payload{Ok: true})
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
	data := getData{
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

func popHandler(queue *queue) http.Handler {
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

func statHandler(queue *queue) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := newResponse(http.StatusOK, queue.Length())
		response.Write(w)
	})
}

func lockHandler(lock *lock) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			response := newErrorResponse(http.StatusNoContent, "invald request")
			response.Write(w)
			return
		}

		data := &lockPayload{}

		if len(body) > 0 {
			err = json.Unmarshal(body, &data)
			if err != nil {
				response := newErrorResponse(http.StatusNoContent, err.Error())
				response.Write(w)
				return
			}
		}

		duration := lockDefaultDuration
		if data.Seconds > 0 {
			duration = data.Seconds
		}

		lock.Lock(time.Duration(duration) * time.Second)
		message := fmt.Sprintf("locked till: %s", lock.lockedUntil.Format(time.UnixDate))
		response := newResponse(http.StatusCreated, message)
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

func createLockMiddleware(lock *lock) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if lock.IsLocked() {
				message := fmt.Sprintf("queue is locked till: %s", lock.lockedUntil.Format(time.UnixDate))
				response := newErrorResponse(http.StatusNotAcceptable, message)
				response.Write(w)
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

type payload struct {
	Ok bool `json:"ok"`
}

type errorPayload struct {
	Ok      bool   `json:"ok"`
	Message string `json:"message"`
}

type response struct {
	StatusCode int
	Payload    interface{}
}

func newResponse(status int, payload interface{}) *response {
	return &response{
		StatusCode: status,
		Payload:    payload,
	}
}

func newErrorResponse(status int, message string) *response {
	return &response{
		StatusCode: status,
		Payload: errorPayload{
			Ok:      false,
			Message: message,
		},
	}
}

func (response response) Write(w http.ResponseWriter) {
	data, err := json.Marshal(response.Payload)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("{\"ok\": false}"))
		return
	}

	w.WriteHeader(response.StatusCode)
	w.Write(data)
}
