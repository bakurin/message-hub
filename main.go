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

type statPayload struct {
	MessageCount int    `json:"message_count"`
	LockedUntil  string `json:"queue_locked_until,omitempty"`
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
	http.Handle("/stat", authMiddleware(responseHeaderMiddleware(statHandler(messageQueue, lock))))
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
			newErrorResponse(http.StatusMethodNotAllowed, "only GET and POST are allowed").Write(w)
			return
		}

		if err != nil {
			newErrorResponse(http.StatusInternalServerError, err.Error()).Write(w)
			return
		}

		queue.Push(&node{Data: data, DateTime: time.Now().Format(time.RFC3339)})
		newResponse(http.StatusCreated, payload{Ok: true}).Write(w)
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
			newErrorResponse(http.StatusNoContent, "queue is empty").Write(w)
			return
		}

		newResponse(http.StatusOK, node).Write(w)
	})
}

func statHandler(queue *queue, lock *lock) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lockedUntil := ""
		if lock.IsLocked() {
			lockedUntil = lock.lockedUntil.Format(time.UnixDate)
		}

		newResponse(http.StatusOK, statPayload{
			MessageCount: queue.Length(),
			LockedUntil:  lockedUntil,
		}).Write(w)
	})
}

func lockHandler(lock *lock) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			newErrorResponse(http.StatusMethodNotAllowed, "only POST is allowed").Write(w)
			return
		}

		data := &lockPayload{Seconds: lockDefaultDuration}
		if r.ContentLength > 0 {
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				newErrorResponse(http.StatusBadRequest, "invald request").Write(w)
				return
			}

			err = json.Unmarshal(body, &data)
			if err != nil {
				newErrorResponse(http.StatusNoContent, err.Error()).Write(w)
				return
			}
		}

		if data.Seconds <= 0 {
			newErrorResponse(http.StatusNoContent, "locking time must be positive number").Write(w)
			return
		}

		lock.Lock(time.Duration(data.Seconds) * time.Second)
		newResponse(
			http.StatusCreated,
			payload{
				Ok:      true,
				Message: fmt.Sprintf("locked till %s", lock.lockedUntil.Format(time.UnixDate)),
			}).Write(w)
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
				message := fmt.Sprintf("queue is locked till %s", lock.lockedUntil.Format(time.UnixDate))
				newErrorResponse(http.StatusNotAcceptable, message).Write(w)
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
	Ok      bool   `json:"ok"`
	Message string `json:"message,omitempty"`
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
		Payload: payload{
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
