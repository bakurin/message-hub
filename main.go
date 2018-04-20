package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"
)

const lockDefaultDuration = 60

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
	MessageCount int         `json:"message_count"`
	LockedUntil  string      `json:"queue_locked_until,omitempty"`
	RequestLog   *requestLog `json:"request_log"`
}

var (
	host         string
	port         int
	messageQueue *queue
	users        []*user
	usersList    string
)

func main() {
	messageQueue = newQueue(1)
	lock := newLock()
	requestLog := make(requestLog, 0, 10)
	authMiddleware := createAuthMiddleware(users)
	lockMiddleware := createLockMiddleware(lock)
	logMiddleware := createLogMiddleware(&requestLog)

	http.Handle("/push", httpHeaderMiddleware(authMiddleware(logMiddleware(lockMiddleware(pushHandler(messageQueue))))))
	http.Handle("/pop", httpHeaderMiddleware(authMiddleware(popHandler(messageQueue))))
	http.Handle("/stat", httpHeaderMiddleware(authMiddleware(statHandler(messageQueue, lock, &requestLog))))
	http.Handle("/lock", httpHeaderMiddleware(authMiddleware(lockHandler(lock))))

	err := http.ListenAndServe(fmt.Sprintf("%s:%d", host, port), nil)
	if err != nil {
		log.Fatal("listen and serve: ", err)
	}
}

func init() {
	flag.StringVar(&host, "host", "127.0.0.1", "HTTP server host")
	flag.IntVar(&port, "port", 7001, "HTTP server port")
	flag.StringVar(&usersList, "users", "./users.json", "File path of users list in json format")
	flag.Parse()

	file, err := ioutil.ReadFile(usersList)
	if err != nil {
		panic(err)
	}

	json.Unmarshal(file, &users)
	if len(users) == 0 {
		panic(errors.New("user list is empty"))
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

func statHandler(queue *queue, lock *lock, log *requestLog) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lockedUntil := ""
		if lock.IsLocked() {
			lockedUntil = lock.lockedUntil.Format(time.UnixDate)
		}

		newResponse(http.StatusOK, statPayload{
			MessageCount: queue.Length(),
			LockedUntil:  lockedUntil,
			RequestLog:   log,
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
