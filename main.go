package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/logutils"
	"github.com/jessevdk/go-flags"
)

const lockDefaultDuration = 60

var revision = "dev"

type opts struct {
	Host  string `long:"host" env:"MESSAGE_HUB_HOST" description:"HTTP server host" default:"127.0.0.1"`
	Port  int    `long:"port" env:"MESSAGE_HUB_PORT" description:"HTTP server port" default:"7001"`
	Users string `long:"users" env:"MESSAGE_HUB_USERS" description:"file path of users list in json format" default:"./users.json"`
	Debug bool   `long:"debug" env:"DEBUG" description:"debug mode"`
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
	MessageCount int         `json:"message_count"`
	LockedUntil  string      `json:"queue_locked_until,omitempty"`
	RequestLog   *requestLog `json:"request_log"`
}

func main() {
	fmt.Printf("Message Hub version %s\n", revision)

	var opts opts
	p := flags.NewParser(&opts, flags.Default)
	if _, err := p.ParseArgs(os.Args[1:]); err != nil {
		os.Exit(1)
	}

	setupLog(opts.Debug)

	users := loadUsers(opts.Users)
	messageQueue := newQueue(50)
	lock := newLock()
	requestLog := newRequestLog(20)
	authMiddleware := createAuthMiddleware(users)
	lockMiddleware := createLockMiddleware(lock)
	logMiddleware := createLogMiddleware(requestLog)

	http.Handle("/push", httpHeaderMiddleware(authMiddleware(logMiddleware(lockMiddleware(pushHandler(messageQueue))))))
	http.Handle("/pop", httpHeaderMiddleware(authMiddleware(popHandler(messageQueue))))
	http.Handle("/stat", httpHeaderMiddleware(authMiddleware(statHandler(messageQueue, lock, requestLog))))
	http.Handle("/lock", httpHeaderMiddleware(authMiddleware(lockHandler(lock))))

	err := http.ListenAndServe(fmt.Sprintf("%s:%d", opts.Host, opts.Port), nil)
	if err != nil {
		log.Fatalf("[ERROR] listen and serve: %v", err)
	}
}

func loadUsers(path string) []*user {
	file, err := ioutil.ReadFile(path)
	if err != nil {
		log.Panicf("[ERROR] unable read file %s", path)
	}

	var users []*user
	err = json.Unmarshal(file, &users)
	if err != nil {
		log.Panicf("[ERROR] unable to load user list")
	}

	if len(users) == 0 {
		log.Panicf("[WARNING] user list is empty")
	}

	return users
}

func setupLog(debug bool) {
	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"DEBUG", "INFO", "WARN", "ERROR"},
		MinLevel: logutils.LogLevel("INFO"),
		Writer:   os.Stdout,
	}

	log.SetFlags(log.Ldate | log.Ltime)

	if debug {
		log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
		filter.MinLevel = logutils.LogLevel("DEBUG")
	}
	log.SetOutput(filter)
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
