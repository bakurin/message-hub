package main

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type requestUser string

type user struct {
	Name     string `json:"name"`
	PassHash string `json:"pass_hash"`
	Limit    int    `json:"limit"`
}

type requestLogEntry struct {
	UserName  string `json:"user_name"`
	Timestamp string `json:"timestamp"`
}

type requestLog []*requestLogEntry

func (log *requestLog) Add(entry *requestLogEntry) {
	if cap(*log) == len(*log) {
		*log = (*log)[1:len(*log)]
	}

	*log = append(*log, entry)
}

const requestUserContextKey requestUser = "user"

func createAuthMiddleware(users []*user) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			var user *user

			userName, pass, err := getAuthCedentials(r)
			if err != nil {
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}

			passHash := fmt.Sprintf("%x", md5.Sum([]byte(pass)))

			for _, u := range users {
				if u.Name == userName {
					user = u
					break
				}
			}

			if user == nil || user.PassHash != passHash {
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}

			ctx := context.WithValue(r.Context(), requestUserContextKey, user)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func getAuthCedentials(r *http.Request) (user string, pass string, err error) {
	auth := strings.SplitN(r.Header.Get("Authorization"), " ", 2)
	if len(auth) != 2 || auth[0] != "Basic" {
		err = errors.New("authorization failed")
		return
	}

	payload, _ := base64.StdEncoding.DecodeString(auth[1])
	pair := strings.SplitN(string(payload), ":", 2)

	if len(pair) != 2 {
		err = errors.New("authorization failed")
		return
	}

	user, pass = pair[0], pair[1]
	return
}

func createLogMiddleware(log *requestLog) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			user := r.Context().Value(requestUserContextKey).(*user)

			log.Add(&requestLogEntry{
				UserName:  user.Name,
				Timestamp: time.Now().Format(time.UnixDate),
			})

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

func httpHeaderMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		next.ServeHTTP(w, r)
	})
}
