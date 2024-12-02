package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

func NewHTTPServer(addr string) *http.Server {
	httpsrv := newHTTPServer()
	r := mux.NewRouter()
	r.HandleFunc("/", httpsrv.handleProduce).Methods("POST")
	r.HandleFunc("/", httpsrv.handleConsume).Methods("GET")
	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

type httpServer struct {
	Log *Log
}

func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

// 호출자가 로그에 추가하길 원하는 레코드를 담는다.
type ProduceRequest struct {
	Record Record `json:"record"`
}

// 호출자에게 저장한 오프셋을 알려준다.
type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

// 원하는 레코드의 오프셋을 담는다.
type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

// 오프셋에 위치하는 레코드를 보내준다.
type ConsumeResponse struct {
	Record Record `json:"record"`
}

func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	off, err := s.Log.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := ProduceResponse{Offset: off}
	err = json.NewEncoder(w).Encode(res)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

}

func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req ConsumeRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	record, err := s.Log.Read(req.Offset)
	if err == ErrOffsetNotFound {
		http.Error(w, err.Error(), http.StatusNotFound)

		return
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := ConsumeResponse{Record: record}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

/*
	consume 핸들러는 produce 핸들러와 비슷한 구조이지만 Read(offset uint64)를 호출하여 로그에서 레코드를
	읽어낸다. 이 핸들러는 좀 더 많은 에러 체크를 하여 정확한 상태 코드(status code)를 클라이언트에 제공한다.
	서버가 요청을 핸들링할 수 없다는 에러도 있고, 클라이언트가 요청한 레코드가 존재하지 않는다는 에러도 있다.
*/
