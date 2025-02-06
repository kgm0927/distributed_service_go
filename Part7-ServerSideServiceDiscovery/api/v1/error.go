package api_v1

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ErrOffsetOutOfRange struct {
	Offset uint64
}

func (e ErrOffsetOutOfRange) GRPStatus() *status.Status {
	st := status.New(
		codes.Unknown, // 이상하게도 여기서 404나 codes.NotFound를 사용하면 문제가 생긴다. 그러므로 일단 Unknown으로 수정해 놓겠다.
		fmt.Sprintf("offset out of range: %d", e.Offset),
	)

	msg := fmt.Sprintf(
		"The requested offset is outside the log's range: %d",
		e.Offset,
	)

	d := &errdetails.LocalizedMessage{
		Locale:  "en-US",
		Message: msg,
	}

	std, err := st.WithDetails(d)
	if err != nil {
		return st
	}

	return std

}

func (e ErrOffsetOutOfRange) Error() string {
	return e.GRPStatus().Err().Error()
}
