package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		// len(p)를 data 파라미터로 넣는 이유는 보통 데이터의 크기를 기록하기 위해서입니다.
		// 이를 통해 후속 처리나 데이터 구조를 읽는 사람이 데이터의 정확한 크기를 알 수 있게 됩니다.

		return 0, 0, err
	}
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	w += lenWidth

	s.size += uint64(w)
	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {

		// Flush()는 보통 버퍼링된 데이터를 실제 출력 스트림으로 강제로 전송하는 메서드입니다. 예를 들어, buf가 *bufio.Writer 타입이라면,
		//  이 Flush 메서드는 내부 버퍼에 저장된 데이터를 디스크나 네트워크 같은 실제 대상에 출력합니다.

		return nil, err
	}

	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

// func (s *store) Read(pos uint64) ([]byte, error)
// 해당 위치의 저장된 레코드를 리턴한다. 읽으려는 레코드가 아직 버퍼에 있을 때를 대비해서 우선은 버퍼의
// 내용을 플러시(flush)해서 디스크에 쓴다. 다음으로 읽을 레코드의 바이트 크기를 알아내고 그 만큼의 바이트를
// 읽어 리턴한다. 함수 내에서 할당하는 메모리가 함수 바깥에서 쓰이지 않으면, 컴파일러는 그 메모리를 스택(stack)
// 에 할당한다. 반대로 함수가 종료해도 함수 외부에서 계속 쓰이는 값이면 힙(heap)에 할당한다.

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

// func (s *store) ReadAt(p []byte, off int64) (int,error)
// 스토어 파일에서 off 오프셋부터 len(p) 바이트만큼 p에 넣어준다. 이 메서드는
// io.ReaderAt 인터페이스를 store 자료형에 구현한 것이다.

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.File.Close()
}

// Close() 메서드는 파일을 닫기 전 버퍼의 데이터를 파일에 쓴다.
