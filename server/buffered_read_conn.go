// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bufio"
	"net"
)

const defaultReaderSize = 16 * 1024

// bufferedReadConn is a net.Conn compatible interface that support peek and discard operation.
type bufferedReadConn interface {
	net.Conn
	Peek(n int) ([]byte, error)
	Discard(n int) (int, error)
	Buffered() int
}

// bufReadConn is a implements for bufferedReadConn
type bufReadConn struct {
	net.Conn
	rb *bufio.Reader
}

func (conn bufReadConn) Read(b []byte) (n int, err error) {
	return conn.rb.Read(b)
}

func (conn bufReadConn) Peek(n int) ([]byte, error) {
	return conn.rb.Peek(n)
}

func (conn bufReadConn) Discard(n int) (int, error) {
	return conn.rb.Discard(n)
}

func (conn bufReadConn) Buffered() int {
	return conn.rb.Buffered()
}

func newBufferedReadConn(conn net.Conn) bufferedReadConn {
	return &bufReadConn{
		Conn: conn,
		rb:   bufio.NewReaderSize(conn, defaultReaderSize),
	}
}
