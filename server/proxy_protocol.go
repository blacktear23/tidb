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
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/juju/errors"
)

// Ref: https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt .
const (
	proxyProtocolV1MaxHeaderLen = 108
	unknownProtocol             = 0
	proxyProtocolV1             = 1
	proxyProtocolV2             = 2
	v2CmdPos                    = 12
	v2FamlyPos                  = 13
	v2LenPos                    = 14
	v2AddrsPos                  = 16
)

var (
	errProxyProtocolV1HeaderInvalid = errors.New("PROXY Protocol v1 header is invalid")
	errProxyProtocolV2HeaderInvalid = errors.New("PROXY Protocol v2 header is invalid")
	errProxyAddressNotAllowed       = errors.New("Proxy address is not allowed")
	proxyProtocolV2Sig              = []byte{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A}

	_ net.Conn = &proxyProtocolConn{}
)

type proxyProtocolConnBuilder struct {
	allowAll          bool
	allowedNets       []*net.IPNet
	headerReadTimeout int // Unit is second
}

func newProxyProtocolConnBuilder(allowedIPs string, headerReadTimeout int) (*proxyProtocolConnBuilder, error) {
	allowAll := false
	allowedNets := []*net.IPNet{}
	if allowedIPs == "*" {
		allowAll = true
	} else {
		for _, aip := range strings.Split(allowedIPs, ",") {
			saip := strings.TrimSpace(aip)
			_, ipnet, err := net.ParseCIDR(saip)
			if err == nil {
				allowedNets = append(allowedNets, ipnet)
				continue
			}
			psaip := fmt.Sprintf("%s/32", saip)
			_, ipnet, err = net.ParseCIDR(psaip)
			if err != nil {
				return nil, errors.Trace(err)
			}
			allowedNets = append(allowedNets, ipnet)
		}
	}
	return &proxyProtocolConnBuilder{
		allowAll:          allowAll,
		allowedNets:       allowedNets,
		headerReadTimeout: headerReadTimeout,
	}, nil
}

func (b *proxyProtocolConnBuilder) wrapConn(conn bufferedReadConn) (*proxyProtocolConn, error) {
	ppconn := &proxyProtocolConn{
		bufferedReadConn: conn,
		builder:          b,
	}
	err := ppconn.readClientAddrBehindProxy()
	return ppconn, errors.Trace(err)
}

func (b *proxyProtocolConnBuilder) checkAllowed(raddr net.Addr) bool {
	if b.allowAll {
		return true
	}
	taddr, ok := raddr.(*net.TCPAddr)
	if !ok {
		return false
	}
	cip := taddr.IP
	for _, ipnet := range b.allowedNets {
		if ipnet.Contains(cip) {
			return true
		}
	}
	return false
}

type proxyProtocolConn struct {
	bufferedReadConn
	builder  *proxyProtocolConnBuilder
	clientIP net.Addr
}

func (c *proxyProtocolConn) readClientAddrBehindProxy() error {
	connRemoteAddr := c.bufferedReadConn.RemoteAddr()
	allowed := c.builder.checkAllowed(connRemoteAddr)
	if !allowed {
		return errProxyAddressNotAllowed
	}
	return c.parseHeader(connRemoteAddr)
}

func (c *proxyProtocolConn) parseHeader(connRemoteAddr net.Addr) error {
	ver, buffer, err := c.readHeader()
	if err != nil {
		return errors.Trace(err)
	}
	switch ver {
	case proxyProtocolV1:
		raddr, v1err := c.extractClientIPV1(buffer, connRemoteAddr)
		if v1err != nil {
			return errors.Trace(v1err)
		}
		c.clientIP = raddr
		return nil
	case proxyProtocolV2:
		raddr, v2err := c.extraceClientIPV2(buffer, connRemoteAddr)
		if v2err != nil {
			return errors.Trace(v2err)
		}
		c.clientIP = raddr
		return nil
	default:
		panic("Should not come here")
	}
}

func (c *proxyProtocolConn) extractClientIPV1(buffer []byte, connRemoteAddr net.Addr) (net.Addr, error) {
	header := string(buffer)
	parts := strings.Split(header, " ")
	if len(parts) != 6 {
		if len(parts) > 1 && parts[1] == "UNKNOWN\r\n" {
			return connRemoteAddr, nil
		}
		return nil, errProxyProtocolV1HeaderInvalid
	}
	clientIPStr := parts[2]
	clientPortStr := parts[4]
	iptype := parts[1]
	switch iptype {
	case "TCP4":
		addrStr := fmt.Sprintf("%s:%s", clientIPStr, clientPortStr)
		return net.ResolveTCPAddr("tcp4", addrStr)
	case "TCP6":
		addrStr := fmt.Sprintf("[%s]:%s", clientIPStr, clientPortStr)
		return net.ResolveTCPAddr("tcp6", addrStr)
	case "UNKNOWN":
		return connRemoteAddr, nil
	default:
		return nil, errProxyProtocolV1HeaderInvalid
	}
}

func (c *proxyProtocolConn) extraceClientIPV2(buffer []byte, connRemoteAddr net.Addr) (net.Addr, error) {
	verCmd := buffer[v2CmdPos]
	famly := buffer[v2FamlyPos]
	switch verCmd & 0x0F {
	case 0x01: /* PROXY command */
		switch famly {
		case 0x11: /* TCPv4 */
			srcAddrV4 := net.IP(buffer[v2AddrsPos : v2AddrsPos+4])
			srcPortV4 := binary.BigEndian.Uint16(buffer[v2AddrsPos+8 : v2AddrsPos+10])
			return &net.TCPAddr{
				IP:   srcAddrV4,
				Port: int(srcPortV4),
			}, nil
		case 0x21: /* TCPv6 */
			srcAddrV6 := net.IP(buffer[v2AddrsPos : v2AddrsPos+16])
			srcPortV6 := binary.BigEndian.Uint16(buffer[v2AddrsPos+32 : v2AddrsPos+34])
			return &net.TCPAddr{
				IP:   srcAddrV6,
				Port: int(srcPortV6),
			}, nil
		default:
			// unsupported protocol, keep local connection address
			return connRemoteAddr, nil
		}
	case 0x00: /* LOCAL command */
		// keep local connection address for LOCAL
		return connRemoteAddr, nil
	default:
		// not a supported command
		return nil, errProxyProtocolV2HeaderInvalid
	}
}

func (c *proxyProtocolConn) RemoteAddr() net.Addr {
	return c.clientIP
}

func (c *proxyProtocolConn) readHeader() (int, []byte, error) {
	// This mean all header data should be read in headerReadTimeout seconds.
	c.bufferedReadConn.SetReadDeadline(time.Now().Add(time.Duration(c.builder.headerReadTimeout) * time.Second))
	// When function return clean read deadline.
	defer c.bufferedReadConn.SetReadDeadline(time.Time{})

	// Below is a workaround for bufio.Reader
	// need to peek 1 byte to let bufio.Reader fill buffer
	c.bufferedReadConn.Peek(1)
	// Calculate min size to peek, peekSize should not greater than bufio.Reader buffered size
	peekSize := c.bufferedReadConn.Buffered()
	if peekSize > proxyProtocolV1MaxHeaderLen {
		peekSize = proxyProtocolV1MaxHeaderLen
	}

	buf, err := c.bufferedReadConn.Peek(peekSize)
	if err != nil && err != bufio.ErrBufferFull {
		return unknownProtocol, nil, errors.Trace(err)
	}
	n := len(buf)
	if n >= 16 {
		if bytes.Equal(buf[0:12], proxyProtocolV2Sig) && (buf[v2CmdPos]&0xF0) == 0x20 {
			endPos := 16 + int(binary.BigEndian.Uint16(buf[v2LenPos:v2LenPos+2]))
			if n < endPos {
				return unknownProtocol, nil, errProxyProtocolV2HeaderInvalid
			}
			_, err := c.bufferedReadConn.Discard(endPos)
			if err != nil {
				return unknownProtocol, nil, errProxyProtocolV2HeaderInvalid
			}
			return proxyProtocolV2, buf[0:endPos], nil
		}
	}
	if n >= 5 {
		if string(buf[0:5]) != "PROXY" {
			return unknownProtocol, nil, errProxyProtocolV1HeaderInvalid
		}
		pos := bytes.IndexByte(buf, byte(10))
		if pos == -1 || pos == 0 {
			return unknownProtocol, nil, errProxyProtocolV1HeaderInvalid
		}
		if buf[pos-1] != byte(13) {
			return unknownProtocol, nil, errProxyProtocolV1HeaderInvalid
		}
		endPos := pos + 1
		_, err := c.bufferedReadConn.Discard(endPos)
		if err != nil {
			return unknownProtocol, nil, errProxyProtocolV1HeaderInvalid
		}
		return proxyProtocolV1, buf[0:endPos], nil
	}
	return unknownProtocol, nil, errProxyProtocolV1HeaderInvalid
}
