package GeeRPC_client

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/lwyOAO/GeeRPC"
	"github.com/lwyOAO/GeeRPC/codec"
	"io"
	"log"
	"net"
	"sync"
)

// Call 承载一次RPC调用所需信息
type Call struct {
	Seq           uint64
	ServiceMethod string // format 'service.method'
	Args          any    // params
	Reply         any    // 服务端返回信息
	Error         error
	Done          chan *Call // inform when call is complete
}

func (call *Call) done() {
	call.Done <- call
}

type Client struct {
	cc       codec.Codec
	opt      *GeeRPC.Option
	sending  sync.Mutex
	header   codec.Header
	mu       sync.Mutex
	seq      uint64           // request id
	pending  map[uint64]*Call // store requests that have not been handled
	closing  bool             // user has call Close
	shutdown bool             // server has told us to stop
}

var _ io.Closer = (*Client)(nil)

var ErrShutdown = errors.New("connection is shut down")

// Close connection
func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing {
		return ErrShutdown
	}
	client.closing = true
	return client.cc.Close()
}

// IsAvailable return true if client still work
func (client *Client) IsAvailable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.shutdown && !client.closing
}

// return call id when successful
func (client *Client) registerCall(call *Call) (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing || client.shutdown {
		return 0, ErrShutdown
	}

	call.Seq = client.seq
	client.pending[call.Seq] = call
	client.seq++
	return call.Seq, nil
}

func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

// terminate all pending calls when client or server error
func (client *Client) terminateCalls(err error) {
	client.sending.Lock()
	defer client.sending.Unlock()
	client.mu.Lock()
	defer client.mu.Unlock()
	client.shutdown = true
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}
}

// 接收相应，有三种情况
// 1. call不存在，可能是请求没有发送完整，或者因为其他原因被取消，但服务端依旧处理了
// 2. call存在，但服务端处理出错，即H.Error不为空
// 3. call存在，服务端处理正常，那么需要从body中读取Reply的值
func (client *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		if err = client.cc.ReadHeader(&h); err != nil {
			break
		}
		call := client.removeCall(h.Seq)
		switch {
		case call == nil:
			// 通常是写部分失败了，call已经被移除
			err = client.cc.ReadBody(nil)
		case h.Error != "":
			call.Error = fmt.Errorf(h.Error)
			err = client.cc.ReadBody(nil)
			call.done()
		default:
			err = client.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body " + err.Error())
			}
			call.done()
		}
	}

	// error occurs, terminateCalls pending calls
	client.terminateCalls(err)
}

// NewClient
// 创建client实例时，先完成一开始的协议交换，即发送Option信息给服务端，
// 协商好信息的编码方式后，再创建子协程调用receive()接收响应
func NewClient(conn net.Conn, opt *GeeRPC.Option) (*Client, error) {
	// 准备编解码器
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client: codec error:", err)
		return nil, err
	}

	// 发送opt到服务端
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: options error: ", err)
		_ = conn.Close()
		return nil, err
	}
	// 创建客户端
	return newClientCodec(f(conn), opt), nil
}

func newClientCodec(cc codec.Codec, opt *GeeRPC.Option) *Client {
	client := &Client{
		seq:     1,
		cc:      cc,
		opt:     opt,
		pending: make(map[uint64]*Call),
	}
	// 启用协程接收响应
	go client.receive()
	return client
}

// opts实现为可选参数
func parseOption(opts ...*GeeRPC.Option) (*GeeRPC.Option, error) {
	// 如果opts为nil，或者传递nil作为参数
	if len(opts) == 0 || opts[0] == nil {
		return GeeRPC.DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}
	opt := opts[0]
	opt.MagicNumber = GeeRPC.DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = GeeRPC.DefaultOption.CodecType
	}
	return opt, nil
}

// Dial 连接一个特定IP的RPC服务器
func Dial(network, address string, opts ...*GeeRPC.Option) (client *Client, err error) {
	opt, err := parseOption(opts...)
	if err != nil {
		return nil, err
	}
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}

	// close the connection if client is nil
	defer func() {
		if client == nil {
			_ = conn.Close()
		}
	}()
	return NewClient(conn, opt)
}

// 发送请求
func (client *Client) send(call *Call) {
	// 确保客户端发送完整的请求
	client.sending.Lock()
	defer client.sending.Unlock()
	// 注册call
	seq, err := client.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}
	// 准备请求头
	client.header.ServiceMethod = call.ServiceMethod
	client.header.Seq = seq
	client.header.Error = ""
	// 编码，发送请求
	if err := client.cc.Write(&client.header, call.Args); err != nil {
		call := client.removeCall(seq)
		// call可能为nil，通常代表Write失败
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

// Go 和 Call 是客户端暴露给用户的两个RPC服务调用接口
// Go 异步调用函数
func (client *Client) Go(serviceMethod string, args, reply any, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel is unbuffered")
	}
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	client.send(call)
	return call
}

// Call 是对Go的封装，阻塞call.Done，等待响应返回，是一个同步接口
func (client *Client) Call(serviceMethod string, args, reply any) error {
	call := <-client.Go(serviceMethod, args, reply, make(chan *Call, 1)).Done
	return call.Error
}
