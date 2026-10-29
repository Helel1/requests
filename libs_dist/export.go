package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	url2 "net/url"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/google/uuid"
	http "github.com/wangluozhe/chttp"
	"github.com/wangluozhe/chttp/cookiejar"
	"github.com/wangluozhe/requests"
	"github.com/wangluozhe/requests/libs"
	"github.com/wangluozhe/requests/models"
	ja3 "github.com/wangluozhe/requests/transport"
	"github.com/wangluozhe/requests/url"
	"github.com/wangluozhe/requests/utils"
)

var unsafePointers = make(map[string]*C.char)
var unsafePointersLock = sync.Mutex{}

var sessionsPool = sync.Map{}

// StreamEntry 保存一个活跃的流式响应连接
type StreamEntry struct {
	Response *models.Response
	Body     io.ReadCloser
	// owner 用于在流结束时给已关闭的会话做一次最终 idle 清理：Session.Close
	// 时本流还占着连接（chttp 不打断 in-use 连接），连接回到 idle 是流结束之后的事。
	owner *requests.Session
	mu     sync.Mutex
	closed bool
}

// finishLocked 关闭 body 并清理已关闭会话的空闲连接。调用方必须持有 e.mu；
// 重复调用是幂等的。
func (e *StreamEntry) finishLocked() {
	if e.closed {
		return
	}
	e.closed = true
	if e.Body != nil {
		e.Body.Close()
	}
	if e.owner != nil && e.owner.IsClosed() {
		e.owner.CloseIdleConnections()
	}
}

var streamPool = sync.Map{}

// GetSession 按 id 取会话，不存在则新建。cookie jar 由 NewSession 建立并在
// 会话内保留；这里不再每请求替换它，避免与并发请求读 jar 指针产生竞态。
func GetSession(id string) *requests.Session {
	if actual, ok := sessionsPool.Load(id); ok {
		return actual.(*requests.Session)
	}
	s := requests.NewSession()
	actual, _ := sessionsPool.LoadOrStore(id, s)
	return actual.(*requests.Session)
}

// registerCString 把已序列化的 JSON 拷成 C 字符串并按 key 登记，之后由
// freeMemory(key) 释放。
func registerCString(key string, encoded []byte) *C.char {
	ptr := C.CString(string(encoded))
	unsafePointersLock.Lock()
	unsafePointers[key] = ptr
	unsafePointersLock.Unlock()
	return ptr
}

// replaceCString 与 registerCString 相同，但先释放该 key 上一次的指针。
// stream_read 使用 "streamId_read" 单槽协议，旧指针必须在这里回收。
func replaceCString(key string, encoded []byte) *C.char {
	ptr := C.CString(string(encoded))
	unsafePointersLock.Lock()
	if old, exists := unsafePointers[key]; exists && old != nil {
		C.free(unsafe.Pointer(old))
	}
	unsafePointers[key] = ptr
	unsafePointersLock.Unlock()
	return ptr
}

// cErr 把错误编码成 {"err": ..., "id": ...} 并按 id 登记指针，让 Python 的
// freeMemory(id) 与成功响应走同一条释放路径。json.Marshal 保证错误文本里的
// 引号/反斜杠/换行不会产出非法 JSON。
func cErr(prefix string, err error) *C.char {
	message := prefix
	if err != nil {
		if message != "" {
			message += ": "
		}
		message += err.Error()
	}
	id := uuid.New().String()
	encoded, marshalErr := json.Marshal(map[string]interface{}{"err": message, "id": id})
	if marshalErr != nil {
		encoded = []byte(`{"err": "json.Marshal failed", "id": "` + id + `"}`)
	}
	return registerCString(id, encoded)
}

//export request
func request(requestParamsChar *C.char) *C.char {
	requestParamsString := C.GoString(requestParamsChar)
	requestParams := libs.RequestParams{}
	err := json.Unmarshal([]byte(requestParamsString), &requestParams)
	if err != nil {
		return cErr("request->err := json.Unmarshal([]byte(requestParamsString), &requestParams) failed", err)
	}

	req, err := buildRequest(requestParams)
	if err != nil {
		return cErr("request->req, err := buildRequest(requestParams) failed", err)
	}

	session := GetSession(requestParams.Id)
	response, err := session.Request(requestParams.Method, requestParams.Url, req)
	if err != nil {
		return cErr("request->response, err := GetSession(requestParams.Id).Request(requestParams.Method, requestParams.Url, req) failed", err)
	}
	defer response.Body.Close()

	responseParams := make(map[string]interface{})
	responseParams["id"] = uuid.New().String()
	responseParams["url"] = response.Url
	responseParams["headers"] = response.Headers
	responseParams["cookies"] = response.Cookies
	responseParams["status_code"] = response.StatusCode
	responseParams["content"] = utils.Base64Encode(response.Content)
	responseParams["raw"] = response.RawResponse

	responseParamsString, err := json.Marshal(responseParams)
	if err != nil {
		return cErr("request->responseParamsString, err := json.Marshal(responseParams) failed", err)
	}
	return registerCString(responseParams["id"].(string), responseParamsString)
}

func buildRequest(requestParams libs.RequestParams) (*url.Request, error) {
	if requestParams.Method == "" {
		return nil, errors.New("method is null")
	}

	if requestParams.Url == "" {
		return nil, errors.New("url is null")
	}

	req := url.NewRequest()
	if requestParams.Params != nil {
		params := url.NewParams()
		for key, value := range requestParams.Params {
			params.Set(key, value)
		}
		req.Params = params
	}

	if requestParams.Headers != nil {
		headers := url.NewHeaders()
		if requestParams.UnChangedHeaderKey != nil {
			(*headers)[http.UnChangedHeaderKey] = requestParams.UnChangedHeaderKey
		}
		if requestParams.HeadersOrder != nil {
			(*headers)[http.HeaderOrderKey] = requestParams.HeadersOrder
		}
		for key, value := range requestParams.Headers {
			if strings.ToLower(key) != "content-length" {
				headers.Set(key, value)
			}
		}
		req.Headers = headers
	}

	if requestParams.Cookies != nil {
		cookies, _ := cookiejar.New(nil)
		u, _ := url2.Parse(requestParams.Url)
		for key, value := range requestParams.Cookies {
			cookies.SetCookies(u, []*http.Cookie{&http.Cookie{
				Name:  key,
				Value: value,
			}})
		}
		req.Cookies = cookies
	}

	if requestParams.Data != nil {
		data := url.NewData()
		for key, value := range requestParams.Data {
			data.Set(key, value)
		}
		req.Data = data
	}

	if requestParams.Json != nil {
		req.Json = requestParams.Json
	}

	if requestParams.Body != "" {
		req.Body = bytes.NewReader(utils.Base64DecodeToBytes(requestParams.Body))
	}

	if requestParams.Auth != nil {
		req.Auth = requestParams.Auth
	}

	if requestParams.Timeout != 0 {
		timeout := requestParams.Timeout
		req.Timeout = time.Duration(timeout) * time.Second
	}

	req.AllowRedirects = url.Bool(requestParams.AllowRedirects)

	if requestParams.Proxies != "" {
		req.Proxies = requestParams.Proxies
	}

	req.Verify = url.Bool(requestParams.Verify)

	if requestParams.Cert != nil {
		req.Cert = requestParams.Cert
	}

	if requestParams.Ja3 != "" {
		req.Ja3 = requestParams.Ja3
	}

	if requestParams.RandomJA3 {
		req.RandomJA3 = url.Bool(requestParams.RandomJA3)
	}

	if requestParams.ForceHTTP1 {
		req.ForceHTTP1 = url.Bool(requestParams.ForceHTTP1)
	}

	if requestParams.PseudoHeaderOrder != nil {
		if req.Headers == nil {
			req.Headers = url.NewHeaders()
		}
		(*req.GetHeaders())[http.PHeaderOrderKey] = requestParams.PseudoHeaderOrder
	}

	if requestParams.TLSExtensions != "" {
		tlsExtensions := &ja3.Extensions{}
		err := json.Unmarshal([]byte(requestParams.TLSExtensions), tlsExtensions)
		if err != nil {
			return nil, err
		}
		req.TLSExtensions = ja3.ToTLSExtensions(tlsExtensions)
	}

	if requestParams.HTTP2Settings != "" {
		http2Settings := &ja3.H2Settings{}
		err := json.Unmarshal([]byte(requestParams.HTTP2Settings), http2Settings)
		if err != nil {
			return nil, err
		}
		req.HTTP2Settings = ja3.ToHTTP2Settings(http2Settings)
	}

	if requestParams.Stream {
		req.Stream = true
	}
	// DLL 调用方（Python）每请求都传入完整的 cookie 状态：会话自累积的 jar
	// 不能再合并进来，否则 Python 侧清掉的 cookie 会被补回。
	req.IsolatedCookies = true
	return req, nil
}

//export freeMemory
func freeMemory(responseId *C.char) {
	responseIdString := C.GoString(responseId)

	unsafePointersLock.Lock()
	defer unsafePointersLock.Unlock()

	ptr, ok := unsafePointers[responseIdString]
	if !ok {
		return
	}
	delete(unsafePointers, responseIdString)

	if ptr != nil {
		C.free(unsafe.Pointer(ptr))
	}
}

//export freeSession
func freeSession(sessionId *C.char) {
	sessionIdString := C.GoString(sessionId)
	if actual, loaded := sessionsPool.LoadAndDelete(sessionIdString); loaded {
		if session, ok := actual.(*requests.Session); ok && session != nil {
			session.Close()
		}
	}
}

//export setDebug
func setDebug(enable bool) {
	requests.SetDebug(enable)
}

//export stream_request
func stream_request(requestParamsChar *C.char) *C.char {
	requestParamsString := C.GoString(requestParamsChar)
	requestParams := libs.RequestParams{}
	err := json.Unmarshal([]byte(requestParamsString), &requestParams)
	if err != nil {
		return cErr("stream_request->json.Unmarshal failed", err)
	}

	// 强制开启 Stream 模式
	requestParams.Stream = true

	req, err := buildRequest(requestParams)
	if err != nil {
		return cErr("stream_request->buildRequest failed", err)
	}

	session := GetSession(requestParams.Id)
	response, err := session.Request(requestParams.Method, requestParams.Url, req)
	if err != nil {
		return cErr("stream_request->session.Request failed", err)
	}

	// 返回元信息（不包含 body 内容）
	responseParams := make(map[string]interface{})
	responseParams["url"] = response.Url
	responseParams["headers"] = response.Headers
	responseParams["cookies"] = response.Cookies
	responseParams["status_code"] = response.StatusCode

	// 生成 stream_id，先序列化再登记 entry：序列化失败时不能把 body/entry 留在池里。
	streamId := uuid.New().String()
	responseParams["stream_id"] = streamId
	responseParamsString, err := json.Marshal(responseParams)
	if err != nil {
		if response.Body != nil {
			response.Body.Close()
		}
		return cErr("stream_request->json.Marshal failed", err)
	}
	streamPool.Store(streamId, &StreamEntry{
		Response: response,
		Body:     response.Body,
		owner:    session,
	})

	return registerCString(streamId, responseParamsString)
}

//export stream_read
func stream_read(streamIdChar *C.char, size C.int) *C.char {
	streamId := C.GoString(streamIdChar)

	entry, ok := streamPool.Load(streamId)
	if !ok {
		return cErr("stream_read->stream not found: "+streamId, nil)
	}

	streamEntry := entry.(*StreamEntry)
	streamEntry.mu.Lock()
	defer streamEntry.mu.Unlock()

	if streamEntry.closed || streamEntry.Body == nil {
		return cErr("stream_read->stream is closed: "+streamId, nil)
	}

	bufSize := int(size)
	if bufSize <= 0 {
		bufSize = 4096
	}

	buf := make([]byte, bufSize)
	n, err := streamEntry.Body.Read(buf)

	result := make(map[string]interface{})
	result["data"] = ""
	result["eof"] = false
	if n > 0 {
		result["data"] = utils.Base64Encode(buf[:n])
	}

	if err != nil {
		// EOF 或读错误都结束这个流；已读到的 n 字节仍照常返回，不能丢末尾数据。
		result["eof"] = true
		if err != io.EOF {
			result["err"] = "stream_read->Read failed: " + err.Error()
		}
		streamEntry.finishLocked()
	}

	resultString, err := json.Marshal(result)
	if err != nil {
		return cErr("stream_read->json.Marshal failed", err)
	}

	// stream_read 返回的指针用 stream_id + "_read" 作为 key 管理，
	// 每次调用会释放上一次的指针，避免内存泄漏。
	return replaceCString(streamId+"_read", resultString)
}

//export stream_close
func stream_close(streamIdChar *C.char) {
	streamId := C.GoString(streamIdChar)

	// LoadAndDelete 保证并发/重复 close 只有一次拿到 entry，关 body 是幂等的。
	entry, ok := streamPool.LoadAndDelete(streamId)
	if !ok {
		return
	}

	streamEntry := entry.(*StreamEntry)
	streamEntry.mu.Lock()
	streamEntry.finishLocked()
	streamEntry.mu.Unlock()

	// 清理相关的 unsafePointers
	unsafePointersLock.Lock()
	defer unsafePointersLock.Unlock()

	for _, key := range []string{streamId, streamId + "_read"} {
		if ptr, exists := unsafePointers[key]; exists && ptr != nil {
			C.free(unsafe.Pointer(ptr))
		}
		delete(unsafePointers, key)
	}
}

func main() {
	defer func() {
		if r := recover(); r != nil {
			// 处理 panic，可以记录日志或采取其他措施
			fmt.Println("Recovered from panic:", r)
		}
	}()
}
