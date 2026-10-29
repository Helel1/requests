//go:build probe

package main

/*
#include <stdlib.h>
*/
import "C"

// 测试专用导出（仅 -tags probe 构建）：用于观察会话 / 流 / C 字符串登记表的
// 条目数，验证配平。生产 so 用 `go build -buildmode=c-shared export.go` 构建，
// 不包含本文件的任何符号。

//export probeUnsafePointerCount
func probeUnsafePointerCount() C.int {
	unsafePointersLock.Lock()
	defer unsafePointersLock.Unlock()
	return C.int(len(unsafePointers))
}

//export probeStreamCount
func probeStreamCount() C.int {
	count := 0
	streamPool.Range(func(_, _ any) bool {
		count++
		return true
	})
	return C.int(count)
}

//export probeSessionCount
func probeSessionCount() C.int {
	count := 0
	sessionsPool.Range(func(_, _ any) bool {
		count++
		return true
	})
	return C.int(count)
}
