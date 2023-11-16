GO_LIBRARY()

SRCS(
    cond.go
    map.go
    mutex.go
    once.go
    pool.go
    poolqueue.go
    runtime.go
    runtime2.go
    rwmutex.go
    waitgroup.go
)

END()

RECURSE(
    atomic
)
