#include <util/system/platform.h>

#ifdef _linux_
#include <sys/syscall.h>
#include <linux/futex.h>

#if !defined(SYS_futex)
#define SYS_futex __NR_futex
#endif
#endif

#include <errno.h>

#include <util/system/yassert.h>

#include "futex_like.h"

#ifdef _linux_
namespace {
    int futex(int* uaddr, int op, int val, const struct timespec* timeout,
              int* uaddr2, int val3) {
        return syscall(SYS_futex, uaddr, op, val, timeout, uaddr2, val3);
    }
}
#endif

void TFutexLike::Wake(size_t count) {
    Y_ASSERT(count > 0);
#ifdef _linux_
    if (count > unsigned(Max<int>())) {
        count = Max<int>();
    }
    int r = futex(&Value, FUTEX_WAKE, count, nullptr, nullptr, 0);
    Y_ABORT_UNLESS(r >= 0, "futex_wake failed: %s", strerror(errno));
#else
    TGuard<TMutex> guard(Mutex);
    if (count == 1) {
        CondVar.Signal();
    } else {
        CondVar.BroadCast();
    }
#endif
}

void TFutexLike::Wait(int expected) {
#ifdef _linux_
    int r = futex(&Value, FUTEX_WAIT, expected, nullptr, nullptr, 0);
    Y_ABORT_UNLESS(r >= 0 || errno == EWOULDBLOCK, "futex_wait failed: %s", strerror(errno));
#else
    TGuard<TMutex> guard(Mutex);
    if (expected == Get()) {
        CondVar.WaitI(Mutex);
    }
#endif
}
