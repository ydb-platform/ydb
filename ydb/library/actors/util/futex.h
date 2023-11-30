#pragma once

#ifdef _linux_

#include <linux/futex.h>
#include <unistd.h>
#include <sys/syscall.h>

static long SysFutex(void* addr1, int op, int val1, struct timespec* timeout, void* addr2, int val3) {
    return syscall(SYS_futex, addr1, op, val1, timeout, addr2, val3);
}

#endif
