#pragma once

#include "datetime.h"

#include <util/generic/noncopyable.h>

#ifdef _linux_

#include <util/system/yassert.h>
#include <errno.h>
#include <sys/timerfd.h>

struct TTimerFd: public TNonCopyable {
    int Fd;

    TTimerFd() {
        Fd = timerfd_create(CLOCK_MONOTONIC, 0);
        Y_ABORT_UNLESS(Fd != -1, "timerfd_create(CLOCK_MONOTONIC, 0) -> -1; errno:%d: %s", int(errno), strerror(errno));
    }

    ~TTimerFd() {
        close(Fd);
    }

    void Set(ui64 ts) {
        ui64 now = GetCycleCountFast();
        Arm(now >= ts? 1: NHPTimer::GetSeconds(ts - now) * 1e9);
    }

    void Reset() {
        Arm(0); // disarm timer
    }

    void Wait() {
        ui64 expirations;
        ssize_t s = read(Fd, &expirations, sizeof(ui64));
        Y_UNUSED(s); // Y_ABORT_UNLESS(s == sizeof(ui64));
    }

    void Wake() {
        Arm(1);
    }
private:
    void Arm(ui64 ns) {
        struct itimerspec spec;
        spec.it_value.tv_sec = ns / 1'000'000'000;
        spec.it_value.tv_nsec = ns % 1'000'000'000;
        spec.it_interval.tv_sec = 0;
        spec.it_interval.tv_nsec = 0;
        int ret = timerfd_settime(Fd, 0, &spec, nullptr);
        Y_ABORT_UNLESS(ret != -1, "timerfd_settime(%d, 0, %" PRIu64 "ns, 0) -> %d; errno:%d: %s", Fd, ns, ret, int(errno), strerror(errno));
    }
};

#else

struct TTimerFd: public TNonCopyable {
    int Fd = 0;
    void Set(ui64) {}
    void Reset() {}
    void Wait() {}
    void Wake() {}
};

#endif
