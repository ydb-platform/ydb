#pragma once

#include "throttle.h"

#include <util/datetime/base.h>
#include <util/stream/input.h>
#include <util/stream/output.h>

class TThrottledOutputStream: public IOutputStream {
public:
    TThrottledOutputStream(TAtomicSharedPtr<IOutputStream> stream, ui32 maxBytesPerSecond, TDuration samplingInterval = TDuration::Seconds(1));
    TThrottledOutputStream(TAtomicSharedPtr<IOutputStream> stream, TThrottle::TOptions throttleOptions);

protected:
    void DoWrite(const void* buf, size_t len) override;
    void DoFlush() override;

private:
    TAtomicSharedPtr<IOutputStream> Stream;
    TThrottle ThrottleBytes;
};

class TThrottledInputStream: public IInputStream {
public:
    TThrottledInputStream(TAtomicSharedPtr<IInputStream> stream, TThrottle::TOptions throttleOptions);

protected:
    size_t DoRead(void* buf, size_t len) override;

private:
    TAtomicSharedPtr<IInputStream> Stream;
    TThrottle ThrottleBytes;
};

void ThrottledLockMemory(const void* addr, ui64 len, TThrottle::TOptions throttleOptions);
