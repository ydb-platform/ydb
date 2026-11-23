#include "throttled.h"

#include <util/system/info.h>
#include <util/system/mlock.h>
#include <util/system/align.h>
#include <util/system/mincore.h>
#include <util/generic/yexception.h>
#include <util/generic/size_literals.h>
#include <util/generic/vector.h>

TThrottledOutputStream::TThrottledOutputStream(TAtomicSharedPtr<IOutputStream> stream, ui32 maxBytesPerSecond, TDuration samplingInterval)
    : TThrottledOutputStream(std::move(stream), TThrottle::TOptions::FromMaxPerSecond((ui64)maxBytesPerSecond, samplingInterval))
{
}

TThrottledOutputStream::TThrottledOutputStream(TAtomicSharedPtr<IOutputStream> stream, TThrottle::TOptions throttleOptions)
    : Stream(std::move(stream))
    , ThrottleBytes(throttleOptions)
{
}

void TThrottledOutputStream::DoWrite(const void* buf, size_t len) {
    size_t remaining = len;
    const char* ptr = static_cast<const char*>(buf);

    while (remaining != 0) {
        const size_t step = ThrottleBytes.GetQuota(remaining);
        Y_ASSERT(step > 0);
        Y_ASSERT(step <= remaining);

        Stream->Write(ptr, step);
        ptr += step;
        remaining -= step;
    }
}

void TThrottledOutputStream::DoFlush() {
    Stream->Flush();
}

TThrottledInputStream::TThrottledInputStream(TAtomicSharedPtr<IInputStream> stream, TThrottle::TOptions options)
    : Stream(std::move(stream))
    , ThrottleBytes(options)
{
}

size_t TThrottledInputStream::DoRead(void* buf, size_t len) {
    const size_t availableQuota = ThrottleBytes.GetQuota(len);
    Y_ASSERT(availableQuota <= len);
    return Stream->Read(buf, availableQuota);
}

void ThrottledLockMemory(const void* addr, ui64 len, TThrottle::TOptions throttleOptions) {
    if (!len) {
        return;
    }

    const size_t pageSize = NSystemInfo::GetPageSize();
    len = AlignUp(len, pageSize);
    char* pageForRead = static_cast<char*>(AlignDown(const_cast<void*>(addr), pageSize));
    ui64 numPage = (len + pageSize - 1) / pageSize;
    ui64 numPageForRead = 2_MB / pageSize;

    throttleOptions.MaxUnitsPerInterval /= pageSize;
    if (!throttleOptions.MaxUnitsPerInterval) {
        ythrow yexception() << "BytesPerInterval is not enough to read one page";
    }

    TThrottle throttlePages(throttleOptions);

    if (!numPageForRead) {
        ythrow yexception() << "PageSize more than 2_MB";
    }

    ui64 pageQuota = 0;
    ui64 numBytesForRead = 0;
    TVector<unsigned char> page(numPageForRead);
    for (ui64 lastReadPage = 0; lastReadPage < numPage; lastReadPage += numPageForRead) {
        ui64 numPageCheck = Min<ui64>(numPageForRead, numPage - lastReadPage);
        InCoreMemory(pageForRead, numPageCheck * pageSize, &page[0], numPageForRead);
        for (ui64 j = 0; j < numPageCheck; ++j) {
            if (!pageQuota) {
                pageQuota = throttlePages.GetQuota(numPageForRead);
            }
            numBytesForRead += pageSize;
            if (!IsPageInCore(page[j])) {
                pageQuota -= 1;
            }
            if (!pageQuota || j + 1 == numPageCheck && lastReadPage + numPageForRead >= numPage) {
                LockMemory(pageForRead, numBytesForRead);
                pageForRead += numBytesForRead;
                numBytesForRead = 0;
            }
        }
    }
}
