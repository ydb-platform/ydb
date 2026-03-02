#include "span_buffer.h"

#include <util/generic/size_literals.h>

#include <algorithm>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

#include <util/generic/bitops.h>

namespace NRetroTracing {

class TSpanCircleBuffer {
public:
    TSpanCircleBuffer()
        : Head(0)
        , Capacity(BufferSize / CellSize)
        , CapacityMask(FastClp2(Capacity) - 1)
    {
        std::fill(Buffer, Buffer + BufferSize, 0);
    }

    void WriteSpan(const TRetroSpan* span) {
        if (!span->IsEnded()) {
            // unable to write non-ended span
            return;
        }

        ui32 spanSize = span->GetSize();
        if (spanSize == 0 || spanSize > CellSize) {
            // invalid span size, reject span
            return;
        }

        { // critical section
            std::unique_lock guard(Lock, std::try_to_lock);
            if (!guard.owns_lock()) {
                // read is in progress, reject span
                return;
            }
            ui64 head = Head & CapacityMask;
            std::memcpy(static_cast<void*>(Buffer + head * CellSize), static_cast<const void*>(span), spanSize);
            ++Head;
        }
    }

    std::vector<std::unique_ptr<TRetroSpan>> GetSpans(const NWilson::TTraceId& traceId, bool getAll) {
        {
            std::lock_guard guard(Lock);
            std::memcpy(static_cast<void*>(TmpBuffer), static_cast<const void*>(Buffer), BufferSize);
        }
        TRetroSpan spanHeader(0, sizeof(TRetroSpan));
        std::vector<std::unique_ptr<TRetroSpan>> res;
        for (ui32 pos = 0; pos < BufferSize; pos += CellSize) {
            const void* ptr = TmpBuffer + pos;
            std::memcpy(reinterpret_cast<void*>(&spanHeader), ptr, sizeof(TRetroSpan));
            if (spanHeader.GetTraceId() && (getAll || spanHeader.GetTraceId().IsSameTrace(traceId))) {
                res.push_back(TRetroSpan::DeserializeToUnique(ptr));
            }
        }
        return res;
    }

private:
    static constexpr ui32 CellSize = 1_KB;
    static constexpr ui32 BufferSize = 5_MB;
    char Buffer[BufferSize] = {0};
    static char TmpBuffer[BufferSize];

    ui64 Head = 0;
    const ui64 Capacity = 0;
    const ui64 CapacityMask = 0;

    std::mutex Lock;
};

char TSpanCircleBuffer::TmpBuffer[BufferSize] = {0};

static thread_local std::shared_ptr<TSpanCircleBuffer> SpanBuffer;
static std::mutex Mutex;
static std::unordered_map<std::thread::id, std::weak_ptr<TSpanCircleBuffer>> SpanBuffers;

void InitializeThreadLocalBuffer() {
    SpanBuffer = std::make_shared<TSpanCircleBuffer>();
    std::lock_guard guard(Mutex);
    SpanBuffers[std::this_thread::get_id()] = SpanBuffer;
}

void WriteSpan(const TRetroSpan* span) {
    if (!SpanBuffer) {
        InitializeThreadLocalBuffer();
    }
    SpanBuffer->WriteSpan(span);
}

static std::vector<std::unique_ptr<TRetroSpan>> GetSpans(const NWilson::TTraceId& traceId, bool getAll) {
    std::vector<std::unique_ptr<TRetroSpan>> res;
    std::lock_guard guard(Mutex);
    for (const auto& [id, buffer] : SpanBuffers) {
        if (std::shared_ptr<TSpanCircleBuffer> locked = buffer.lock()) {
            std::vector<std::unique_ptr<TRetroSpan>> spans = locked->GetSpans(traceId, getAll);
            std::move(spans.begin(), spans.end(), std::back_inserter(res));
        }
    }
    return res;
}

std::vector<std::unique_ptr<TRetroSpan>> GetSpansOfTrace(const NWilson::TTraceId& traceId) {
    return GetSpans(traceId, false);
}

std::vector<std::unique_ptr<TRetroSpan>> GetAllSpans() {
    return GetSpans({}, true);
}

void DropThreadLocalBuffer() {
    SpanBuffer.reset();
}

} // namespace NRetroTracing
