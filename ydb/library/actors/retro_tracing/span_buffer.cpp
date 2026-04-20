#include "span_buffer.h"

#include <util/generic/size_literals.h>

#include <algorithm>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace NRetroTracing {

class TSpanCircleBuffer {
private:
    static constexpr ui32 CellSize = 1 << 10;
    static constexpr ui32 BufferSize = 4 << 20;
    static constexpr ui32 Capacity = BufferSize / CellSize;
    static constexpr ui32 CapacityMask = Capacity - 1;

public:
    using TBuffer = std::array<char, BufferSize>;

public:
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
            std::memcpy(
                static_cast<void*>(Buffer.data() + head * CellSize),
                static_cast<const void*>(span),
                spanSize);
            ++Head;
        }
    }

    // Hash functor for trace ID portion only (128-bit)
    struct TTraceIdHash {
        size_t operator()(const NWilson::TTraceId& id) const {
            const auto* p = static_cast<const ui64*>(id.GetTraceIdPtr());
            return std::hash<ui64>()(p[0]) ^ (std::hash<ui64>()(p[1]) << 1);
        }
    };

    // Equality functor comparing only the trace portion
    struct TTraceIdEqual {
        bool operator()(const NWilson::TTraceId& a, const NWilson::TTraceId& b) const {
            return a.IsSameTrace(b);
        }
    };

    using TTraceIdSet = std::unordered_set<NWilson::TTraceId, TTraceIdHash, TTraceIdEqual>;

    std::vector<std::unique_ptr<TRetroSpan>> GetSpans(TBuffer& readBuffer,
            const TTraceIdSet& traceIds, bool getAll) {
        {
            std::lock_guard guard(Lock);
            std::memcpy(
                static_cast<void*>(readBuffer.data()),
                static_cast<const void*>(Buffer.data()),
                BufferSize);
        }
        TRetroSpan spanHeader(0, sizeof(TRetroSpan));
        std::vector<std::unique_ptr<TRetroSpan>> res;
        for (ui32 pos = 0; pos < BufferSize; pos += CellSize) {
            const void* ptr = readBuffer.data() + pos;
            std::memcpy(
                reinterpret_cast<void*>(&spanHeader),
                ptr,
                sizeof(TRetroSpan));
            if (spanHeader.GetTraceId() && (getAll || traceIds.count(spanHeader.GetTraceId()))) {
                res.push_back(TRetroSpan::DeserializeToUnique(ptr));
            }
        }
        return res;
    }

private:
    TBuffer Buffer = {0};
    ui64 Head = 0;
    std::mutex Lock;
};

// char TSpanCircleBuffer::TmpBuffer[BufferSize] = {0};

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

static std::vector<std::unique_ptr<TRetroSpan>> GetSpansImpl(const TSpanCircleBuffer::TTraceIdSet& traceIds, bool getAll) {
    static TSpanCircleBuffer::TBuffer readBuffer;
    std::vector<std::unique_ptr<TRetroSpan>> res;
    std::lock_guard guard(Mutex);
    for (const auto& [id, buffer] : SpanBuffers) {
        if (std::shared_ptr<TSpanCircleBuffer> locked = buffer.lock()) {
            std::vector<std::unique_ptr<TRetroSpan>> spans = locked->GetSpans(readBuffer, traceIds, getAll);
            std::move(spans.begin(), spans.end(), std::back_inserter(res));
        }
    }
    return res;
}

std::vector<std::unique_ptr<TRetroSpan>> GetSpansOfTrace(const NWilson::TTraceId& traceId) {
    TSpanCircleBuffer::TTraceIdSet traceIds;
    traceIds.insert(NWilson::TTraceId(traceId));
    return GetSpansImpl(traceIds, false);
}

std::vector<std::unique_ptr<TRetroSpan>> GetSpansOfTraces(const std::vector<NWilson::TTraceId>& traceIds) {
    TSpanCircleBuffer::TTraceIdSet traceIdSet(traceIds.begin(), traceIds.end());
    return GetSpansImpl(traceIdSet, false);
}

std::vector<std::unique_ptr<TRetroSpan>> GetAllSpans() {
    return GetSpansImpl({}, true);
}

void DropThreadLocalBuffer() {
    SpanBuffer.reset();
}

} // namespace NRetroTracing
