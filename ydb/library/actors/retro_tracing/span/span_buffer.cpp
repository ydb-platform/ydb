#include "span_buffer.h"

#include <util/generic/size_literals.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace NRetroTracing {

class TSpanCircleBuffer {
private:
    static constexpr ui32 Capacity = SpanBufferSize / SpanCellSize;
    static constexpr ui32 CapacityMask = Capacity - 1;

public:
    using TBuffer = std::array<char, SpanBufferSize>;

public:
    void WriteSpan(const TRetroSpan* span) {
        if (!span->IsEnded()) {
            // unable to write non-ended span
            return;
        }

        ui32 spanSize = span->GetSize();
        if (spanSize == 0 || spanSize > SpanCellSize) {
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
                static_cast<void*>(Buffer.data() + head * SpanCellSize),
                static_cast<const void*>(span),
                spanSize);
            ++Head;
        }
    }

    void CopyData(TBufferData* destination) {
        std::lock_guard guard(Lock);
        std::memcpy(static_cast<void*>(destination->data()),
                static_cast<const void*>(Buffer.data()), SpanBufferSize);
    }

private:
    TBuffer Buffer = {0};
    ui64 Head = 0;
    std::mutex Lock;
};

static thread_local std::shared_ptr<TSpanCircleBuffer> SpanBuffer;
static std::mutex SpanBufferMutex;
static std::unordered_map<std::thread::id, std::weak_ptr<TSpanCircleBuffer>> SpanBuffers;

void InitializeThreadLocalBuffer() {
    SpanBuffer = std::make_shared<TSpanCircleBuffer>();
    std::lock_guard guard(SpanBufferMutex);
    SpanBuffers[std::this_thread::get_id()] = SpanBuffer;
}

void WriteSpan(const TRetroSpan* span) {
    if (!SpanBuffer) {
        InitializeThreadLocalBuffer();
    }
    SpanBuffer->WriteSpan(span);
}

void DropThreadLocalBuffer() {
    SpanBuffer.reset();
}

void AccessBuffers(TBufferData* readBuffer, std::function<void()> callback) {
    std::lock_guard guard(SpanBufferMutex);
    for (auto& [_, buffer] : SpanBuffers) {
        if (std::shared_ptr<TSpanCircleBuffer> locked = buffer.lock()) {
            locked->CopyData(readBuffer);
            callback();
        }
    }
}

} // namespace NRetroTracing
