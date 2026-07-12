#include "retro_span_deserialization.h"

#include <ydb/library/actors/retro_tracing/span/span_buffer.h>

#include <cstring>
#include <unordered_set>
#include <vector>

namespace NRetroTracing {

std::unique_ptr<TRetroSpan> DeserializeRetroSpanToUnique(const void* data) {
    const TRetroSpan* base = reinterpret_cast<const TRetroSpan*>(data);
    return std::unique_ptr<TRetroSpan>(DeserializeRetroSpanImpl(
            base->GetType(), base->GetSize(), data));
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

std::vector<std::unique_ptr<TRetroSpan>> GetSpansImpl(const TTraceIdSet& traceIds, bool getAll) {
    static TBufferData readBuffer;
    std::vector<std::unique_ptr<TRetroSpan>> res;
    TRetroSpan spanHeader(0, sizeof(TRetroSpan));

    auto callback = [&] {
        for (ui32 pos = 0; pos < SpanBufferSize; pos += SpanCellSize) {
            const void* ptr = readBuffer.data() + pos;
            std::memcpy(
                reinterpret_cast<void*>(&spanHeader),
                ptr,
                sizeof(TRetroSpan));
            if (spanHeader.GetTraceId() && (getAll || traceIds.count(spanHeader.GetTraceId()))) {
                if (std::unique_ptr<TRetroSpan> retroSpan = DeserializeRetroSpanToUnique(ptr)) {
                    res.push_back(std::move(retroSpan));
                } else {
                    Y_DEBUG_ABORT();
                }
            }
        }
    };

    AccessBuffers(&readBuffer, callback);
    return res;
}

std::vector<std::unique_ptr<TRetroSpan>> GetSpansOfTrace(const NWilson::TTraceId& traceId) {
    TTraceIdSet traceIds;
    traceIds.insert(NWilson::TTraceId(traceId));
    return GetSpansImpl(traceIds, false);
}

std::vector<std::unique_ptr<TRetroSpan>> GetSpansOfTraces(const std::vector<NWilson::TTraceId>& traceIds) {
    TTraceIdSet traceIdSet(traceIds.begin(), traceIds.end());
    return GetSpansImpl(traceIdSet, false);
}

std::vector<std::unique_ptr<TRetroSpan>> GetAllSpans() {
    return GetSpansImpl({}, true);
}

};
