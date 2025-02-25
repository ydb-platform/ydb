#include "span_circlebuf.h"

#include <cstring>
#include <mutex>

namespace NRetro {

TSpanCircleBuf::TSpanCircleBuf(ui32 cellsNum)
    : Buffer(cellsNum)
    , Head(0)
    , StatsSnapshot(nullptr)
{}

std::vector<TRetroSpan::TPtr> TSpanCircleBuf::ReadSpansOfTrace(ui64 traceId) {
    std::vector<TRetroSpan::TPtr> spans;

    std::lock_guard guard(ReadLock);
    ++Stats.Reads;

    for (ui32 i = 0; i < Buffer.size(); ++i) {
        TRetroSpan* spanPtr = Buffer[i].GetSpanPtr();
        if (spanPtr->GetId().TraceId == traceId) {
            spans.push_back(TRetroSpan::FromRawData((ui32)Buffer[i].Type, spanPtr));
        }
    }

    CreateStatsSnapshot();

    return spans;
}

void TSpanCircleBuf::Write(ERetroSpanType type, const ui8* span) {
    ui32 spanSize = SizeOfRetroSpan(type);
    ui32 size = spanSize + sizeof(type);

    std::unique_lock guard(ReadLock, std::try_to_lock);
    if (size > sizeof(TCell)) {
        ++Stats.SpansRejectedDueToOversize;
        return;  // span is too big, write rejected
    }

    ++Stats.Writes;
    if (guard.owns_lock()) {
        Buffer[Head].Type = type;
        std::memcpy(&Buffer[Head].Data, span, spanSize);

        if (++Head >= Buffer.size()) {
            Head = 0;
            ++Stats.Overflows;
        }
        ++Stats.SuccessfulWrites;
    } else {
        ++Stats.FailedLocks;
    }
}

std::shared_ptr<TSpanCircleBufStats> TSpanCircleBuf::GetStatsSnapshot() {
    return StatsSnapshot;
}

void TSpanCircleBuf::CreateStatsSnapshot() {
    StatsSnapshot = std::make_shared<TSpanCircleBufStats>(Stats);
}

} // namespace NRetro
