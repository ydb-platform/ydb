#include "hints.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_roles.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

namespace {

template <typename T>
TVector<TPBufferKey> DoMakePBufferKeys(std::span<const T> segments)
{
    TVector<TPBufferKey> result;
    result.reserve(segments.size());
    for (const auto& segment: segments) {
        result.push_back(segment.PBufferKey);
    }
    return result;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TReadRangeHint::TReadRangeHint(
    THostMask hostMask,
    TPBufferKey pBufferKey,
    TBlockRange64 requestRelativeRange,
    TBlockRange64 vchunkRange,
    TRangeLock&& lock)
    : HostMask(hostMask)
    , PBufferKey(pBufferKey)
    , RequestRelativeRange(requestRelativeRange)
    , VChunkRange(vchunkRange)
    , Lock(std::move(lock))
{}

TReadRangeHint::TReadRangeHint(TReadRangeHint&& other) noexcept = default;
TReadRangeHint& TReadRangeHint::operator=(
    TReadRangeHint&& other) noexcept = default;

TString TReadRangeHint::DebugPrint() const
{
    TStringBuilder result;
    if (PBufferKey.Lsn == 0) {
        result << "0";
    } else {
        result << PBufferKey.Print();
    }
    result << "{" << HostMask.Print() << VChunkRange.Print()
           << RequestRelativeRange.Print() << "};";
    return result;
}

TString TReadHint::DebugPrint() const
{
    if (RangeHints.empty()) {
        return (WaitReady.IsReady()) ? "WaitReady:Ready" : "WaitReady:NotReady";
    }

    TStringBuilder result;
    for (const auto& hint: RangeHints) {
        result << hint.DebugPrint();
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

// static
TVector<TPBufferKey> TPBufferSegment::MakePBufferKeys(
    std::span<const TPBufferSegment> segments)
{
    return DoMakePBufferKeys(segments);
}

TString TPBufferSegment::DebugPrint(bool brief) const
{
    if (brief) {
        return ToString(PBufferKey.Lsn);
    }
    return TStringBuilder() << PBufferKey.Print() << Range.Print();
}

////////////////////////////////////////////////////////////////////////////////

TString TFlushHint::DebugPrint(bool brief) const
{
    TStringBuilder builder;
    bool first = true;
    for (const auto& segment: Segments) {
        if (!first) {
            builder << ",";
        }
        builder << segment.DebugPrint(brief);
        first = false;
    }
    return builder;
}

////////////////////////////////////////////////////////////////////////////////

void TFlushHints::AddHint(
    THostIndex source,
    THostIndex destination,
    TPBufferKey pBufferKey,
    TBlockRange64 range)
{
    Hints[THostRoute{
              .SourceHostIndex = source,
              .DestinationHostIndex = destination}]
        .Segments.emplace_back(pBufferKey, range);
}

bool TFlushHints::Empty() const
{
    return Hints.empty();
}

const TFlushHints::THints& TFlushHints::GetAllHints() const
{
    return Hints;
}

TFlushHints::THints TFlushHints::TakeAllHints()
{
    return std::move(Hints);
}

TString TFlushHints::DebugPrint() const
{
    TStringBuilder builder;
    for (const auto& [route, hint]: Hints) {
        builder << route.DebugPrint() << ":" << hint.DebugPrint(false) << ";";
    }
    return builder;
}

////////////////////////////////////////////////////////////////////////////////

TString TEraseSegment::DebugPrint(bool brief) const
{
    if (brief) {
        return ToString(PBufferKey.Lsn);
    }
    return PBufferKey.Print();
}

TString TEraseHint::DebugPrint(bool brief) const
{
    TStringBuilder builder;
    bool first = true;
    for (const auto& segment: Segments) {
        if (!first) {
            builder << ",";
        }
        builder << segment.DebugPrint(brief);
        first = false;
    }
    return builder;
}

void TEraseHints::AddHint(THostIndex host, TPBufferKey pBufferKey)
{
    Hints[host].Segments.push_back(TEraseSegment{.PBufferKey = pBufferKey});
}

bool TEraseHints::Empty() const
{
    return Hints.empty();
}

const TEraseHints::THints& TEraseHints::GetAllHints() const
{
    return Hints;
}

TEraseHints::THints TEraseHints::TakeAllHints()
{
    return std::move(Hints);
}

TString TEraseHints::DebugPrint() const
{
    TStringBuilder builder;
    for (const auto& [host, hint]: Hints) {
        builder << PrintHostIndex(host) << ":" << hint.DebugPrint(false) << ";";
    }
    return builder;
}

////////////////////////////////////////////////////////////////////////////////

TVector<TPBufferKey> MakePBufferKeys(std::span<const TPBufferSegment> segments)
{
    return DoMakePBufferKeys<TPBufferSegment>(segments);
}

TVector<TPBufferKey> MakePBufferKeys(std::span<const TEraseSegment> segments)
{
    return DoMakePBufferKeys<TEraseSegment>(segments);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
