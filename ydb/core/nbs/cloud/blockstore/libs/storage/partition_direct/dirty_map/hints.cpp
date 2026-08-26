#include "hints.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_roles.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

namespace {

template <typename T>
TVector<ui64> DoMakeLsnVector(std::span<const T> segments)
{
    TVector<ui64> result;
    result.reserve(segments.size());
    for (const auto& segment: segments) {
        result.push_back(segment.Lsn);
    }
    return result;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TReadRangeHint::TReadRangeHint(
    THostMask hostMask,
    ui64 lsn,
    TBlockRange64 requestRelativeRange,
    TBlockRange64 vchunkRange,
    TRangeLock&& lock)
    : HostMask(hostMask)
    , Lsn(lsn)
    , RequestRelativeRange(requestRelativeRange)
    , VChunkRange(vchunkRange)
    , Lock(std::move(lock))
{}

TReadRangeHint::TReadRangeHint(TReadRangeHint&& other) noexcept = default;
TReadRangeHint& TReadRangeHint::operator=(
    TReadRangeHint&& other) noexcept = default;

TString TReadRangeHint::DebugPrint() const
{
    return TStringBuilder()
           << Lsn << "{" << HostMask.Print() << VChunkRange.Print()
           << RequestRelativeRange.Print() << "};";
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
TVector<ui64> TPBufferSegment::MakeLsnVector(
    std::span<const TPBufferSegment> segments)
{
    TVector<ui64> result;
    result.reserve(segments.size());
    for (const auto& segment: segments) {
        result.push_back(segment.Lsn);
    }
    return result;
}

TString TPBufferSegment::DebugPrint(bool brief) const
{
    if (brief) {
        return ToString(Lsn);
    }
    return TStringBuilder() << Lsn << Range.Print();
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
    ui64 lsn,
    TBlockRange64 range)
{
    Hints[THostRoute{
              .SourceHostIndex = source,
              .DestinationHostIndex = destination}]
        .Segments.emplace_back(lsn, range);
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
        return ToString(Lsn);
    }
    return TStringBuilder() << Generation << ":" << Lsn;
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

void TEraseHints::AddHint(THostIndex host, ui64 lsn)
{
    Hints[host].Segments.emplace_back(
        0,   // TODO(drbasic)
        lsn);
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

TVector<ui64> MakeLsnVector(std::span<const TPBufferSegment> segments)
{
    return DoMakeLsnVector<TPBufferSegment>(segments);
}

TVector<ui64> MakeLsnVector(std::span<const TEraseSegment> segments)
{
    return DoMakeLsnVector<TEraseSegment>(segments);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
