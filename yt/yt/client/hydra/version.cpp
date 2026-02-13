#include "version.h"

#include <yt/yt/core/misc/serialize.h>

#include <library/cpp/yt/string/format.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

TReachableState::TReachableState(int segmentId, i64 sequenceNumber) noexcept
    : SegmentId(segmentId)
    , SequenceNumber(sequenceNumber)
{ }

std::strong_ordering TReachableState::operator<=>(const TReachableState& other) const
{
    if (SegmentId != other.SegmentId) {
        return SegmentId <=> other.SegmentId;
    }
    return SequenceNumber <=> other.SequenceNumber;
}

void FormatValue(TStringBuilderBase* builder, TReachableState state, TStringBuf /* spec */)
{
    builder->AppendFormat("%v:%v", state.SegmentId, state.SequenceNumber);
}

////////////////////////////////////////////////////////////////////////////////

TElectionPriority::TElectionPriority(int lastMutationTerm, int segmentId, i64 sequenceNumber) noexcept
    : LastMutationTerm(lastMutationTerm)
    , ReachableState(segmentId, sequenceNumber)
{ }

TElectionPriority::TElectionPriority(int lastMutationTerm, TReachableState reachableState) noexcept
    : LastMutationTerm(lastMutationTerm)
    , ReachableState(reachableState)
{ }

std::strong_ordering TElectionPriority::operator<=>(const TElectionPriority& other) const
{
    if (LastMutationTerm != other.LastMutationTerm) {
        return LastMutationTerm <=> other.LastMutationTerm;
    }
    return ReachableState.SequenceNumber <=> other.ReachableState.SequenceNumber;
}

void FormatValue(TStringBuilderBase* builder, TElectionPriority priority, TStringBuf /* spec */)
{
    builder->AppendFormat("{MutationTerm: %v, State: %v}",
        priority.LastMutationTerm,
        priority.ReachableState);
}

////////////////////////////////////////////////////////////////////////////////

TVersion::TVersion(int segmentId, int recordId) noexcept
    : SegmentId(segmentId)
    , RecordId(recordId)
{ }

TRevision TVersion::ToRevision() const
{
    return TRevision((static_cast<ui64>(SegmentId) << 32) | static_cast<ui64>(RecordId));
}

void TVersion::Save(TStreamSaveContext& context) const
{
    using NYT::Save;

    Save(context, SegmentId);
    Save(context, RecordId);
}

void TVersion::Load(TStreamLoadContext& context)
{
    using NYT::Load;

    Load(context, SegmentId);
    Load(context, RecordId);
}

void FormatValue(TStringBuilderBase* builder, TVersion version, TStringBuf /* spec */)
{
    builder->AppendFormat("%v:%v", version.SegmentId, version.RecordId);
}

////////////////////////////////////////////////////////////////////////////////

TLogicalVersion TLogicalVersion::FromRevision(TRevision revision)
{
    return TLogicalVersion(revision.Underlying() >> 32, revision.Underlying() & 0xffffffff);
}

////////////////////////////////////////////////////////////////////////////////

TAutomatonVersion::TAutomatonVersion(
    int segmentId,
    int physicalRecordId,
    int logicalRecordId)
    : SegmentId_(segmentId)
    , PhysicalRecordId_(physicalRecordId)
    , LogicalRecordId_(logicalRecordId)
{
    YT_VERIFY(physicalRecordId <= logicalRecordId);
}

TAutomatonVersion::TAutomatonVersion(
    TPhysicalVersion physicalVersion,
    TLogicalVersion logicalVersion)
    : SegmentId_(physicalVersion.SegmentId)
    , PhysicalRecordId_(physicalVersion.RecordId)
    , LogicalRecordId_(logicalVersion.RecordId)
{
    YT_VERIFY(physicalVersion.SegmentId == logicalVersion.SegmentId);
    YT_VERIFY(physicalVersion.RecordId <= logicalVersion.RecordId);
}

TPhysicalVersion TAutomatonVersion::GetPhysicalVersion() const
{
    return TPhysicalVersion(SegmentId_, PhysicalRecordId_);
}

TLogicalVersion TAutomatonVersion::GetLogicalVersion() const
{
    return TLogicalVersion(SegmentId_, LogicalRecordId_);
}

int TAutomatonVersion::GetSegmentId() const
{
    return SegmentId_;
}

TRevision TAutomatonVersion::GetLogicalRevision() const
{
    return TLogicalVersion(SegmentId_, LogicalRecordId_).ToRevision();
}

TAutomatonVersion TAutomatonVersion::Advance() const
{
    return TAutomatonVersion(
        std::move(GetPhysicalVersion().Advance()),
        std::move(GetLogicalVersion().Advance()));
}

void FormatValue(TStringBuilderBase* builder, TAutomatonVersion version, TStringBuf /*spec*/)
{
    auto logicalVersion = version.GetLogicalVersion();
    auto physicalVersion = version.GetPhysicalVersion();

    YT_ASSERT(physicalVersion.SegmentId == logicalVersion.SegmentId);

    builder->AppendFormat("%v:%v(%v)",
        physicalVersion.SegmentId,
        physicalVersion.RecordId,
        logicalVersion.RecordId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
