#pragma once

#include "public.h"

#include <util/generic/typetraits.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TReachableState
{
    int SegmentId = 0;
    i64 SequenceNumber = 0;

    TReachableState() = default;
    TReachableState(int segmentId, i64 sequenceNumber) noexcept;

    std::strong_ordering operator <=> (const TReachableState& other) const;
    bool operator == (const TReachableState& other) const = default;
};

void FormatValue(TStringBuilderBase* builder, TReachableState state, TStringBuf /*spec*/);

////////////////////////////////////////////////////////////////////////////////

struct TElectionPriority
{
    // Actual election priority is a pair (LastMutationTerm, ReachableState.SequenceNumber).
    // Term is used to determine a new leader's term.
    // ReachableState.SegmentId is used as a hint for recovery.

    int LastMutationTerm = 0;
    TReachableState ReachableState;

    TElectionPriority() = default;
    TElectionPriority(
        int lastMutationTerm,
        int segmentId,
        i64 sequenceNumber) noexcept;
    TElectionPriority(
        int lastMutationTerm,
        TReachableState reachableState) noexcept;

    std::strong_ordering operator <=> (const TElectionPriority& other) const;
    bool operator == (const TElectionPriority& other) const = default;
};

void FormatValue(TStringBuilderBase* builder, TElectionPriority state, TStringBuf /*spec*/);

////////////////////////////////////////////////////////////////////////////////

struct TVersion
{
    int SegmentId = 0;
    int RecordId = 0;

    TVersion() = default;
    TVersion(int segmentId, int recordId) noexcept;

    std::strong_ordering operator <=> (const TVersion& other) const;
    bool operator == (const TVersion& other) const = default;

    TRevision ToRevision() const;
    static TVersion FromRevision(TRevision revision);

    TVersion Advance(int delta = 1) const;
    TVersion Rotate() const;
};

void FormatValue(TStringBuilderBase* builder, TVersion version, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra

Y_DECLARE_PODTYPE(NYT::NHydra::TVersion);
