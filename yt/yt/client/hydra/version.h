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

    std::strong_ordering operator<=>(const TReachableState& other) const;
    bool operator==(const TReachableState& other) const = default;
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

    std::strong_ordering operator<=>(const TElectionPriority& other) const;
    bool operator==(const TElectionPriority& other) const = default;
};

void FormatValue(TStringBuilderBase* builder, TElectionPriority state, TStringBuf /*spec*/);

////////////////////////////////////////////////////////////////////////////////

struct TVersion
{
    int SegmentId = 0;
    int RecordId = 0;

    TVersion() = default;
    TVersion(int segmentId, int recordId) noexcept;

    std::strong_ordering operator<=>(const TVersion& other) const = default;
    bool operator==(const TVersion& other) const = default;

    // COMPAT(h0pless): HydraLogicalRecordId. Move this method to TLogicalVersion.
    TRevision ToRevision() const;

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);
};

void FormatValue(TStringBuilderBase* builder, TVersion version, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
struct TExtendedVersionBase
    : public TVersion
{
    using TVersion::TVersion;

    [[nodiscard]] TImpl Advance(int delta = 1) const;
    [[nodiscard]] TImpl Rotate() const;
};

struct TLogicalVersion
    : public TExtendedVersionBase<TLogicalVersion>
{
    using TExtendedVersionBase::TExtendedVersionBase;

    static TLogicalVersion FromRevision(TRevision revision);
};

struct TPhysicalVersion
    : public TExtendedVersionBase<TPhysicalVersion>
{
    using TExtendedVersionBase::TExtendedVersionBase;
};

////////////////////////////////////////////////////////////////////////////////

class TAutomatonVersion
{
public:
    TAutomatonVersion() = default;
    TAutomatonVersion(
        int segmentId,
        int physicalRecordId,
        int logicalRecordId);

    TPhysicalVersion GetPhysicalVersion() const;
    TLogicalVersion GetLogicalVersion() const;
    int GetSegmentId() const;

    TRevision GetLogicalRevision() const;

    [[nodiscard]] TAutomatonVersion Advance() const;

private:
    explicit TAutomatonVersion(
        TPhysicalVersion physicalVersion,
        TLogicalVersion logicalVersion);

    /*
     * Sometimes a single physical mutation can consist of several independent
     * logical mutations. This means that a single entry in the changelog is
     * responsible for a multitude of changes, some of which are not related
     * to each other.
     *
     * This is often the case with Hive mutations.
     *
     * This results in the following problem: two or more changes happened
     * independently on cell C1 and resulted in different versions and
     * revisions associated with said changes. But the same logical mutations
     * happened in one Hive mutation on cell C2, meaning that now the
     * version/revision is the same.
     *
     * This means that, technically speaking, it's very unsafe to compare
     * mutation version and mutation/object revisions in a mutation.
     * To remedy this, it's sufficient to introduce a distinction between:
     * 1. PhysicalRecordId - changelog entry index; this is the value that
     *    Hydra uses "under the hood".
     * 2. LogicalRecordId - the index that mutation would have had, if every
     *    mutation consisted of related changes only.
     *
     * These record IDs are saved in PhysicalVersion and LogicalVersion.
     *
     * Example:
     * Physical Index:  +----- 1 -----+--------- 2 --------+------ 3 -----+
     * Changelog entry: |  CreateNode | ApplyHiveMutations | SetAttribute |
     * Logical entry:   |  CreateNode | StartTx | LockNode | SetAttribute |
     * Logical Index:   +----- 1 -----+--- 2 ---+---- 3 ---+------ 4 -----+
     *
     * If you are unsure which version to use - use logical.
     */
    int SegmentId_ = 0;
    int PhysicalRecordId_ = 0;
    int LogicalRecordId_ = 0;
};

void FormatValue(TStringBuilderBase* builder, TAutomatonVersion version, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra

Y_DECLARE_PODTYPE(NYT::NHydra::TVersion);

#define VERSION_INL_H_
#include "version-inl.h"
#undef VERSION_INL_H_
