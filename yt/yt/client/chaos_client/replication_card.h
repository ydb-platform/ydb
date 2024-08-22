#pragma once

#include "public.h"

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/tablet_client/public.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

struct TReplicationProgress
{
    struct TSegment
    {
        NTableClient::TUnversionedOwningRow LowerKey;
        NTransactionClient::TTimestamp Timestamp;

        void Persist(const TStreamPersistenceContext& context);
    };

    std::vector<TSegment> Segments;
    NTableClient::TUnversionedOwningRow UpperKey;

    void Persist(const TStreamPersistenceContext& context);
};

struct TReplicaHistoryItem
{
    NChaosClient::TReplicationEra Era;
    NTransactionClient::TTimestamp Timestamp;
    NTabletClient::ETableReplicaMode Mode;
    NTabletClient::ETableReplicaState State;

    bool IsSync() const;
    void Persist(const TStreamPersistenceContext& context);
};

struct TReplicaInfo
{
    TString ClusterName;
    NYPath::TYPath ReplicaPath;
    NTabletClient::ETableReplicaContentType ContentType;
    NTabletClient::ETableReplicaMode Mode;
    NTabletClient::ETableReplicaState State;
    TReplicationProgress ReplicationProgress;
    std::vector<TReplicaHistoryItem> History;
    bool EnableReplicatedTableTracker = true;

    //! Returns index of history item corresponding to timestamp, -1 if none.
    int FindHistoryItemIndex(NTransactionClient::TTimestamp timestamp) const;
};

struct TReplicationProgressProjection
{
    NTableClient::TUnversionedRow From;
    NTableClient::TUnversionedRow To;
};

struct TReplicationCard
    : public TRefCounted
{
    THashMap<TReplicaId, TReplicaInfo> Replicas;
    std::vector<NObjectClient::TCellId> CoordinatorCellIds;
    TReplicationEra Era = InvalidReplicationEra;
    NTableClient::TTableId TableId;
    NYPath::TYPath TablePath;
    TString TableClusterName;
    NTransactionClient::TTimestamp CurrentTimestamp = NTransactionClient::NullTimestamp;
    NTabletClient::TReplicatedTableOptionsPtr ReplicatedTableOptions;
    TReplicationCardCollocationId ReplicationCardCollocationId;

    //! Returns pointer to replica with a given id, nullptr if none.
    TReplicaInfo* FindReplica(TReplicaId replicaId);
    TReplicaInfo* GetReplicaOrThrow(TReplicaId replicaId, TReplicationCardId replicationCardId);
};

DEFINE_REFCOUNTED_TYPE(TReplicationCard)

////////////////////////////////////////////////////////////////////////////////

struct TReplicationCardFetchOptions
{
    bool IncludeCoordinators = false;
    bool IncludeProgress = false;
    bool IncludeHistory = false;
    bool IncludeReplicatedTableOptions = false;

    operator size_t() const;
    bool operator == (const TReplicationCardFetchOptions& other) const = default;

    bool Contains(const TReplicationCardFetchOptions& other) const;
};

void FormatValue(TStringBuilderBase* builder, const TReplicationCardFetchOptions& options, TStringBuf /*spec*/);

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TReplicationProgress& replicationProgress,
    TStringBuf /*spec*/,
    std::optional<TReplicationProgressProjection> replicationProgressProjection = std::nullopt);

void FormatValue(TStringBuilderBase* builder, const TReplicaHistoryItem& replicaHistoryItem, TStringBuf /*spec*/);

void FormatValue(
    TStringBuilderBase* builder,
    const TReplicaInfo& replicaInfo,
    TStringBuf /*spec*/,
    std::optional<TReplicationProgressProjection> replicationProgressProjection = std::nullopt);

void FormatValue(
    TStringBuilderBase* builder,
    const TReplicationCard& replicationCard,
    TStringBuf /*spec*/,
    std::optional<TReplicationProgressProjection> replicationProgressProjection = std::nullopt);
TString ToString(
    const TReplicationCard& replicationCard,
    std::optional<TReplicationProgressProjection> replicationProgressProjection = std::nullopt);

////////////////////////////////////////////////////////////////////////////////

bool IsReplicaSync(NTabletClient::ETableReplicaMode mode, const TReplicaHistoryItem& lastReplicaHistoryItem);
bool IsReplicaAsync(NTabletClient::ETableReplicaMode mode);
bool IsReplicaEnabled(NTabletClient::ETableReplicaState state);
bool IsReplicaDisabled(NTabletClient::ETableReplicaState state);
bool IsReplicaReallySync(
    NTabletClient::ETableReplicaMode mode,
    NTabletClient::ETableReplicaState state,
    const std::vector<TReplicaHistoryItem>& replicaHistory);
NTabletClient::ETableReplicaMode GetTargetReplicaMode(NTabletClient::ETableReplicaMode mode);
NTabletClient::ETableReplicaState GetTargetReplicaState(NTabletClient::ETableReplicaState state);

void UpdateReplicationProgress(TReplicationProgress* progress, const TReplicationProgress& update);

bool IsReplicationProgressEqual(const TReplicationProgress& progress, const TReplicationProgress& other);
bool IsReplicationProgressGreaterOrEqual(const TReplicationProgress& progress, const TReplicationProgress& other);
bool IsReplicationProgressGreaterOrEqual(const TReplicationProgress& progress, NTransactionClient::TTimestamp timestamp);

TReplicationProgress ExtractReplicationProgress(
    const TReplicationProgress& progress,
    NTableClient::TLegacyKey lower,
    NTableClient::TLegacyKey upper);
TReplicationProgress AdvanceReplicationProgress(const TReplicationProgress& progress, NTransactionClient::TTimestamp timestamp);
TReplicationProgress LimitReplicationProgressByTimestamp(const TReplicationProgress& progress, NTransactionClient::TTimestamp timestamp);
void CanonizeReplicationProgress(TReplicationProgress* progress);

NTransactionClient::TTimestamp GetReplicationProgressMinTimestamp(const TReplicationProgress& progress);
NTransactionClient::TTimestamp GetReplicationProgressMaxTimestamp(const TReplicationProgress& progress);
NTransactionClient::TTimestamp GetReplicationProgressMinTimestamp(
    const TReplicationProgress& progress,
    NTableClient::TLegacyKey lower,
    NTableClient::TLegacyKey upper);

std::optional<NTransactionClient::TTimestamp> FindReplicationProgressTimestampForKey(
    const TReplicationProgress& progress,
    NTableClient::TUnversionedValueRange key);
NTransactionClient::TTimestamp GetReplicationProgressTimestampForKeyOrThrow(
    const TReplicationProgress& progress,
    NTableClient::TUnversionedRow key);

// Gathers replication progresses into a single one.
// Pivot key should match first segment lower keys of a corresponding progress.
// If source progress is empty it is is considered to span corresponding pivot keys range and have MinTimestamp timestamp,
TReplicationProgress GatherReplicationProgress(
    std::vector<TReplicationProgress> progresses,
    const std::vector<NTableClient::TUnversionedRow>& pivotKeys,
    NTableClient::TUnversionedRow upperKey);

// Splits replication progress into ranges [pivotKeys[0]: pivotKeys[1]], ..., [pivotKeys[-1]: upperKey].
std::vector<TReplicationProgress> ScatterReplicationProgress(
    TReplicationProgress progress,
    const std::vector<NTableClient::TUnversionedRow>& pivotKeys,
    NTableClient::TUnversionedRow upperKey);

bool IsReplicaLocationValid(
    const TReplicaInfo* replica,
    const NYPath::TYPath& tablePath,
    const TString& clusterName);

TReplicationProgress BuildMaxProgress(const TReplicationProgress& progress1, const TReplicationProgress& progress2);

TDuration ComputeReplicationProgressLag(
    const TReplicationProgress& maxProgress,
    const TReplicationProgress& replicaProgress);

THashMap<TReplicaId, TDuration> ComputeReplicasLag(const THashMap<TReplicaId, TReplicaInfo>& replicas);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
