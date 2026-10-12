#pragma once

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/common/simple/reattach.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/util/ulid.h>
#include <ydb/library/mkql_proto/protos/minikql.pb.h>
#include <ydb/core/protos/data_events.pb.h>


namespace NKikimr {
namespace NKqp {

struct TTableInfo {
    bool IsOlap = false;
    THashSet<TStringBuf> Pathes;
};

// Known metadata of a tx participant datashard: which table it belongs to and
// which key range the shard covered at the moment it was first registered in
// the transaction. Borders always come from a scheme-cache-resolved partitioning
// whose version is recorded alongside.
// Borders default-constructed (undefined From/To) means "unknown" — e.g. for
// shards that never were a part of any resolved partitioning in this process.
struct TShardMeta {
    TTableId TableId;
    TOwnedTableRange Borders;
    ui64 PartitioningVersion = 0;

    bool IsKnown() const {
        return PartitioningVersion != 0;
    }
};

// True when both ranges carry the same key borders: same From/To cells (compared
// byte-wise; borders always come from the same SchemeShard serialization), plus
// matching inclusivity and point flags.
bool SameBorders(const TTableRange& lhs, const TTableRange& rhs);

class IKqpTransactionManager {
public:
    virtual ~IKqpTransactionManager() = default;

    enum EShardState {
        PROCESSING,
        PREPARING,
        PREPARED,
        EXECUTING,
        FINISHED,
        ERROR,
    };

    enum EAction {
        READ = 1,
        WRITE = 2,
    };

    using TActionFlags = ui8;

    virtual void AddShard(ui64 shardId, bool isOlap, const TString& path, const TShardMeta& meta = {}) = 0;
    virtual void AddAction(ui64 shardId, ui8 action) = 0;
    virtual void AddAction(ui64 shardId, ui8 action, ui64 querySpanId) = 0;
    virtual void AddTopic(ui64 topicId, const TString& path) = 0;
    virtual void AddTopicsToShards() = 0;
    virtual bool AddLock(ui64 shardId, const NKikimrDataEvents::TLock& lock, ui64 querySpanId = 0, ui64 deferredVictimQuerySpanId = 0) = 0;

    // A split/merge removed shard `fromShardId` (its tablet is gone): move its participant
    // state to the shards that now cover its key range. Locks keep DataShard == `fromShardId`
    // (ancestor locks of the targets, validated by ValidateLocks(allowAncestorLocks)).
    // Targets must exactly cover the removed shard's range (computed by the caller
    // from the partitionings) — a wrong target does not hold the transferred lock
    // and would break the commit.
    // The move is verified when both the removed shard and a target carry filled
    // partitioning metadata: the target's partitioning version must be strictly
    // newer and its borders must intersect the removed shard's range (AFL_ENSURE,
    // fail closed). The border comparison uses the moved table's key schema stored
    // by SetTableMeta (the table metadata must be stored); shards without filled
    // metadata (e.g. a fresh EnsureShards-only target) are safely skipped.
    // Returns false when a transferred lock merged inconsistently with a lock already
    // stored on a target (a WriteSeqNum regression: the shard's uncommitted write chain
    // collapsed underneath the stored lock). The lock is marked invalidated and the
    // locks issue is recorded; the caller must abort the transaction.
    virtual bool MoveShardTo(ui64 fromShardId, const TVector<ui64>& toShardIds) = 0;

    virtual void BreakLock(ui64 shardId) = 0;
    virtual TVector<NKikimrDataEvents::TLock> GetLocks() const = 0;
    virtual TVector<NKikimrDataEvents::TLock> GetLocks(ui64 shardId) const = 0;

    virtual TTableInfo GetShardTableInfo(ui64 shardId) const = 0;

    // Known metadata of a transaction participant shard. Returns an unknown-meta
    // (IsKnown() == false) object for unregistered shards.
    virtual const TShardMeta& GetShardMeta(ui64 shardId) const = 0;

    virtual bool ShouldReattach(ui64 shardId, TInstant now) = 0;
    virtual void Reattached(ui64 shardId) = 0;
    virtual void SetRestarting(ui64 shardId) = 0;

    struct TReattachState {
        TReattachInfo ReattachInfo;
        ui64 Cookie = 0;
    };

    virtual TReattachState& GetReattachState(ui64 shardId) = 0;

    virtual EShardState GetState(ui64 shardId) const = 0;

    virtual bool HasShard(ui64 shardId) const = 0;

    virtual void SetError(ui64 shardId) = 0;
    virtual void SetError() = 0;

    // Metadata of a table participating in the transaction: the partitioning the
    // table was resolved with when it first joined the transaction (the "initial"
    // partitioning — it does not track later split/merges) and the table's key
    // schema, used to compare shard key borders (e.g. in MoveShardTo).
    struct TTableMeta {
        TPartitioning::TCPtr InitialPartitioning;
        TVector<NScheme::TTypeInfo> KeyColumnTypes;
    };

    // Overwrites any previously stored metadata of the table.
    virtual void SetTableMeta(const TTableId tableId, TTableMeta tableMeta) = 0;
    // Empty (null InitialPartitioning) when the table's metadata was never stored.
    virtual TTableMeta GetTableMeta(const TTableId tableId) const = 0;

    virtual void SetTopicOperations(NTopic::TTopicOperations&& topicOperations) = 0;
    virtual const NTopic::TTopicOperations& GetTopicOperations() const = 0;
    virtual void BuildTopicTxs(NTopic::TTopicOperationTransactions& txs) = 0;
    virtual bool HasTopics() const = 0;
    virtual void SetSkipTopicsConflictCheck(bool skipConflictCheck) = 0;

    virtual void AddParticipantNode(const ui32 nodeId) = 0;
    virtual const THashSet<ui32>& GetParticipantNodes() const = 0;

    virtual bool IsTxPrepared() const = 0;
    virtual bool IsTxFinished() const = 0;

    virtual bool IsReadOnly() const = 0;
    virtual bool IsSingleShard() const = 0;
    virtual bool HasOlapTable() const = 0;

    virtual bool IsEmpty() const = 0;
    virtual bool HasLocks() const = 0;

    virtual void SetAllowVolatile(bool allowVolatile) = 0;
    virtual bool IsVolatile() const = 0;

    virtual bool HasSnapshot() const = 0;
    virtual void SetHasSnapshot(bool hasSnapshot) = 0;

    virtual void SetIsolationLevel(NKqpProto::EIsolationLevel level) = 0;
    virtual NKqpProto::EIsolationLevel GetIsolationLevel() const = 0;

    virtual bool CanUseImmediateCommit() const = 0;

    virtual bool BrokenLocks() const = 0;
    virtual ui64 GetBrokenLocksCount() const = 0;
    virtual const std::optional<NYql::TIssue>& GetLockIssue() const = 0;
    virtual void SetVictimQuerySpanId(ui64 querySpanId) = 0;
    virtual std::optional<ui64> GetVictimQuerySpanId() const = 0;
    virtual std::optional<ui64> LookupVictimQuerySpanId(ui64 shardId, const NKikimrDataEvents::TLock& lock) const = 0;
    virtual void SetShardBreakerQuerySpanId(ui64 shardId, ui64 querySpanId) = 0;
    virtual TVector<ui64> GetShardBreakerQuerySpanIds(ui64 shardId) const = 0;

    virtual const THashSet<ui64>& GetShards() const = 0;
    virtual ui64 GetShardsCount() const = 0;

    virtual bool NeedCommit() const = 0;

    virtual ui64 GetCoordinator() const = 0;

    virtual void StartPrepare() = 0;

    struct TPrepareInfo {
        const THashSet<ui64>& SendingShards;
        const THashSet<ui64>& ReceivingShards;
        std::optional<ui64> Arbiter;
        std::optional<ui64> ArbiterColumnShard;
    };

    virtual TPrepareInfo GetPrepareTransactionInfo() = 0;

    struct TPrepareResult {
        ui64 ShardId;
        ui64 MinStep;
        ui64 MaxStep;
        ui64 Coordinator;
    };

    virtual bool ConsumePrepareTransactionResult(TPrepareResult&& result) = 0;

    virtual void StartExecute() = 0;

    struct TCommitShardInfo {
        ui64 ShardId;
        ui32 AffectedFlags;
    };

    struct TCommitInfo {
        ui64 MinStep;
        ui64 MaxStep;
        ui64 Coordinator;

        TVector<TCommitShardInfo> ShardsInfo;
    };

    virtual TCommitInfo GetCommitInfo() = 0;

    virtual bool ConsumeCommitResult(ui64 shardId) = 0;

    virtual const THashSet<ui64>& StartRollback() = 0;
    virtual bool ConsumeRollbackResult(ui64 shardId) = 0;
    virtual bool IsRollBack() const = 0;
};

using IKqpTransactionManagerPtr = std::shared_ptr<IKqpTransactionManager>;

IKqpTransactionManagerPtr CreateKqpTransactionManager(bool collectOnly = false);

}
}
