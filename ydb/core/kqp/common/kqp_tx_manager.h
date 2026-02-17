#pragma once

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/common/simple/reattach.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/util/ulid.h>
#include <ydb/library/mkql_proto/protos/minikql.pb.h>
#include <ydb/core/protos/data_events.pb.h>


namespace NKikimr {
namespace NKqp {

struct TTableInfo {
    bool IsOlap = false;
    THashSet<TStringBuf> Pathes;
};

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

    virtual void AddShard(ui64 shardId, bool isOlap, const TString& path) = 0;
    virtual void AddAction(ui64 shardId, ui8 action) = 0;
    virtual void AddAction(ui64 shardId, ui8 action, ui64 querySpanId) = 0;
    virtual void AddTopic(ui64 topicId, const TString& path) = 0;
    virtual void AddTopicsToShards() = 0;
    virtual bool AddLock(ui64 shardId, const NKikimrDataEvents::TLock& lock, ui64 querySpanId = 0, ui64 deferredVictimQuerySpanId = 0) = 0;

    virtual void BreakLock(ui64 shardId) = 0;
    virtual TVector<NKikimrDataEvents::TLock> GetLocks() const = 0;
    virtual TVector<NKikimrDataEvents::TLock> GetLocks(ui64 shardId) const = 0;

    virtual TTableInfo GetShardTableInfo(ui64 shardId) const = 0;

    virtual bool ShouldReattach(ui64 shardId, TInstant now) = 0;
    virtual void Reattached(ui64 shardId) = 0;
    virtual void SetRestarting(ui64 shardId) = 0;

    struct TReattachState {
        TReattachInfo ReattachInfo;
        ui64 Cookie = 0;
    };

    virtual TReattachState& GetReattachState(ui64 shardId) = 0;

    virtual EShardState GetState(ui64 shardId) const = 0;
    virtual void SetError(ui64 shardId) = 0;
    virtual void SetError() = 0;

    virtual void SetPartitioning(const TTableId tableId, const std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>>& partitioning) = 0;
    virtual std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> GetPartitioning(const TTableId tableId) const = 0;

    virtual void SetTopicOperations(NTopic::TTopicOperations&& topicOperations) = 0;
    virtual const NTopic::TTopicOperations& GetTopicOperations() const = 0;
    virtual void BuildTopicTxs(NTopic::TTopicOperationTransactions& txs) = 0;
    virtual bool HasTopics() const = 0;

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
