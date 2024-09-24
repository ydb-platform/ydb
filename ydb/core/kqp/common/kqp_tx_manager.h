#pragma once

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/util/ulid.h>
#include <ydb/library/mkql_proto/protos/minikql.pb.h>
#include <ydb/core/protos/data_events.pb.h>


namespace NKikimr {
namespace NKqp {

struct TKqpLock {
    using TKey = std::tuple<ui64, ui64, ui64, ui64>;
    TKey GetKey() const { return std::make_tuple(Proto.GetLockId(), Proto.GetDataShard(), Proto.GetSchemeShard(), Proto.GetPathId()); }

    bool Invalidated(const TKqpLock& newLock) const {
        YQL_ENSURE(GetKey() == newLock.GetKey());
        return Proto.GetGeneration() != newLock.Proto.GetGeneration() || Proto.GetCounter() != newLock.Proto.GetCounter();
    }

    TKqpLock(const NKikimrDataEvents::TLock& proto)
        : Proto(proto) {}

    NKikimrDataEvents::TLock Proto;
};

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
        FINISHED
    };

    enum EAction {
        READ = 1,
        WRITE = 2,
    };

    using TActionFlags = ui8;

    // TODO: ???
    // virutal std::optional<ui64> GetLockTxId() const = 0;

    virtual void AddShard(ui64 shardId, bool isOlap, const TString& path) = 0;
    virtual void AddAction(ui64 shardId, ui8 action) = 0;
    virtual bool AddLock(ui64 shardId, TKqpLock lock) = 0;

    virtual TTableInfo GetShardTableInfo(ui64 shardId) const = 0;

    virtual EShardState GetState(ui64 shardId) const = 0;
    virtual void SetState(ui64 shardId, EShardState state) = 0;

    virtual bool IsTxPrepared() const = 0;
    virtual bool IsTxFinished() const = 0;

    virtual bool IsReadOnly() const = 0;
    virtual bool IsSingleShard() const = 0;
    virtual bool IsEmpty() const = 0;

    virtual bool HasSnapshot() const = 0;
    virtual void SetHasSnapshot(bool hasSnapshot) = 0;

    /*struct TCheckLocksResult {
        bool Ok = false;
        std::vector<TKqpTxLock> BrokenLocks;
        bool LocksAcquireFailure = false;
    };
    virtual TCheckLocksResult CheckLocks() const = 0;*/
    virtual bool BrokenLocks() const = 0;
    virtual const std::optional<NYql::TIssue>& GetLockIssue() const = 0;

    virtual const THashSet<ui64>& GetShards() const = 0;
    virtual ui64 GetShardsCount() const = 0;

    virtual void StartPrepare() = 0;

    struct TPrepareInfo {
        const THashSet<ui64>& SendingShards;
        const THashSet<ui64>& ReceivingShards;
        std::optional<ui64> Arbiter; // TODO: support volatile
        std::optional<ui64> ArbiterColumnShard; // TODO: support columnshard&topic
        TVector<TKqpLock> Locks;
    };

    virtual TPrepareInfo GetPrepareTransactionInfo(ui64 shardId) = 0;

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
};

using IKqpTransactionManagerPtr = std::shared_ptr<IKqpTransactionManager>;

IKqpTransactionManagerPtr CreateKqpTransactionManager();

}
}
