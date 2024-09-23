#pragma once

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/util/ulid.h>
#include <ydb/library/mkql_proto/protos/minikql.pb.h>
#include <ydb/core/protos/data_events.pb.h>


namespace NKikimr {
namespace NKqp {

class TKqpTxLock {
public:
    using TKey = std::tuple<ui64, ui64, ui64, ui64>;

    TKqpTxLock(const NKikimrMiniKQL::TValue& lockValue)
        : LockValue(lockValue) {}

    ui64 GetLockId() const { return LockValue.GetStruct(3).GetUint64(); }
    ui64 GetDataShard() const { return LockValue.GetStruct(1).GetUint64(); }
    ui64 GetSchemeShard() const { return LockValue.GetStruct(5).GetUint64(); }
    ui64 GetPathId() const { return LockValue.GetStruct(4).GetUint64(); }
    ui32 GetGeneration() const { return LockValue.GetStruct(2).GetUint32(); }
    ui64 GetCounter() const { return LockValue.GetStruct(0).GetUint64(); }
    bool HasWrites() const { return LockValue.GetStruct(6).GetBool(); }
    void SetHasWrites() {
        LockValue.MutableStruct(6)->SetBool(true);
    }

    TKey GetKey() const { return std::make_tuple(GetLockId(), GetDataShard(), GetSchemeShard(), GetPathId()); }
    NKikimrMiniKQL::TValue GetValue() const { return LockValue; }
    NYql::NDq::TMkqlValueRef GetValueRef(const NKikimrMiniKQL::TType& type) const { return NYql::NDq::TMkqlValueRef(type, LockValue); }

    bool Invalidated(const TKqpTxLock& newLock) const {
        YQL_ENSURE(GetKey() == newLock.GetKey());
        return GetGeneration() != newLock.GetGeneration() || GetCounter() != newLock.GetCounter();
    }

private:
    NKikimrMiniKQL::TValue LockValue;
};

struct TKqpTxLocks {
    NKikimrMiniKQL::TType LockType;
    NKikimrMiniKQL::TListType LocksListType;
    THashMap<TKqpTxLock::TKey, TKqpTxLock> LocksMap;
    NLongTxService::TLockHandle LockHandle;

    TMaybe<NYql::TIssue> LockIssue;

    bool HasLocks() const { return !LocksMap.empty(); }
    bool Broken() const { return LockIssue.Defined(); }
    void MarkBroken(NYql::TIssue lockIssue) { LockIssue.ConstructInPlace(std::move(lockIssue)); }
    ui64 GetLockTxId() const { return LockHandle ? LockHandle.GetLockId() : HasLocks() ? LocksMap.begin()->second.GetLockId() : 0; }
    size_t Size() const { return LocksMap.size(); }

    NYql::TIssue GetIssue() {
        Y_ENSURE(LockIssue);
        return *LockIssue;
    }

    void ReportIssues(NYql::TExprContext& ctx) {
        if (LockIssue)
            ctx.AddError(*LockIssue);
    }

    void Clear() {
        LocksMap.clear();
        LockIssue.Clear();
    }
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
    virtual bool AddLock(ui64 shardId, TKqpTxLock lock) = 0;

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
        TVector<TKqpTxLock> Locks;
    };

    virtual TPrepareInfo GetPrepareTransactionInfo(ui64 shardId) = 0;

    struct TPrepareResult {
        ui64 ShardId;
        ui64 MinStep;
        ui64 MaxStep;
        ui64 Coordinator;
    };

    virtual bool ConsumePrepareTransactionResult(TPrepareResult&& result) = 0;

    virtual void StartExecuting() = 0;

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
