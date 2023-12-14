#pragma once

#include <ydb/core/base/feature_flags.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>

#include <ydb/core/util/ulid.h>

#include <ydb/library/mkql_proto/protos/minikql.pb.h>

#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NKqp {

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

struct TDeferredEffect {
    TKqpPhyTxHolder::TConstPtr PhysicalTx;
    TQueryData::TPtr Params;

    explicit TDeferredEffect(const TKqpPhyTxHolder::TConstPtr& physicalTx)
        : PhysicalTx(physicalTx) {}
};


class TKqpTransactionContext;

struct TDeferredEffects {
public:
    bool Empty() const {
        return DeferredEffects.empty();
    }

    ui64 Size() const {
        return DeferredEffects.size();
    }

    decltype(auto) begin() const {
        return DeferredEffects.begin();
    }

    decltype(auto) end() const {
        return DeferredEffects.end();
    }

private:
    [[nodiscard]]
    bool Add(const TKqpPhyTxHolder::TConstPtr& physicalTx, const TQueryData::TPtr& params) {
        DeferredEffects.emplace_back(physicalTx);
        DeferredEffects.back().Params = params;
        return true;
    }

    void Clear() {
        DeferredEffects.clear();
    }

private:
    TVector<TDeferredEffect> DeferredEffects;

    friend class TKqpTransactionContext;
};

class TKqpTransactionContext : public NYql::TKikimrTransactionContextBase  {
public:
    explicit TKqpTransactionContext(bool implicit, const NMiniKQL::IFunctionRegistry* funcRegistry,
        TIntrusivePtr<ITimeProvider> timeProvider, TIntrusivePtr<IRandomProvider> randomProvider, bool enableImmediateEffects)
        : NYql::TKikimrTransactionContextBase(enableImmediateEffects)
        , Implicit(implicit)
        , ParamsState(MakeIntrusive<TParamsState>())
    {
        CreationTime = TInstant::Now();
        TxAlloc = std::make_shared<TTxAllocatorState>(funcRegistry, timeProvider, randomProvider);
        Touch();
    }

    TString NewParamName() {
        return TStringBuilder() << ParamNamePrefix << (++ParamsState->LastIndex);
    }

    void ClearDeferredEffects() {
        DeferredEffects.Clear();
    }

    [[nodiscard]]
    bool AddDeferredEffect(const TKqpPhyTxHolder::TConstPtr& physicalTx, const TQueryData::TPtr& params) {
        return DeferredEffects.Add(physicalTx, params);
    }

    bool TxHasEffects() const {
        return HasImmediateEffects || !DeferredEffects.Empty();
    }

    const IKqpGateway::TKqpSnapshot& GetSnapshot() const {
        return SnapshotHandle.Snapshot;
    }

    void Finish() final {
        YQL_ENSURE(DeferredEffects.Empty());
        YQL_ENSURE(!Locks.HasLocks());

        FinishTime = TInstant::Now();

        if (Implicit) {
            Reset();
        } else {
            Closed = true;
        }
    }

    void Touch() {
        LastAccessTime = TInstant::Now();
    }

    void OnBeginQuery() {
        ++QueriesCount;
        BeginQueryTime = TInstant::Now();
    }

    void OnEndQuery() {
        QueriesDuration += TInstant::Now() - BeginQueryTime;
    }

    void Reset() {
        TKikimrTransactionContextBase::Reset();

        DeferredEffects.Clear();
        ParamsState = MakeIntrusive<TParamsState>();
        SnapshotHandle.Snapshot = IKqpGateway::TKqpSnapshot::InvalidSnapshot;
        HasImmediateEffects = false;
    }

    TKqpTransactionInfo GetInfo() const;

    void SetIsolationLevel(const Ydb::Table::TransactionSettings& settings) {
        switch (settings.tx_mode_case()) {
            case Ydb::Table::TransactionSettings::kSerializableReadWrite:
                EffectiveIsolationLevel = NKikimrKqp::ISOLATION_LEVEL_SERIALIZABLE;
                Readonly = false;
                break;

            case Ydb::Table::TransactionSettings::kOnlineReadOnly:
                EffectiveIsolationLevel = settings.online_read_only().allow_inconsistent_reads()
                    ? NKikimrKqp::ISOLATION_LEVEL_READ_UNCOMMITTED
                    : NKikimrKqp::ISOLATION_LEVEL_READ_COMMITTED;
                Readonly = true;
                break;

            case Ydb::Table::TransactionSettings::kStaleReadOnly:
                EffectiveIsolationLevel = NKikimrKqp::ISOLATION_LEVEL_READ_STALE;
                Readonly = true;
                break;

            case Ydb::Table::TransactionSettings::kSnapshotReadOnly:
                // TODO: (KIKIMR-3374) Use separate isolation mode to avoid optimistic locks.
                EffectiveIsolationLevel = NKikimrKqp::ISOLATION_LEVEL_SERIALIZABLE;
                Readonly = true;
                break;

            case Ydb::Table::TransactionSettings::TX_MODE_NOT_SET:
                YQL_ENSURE(false, "tx_mode not set, settings: " << settings);
                break;
        };
    }

    bool ShouldExecuteDeferredEffects() const {
        if (HasUncommittedChangesRead) {
            YQL_ENSURE(EnableImmediateEffects);
            return !DeferredEffects.Empty();
        }

        return false;
    }

    void OnNewExecutor(bool isLiteral) {
        if (!isLiteral)
            ++ExecutorId;
    }

    void AcceptIncomingSnapshot(IKqpGateway::TKqpSnapshot& snapshot) {
        // it's be possible that the executor will not be send a valid snapshot
        // because it makes only commit/rollback operation with the locks.
        if (SnapshotHandle.Snapshot.IsValid() && snapshot.IsValid()) {
            YQL_ENSURE(SnapshotHandle.Snapshot == snapshot, "detected unexpected snapshot switch in tx, ["
                << SnapshotHandle.Snapshot.Step << "," << SnapshotHandle.Snapshot.TxId << "] vs ["
                << snapshot.Step << "," << snapshot.TxId << "].");
        }

        if (ExecutorId == 1) {
            if (snapshot.IsValid() && !SnapshotHandle.Snapshot.IsValid()) {
                SnapshotHandle.Snapshot = snapshot;
            }
        }
    }

    bool CanDeferEffects() const {
        if (HasUncommittedChangesRead || AppData()->FeatureFlags.GetEnableForceImmediateEffectsExecution()) {
            YQL_ENSURE(EnableImmediateEffects);
            return false;
        }

        return true;
    }

public:
    struct TParamsState : public TThrRefBase {
        ui32 LastIndex = 0;
    };

    const bool Implicit;

    TInstant CreationTime;
    TInstant LastAccessTime;
    TInstant FinishTime;

    TInstant BeginQueryTime;
    TDuration QueriesDuration;
    ui32 QueriesCount = 0;
    ui32 ExecutorId = 0;

    TKqpTxLocks Locks;

    TDeferredEffects DeferredEffects;
    bool HasImmediateEffects = false;
    NTopic::TTopicOperations TopicOperations;
    TIntrusivePtr<TParamsState> ParamsState;
    TTxAllocatorState::TPtr TxAlloc;

    IKqpGateway::TKqpSnapshotHandle SnapshotHandle;
};

struct TTxId {
    TULID Id;

    TTxId()
        : Id(TULID::Min())
    {}

    TTxId(const TULID& other)
        : Id(other)
    {}

    static TTxId FromString(const TString& str) {
        TTxId res;
        YQL_ENSURE(res.Id.ParseString(str));
        return res;
    }

    friend bool operator==(const TTxId& lhs, const TTxId& rhs) {
        return lhs.Id == rhs.Id;
    }

    TString GetHumanStr() {
        return Id.ToString();
    }
};

class TTransactionsCache {
    struct TCachedTransaction {
        TTxId Id;
        TIntrusivePtr<TKqpTransactionContext> Context;
    };
    ui64 ActiveSize;
    TVector<TCachedTransaction> Active;
    std::deque<TIntrusivePtr<TKqpTransactionContext>> ToBeAborted;

    TCachedTransaction* FindTransactionById(const TTxId& id) {
        for (auto& cachedTransaction : Active) {
            if (cachedTransaction.Context && cachedTransaction.Id == id) {
                return &cachedTransaction;
            }
        }
        return nullptr;
    }

    TCachedTransaction* FindOldestTransaction() {
        TCachedTransaction* result = nullptr;
        for (auto& cachedTransaction : Active) {
            if (cachedTransaction.Context && (!result || result->Context->LastAccessTime < cachedTransaction.Context->LastAccessTime)) {
                result = &cachedTransaction;
            }
        }
        return result;
    }

    bool Insert(const TTxId& id, TIntrusivePtr<TKqpTransactionContext> context) {
        for (auto& cachedTransaction : Active) {
            if (!cachedTransaction.Context) {
                cachedTransaction.Id = id;
                cachedTransaction.Context = std::move(context);
                ++ActiveSize;
                return true;
            }
        }
        return false;
    }

    TIntrusivePtr<TKqpTransactionContext> ReleaseContext(TCachedTransaction* cachedTransaction) {
        if (cachedTransaction->Context) {
            auto result = std::move(cachedTransaction->Context);
            cachedTransaction->Context = nullptr;
            --ActiveSize;
            return result;
        }
        return nullptr;
    }

public:
    ui64 EvictedTx = 0;
    TDuration IdleTimeout;

    TTransactionsCache(size_t size, TDuration idleTimeout)
        : ActiveSize(0)
        , Active(size)
        , IdleTimeout(idleTimeout)
    {}

    size_t Size() const {
        return ActiveSize;
    }

    size_t MaxSize() const {
        return Active.size();
    }

    TIntrusivePtr<TKqpTransactionContext> Find(const TTxId& id) {
        auto* cachedTransaction = FindTransactionById(id);
        if (cachedTransaction) {
            cachedTransaction->Context->Touch();
            return cachedTransaction->Context;
        } else {
            return nullptr;
        }
    }

    TIntrusivePtr<TKqpTransactionContext> ReleaseTransaction(const TTxId& id) {
        auto* cachedTransaction = FindTransactionById(id);
        if (cachedTransaction) {
            return ReleaseContext(cachedTransaction);
        } else {
            return nullptr;
        }
    }

    void AddToBeAborted(TIntrusivePtr<TKqpTransactionContext> ctx) {
        ToBeAborted.emplace_back(std::move(ctx));
    }

    bool RemoveOldTransactions() {
        if (ActiveSize < Active.size()) {
            return true;
        }

        auto* oldest = FindOldestTransaction();
        auto currentIdle = TInstant::Now() - oldest->Context->LastAccessTime;
        if (currentIdle >= IdleTimeout) {
            oldest->Context->Invalidate();
            ToBeAborted.emplace_back(ReleaseContext(oldest));
            ++EvictedTx;
            return true;
        } else {
            return false;
        }
    }

    bool CreateNew(const TTxId& txId, TIntrusivePtr<TKqpTransactionContext> txCtx) {
        if (!RemoveOldTransactions()) {
            return false;
        }
        return Insert(txId, std::move(txCtx));
    }

    void FinalCleanup() {
        for (auto& item : Active) {
            if (item.Context) {
                item.Context->Invalidate();
                ToBeAborted.emplace_back(ReleaseContext(&item));
            }
        }
    }

    size_t ToBeAbortedSize() {
        return ToBeAborted.size();
    }

    std::deque<TIntrusivePtr<TKqpTransactionContext>> ReleaseToBeAborted() {
        return std::exchange(ToBeAborted, {});
    }
};

std::pair<bool, std::vector<NYql::TIssue>> MergeLocks(const NKikimrMiniKQL::TType& type,
    const NKikimrMiniKQL::TValue& value, TKqpTransactionContext& txCtx);

bool NeedSnapshot(const TKqpTransactionContext& txCtx, const NYql::TKikimrConfiguration& config, bool rollbackTx,
    bool commitTx, const NKqpProto::TKqpPhyQuery& physicalQuery);

bool HasOlapTableInTx(const NKqpProto::TKqpPhyQuery& physicalQuery);

}  // namespace NKikimr::NKqp

template<>
struct THash<NKikimr::NKqp::TTxId> {
    inline size_t operator()(const NKikimr::NKqp::TTxId& id) const noexcept {
        return THash<NKikimr::TULID>()(id.Id);
    }
};
