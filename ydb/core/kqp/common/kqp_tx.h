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
        TIntrusivePtr<ITimeProvider> timeProvider, TIntrusivePtr<IRandomProvider> randomProvider)
        : NYql::TKikimrTransactionContextBase()
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
    TString HumanStr;

    TTxId()
        : Id(TULID::Min())
    {}

    TTxId(const TULID& other)
        : Id(other)
        , HumanStr(Id.ToString())
    {}

    static TTxId FromString(const TString& str) {
        TTxId res;
        if (res.Id.ParseString(str)) {
            res.HumanStr = str;
        }
        return res;
    }

    friend bool operator==(const TTxId& lhs, const TTxId& rhs) {
        return lhs.Id == rhs.Id;
    }

    TString GetHumanStr() {
        return HumanStr;
    }
};

}

template<>
struct THash<NKikimr::NKqp::TTxId> {
    inline size_t operator()(const NKikimr::NKqp::TTxId& id) const noexcept {
        return THash<NKikimr::TULID>()(id.Id);
    }
};

namespace NKikimr::NKqp {

class TTransactionsCache {
    size_t MaxActiveSize;
    THashMap<TTxId, TIntrusivePtr<TKqpTransactionContext>, THash<NKikimr::NKqp::TTxId>> Active;
    std::deque<TIntrusivePtr<TKqpTransactionContext>> ToBeAborted;

    auto FindOldestTransaction() {
        if (Active.empty()) {
            return std::end(Active);
        }
        auto oldest = std::begin(Active);
        for (auto it = std::next(oldest); it != std::end(Active); ++it) {
            if (oldest->second->LastAccessTime > it->second->LastAccessTime) {
                oldest = it;
            }
        }
        return oldest;
    }

public:
    ui64 EvictedTx = 0;
    TDuration IdleTimeout;

    TTransactionsCache(size_t size, TDuration idleTimeout)
        : MaxActiveSize(size)
        , IdleTimeout(idleTimeout)
    {
        Active.reserve(MaxActiveSize);
    }

    size_t Size() const {
        return Active.size();
    }

    size_t MaxSize() const {
        return MaxActiveSize;
    }

    TIntrusivePtr<TKqpTransactionContext> Find(const TTxId& id) {
        auto it = Active.find(id);
        if (it != std::end(Active)) {
            it->second->Touch();
            return it->second;
        } else {
            return nullptr;
        }
    }

    TIntrusivePtr<TKqpTransactionContext> ReleaseTransaction(const TTxId& id) {
        const auto it = Active.find(id);
        if (it != std::end(Active)) {
            auto result = std::move(it->second);
            Active.erase(it);
            return result;
        } else {
            return nullptr;
        }
    }

    void AddToBeAborted(TIntrusivePtr<TKqpTransactionContext> ctx) {
        ToBeAborted.emplace_back(std::move(ctx));
    }

    bool RemoveOldTransactions() {
        if (Active.size() < MaxActiveSize) {
            return true;
        }

        auto oldestIt = FindOldestTransaction();
        auto currentIdle = TInstant::Now() - oldestIt->second->LastAccessTime;
        if (currentIdle >= IdleTimeout) {
            oldestIt->second->Invalidate();
            ToBeAborted.emplace_back(std::move(oldestIt->second));
            Active.erase(oldestIt);
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
        return Active.emplace(txId, txCtx).second;
    }

    void FinalCleanup() {
        for (auto& item : Active) {
            item.second->Invalidate();
            ToBeAborted.emplace_back(std::move(item.second));
        }
        Active.clear();
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

bool HasOlapTableReadInTx(const NKqpProto::TKqpPhyQuery& physicalQuery);
bool HasOlapTableWriteInStage(
    const NKqpProto::TKqpPhyStage& stage,
    const google::protobuf::RepeatedPtrField< ::NKqpProto::TKqpPhyTable>& tables);
bool HasOlapTableWriteInTx(const NKqpProto::TKqpPhyQuery& physicalQuery);
bool HasOltpTableReadInTx(const NKqpProto::TKqpPhyQuery& physicalQuery);
bool HasOltpTableWriteInTx(const NKqpProto::TKqpPhyQuery& physicalQuery);

}  // namespace NKikimr::NKqp
