#pragma once

#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>

#include <ydb/core/kqp/common/kqp_gateway.h>
#include <ydb/core/kqp/common/kqp_tx_info.h>
#include <ydb/core/kqp/common/kqp_topic.h>

#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>

#include <ydb/core/tx/long_tx_service/public/lock_handle.h>

#include <ydb/library/yql/dq/common/dq_value.h>
#include <ydb/library/yql/utils/log/log.h>

namespace NKikimr {
namespace NKqp {

const TStringBuf ParamNamePrefix = "%kqp%";
const TStringBuf LocksAcquireParamName = "%kqp%locks_acquire";
const TStringBuf LocksTxIdParamName = "%kqp%locks_txid";
const TStringBuf LocksListParamName = "%kqp%locks_list";
const TStringBuf ReadTargetParamName = "%kqp%read_target";

/* Non-deterministic internal params */
const std::string_view NowParamName = "%kqp%now";
const std::string_view CurrentDateParamName = "%kqp%current_utc_date";
const std::string_view CurrentDatetimeParamName = "%kqp%current_utc_datetime";
const std::string_view CurrentTimestampParamName = "%kqp%current_utc_timestamp";
const std::string_view RandomParamName = "%kqp%random";
const std::string_view RandomNumberParamName = "%kqp%random_number";
const std::string_view RandomUuidParamName = "%kqp%random_uuid";

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

    void ReportIssues(NYql::TExprContext& ctx) {
        if (LockIssue)
            ctx.AddError(*LockIssue);
    }

    void Clear() {
        LocksMap.clear();
        LockIssue.Clear();
    }
};

using TParamValueMap = THashMap<TString, NKikimrMiniKQL::TParams>;

struct TDeferredEffect {
    NYql::NNodes::TMaybeNode<NYql::NNodes::TExprBase> Node;
    std::shared_ptr<const NKqpProto::TKqpPhyTx> PhysicalTx;
    TParamValueMap Params;

    explicit TDeferredEffect(const NYql::NNodes::TExprBase& node)
        : Node(node) {}

    explicit TDeferredEffect(std::shared_ptr<const NKqpProto::TKqpPhyTx>&& physicalTx)
        : PhysicalTx(std::move(physicalTx)) {}
};

class TKqpTransactionContext;

struct TDeferredEffects {
public:
    bool Empty() const {
        return DeferredEffects.empty();
    }

    std::optional<TKqpTransactionInfo::EEngine> GetEngine() const {
        return Engine;
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
    bool Add(const NYql::NNodes::TExprBase& node) {
        if (Engine.has_value() && *Engine != TKqpTransactionInfo::EEngine::OldEngine) {
            return false;
        }
        Engine.emplace(TKqpTransactionInfo::EEngine::OldEngine);
        DeferredEffects.emplace_back(node);
        return true;
    }

    [[nodiscard]]
    bool Add(std::shared_ptr<const NKqpProto::TKqpPhyTx>&& physicalTx, TParamValueMap&& params) {
        if (Engine.has_value() && *Engine != TKqpTransactionInfo::EEngine::NewEngine) {
            return false;
        }
        Engine.emplace(TKqpTransactionInfo::EEngine::NewEngine);
        DeferredEffects.emplace_back(std::move(physicalTx));
        DeferredEffects.back().Params = std::move(params);
        return true;
    }

    void Clear() {
        DeferredEffects.clear();
        Engine.reset();
    }

private:
    TVector<TDeferredEffect> DeferredEffects;
    std::optional<TKqpTransactionInfo::EEngine> Engine;

    friend class TKqpTransactionContext;
};

class TKqpTransactionContext : public NYql::TKikimrTransactionContextBase, public NYql::TTimeAndRandomProvider {
public:
    explicit TKqpTransactionContext(bool implicit)
        : Implicit(implicit)
        , ParamsState(MakeIntrusive<TParamsState>())
    {
        CreationTime = TInstant::Now();
        Touch();
    }

    TString NewParamName() {
        return TStringBuilder() << ParamNamePrefix << (++ParamsState->LastIndex);
    }

    void ClearDeferredEffects() {
        DeferredEffects.Clear();
    }

    [[nodiscard]]
    bool AddDeferredEffect(const NYql::NNodes::TExprBase& node) {
        return DeferredEffects.Add(node);
    }

    [[nodiscard]]
    bool AddDeferredEffect(std::shared_ptr<const NKqpProto::TKqpPhyTx> physicalTx, TParamValueMap&& params) {
        return DeferredEffects.Add(std::move(physicalTx), std::move(params));
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
        TTimeAndRandomProvider::Reset();

        DeferredEffects.Clear();
        ParamsState = MakeIntrusive<TParamsState>();
        SnapshotHandle.Snapshot = IKqpGateway::TKqpSnapshot::InvalidSnapshot;
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

public:
    struct TParamsState : public TThrRefBase {
        TParamValueMap Values;
        ui32 LastIndex = 0;
    };

    const bool Implicit;

    TInstant CreationTime;
    TInstant LastAccessTime;
    TInstant FinishTime;

    TInstant BeginQueryTime;
    TDuration QueriesDuration;
    ui32 QueriesCount = 0;

    TKqpTxLocks Locks;

    TDeferredEffects DeferredEffects;
    NTopic::TTopicOperations TopicOperations;
    TIntrusivePtr<TParamsState> ParamsState;

    IKqpGateway::TKqpSnapshotHandle SnapshotHandle;
};

class TLogExprTransformer {
public:
    TLogExprTransformer(const TString& description, NYql::NLog::EComponent component, NYql::NLog::ELevel level)
        : Description(description)
        , Component(component)
        , Level(level) {}

    NYql::IGraphTransformer::TStatus operator()(const NYql::TExprNode::TPtr& input, NYql::TExprNode::TPtr& output,
        NYql::TExprContext& ctx);

    static TAutoPtr<NYql::IGraphTransformer> Sync(const TString& description,
        NYql::NLog::EComponent component = NYql::NLog::EComponent::ProviderKqp,
        NYql::NLog::ELevel level = NYql::NLog::ELevel::INFO);

    static void LogExpr(const NYql::TExprNode& input, NYql::TExprContext& ctx, const TString& description,
        NYql::NLog::EComponent component = NYql::NLog::EComponent::ProviderKqp,
        NYql::NLog::ELevel level = NYql::NLog::ELevel::INFO);

private:
    TString Description;
    NYql::NLog::EComponent Component;
    NYql::NLog::ELevel Level;
};

TMaybe<NYql::NDq::TMkqlValueRef> GetParamValue(bool ensure, NYql::TTimeAndRandomProvider& randomCtx, NYql::TKikimrParamsMap& parameters,
    const TVector<TVector<NKikimrMiniKQL::TResult>>& txResults, const NKqpProto::TKqpPhyParamBinding& paramBinding);

} // namespace NKqp
} // namespace NKikimr
