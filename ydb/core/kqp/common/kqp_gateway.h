#pragma once

#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/core/protos/tx_proxy.pb.h>

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/dq/common/dq_value.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>

#include <library/cpp/actors/core/actorid.h>

namespace NKikimr {
namespace NKqp {

struct TKqpParamsMap {
    TKqpParamsMap() {}

    TKqpParamsMap(std::shared_ptr<void> owner)
        : Owner(owner) {}

    std::shared_ptr<void> Owner;
    TMap<TString, NYql::NDq::TMkqlValueRef> Values;
};

class IKqpGateway : public NYql::IKikimrGateway {
public:
    struct TMkqlResult : public TGenericResult {
        TString CompiledProgram;
        NKikimrMiniKQL::TResult Result;
        NKikimrQueryStats::TTxStats TxStats;
    };

    struct TMkqlSettings {
        bool LlvmRuntime = false;
        bool CollectStats = false;
        TMaybe<ui64> PerShardKeysSizeLimitBytes;
        ui64 CancelAfterMs = 0;
        ui64 TimeoutMs = 0;
        NYql::TKikimrQueryPhaseLimits Limits;
    };

    struct TRunResponse {
        bool HasProxyError;
        ui32 ProxyStatus;
        TString ProxyStatusName;
        TString ProxyStatusDesc;

        bool HasExecutionEngineError;
        TString ExecutionEngineStatusName;
        TString ExecutionEngineStatusDesc;

        NYql::TIssues Issues;

        TString MiniKQLErrors;
        TString DataShardErrors;
        NKikimrTxUserProxy::TMiniKQLCompileResults MiniKQLCompileResults;

        NKikimrMiniKQL::TResult ExecutionEngineEvaluatedResponse;
        NKikimrQueryStats::TTxStats TxStats;
    };

    struct TPhysicalTxData : private TMoveOnly {
        const NKqpProto::TKqpPhyTx& Body;
        TKqpParamsMap Params;

        TPhysicalTxData(const NKqpProto::TKqpPhyTx& body, TKqpParamsMap&& params)
            : Body(body)
            , Params(std::move(params)) {}
    };

    struct TKqpSnapshot {
        ui64 Step;
        ui64 TxId;

        constexpr TKqpSnapshot()
            : Step(0)
            , TxId(0)
        {}

        TKqpSnapshot(ui64 step, ui64 txId)
            : Step(step)
            , TxId(txId)
        {}

        bool IsValid() const {
            return Step != 0 || TxId != 0;
        }

        bool operator ==(const TKqpSnapshot &snapshot) const {
            return snapshot.Step == Step && snapshot.TxId == TxId;
        }

        size_t GetHash() const noexcept {
            auto tuple = std::make_tuple(Step, TxId);
            return THash<decltype(tuple)>()(tuple);
        }

        static const TKqpSnapshot InvalidSnapshot;
    };

    struct TKqpSnapshotHandle : public IKqpGateway::TGenericResult {
        TKqpSnapshot Snapshot;
        NActors::TActorId ManagingActor;
        NKikimrIssues::TStatusIds::EStatusCode Status =  NKikimrIssues::TStatusIds::UNKNOWN;
    };

    struct TExecPhysicalRequest : private TMoveOnly {
        TVector<TPhysicalTxData> Transactions;
        TVector<NYql::NDq::TMkqlValueRef> Locks;
        bool ValidateLocks = false;
        bool EraseLocks = false;
        TMaybe<ui64> AcquireLocksTxId;
        TDuration Timeout;
        TMaybe<TDuration> CancelAfter;
        ui32 MaxComputeActors = 10'000;
        ui32 MaxAffectedShards = 0;
        ui64 TotalReadSizeLimitBytes = 0;
        ui64 MkqlMemoryLimit = 0; // old engine compatibility
        ui64 PerShardKeysSizeLimitBytes = 0;
        Ydb::Table::QueryStatsCollection::Mode StatsMode = Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE;
        bool DisableLlvmForUdfStages = false;
        bool LlvmEnabled = true;
        TKqpSnapshot Snapshot = TKqpSnapshot();
        NKikimrKqp::EIsolationLevel IsolationLevel = NKikimrKqp::ISOLATION_LEVEL_UNDEFINED;
        TMaybe<NKikimrKqp::TRlPath> RlPath;
        bool NeedTxId = true;
    };

    struct TExecPhysicalResult : public TGenericResult {
        NKikimrKqp::TExecuterTxResult ExecuterResult;
    };

    struct TAstQuerySettings {
        Ydb::Table::QueryStatsCollection::Mode CollectStats = Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE;
    };

public:
    /* Mkql */
    virtual NThreading::TFuture<TMkqlResult> ExecuteMkql(const TString& cluster, const TString& program,
        TKqpParamsMap&& params, const TMkqlSettings& settings, const TKqpSnapshot& snapshot) = 0;

    virtual NThreading::TFuture<TMkqlResult> ExecuteMkqlPrepared(const TString& cluster, const TString& program,
        TKqpParamsMap&& params, const TMkqlSettings& settings, const TKqpSnapshot& snapshot) = 0;

    virtual NThreading::TFuture<TMkqlResult> PrepareMkql(const TString& cluster, const TString& program) = 0;

    /* Snapshots */
    virtual NThreading::TFuture<TKqpSnapshotHandle> CreatePersistentSnapshot(const TVector<TString>& tablePaths,
        TDuration queryTimeout) = 0;

    virtual void DiscardPersistentSnapshot(const TKqpSnapshotHandle& handle) = 0;

    virtual NThreading::TFuture<TKqpSnapshotHandle> AcquireMvccSnapshot(TDuration queryTimeout) = 0;

    /* Physical */
    virtual NThreading::TFuture<TExecPhysicalResult> ExecutePhysical(TExecPhysicalRequest&& request,
        const NActors::TActorId& target) = 0;

    virtual NThreading::TFuture<TExecPhysicalResult> ExecuteScanQuery(TExecPhysicalRequest&& request,
        const NActors::TActorId& target) = 0;

    virtual NThreading::TFuture<TExecPhysicalResult> ExecutePure(TExecPhysicalRequest&& request,
        const NActors::TActorId& target) = 0;

    /* Scripting */
    virtual NThreading::TFuture<TQueryResult> ExplainDataQueryAst(const TString& cluster, const TString& query) = 0;

    virtual NThreading::TFuture<TQueryResult> ExecDataQueryAst(const TString& cluster, const TString& query,
        TKqpParamsMap&& params, const TAstQuerySettings& settings,
        const Ydb::Table::TransactionSettings& txSettings) = 0;

    virtual NThreading::TFuture<TQueryResult> ExplainScanQueryAst(const TString& cluster, const TString& query) = 0;

    virtual NThreading::TFuture<TQueryResult> ExecScanQueryAst(const TString& cluster, const TString& query,
        TKqpParamsMap&& params, const TAstQuerySettings& settings, ui64 rowsLimit) = 0;

    virtual NThreading::TFuture<TQueryResult> StreamExecDataQueryAst(const TString& cluster, const TString& query,
        TKqpParamsMap&& params, const TAstQuerySettings& settings,
        const Ydb::Table::TransactionSettings& txSettings, const NActors::TActorId& target) = 0;

    virtual NThreading::TFuture<TQueryResult> StreamExecScanQueryAst(const TString& cluster, const TString& query,
        TKqpParamsMap&& params, const TAstQuerySettings& settings, const NActors::TActorId& target) = 0;

public:
    virtual TInstant GetCurrentTime() const = 0;
};

} // namespace NKqp
} // namespace NKikimr

template<>
struct THash<NKikimr::NKqp::IKqpGateway::TKqpSnapshot> {
    inline size_t operator()(const NKikimr::NKqp::IKqpGateway::TKqpSnapshot& snapshot) const {
        return snapshot.GetHash();
    }
};
