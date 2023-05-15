#pragma once

#include <ydb/core/kqp/query_data/kqp_query_data.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/core/protos/tx_proxy.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>

#include <ydb/core/kqp/topics/kqp_topics.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/tx/long_tx_service/public/lock_handle.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/dq/common/dq_value.h>

#include <library/cpp/actors/wilson/wilson_trace.h>
#include <library/cpp/actors/core/actorid.h>
#include <library/cpp/lwtrace/shuttle.h>
#include <library/cpp/protobuf/util/pb_io.h>

namespace NKikimr::NGRpcService {

class IRequestCtxMtSafe;

}

namespace NKikimr::NKqp {

const TStringBuf ParamNamePrefix = "%kqp%";

struct TKqpSettings {
    using TConstPtr = std::shared_ptr<const TKqpSettings>;

    TKqpSettings(const TVector<NKikimrKqp::TKqpSetting>& settings)
        : Settings(settings)
    {
        auto defaultSettingsData = NResource::Find("kqp_default_settings.txt");
        TStringInput defaultSettingsStream(defaultSettingsData);
        Y_VERIFY(TryParseFromTextFormat(defaultSettingsStream, DefaultSettings));
    }

    TKqpSettings()
        : Settings()
    {
        auto defaultSettingsData = NResource::Find("kqp_default_settings.txt");
        TStringInput defaultSettingsStream(defaultSettingsData);
        Y_VERIFY(TryParseFromTextFormat(defaultSettingsStream, DefaultSettings));
    }

    NKikimrKqp::TKqpDefaultSettings DefaultSettings;
    TVector<NKikimrKqp::TKqpSetting> Settings;
};

struct TModuleResolverState : public TThrRefBase {
    NYql::TExprContext ExprCtx;
    NYql::IModuleResolver::TPtr ModuleResolver;
    THolder<NYql::TExprContext::TFreezeGuard> FreezeGuardHolder;
};

void ApplyServiceConfig(NYql::TKikimrConfiguration& kqpConfig, const NKikimrConfig::TTableServiceConfig& serviceConfig);

class IKqpGateway : public NYql::IKikimrGateway {
public:
    struct TPhysicalTxData : private TMoveOnly {
        TKqpPhyTxHolder::TConstPtr Body;
        NKikimr::NKqp::TQueryData::TPtr Params;

        TPhysicalTxData(const TKqpPhyTxHolder::TConstPtr& body, const TQueryData::TPtr& params)
            : Body(body)
            , Params(params) {}
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
    public:

        TExecPhysicalRequest(NKikimr::NKqp::TTxAllocatorState::TPtr txAlloc)
            : TxAlloc(txAlloc)
        {}

        NKikimr::TControlWrapper PerRequestDataSizeLimit;
        NKikimr::TControlWrapper MaxShardCount;
        TVector<TPhysicalTxData> Transactions;
        TMap<ui64, TVector<NKikimrTxDataShard::TLock>> DataShardLocks;
        NKikimr::NKqp::TTxAllocatorState::TPtr TxAlloc;
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
        TKqpSnapshot Snapshot = TKqpSnapshot();
        NKikimrKqp::EIsolationLevel IsolationLevel = NKikimrKqp::ISOLATION_LEVEL_UNDEFINED;
        TMaybe<NKikimrKqp::TRlPath> RlPath;
        bool NeedTxId = true;
        bool UseImmediateEffects = false;

        NLWTrace::TOrbit Orbit;
        NWilson::TTraceId TraceId;

        NTopic::TTopicOperations TopicOperations;
    };

    struct TExecPhysicalResult : public TGenericResult {
        NKikimrKqp::TExecuterTxResult ExecuterResult;
        NLongTxService::TLockHandle LockHandle;
        TVector<NKikimrMiniKQL::TResult> Results;
    };

    struct TAstQuerySettings {
        Ydb::Table::QueryStatsCollection::Mode CollectStats = Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE;
    };

public:
    /* Compute */
    virtual NThreading::TFuture<TExecPhysicalResult> ExecuteLiteral(TExecPhysicalRequest&& request,
        TQueryData::TPtr params, ui32 txIndex) = 0;

    /* Scripting */
    virtual NThreading::TFuture<TQueryResult> ExplainDataQueryAst(const TString& cluster, const TString& query) = 0;

    virtual NThreading::TFuture<TQueryResult> ExecDataQueryAst(const TString& cluster, const TString& query,
        TQueryData::TPtr params, const TAstQuerySettings& settings,
        const Ydb::Table::TransactionSettings& txSettings) = 0;

    virtual NThreading::TFuture<TQueryResult> ExplainScanQueryAst(const TString& cluster, const TString& query) = 0;

    virtual NThreading::TFuture<TQueryResult> ExecScanQueryAst(const TString& cluster, const TString& query,
         TQueryData::TPtr params, const TAstQuerySettings& settings, ui64 rowsLimit) = 0;

    virtual NThreading::TFuture<TQueryResult> StreamExecDataQueryAst(const TString& cluster, const TString& query,
         TQueryData::TPtr, const TAstQuerySettings& settings,
        const Ydb::Table::TransactionSettings& txSettings, const NActors::TActorId& target) = 0;

    virtual NThreading::TFuture<TQueryResult> StreamExecScanQueryAst(const TString& cluster, const TString& query,
         TQueryData::TPtr, const TAstQuerySettings& settings, const NActors::TActorId& target,
         std::shared_ptr<NGRpcService::IRequestCtxMtSafe> rpcCtx) = 0;
};

TIntrusivePtr<IKqpGateway> CreateKikimrIcGateway(const TString& cluster, const TString& database,
    std::shared_ptr<IKqpGateway::IKqpTableMetadataLoader>&& metadataLoader, NActors::TActorSystem* actorSystem,
    ui32 nodeId, TKqpRequestCounters::TPtr counters);

} // namespace NKikimr::NKqp

template<>
struct THash<NKikimr::NKqp::IKqpGateway::TKqpSnapshot> {
    inline size_t operator()(const NKikimr::NKqp::IKqpGateway::TKqpSnapshot& snapshot) const {
        return snapshot.GetHash();
    }
};
