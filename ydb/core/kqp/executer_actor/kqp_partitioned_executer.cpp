#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/proxy_service/kqp_proxy_service.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/core/kqp/common/kqp_tx.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/kqp/common/buffer/buffer.h>
#include <ydb/core/tx/replication/ydb_proxy/partition_end_watcher_ut.cpp>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <yql/essentials/core/pg_settings/guc_settings.h>

namespace NKikimr {
namespace NKqp {

namespace {

class TKqpPartitionedExecuter : public TActorBootstrapped<TKqpPartitionedExecuter> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_EXECUTER_ACTOR;
    }

    TKqpPartitionedExecuter(
        IKqpGateway::TExecPhysicalRequest&& request,
        const TActorId sessionActorId, const TString& database,
        const TIntrusiveConstPtr<NACLib::TUserToken>& userToken,
        const TIntrusivePtr<TKqpCounters>& counters,
        TKqpRequestCounters::TPtr requestCounters, bool streamResult,
        const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
        NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
        const TIntrusivePtr<TUserRequestContext>& userRequestContext,
        ui32 statementResultIndex, const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup,
        const TGUCSettings::TPtr& GUCSettings,
        const TShardIdToTableInfoPtr& shardIdToTableInfo)
        : Request(std::move(request))
        , SessionActorId(sessionActorId)
        , Database(database)
        , UserToken(userToken)
        , Counters(counters)
        , RequestCounters(requestCounters)
        , StreamResult(streamResult)
        , TableServiceConfig(tableServiceConfig)
        , UserRequestContext(userRequestContext)
        , StatementResultIndex(statementResultIndex)
        , AsyncIoFactory(std::move(asyncIoFactory))lf
        , FederatedQuerySetup(federatedQuerySetup)
        , GUCSettings(GUCSettings)
        , ShardIdToTableInfo(shardIdToTableInfo)
    {
        YQL_ENSURE(Request.Transactions.size() == 1);

        for (const auto& tx : Request.Transactions) {
            YQL_ENSURE(tx.Body->StagesSize() > 0);

            for (const auto& stage : tx.Body->GetStages()) {
                if (stage.SinksSize() != 1) {
                    continue;
                }

                for (auto& sink : stage.GetSinks()) {
                    FillTableMetaInfo(sink);
                }
            }
        }
    }

    void Bootstrap() {
        YQL_ENSURE(!KeyColumnTypes.empty());

        const TVector<TCell> minKey(KeyColumnTypes.size());
        const TTableRange range(minKey, true, {}, false, false);
        YQL_ENSURE(range.IsFullRange(KeyColumnTypes.size()));
        auto keyRange = MakeHolder<TKeyDesc>(
            TableId,
            range,
            TKeyDesc::ERowOperation::Update,
            KeyColumnTypes,
            TVector<TKeyDesc::TColumnOp>{});

        TAutoPtr<NSchemeCache::TSchemeCacheRequest> request(new NSchemeCache::TSchemeCacheRequest());
        request->ResultSet.emplace_back(std::move(keyRange));

        TAutoPtr<TEvTxProxySchemeCache::TEvResolveKeySet> resolveReq(new TEvTxProxySchemeCache::TEvResolveKeySet(request));
        Send(MakeSchemeCacheID(), resolveReq.Release(), 0, 0);
    }

    STFUNC(PrepareState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
            default:
                AFL_ENSURE(false)("unknown message", ev->GetTypeRewrite());
            }
        } catch (...) {
            RuntimeError(
                NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                CurrentExceptionMessage());
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        auto* request = ev->Get()->Request.Get();

        if (request->ErrorCount > 0) {
            CA_LOG_E(TStringBuilder() << "Failed to get table: "
                << TableId << "'");
            return;
        }

        YQL_ENSURE(request->ResultSet.size() == 1);
        Partitioning = std::move(request->ResultSet[0].KeyDescription->Partitioning);

        Prepare();
    }

    void Prepare() {
        Executers.reserve(Partitioning->size());
        BufferActors.reserve(Partitioning->size());

        for (size_t i = 0; i < Partitioning->size(); ++i) {
            IKqpGateway::TExecPhysicalRequest newRequest = MakeRequestWithParams(i);

            auto txManager = CreateKqpTransactionManager();

            TKqpBufferWriterSettings settings {
                .SessionActorId = SessionActorId,
                .TxManager = txManager,
                .TraceId = Request.TraceId.GetTraceId(),
                .Counters = Counters,
                .TxProxyMon = RequestCounters->TxProxyMon,
            };
            auto* bufferActor = CreateKqpBufferWriterActor(std::move(settings));
            auto bufferActorId = RegisterWithSameMailbox(bufferActor);
            BufferActors.push_back(bufferActorId);

            auto executerActor = CreateKqpExecuter(std::move(newRequest), Database, UserToken, RequestCounters, StreamResult,
                TableServiceConfig, AsyncIoFactory, SelfId(), UserRequestContext, StatementResultIndex,
                FederatedQuerySetup, GUCSettings, ShardIdToTableInfo, txManager, bufferActorId);

            auto exId = RegisterWithSameMailbox(executerActor);
            Executers.push_back(exId);

            LOG_D("Created new KQP executer from Partitioned: " << exId);
            auto ev = std::make_unique<TEvTxUserProxy::TEvProposeKqpTransaction>(exId);
            Send(MakeTxProxyID(), ev.release());
        }
    }

    void FillTableMetaInfo(const NKqpProto::TKqpSink& sink) {
        NKikimrKqp::TKqpTableSinkSettings settings;
        YQL_ENSURE(sink.GetInternalSink().GetSettings().UnpackTo(&settings), "Failed to unpack settings");
        YQL_ENSURE(sink.GetOutputIndex() == 0);

        KeyColumnTypes.reserve(settings.GetKeyColumns().size());
        for (const auto& column : settings.GetKeyColumns()) {
            auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(column.GetTypeId(),
                column.HasTypeInfo() ? &column.GetTypeInfo() : nullptr);
            KeyColumnTypes.push_back(typeInfoMod.TypeInfo);
        }

        TableId = MakeTableId(settings.GetTable());
        TablePath = settings.GetTable().GetPath();
    }

    IKqpGateway::TExecPhysicalRequest&& MakeRequestWithParams(size_t partitionIdx) {
        YQL_ENSURE(Partitioning);

        IKqpGateway::TExecPhysicalRequest newRequest = MakeFilledRequest();
        auto& queryData = newRequest.Transactions.front().Params;

        // TODO

        return std::move(newRequest);
    }

    IKqpGateway::TExecPhysicalRequest&& MakeFilledRequest() {
        IKqpGateway::TExecPhysicalRequest newRequest(Request.TxAlloc);

        newRequest.AllowTrailingResults = Request.AllowTrailingResults;
        newRequest.QueryType = Request.QueryType;
        newRequest.PerRequestDataSizeLimit = Request.PerRequestDataSizeLimit;
        newRequest.MaxShardCount = Request.MaxShardCount;
        newRequest.Transactions = Request.Transactions;
        newRequest.DataShardLocks = Request.DataShardLocks;
        newRequest.LocksOp = Request.LocksOp;
        newRequest.AcquireLocksTxId = Request.AcquireLocksTxId;
        newRequest.Timeout = Request.Timeout;
        newRequest.CancelAfter = Request.CancelAfter;
        newRequest.MaxComputeActors = Request.MaxComputeActors;
        newRequest.MaxAffectedShards = Request.MaxAffectedShards;
        newRequest.TotalReadSizeLimitBytes = Request.TotalReadSizeLimitBytes;
        newRequest.MkqlMemoryLimit = Request.MkqlMemoryLimit;
        newRequest.PerShardKeysSizeLimitBytes = Request.PerShardKeysSizeLimitBytes;
        newRequest.StatsMode = Request.StatsMode;
        newRequest.ProgressStatsPeriod = Request.ProgressStatsPeriod;
        newRequest.Snapshot = Request.Snapshot;
        newRequest.ResourceManager_ = Request.ResourceManager_;
        newRequest.CaFactory_ = Request.CaFactory_;
        newRequest.IsolationLevel = Request.IsolationLevel;
        newRequest.RlPath = Request.RlPath;
        newRequest.NeedTxId = Request.NeedTxId;
        newRequest.UseImmediateEffects = Request.UseImmediateEffects;
        newRequest.Orbit = Request.Orbit;
        newRequest.TraceId = Request.TraceId;
        newRequest.UserTraceId = Request.UserTraceId;
        newRequest.OutputChunkMaxSize = Request.OutputChunkMaxSize;

        return std::move(newRequest);
    }

private:
    IKqpGateway::TExecPhysicalRequest Request;
    const TActorId SessionActorId;

    TString Database;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TIntrusivePtr<TKqpCounters> Counters;
    TKqpRequestCounters::TPtr RequestCounters;
    bool StreamResult;
    NKikimrConfig::TTableServiceConfig TableServiceConfig;
    TIntrusivePtr<TUserRequestContext> UserRequestContext;
    ui32 StatementResultIndex;
    NYql::NDq::IDqAsyncIoFactory::TPtr AsyncIoFactory;
    const std::optional<TKqpFederatedQuerySetup> FederatedQuerySetup;
    const TGUCSettings::TPtr GUCSettings;
    TShardIdToTableInfoPtr ShardIdToTableInfo;

    TTableId TableId;
    TTableId TablePath;
    TVector<NScheme::TTypeInfo> KeyColumnTypes;

    std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> Partitioning;
    TVector<const TActorId> Executers;
    TVector<const TActorId> BufferActors;
};

} // namespace

} // namespace NKqp
} // namespace NKikimr

NActors::IActor* CreateKqpPartitionedExecuter(
    IKqpGateway::TExecPhysicalRequest&& request, const TActorId sessionActorId, const TString& database,
    const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, const TIntrusivePtr<TKqpCounters>& counters,
    TKqpRequestCounters::TPtr requestCounters, bool streamResult, const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
    NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory, const TIntrusivePtr<TUserRequestContext>& userRequestContext,
    ui32 statementResultIndex, const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup, const TGUCSettings::TPtr& GUCSettings,
    const TShardIdToTableInfoPtr& shardIdToTableInfo
)
{
    return new TKqpPartitionedExecuter(std::move(request), sessionActorId, database, userToken, counters, requestCounters,
        streamResult, tableServiceConfig, std::move(asyncIoFactory), userRequestContext, statementResultIndex, federatedQuerySetup,
        GUCSettings, shardIdToTableInfo);
}
