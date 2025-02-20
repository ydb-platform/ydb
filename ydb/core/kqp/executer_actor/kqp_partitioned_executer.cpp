#include "kqp_partitioned_executer.h"
#include "kqp_executer_impl.h"

#include <ydb/core/kqp/common/batch/params.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/kqp/common/buffer/buffer.h>
#include <ydb/core/kqp/common/buffer/events.h>
#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_log.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr {
namespace NKqp {

namespace {

template <typename T>
bool FillParamValue(TQueryData::TPtr queryData, const TString& name, T value) {
    auto type = queryData->GetParameterType(name);
    return queryData->AddUVParam(name, type, NUdf::TUnboxedValuePod(value));
}

class TKqpPartitionedExecuter : public TActorBootstrapped<TKqpPartitionedExecuter> {
    enum class EExecuterResponse {
        NONE,
        SUCCESS,
        ERROR
    };

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_EXECUTER_ACTOR;
    }

    TKqpPartitionedExecuter(
        IKqpGateway::TExecPhysicalRequest&& literalRequest,
        IKqpGateway::TExecPhysicalRequest&& physicalRequest,
        const TActorId sessionActorId, const TString& database,
        const TIntrusiveConstPtr<NACLib::TUserToken>& userToken,
        const TIntrusivePtr<TKqpCounters>& counters,
        TKqpRequestCounters::TPtr requestCounters,
        const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
        NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
        TPreparedQueryHolder::TConstPtr preparedQuery,
        const TIntrusivePtr<TUserRequestContext>& userRequestContext,
        ui32 statementResultIndex, const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup,
        const TGUCSettings::TPtr& GUCSettings,
        const TShardIdToTableInfoPtr& shardIdToTableInfo)
        : LiteralRequest(std::move(literalRequest))
        , PhysicalRequest(std::move(physicalRequest))
        , SessionActorId(sessionActorId)
        , Database(database)
        , UserToken(userToken)
        , Counters(counters)
        , RequestCounters(requestCounters)
        , TableServiceConfig(tableServiceConfig)
        , UserRequestContext(userRequestContext)
        , StatementResultIndex(statementResultIndex)
        , AsyncIoFactory(std::move(asyncIoFactory))
        , PreparedQuery(preparedQuery)
        , FederatedQuerySetup(federatedQuerySetup)
        , GUCSettings(GUCSettings)
        , ShardIdToTableInfo(shardIdToTableInfo)
    {
        YQL_ENSURE(PhysicalRequest.Transactions.size() == 1);

        ResponseEv = std::make_unique<TEvKqpExecuter::TEvTxResponse>(LiteralRequest.TxAlloc,
            TEvKqpExecuter::TEvTxResponse::EExecutionType::Data);

        for (const auto& tx : LiteralRequest.Transactions) {
            YQL_ENSURE(tx.Body->StagesSize() > 0);

            for (const auto& stage : tx.Body->GetStages()) {
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
        Send(MakeSchemeCacheID(), resolveReq.Release());

        Become(&TKqpPartitionedExecuter::PrepareState);
    }

    static constexpr char ActorName[] = "KQP_PARTITIONED_EXECUTER";

    STFUNC(PrepareState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
                hFunc(TEvKqp::TEvAbortExecution, HandleAbort);
            default:
                AFL_ENSURE(false)("unknown message", ev->GetTypeRewrite());
            }
        } catch (...) {
            RuntimeError(
                Ydb::StatusIds::INTERNAL_ERROR,
                NYql::TIssues({NYql::TIssue("RuntimeError: PrepareState.")}));
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

        CreateExecuters();
    }

    void HandleAbort(TEvKqp::TEvAbortExecution::TPtr& ev) {
        auto& msg = ev->Get()->Record;
        auto issues = ev->Get()->GetIssues();

        LOG_D("Got EvAbortExecution from ActorId = " << ev->Sender
            << " , abort child executers, status: "
            << NYql::NDqProto::StatusIds_StatusCode_Name(msg.GetStatusCode())
            << ", message: " << issues.ToOneLineString());

        if (auto idx = GetExecuterIdx(ev->Sender); idx != Executers.size()) {
            ExecutersResponses[idx] = EExecuterResponse::ERROR;
        }

        Abort();
    }

    void CreateExecuters() {
        Executers.resize(Partitioning->size());
        BufferActors.resize(Partitioning->size());
        ExecutersResponses.resize(Partitioning->size(), EExecuterResponse::NONE);

        for (size_t i = 0; i < Partitioning->size(); ++i) {
            CreateExecuterWithBuffer(i, /*isRetry*/ false);
        }

        Become(&TKqpPartitionedExecuter::ExecuteState);
    }

    void CreateExecuterWithBuffer(size_t partitionIdx, bool isRetry) {
        IKqpGateway::TExecPhysicalRequest newRequest(PhysicalRequest.TxAlloc);
        FillRequestWithParams(newRequest, partitionIdx);

        auto txManager = CreateKqpTransactionManager();

        TKqpBufferWriterSettings settings {
            .SessionActorId = SelfId(),
            .TxManager = txManager,
            .TraceId = PhysicalRequest.TraceId.GetTraceId(),
            .Counters = Counters,
            .TxProxyMon = RequestCounters->TxProxyMon,
        };
        auto* bufferActor = CreateKqpBufferWriterActor(std::move(settings));
        auto bufferActorId = RegisterWithSameMailbox(bufferActor);
        BufferActors[partitionIdx] = bufferActorId;

        auto executerActor = CreateKqpExecuter(std::move(newRequest), Database, UserToken, RequestCounters,
            TableServiceConfig, AsyncIoFactory, PreparedQuery, SelfId(), UserRequestContext, StatementResultIndex,
            FederatedQuerySetup, GUCSettings, ShardIdToTableInfo, txManager, bufferActorId);

        auto exId = RegisterWithSameMailbox(executerActor);
        Executers[partitionIdx] = exId;

        LOG_D("Created new KQP executer from Partitioned: " << exId << ", isRetry = "
            << isRetry << ", partitionIdx = " << partitionIdx);

        auto ev = std::make_unique<TEvTxUserProxy::TEvProposeKqpTransaction>(exId);
        Send(MakeTxProxyID(), ev.release());
    }

    void Abort() {
        SendAbortToActors();
        Become(&TKqpPartitionedExecuter::AbortState);
    }

    void SendAbortToActors() {
        for (size_t i = 0; i < Executers.size(); ++i) {
            if (ExecutersResponses[i] == EExecuterResponse::SUCCESS) {
                ExecutersResponses[i] = EExecuterResponse::NONE;
            }

            if (ExecutersResponses[i] != EExecuterResponse::ERROR) {
                auto abortEv = TEvKqp::TEvAbortExecution::Aborted("Aborted by Partitioned Executer");
                Send(Executers[i], abortEv.Release());
            }

            Send(BufferActors[i], new TEvKqpBuffer::TEvTerminate{});
        }
    }

    STFUNC(ExecuteState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpExecuter::TEvTxResponse, HandleExecute);
                hFunc(TEvKqp::TEvAbortExecution, HandleAbort);
                hFunc(TEvKqpBuffer::TEvError, HandleExecute);
            default:
                AFL_ENSURE(false)("unknown message", ev->GetTypeRewrite());
            }
        } catch (...) {
            RuntimeError(
                Ydb::StatusIds::INTERNAL_ERROR,
                NYql::TIssues({NYql::TIssue("RuntimeError: ExecuteState.")}));
        }
        return;
    }

    void HandleExecute(TEvKqpExecuter::TEvTxResponse::TPtr& ev) {
        auto* response = ev->Get()->Record.MutableResponse();
        switch (response->GetStatus()) {
            case Ydb::StatusIds::SUCCESS:
                OnSuccessResponse(GetExecuterIdx(ev->Sender));
                break;
            case Ydb::StatusIds::STATUS_CODE_UNSPECIFIED:
            case Ydb::StatusIds::ABORTED:
            case Ydb::StatusIds::UNAVAILABLE:
            case Ydb::StatusIds::OVERLOADED:
                RetryPartExecution(GetExecuterIdx(ev->Sender), /* fromBuffer */ false);
                break;
            default:
                RuntimeError(
                    Ydb::StatusIds::INTERNAL_ERROR,
                    NYql::TIssues({NYql::TIssue("RuntimeError: TEvTxResponse handle execute.")}));
        }
    }

    void OnSuccessResponse(size_t exId) {
        if (exId == Executers.size()) {
            return;
        }

        ExecutersResponses[exId] = EExecuterResponse::SUCCESS;
        if (!CheckExecutersAreSuccess()) {
            return;
        }

        for (size_t i = 0; i < BufferActors.size(); ++i) {
            Send(BufferActors[i], new TEvKqpBuffer::TEvTerminate{});
        }

        auto& response = *ResponseEv->Record.MutableResponse();
        response.SetStatus(Ydb::StatusIds::SUCCESS);

        Send(SessionActorId, ResponseEv.release());
        PassAway();
    }

    void HandleExecute(TEvKqpBuffer::TEvError::TPtr& ev) {
        const auto& msg = *ev->Get();
        LOG_D("Got TEvError from ActorId = " << ev->Sender << ", status = "
            << NYql::NDqProto::StatusIds_StatusCode_Name(msg.StatusCode));

        switch (msg.StatusCode) {
            case NYql::NDqProto::StatusIds::SUCCESS:
                break;
            case NYql::NDqProto::StatusIds::UNSPECIFIED:
            case NYql::NDqProto::StatusIds::ABORTED:
            case NYql::NDqProto::StatusIds::UNAVAILABLE:
            case NYql::NDqProto::StatusIds::OVERLOADED:
                RetryPartExecution(GetBufferIdx(ev->Sender), /* fromBuffer */ true);
                break;
            default:
                RuntimeError(
                    Ydb::StatusIds::INTERNAL_ERROR,
                    NYql::TIssues({NYql::TIssue("RuntimeError: TEvError handle execute.")}));
        }
    }

    void RetryPartExecution(size_t partitionIdx, bool fromBuffer) {
        if (partitionIdx == Executers.size()) {
            return;
        }

        if (fromBuffer) {
            auto abortEv = TEvKqp::TEvAbortExecution::Aborted("Aborted by Partitioned Executer");
            Send(Executers[partitionIdx], abortEv.Release());
        } else {
            Send(BufferActors[partitionIdx], new TEvKqpBuffer::TEvTerminate{});
        }

        ExecutersResponses[partitionIdx] = EExecuterResponse::NONE;
        CreateExecuterWithBuffer(partitionIdx, /*isRetry*/ true);
    }

    STFUNC(AbortState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpExecuter::TEvTxResponse, HandleAbort);
            default:
                LOG_D("Got unknown message from ActorId = " << ev->Sender << ", typeRewrite = " << ev->GetTypeRewrite());
            }
        } catch (...) {
            RuntimeError(
                Ydb::StatusIds::INTERNAL_ERROR,
                NYql::TIssues({NYql::TIssue("RuntimeError: AbortState.")}));
        }
    }

    void HandleAbort(TEvKqpExecuter::TEvTxResponse::TPtr& ev) {
        const auto& response = ev->Get()->Record.MutableResponse();
        auto idx = GetExecuterIdx(ev->Sender);

        LOG_D("Got EvTxResponse from ActorId = " << ev->Sender << ", status = " << response->GetStatus());

        if (idx == Executers.size()) {
            return;
        }

        ExecutersResponses[idx] = EExecuterResponse::ERROR;

        if (CheckExecutersAreFailed()) {
            LOG_D("All executers are aborted. Abort partitioned executer.");
            RuntimeError(
                Ydb::StatusIds::ABORTED,
                NYql::TIssues({NYql::TIssue("RuntimeError: Aborted.")}));
        }
    }

    const TIntrusivePtr<TUserRequestContext>& GetUserRequestContext() const {
        return UserRequestContext;
    }

private:
    size_t GetExecuterIdx(const TActorId exId) const {
        auto end = std::find(Executers.begin(), Executers.end(), exId);
        return std::distance(Executers.begin(), end);
    }

    size_t GetBufferIdx(const TActorId buferId) const {
        auto end = std::find(BufferActors.begin(), BufferActors.end(), buferId);
        return std::distance(BufferActors.begin(), end);
    }

    void FillTableMetaInfo(const NKqpProto::TKqpSink& sink) {
        NKikimrKqp::TKqpTableSinkSettings settings;
        YQL_ENSURE(sink.GetInternalSink().GetSettings().UnpackTo(&settings), "Failed to unpack settings");

        KeyColumnTypes.reserve(settings.GetKeyColumns().size());
        for (const auto& column : settings.GetKeyColumns()) {
            auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(column.GetTypeId(),
                column.HasTypeInfo() ? &column.GetTypeInfo() : nullptr);
            KeyColumnTypes.push_back(typeInfoMod.TypeInfo);
        }

        TableId = MakeTableId(settings.GetTable());
        TablePath = settings.GetTable().GetPath();
    }

    void FillRequestWithParams(IKqpGateway::TExecPhysicalRequest& newRequest, size_t partitionIdx) {
        YQL_ENSURE(Partitioning);
        FillNewRequest(newRequest);

        auto& queryData = newRequest.Transactions.front().Params;

        YQL_ENSURE(FillParamValue(queryData, NBatchParams::IsFirstQuery, partitionIdx == 0));

        auto partition = Partitioning->at(partitionIdx).Range;
        if (!partition) {
            YQL_ENSURE(false);
        }

        auto cells = partition->EndKeyPrefix.GetCells();

        YQL_ENSURE(FillParamValue(queryData, NBatchParams::IsLastQuery, cells.empty()));
        YQL_ENSURE(FillParamValue(queryData, NBatchParams::IsInclusiveRight, partition->IsInclusive));

        for (size_t i = 0; i < cells.size(); ++i) {
            auto endParam = NBatchParams::End + ToString(i + 1);
            auto cellValue = NMiniKQL::GetCellValue(cells[i], KeyColumnTypes[i]);
            YQL_ENSURE(FillParamValue(queryData, endParam, cellValue));
        }

        if (partitionIdx > 0) {
            auto prevPartition = Partitioning->at(partitionIdx - 1).Range;
            if (!prevPartition) {
                YQL_ENSURE(false);
            }

            YQL_ENSURE(FillParamValue(queryData, NBatchParams::IsInclusiveLeft, prevPartition->IsInclusive));

            auto prevCells = prevPartition->EndKeyPrefix.GetCells();
            YQL_ENSURE(!prevCells.empty());

            for (size_t i = 0; i < prevCells.size(); ++i) {
                auto beginParam = NBatchParams::Begin + ToString(i + 1);
                auto prevCellValue = NMiniKQL::GetCellValue(prevCells[i], KeyColumnTypes[i]);
                YQL_ENSURE(FillParamValue(queryData, beginParam, prevCellValue));
            }
        }
    }

    void FillNewRequest(IKqpGateway::TExecPhysicalRequest& newRequest) {
        newRequest.AllowTrailingResults = PhysicalRequest.AllowTrailingResults;
        newRequest.QueryType = PhysicalRequest.QueryType;
        newRequest.PerRequestDataSizeLimit = PhysicalRequest.PerRequestDataSizeLimit;
        newRequest.MaxShardCount = PhysicalRequest.MaxShardCount;
        newRequest.DataShardLocks = PhysicalRequest.DataShardLocks;
        newRequest.LocksOp = PhysicalRequest.LocksOp;
        newRequest.AcquireLocksTxId = PhysicalRequest.AcquireLocksTxId;
        newRequest.Timeout = PhysicalRequest.Timeout;
        newRequest.CancelAfter = PhysicalRequest.CancelAfter;
        newRequest.MaxComputeActors = PhysicalRequest.MaxComputeActors;
        newRequest.MaxAffectedShards = PhysicalRequest.MaxAffectedShards;
        newRequest.TotalReadSizeLimitBytes = PhysicalRequest.TotalReadSizeLimitBytes;
        newRequest.MkqlMemoryLimit = PhysicalRequest.MkqlMemoryLimit;
        newRequest.PerShardKeysSizeLimitBytes = PhysicalRequest.PerShardKeysSizeLimitBytes;
        newRequest.StatsMode = PhysicalRequest.StatsMode;
        newRequest.ProgressStatsPeriod = PhysicalRequest.ProgressStatsPeriod;
        newRequest.Snapshot = PhysicalRequest.Snapshot;
        newRequest.ResourceManager_ = PhysicalRequest.ResourceManager_;
        newRequest.CaFactory_ = PhysicalRequest.CaFactory_;
        newRequest.IsolationLevel = PhysicalRequest.IsolationLevel;
        newRequest.RlPath = PhysicalRequest.RlPath;
        newRequest.NeedTxId = PhysicalRequest.NeedTxId;
        newRequest.UseImmediateEffects = PhysicalRequest.UseImmediateEffects;
        // newRequest.Orbit = Request.Orbit;
        // newRequest.TraceId = Request.TraceId;
        newRequest.UserTraceId = PhysicalRequest.UserTraceId;
        newRequest.OutputChunkMaxSize = PhysicalRequest.OutputChunkMaxSize;

        newRequest.Transactions.emplace_back(PhysicalRequest.Transactions.front().Body, std::make_shared<TQueryData>(PhysicalRequest.TxAlloc));

        auto newParams = newRequest.Transactions.front().Params;
        auto oldParams = PhysicalRequest.Transactions.front().Params;
        for (auto& [name, _] : oldParams->GetParams()) {
            if (!name.StartsWith(NBatchParams::Header)) {
                TTypedUnboxedValue& typedValue = oldParams->GetParameterUnboxedValue(name);
                newParams->AddUVParam(name, typedValue.first, typedValue.second);
            }
        }
    }

    bool CheckExecutersAreSuccess() const {
        return std::all_of(ExecutersResponses.cbegin(), ExecutersResponses.cend(),
            [](const auto& resp) { return resp == EExecuterResponse::SUCCESS; });
    }

    bool CheckExecutersAreFailed() const {
        return std::all_of(ExecutersResponses.cbegin(), ExecutersResponses.cend(),
            [](const auto& resp) { return resp == EExecuterResponse::ERROR; });
    }

    void RuntimeError(Ydb::StatusIds::StatusCode code, const NYql::TIssues& issues) {
        if (this->CurrentStateFunc() != &TKqpPartitionedExecuter::AbortState) {
            Abort();
            return;
        }

        LOG_E(Ydb::StatusIds_StatusCode_Name(code) << ": " << issues.ToOneLineString());
        ReplyErrorAndDie(code, issues);
    }

    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues) {
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> protoIssues;
        IssuesToMessage(issues, &protoIssues);
        ReplyErrorAndDie(status, &protoIssues);
    }

    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status, const NYql::TIssue& issue) {
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> issues;
        IssueToMessage(issue, issues.Add());
        ReplyErrorAndDie(status, &issues);
    }

    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status,
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>* issues)
    {
        auto& response = *ResponseEv->Record.MutableResponse();

        response.SetStatus(status);
        response.MutableIssues()->Swap(issues);

        Send(SessionActorId, ResponseEv.release());
        PassAway();
    }

private:
    std::unique_ptr<TEvKqpExecuter::TEvTxResponse> ResponseEv;
    IKqpGateway::TExecPhysicalRequest LiteralRequest;
    IKqpGateway::TExecPhysicalRequest PhysicalRequest;
    const TActorId SessionActorId;
    TVector<TActorId> Executers;
    TVector<TActorId> BufferActors;
    TVector<EExecuterResponse> ExecutersResponses;
    TVector<NScheme::TTypeInfo> KeyColumnTypes;
    std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> Partitioning;
    TTableId TableId;
    TString TablePath;
    TString LogPrefix;
    ui64 TxId = 0;

    // Args for child executers and buffer write actors
    TString Database;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TIntrusivePtr<TKqpCounters> Counters;
    TKqpRequestCounters::TPtr RequestCounters;
    NKikimrConfig::TTableServiceConfig TableServiceConfig;
    TIntrusivePtr<TUserRequestContext> UserRequestContext;
    ui32 StatementResultIndex;
    NYql::NDq::IDqAsyncIoFactory::TPtr AsyncIoFactory;
    TPreparedQueryHolder::TConstPtr PreparedQuery;
    const std::optional<TKqpFederatedQuerySetup> FederatedQuerySetup;
    const TGUCSettings::TPtr GUCSettings;
    TShardIdToTableInfoPtr ShardIdToTableInfo;
};

} // namespace

NActors::IActor* CreateKqpPartitionedExecuter(
    NKikimr::NKqp::IKqpGateway::TExecPhysicalRequest&& literalRequest, NKikimr::NKqp::IKqpGateway::TExecPhysicalRequest&& physicalRequest,
    const TActorId sessionActorId, const TString& database, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken,
    const TIntrusivePtr<NKikimr::NKqp::TKqpCounters>& counters, NKikimr::NKqp::TKqpRequestCounters::TPtr requestCounters,
    const NKikimrConfig::TTableServiceConfig& tableServiceConfig, NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
    TPreparedQueryHolder::TConstPtr preparedQuery, const TIntrusivePtr<NKikimr::NKqp::TUserRequestContext>& userRequestContext,
    ui32 statementResultIndex, const std::optional<NKikimr::NKqp::TKqpFederatedQuerySetup>& federatedQuerySetup,
    const TGUCSettings::TPtr& GUCSettings, const NKikimr::NKqp::TShardIdToTableInfoPtr& shardIdToTableInfo)
{
    return new TKqpPartitionedExecuter(std::move(literalRequest), std::move(physicalRequest), sessionActorId, database, userToken,
        counters, requestCounters, tableServiceConfig, std::move(asyncIoFactory), std::move(preparedQuery), userRequestContext,
        statementResultIndex, federatedQuerySetup, GUCSettings, shardIdToTableInfo);
}

} // namespace NKqp
} // namespace NKikimr
