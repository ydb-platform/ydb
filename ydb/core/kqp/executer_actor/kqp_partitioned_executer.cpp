#include "kqp_partitioned_executer.h"
#include "kqp_executer.h"

#include <ydb/core/kqp/common/events/events.h>
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
        const TActorId sessionActorId,
        const TString& database,
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
        YQL_ENSURE(PreparedQuery->GetTransactions().size() == 2);

        ResponseEv = std::make_unique<TEvKqpExecuter::TEvTxResponse>(PhysicalRequest.TxAlloc,
            TEvKqpExecuter::TEvTxResponse::EExecutionType::Data);

        for (const auto& tx : PreparedQuery->GetTransactions()) {
            for (const auto& stage : tx->GetStages()) {
                for (const auto& sink : stage.GetSinks()) {
                    FillTableMetaInfo(sink);

                    if (!KeyColumnTypes.empty()) {
                        break;
                    }
                }
            }
        }

        PE_LOG_I("Create " << ActorName << " with KeyColumnTypes.size() = " << KeyColumnTypes.size());
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
        PE_LOG_I("Got TEvTxProxySchemeCache::TEvResolveKeySetResult from ActorId = " << ev->Sender);

        if (request->ErrorCount > 0) {
            RuntimeError(
                Ydb::StatusIds::INTERNAL_ERROR,
                NYql::TIssues({NYql::TIssue("RuntimeError: PrepareState, failed to get table")}));
        }

        YQL_ENSURE(request->ResultSet.size() == 1);
        Partitioning = std::move(request->ResultSet[0].KeyDescription->Partitioning);

        CreateExecuters();
    }

    void HandleAbort(TEvKqp::TEvAbortExecution::TPtr& ev) {
        auto& msg = ev->Get()->Record;
        auto issues = ev->Get()->GetIssues();

        PE_LOG_I("Got TEvKqp::EvAbortExecution from ActorId = " << ev->Sender
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
            CreateExecuterWithBuffer(i, /* isRetry */ false);
        }

        Become(&TKqpPartitionedExecuter::ExecuteState);
    }

    void CreateExecuterWithBuffer(size_t partitionIdx, bool isRetry) {
        IKqpGateway::TExecPhysicalRequest newRequest(PhysicalRequest.TxAlloc);
        FillPhysicalRequest(newRequest, partitionIdx);

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

        PE_LOG_I("Create new KQP executer from Partitioned: ExId = " << exId << ", isRetry = "
            << isRetry << ", PartitionIdx = " << partitionIdx);

        auto ev = std::make_unique<TEvTxUserProxy::TEvProposeKqpTransaction>(exId);
        Send(MakeTxProxyID(), ev.release());
    }

    void Abort() {
        SendAbortToActors();
        Become(&TKqpPartitionedExecuter::AbortState);
    }

    void SendAbortToActors() {
        PE_LOG_I("Send abort to executers");

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

        PE_LOG_I("Got TEvKqpExecuter::TEvTxResponse from ActorId = " << ev->Sender << ", Status = " << response->GetStatus());

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

    void OnSuccessResponse(size_t exIdx) {
        if (exIdx == Executers.size()) {
            return;
        }

        PE_LOG_I("Got success response from ExId = " << Executers[exIdx]);

        ExecutersResponses[exIdx] = EExecuterResponse::SUCCESS;
        if (!CheckExecutersAreSuccess()) {
            return;
        }

        for (size_t i = 0; i < BufferActors.size(); ++i) {
            Send(BufferActors[i], new TEvKqpBuffer::TEvTerminate{});
        }

        PE_LOG_I("All executers are success. Send success to SessionActor");

        auto& response = *ResponseEv->Record.MutableResponse();
        response.SetStatus(Ydb::StatusIds::SUCCESS);

        Send(SessionActorId, ResponseEv.release());
        PassAway();
    }

    void HandleExecute(TEvKqpBuffer::TEvError::TPtr& ev) {
        const auto& msg = *ev->Get();
        PE_LOG_I("Got TEvError from ActorId = " << ev->Sender << ", status = "
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
        PE_LOG_I("Got error for PartitionIdx = " << partitionIdx << ", retry execution");

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
                PE_LOG_I("Got unknown message from ActorId = " << ev->Sender);
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

        PE_LOG_I("Got TEvKqpExecuter::TEvTxResponse from ActorId = " << ev->Sender << ", status = " << response->GetStatus());

        if (idx == Executers.size()) {
            return;
        }

        ExecutersResponses[idx] = EExecuterResponse::ERROR;

        if (CheckExecutersAreFailed()) {
            PE_LOG_I("All executers are aborted. Abort partitioned executer.");
            RuntimeError(
                Ydb::StatusIds::ABORTED,
                NYql::TIssues({NYql::TIssue("RuntimeError: Aborted.")}));
        }
    }

    TString LogPrefix() const {
        TStringBuilder result = TStringBuilder()
            << "(PARTITIONED) ActorId: " << SelfId() << ", "
            << "ActorState: " << CurrentStateFuncName() << ", ";
        return result;
    }

private:
    TString CurrentStateFuncName() const {
        const auto& func = CurrentStateFunc();
        if (func == &TThis::PrepareState) {
            return "PrepareState";
        } else if (func == &TThis::ExecuteState) {
            return "ExecuteState";
        } else if (func == &TThis::AbortState) {
            return "AbortState";
        } else {
            return "unknown state";
        }
    }

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

    void FillPhysicalRequest(IKqpGateway::TExecPhysicalRequest& physicalRequest, size_t partitionIdx) {
        IKqpGateway::TExecPhysicalRequest newLiteralRequest(LiteralRequest.TxAlloc);
        FillRequestWithParams(newLiteralRequest, partitionIdx, /* literal */ true);

        auto ev = ExecuteLiteral(std::move(newLiteralRequest), RequestCounters, SelfId(), UserRequestContext);
        auto* response = ev->Record.MutableResponse();

        if (response->GetStatus() != Ydb::StatusIds::SUCCESS) {
            RuntimeError(
                Ydb::StatusIds::BAD_REQUEST,
                NYql::TIssues({NYql::TIssue("RuntimeError: Error status from literal.")}));
        }

        FillRequestWithParams(physicalRequest, partitionIdx, /* literal */ false);

        auto queryData = physicalRequest.Transactions.front().Params;
        queryData->ClearPrunedParams();

        if (!ev->GetTxResults().empty()) {
            queryData->AddTxResults(0, std::move(ev->GetTxResults()));
        }
        queryData->AddTxHolders(std::move(ev->GetTxHolders()));

        PrepareParameters(physicalRequest);
    }

    void FillRequestWithParams(IKqpGateway::TExecPhysicalRequest& newRequest, size_t partitionIdx, bool literal)
    {
        YQL_ENSURE(Partitioning);
        FillNewRequest(newRequest, literal);

        auto& queryData = newRequest.Transactions.front().Params;
        YQL_ENSURE(FillParamValue(queryData, NBatchParams::IsFirstQuery, partitionIdx == 0));

        auto partition = Partitioning->at(partitionIdx).Range;
        if (!partition) {
            YQL_ENSURE(false);
        }

        auto cells = partition->EndKeyPrefix.GetCells();

        YQL_ENSURE(FillParamValue(queryData, NBatchParams::IsLastQuery, cells.empty()));
        YQL_ENSURE(FillParamValue(queryData, NBatchParams::IsInclusiveRight, partition->IsInclusive));

        for (size_t i = 0; i < KeyColumnTypes.size(); ++i) {
            auto endParam = NBatchParams::End + ToString(i + 1);

            if (i >= cells.size()) {
                YQL_ENSURE(FillParamValue(queryData, endParam, false, /* setDefault */ true));
                continue;
            }

            auto cellValue = NMiniKQL::GetCellValue(cells[i], KeyColumnTypes[i]);
            YQL_ENSURE(FillParamValue(queryData, endParam, cellValue));
        }

        if (partitionIdx > 0) {
            auto prevPartition = Partitioning->at(partitionIdx - 1).Range;
            if (!prevPartition) {
                YQL_ENSURE(false);
            }

            YQL_ENSURE(FillParamValue(queryData, NBatchParams::IsInclusiveLeft, !prevPartition->IsInclusive));

            auto prevCells = prevPartition->EndKeyPrefix.GetCells();
            YQL_ENSURE(!prevCells.empty());

            for (size_t i = 0; i < KeyColumnTypes.size(); ++i) {
                auto beginParam = NBatchParams::Begin + ToString(i + 1);

                if (i >= prevCells.size()) {
                    YQL_ENSURE(FillParamValue(queryData, beginParam, false, /* setDefault */ true));
                    continue;
                }

                auto prevCellValue = NMiniKQL::GetCellValue(prevCells[i], KeyColumnTypes[i]);
                YQL_ENSURE(FillParamValue(queryData, beginParam, prevCellValue));
            }
        } else {
            YQL_ENSURE(FillParamValue(queryData, NBatchParams::IsInclusiveLeft, false));

            for (size_t i = 0; i < KeyColumnTypes.size(); ++i) {
                auto beginParam = NBatchParams::Begin + ToString(i + 1);
                YQL_ENSURE(FillParamValue(queryData, beginParam, false, /* setDefault */ true));
            }
        }

        if (literal) {
            PrepareParameters(newRequest);
        }
    }

    void FillNewRequest(IKqpGateway::TExecPhysicalRequest& newRequest, bool literal) {
        auto& from = (literal) ? LiteralRequest : PhysicalRequest;

        newRequest.AllowTrailingResults = from.AllowTrailingResults;
        newRequest.QueryType = from.QueryType;
        newRequest.PerRequestDataSizeLimit = from.PerRequestDataSizeLimit;
        newRequest.MaxShardCount = from.MaxShardCount;
        newRequest.DataShardLocks = from.DataShardLocks;
        newRequest.LocksOp = from.LocksOp;
        newRequest.AcquireLocksTxId = from.AcquireLocksTxId;
        newRequest.Timeout = from.Timeout;
        newRequest.CancelAfter = from.CancelAfter;
        newRequest.MaxComputeActors = from.MaxComputeActors;
        newRequest.MaxAffectedShards = from.MaxAffectedShards;
        newRequest.TotalReadSizeLimitBytes = from.TotalReadSizeLimitBytes;
        newRequest.MkqlMemoryLimit = from.MkqlMemoryLimit;
        newRequest.PerShardKeysSizeLimitBytes = from.PerShardKeysSizeLimitBytes;
        newRequest.StatsMode = from.StatsMode;
        newRequest.ProgressStatsPeriod = from.ProgressStatsPeriod;
        newRequest.Snapshot = from.Snapshot;
        newRequest.ResourceManager_ = from.ResourceManager_;
        newRequest.CaFactory_ = from.CaFactory_;
        newRequest.IsolationLevel = from.IsolationLevel;
        newRequest.RlPath = from.RlPath;
        newRequest.NeedTxId = from.NeedTxId;
        newRequest.UseImmediateEffects = from.UseImmediateEffects;
        newRequest.TraceId = NWilson::TTraceId();
        newRequest.UserTraceId = from.UserTraceId;
        newRequest.OutputChunkMaxSize = from.OutputChunkMaxSize;

        newRequest.Transactions.emplace_back(PreparedQuery->GetTransactions()[(literal) ? 0 : 1], std::make_shared<TQueryData>(from.TxAlloc));

        auto newParams = newRequest.Transactions.front().Params;
        auto oldParams = LiteralRequest.Transactions.front().Params;
        for (auto& [name, _] : oldParams->GetParams()) {
            if (!name.StartsWith(NBatchParams::Header)) {
                TTypedUnboxedValue& typedValue = oldParams->GetParameterUnboxedValue(name);
                newParams->AddUVParam(name, typedValue.first, typedValue.second);
            }
        }
    }

    template <typename T>
    bool FillParamValue(TQueryData::TPtr queryData, const TString& name, T value, bool setDefault = false) {
        for (const auto& paramDesc : PreparedQuery->GetParameters()) {
            if (paramDesc.GetName() != name) {
                continue;
            }

            NKikimrMiniKQL::TType protoType = paramDesc.GetType();
            NKikimr::NMiniKQL::TType* paramType = ImportTypeFromProto(protoType, PhysicalRequest.TxAlloc->TypeEnv);

            if (setDefault) {
                auto defaultValue = MakeDefaultValueByType(paramType);
                queryData->AddUVParam(name, paramType, defaultValue);
                return true;
            }

            queryData->AddUVParam(name, paramType, NUdf::TUnboxedValuePod(value));
            return true;
        }
        return false;
    }

    void PrepareParameters(IKqpGateway::TExecPhysicalRequest& request) {
        auto& queryData = request.Transactions.front().Params;

        try {
            for (const auto& paramDesc : PreparedQuery->GetParameters()) {
                queryData->ValidateParameter(paramDesc.GetName(), paramDesc.GetType(), request.TxAlloc->TypeEnv);
            }

            for(const auto& paramBinding: request.Transactions.front().Body->GetParamBindings()) {
                queryData->MaterializeParamValue(true, paramBinding);
            }
        } catch (const yexception& ex) {
            RuntimeError(
                Ydb::StatusIds::BAD_REQUEST,
                NYql::TIssues({NYql::TIssue("RuntimeError: Cannot prepare parameters for request.")}));
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

        PE_LOG_E(Ydb::StatusIds_StatusCode_Name(code) << ": " << issues.ToOneLineString());
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
