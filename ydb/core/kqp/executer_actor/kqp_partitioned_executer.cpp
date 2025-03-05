#include "kqp_partitioned_executer.h"
#include "kqp_executer.h"

#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/batch/params.h>
#include <ydb/core/kqp/common/buffer/buffer.h>
#include <ydb/core/kqp/common/buffer/events.h>
#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_log.h>

#include <util/string/split.h>

namespace NKikimr {
namespace NKqp {

namespace {

class TKqpPartitionedExecuter : public TActorBootstrapped<TKqpPartitionedExecuter> {
    enum class EExecuterResponse {
        NONE,
        SUCCESS,
        ERROR
    };

    struct TBatchPartitionInfo {
        TMaybe<TKeyDesc::TPartitionRangeInfo> BeginRange;
        TMaybe<TKeyDesc::TPartitionRangeInfo> EndRange;
        size_t PartitionIdx = 0;
        bool IsFirstQuery = false;
        bool IsLastQuery = false;
        ui64 LimitSize = 0;
        TActorId ExecuterId;
        TActorId BufferId;
        EExecuterResponse Response = EExecuterResponse::NONE;

        using TPtr = std::shared_ptr<TBatchPartitionInfo>;
    };

    struct TKeyColumnInfo {
        ui32 Id;
        NScheme::TTypeInfo Type;
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

                    if (!KeyColumnInfo.empty()) {
                        break;
                    }
                }
            }
        }

        PE_LOG_I("Create " << ActorName << " with KeyColumnInfo.size() = " << KeyColumnInfo.size());
    }

    void Bootstrap() {
        YQL_ENSURE(!KeyColumnInfo.empty());

        const TVector<TCell> minKey(KeyColumnInfo.size());
        const TTableRange range(minKey, true, {}, false, false);

        YQL_ENSURE(range.IsFullRange(KeyColumnInfo.size()));

        TVector<NScheme::TTypeInfo> keyColumnTypes;
        for (const auto& [_, type] : KeyColumnInfo) {
            keyColumnTypes.push_back(type);
        }

        auto keyRange = MakeHolder<TKeyDesc>(TableId, range, TKeyDesc::ERowOperation::Update,
            keyColumnTypes, TVector<TKeyDesc::TColumnOp>{});

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
                NYql::TIssues({NYql::TIssue(CurrentStateFuncName())}));
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        auto* request = ev->Get()->Request.Get();

        PE_LOG_I("Got TEvTxProxySchemeCache::TEvResolveKeySetResult from ActorId = " << ev->Sender);

        if (request->ErrorCount > 0) {
            RuntimeError(
                Ydb::StatusIds::INTERNAL_ERROR,
                NYql::TIssues({NYql::TIssue(TStringBuilder() << CurrentStateFuncName()
                    << ", failed to get table")}));
        }

        YQL_ENSURE(request->ResultSet.size() == 1);

        auto partitioning = std::move(request->ResultSet[0].KeyDescription->Partitioning);
        Partitions.reserve(partitioning->size());

        for (size_t i = 0; i < partitioning->size(); ++i) {
            auto ptr = std::make_shared<TBatchPartitionInfo>();
            ptr->EndRange = partitioning->at(i).Range;
            ptr->PartitionIdx = i;
            ptr->IsFirstQuery = (i == 0);
            ptr->IsLastQuery = (i + 1 == partitioning->size());
            ptr->LimitSize = MaxLimitSize;

            if (i > 0) {
                ptr->BeginRange = partitioning->at(i - 1).Range;
                if (ptr->BeginRange) {
                    ptr->BeginRange->IsInclusive = !ptr->BeginRange->IsInclusive;
                }
            }

            Partitions.push_back(std::move(ptr));
        }

        CreateExecuters();
    }

    void HandleAbort(TEvKqp::TEvAbortExecution::TPtr& ev) {
        auto& msg = ev->Get()->Record;
        auto issues = ev->Get()->GetIssues();

        PE_LOG_I("Got TEvKqp::EvAbortExecution from ActorId = " << ev->Sender
            << " , abort child executers, status: "
            << NYql::NDqProto::StatusIds_StatusCode_Name(msg.GetStatusCode())
            << ", message: " << issues.ToOneLineString());

        if (auto it = ExecuterPartition.find(ev->Sender); it != ExecuterPartition.end()) {
            auto& [_, exInfo] = *it;
            exInfo->Response = EExecuterResponse::ERROR;
            Send(exInfo->BufferId, new TEvKqpBuffer::TEvTerminate{});
        }

        Abort();
    }

    void CreateExecuters() {
        for (size_t i = 0; i < Partitions.size(); ++i) {
            CreateExecuterWithBuffer(i, /* isRetry */ false);
        }

        Become(&TKqpPartitionedExecuter::ExecuteState);
    }

    void CreateExecuterWithBuffer(size_t partitionIdx, bool isRetry) {
        IKqpGateway::TExecPhysicalRequest newRequest(PhysicalRequest.TxAlloc);
        FillPhysicalRequest(newRequest, partitionIdx);

        auto& partInfo = Partitions[partitionIdx];

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

        auto executerActor = CreateKqpExecuter(std::move(newRequest), Database, UserToken, RequestCounters,
            TableServiceConfig, AsyncIoFactory, PreparedQuery, SelfId(), UserRequestContext, StatementResultIndex,
            FederatedQuerySetup, GUCSettings, ShardIdToTableInfo, txManager, bufferActorId, partInfo->LimitSize);
        auto exId = RegisterWithSameMailbox(executerActor);

        PE_LOG_I("Create new KQP executer from Partitioned: ExId = " << exId << ", isRetry = "
            << isRetry << ", PartitionIdx = " << partitionIdx);

        partInfo->Response = EExecuterResponse::NONE;
        partInfo->ExecuterId = exId;
        partInfo->BufferId = bufferActorId;
        ExecuterPartition[exId] = BufferPartition[bufferActorId] = partInfo;

        auto ev = std::make_unique<TEvTxUserProxy::TEvProposeKqpTransaction>(exId);
        Send(MakeTxProxyID(), ev.release());
    }

    void Abort() {
        SendAbortToActors();
        Become(&TKqpPartitionedExecuter::AbortState);

        if (CheckExecutersAreFailed()) {
            PE_LOG_I("All executers are aborted. Abort partitioned executer.");
            RuntimeError(
                Ydb::StatusIds::ABORTED,
                NYql::TIssues({NYql::TIssue("Aborted.")}));
        }
    }

    void SendAbortToActors() {
        PE_LOG_I("Send abort to executers");

        for (auto& [exId, partInfo] : ExecuterPartition) {
            if (partInfo->Response != EExecuterResponse::ERROR) {
                auto abortEv = TEvKqp::TEvAbortExecution::Aborted("Aborted by Partitioned Executer");
                Send(exId, abortEv.Release());
                Send(partInfo->BufferId, new TEvKqpBuffer::TEvTerminate{});
            }
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
                NYql::TIssues({NYql::TIssue(CurrentStateFuncName())}));
        }
        return;
    }

    void HandleExecute(TEvKqpExecuter::TEvTxResponse::TPtr& ev) {
        auto* response = ev->Get()->Record.MutableResponse();

        PE_LOG_I("Got TEvKqpExecuter::TEvTxResponse from ActorId = " << ev->Sender << ", Status = " << response->GetStatus());

        if (ExecuterPartition.find(ev->Sender) == ExecuterPartition.end()) {
            return;
        }

        auto partInfo = ExecuterPartition[ev->Sender];
        switch (response->GetStatus()) {
            case Ydb::StatusIds::SUCCESS:
                if (partInfo->LimitSize < MaxLimitSize) {
                    partInfo->LimitSize = MaxLimitSize;
                }

                OnSuccessResponse(ev->Sender, ev->Get());
                break;
            case Ydb::StatusIds::STATUS_CODE_UNSPECIFIED:
            case Ydb::StatusIds::ABORTED:
            case Ydb::StatusIds::UNAVAILABLE:
            case Ydb::StatusIds::OVERLOADED:
                if (partInfo->LimitSize <= MinLimitSize) {
                    partInfo->Response = EExecuterResponse::ERROR;
                    RuntimeError(
                    Ydb::StatusIds::INTERNAL_ERROR,
                    NYql::TIssues({NYql::TIssue(TStringBuilder() << CurrentStateFuncName()
                        << ", executer cannot be retried")}));
                }

                partInfo->LimitSize = partInfo->LimitSize / 2;
                RetryPartExecution(ev->Sender, /* fromBuffer */ false);
                break;
            default:
                RuntimeError(
                    Ydb::StatusIds::INTERNAL_ERROR,
                    NYql::TIssues({NYql::TIssue(TStringBuilder() << CurrentStateFuncName()
                        << ", error from TEvKqpExecuter::TEvTxResponse")}));
        }
    }

    void OnSuccessResponse(TActorId exId, TEvKqpExecuter::TEvTxResponse* ev) {
        if (ExecuterPartition.find(exId) == ExecuterPartition.end()) {
            return;
        }

        PE_LOG_I("Got success response from ExId = " << exId);

        auto partInfo = ExecuterPartition[exId];
        auto endRows = ev->BatchEndRows;
        auto endKeyIds = ev->BatchKeyIds;

        if (NeedReordering.Empty()) {
            NeedReordering = false;

            if (!endKeyIds.empty()) {
                YQL_ENSURE(KeyColumnInfo.size() == endKeyIds.size());

                for (size_t i = 0; i < KeyColumnInfo.size(); ++i) {
                    if (KeyColumnInfo[i].Id != endKeyIds[i]) {
                        NeedReordering = true;
                        break;
                    }
                }
            }

            if (NeedReordering.GetRef()) {
                for (auto& curPart : Partitions) {
                    auto& beginRow = curPart->BeginRange;
                    if (!beginRow.Empty()) {
                        beginRow->EndKeyPrefix = ReorderRowKeyColumns(beginRow->EndKeyPrefix, endKeyIds);
                    }

                    auto& endRow = curPart->EndRange;
                    if (!endRow.Empty()) {
                        endRow->EndKeyPrefix = ReorderRowKeyColumns(endRow->EndKeyPrefix, endKeyIds);
                    }
                }
                ReorderKeyColumnInfo(endKeyIds);
            }
        }

        TSerializedCellVec maxRow;
        if (!endRows.empty()) {
            maxRow = endRows.front();
            YQL_ENSURE(maxRow.GetCells().size() == KeyColumnInfo.size());
        }

        for (size_t i = 1; i < endRows.size(); ++i) {
            auto row = endRows[i];
            YQL_ENSURE(row.GetCells().size() == maxRow.GetCells().size());

            for (size_t j = 0; j < row.GetCells().size(); ++j) {
                NScheme::TTypeInfoOrder typeOrder(KeyColumnInfo[j].Type, NScheme::EOrder::Ascending);
                if (CompareTypedCells(maxRow.GetCells()[j], row.GetCells()[j], typeOrder) < 0) {
                    maxRow = row;
                    break;
                }
            }
        }

        if (!maxRow.GetCells().empty()) {
            partInfo->BeginRange = TKeyDesc::TPartitionRangeInfo(maxRow,
                /* IsInclusive */ false,
                /* IsPoint */ false
            );
            partInfo->IsFirstQuery = false;
            RetryPartExecution(exId, /* fromBuffer */ false);
            return;
        }

        ExecuterPartition[exId]->Response = EExecuterResponse::SUCCESS;
        if (!CheckExecutersAreSuccess()) {
            return;
        }

        for (auto& [_, partInfo] : ExecuterPartition) {
            Send(partInfo->BufferId, new TEvKqpBuffer::TEvTerminate{});
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
                RetryPartExecution(ev->Sender, /* fromBuffer */ true);
                break;
            default:
                RuntimeError(
                    Ydb::StatusIds::INTERNAL_ERROR,
                    NYql::TIssues({NYql::TIssue(TStringBuilder() << CurrentStateFuncName()
                        << ", error from TEvError")}));
        }
    }

    void RetryPartExecution(TActorId actorId, bool fromBuffer) {
        PE_LOG_I("Got retry error from ActorId = " << actorId << ", retry execution");

        auto it = (fromBuffer) ? BufferPartition.find(actorId) : ExecuterPartition.find(actorId);
        if (it == BufferPartition.end() || it == ExecuterPartition.end()) {
            return;
        }

        auto [_, partInfo] = *it;
        if (fromBuffer) {
            auto abortEv = TEvKqp::TEvAbortExecution::Aborted("Aborted by Partitioned Executer");
            Send(partInfo->ExecuterId, abortEv.Release());
        } else {
            Send(partInfo->BufferId, new TEvKqpBuffer::TEvTerminate{});
        }

        ExecuterPartition.erase(partInfo->ExecuterId);
        BufferPartition.erase(partInfo->BufferId);
        CreateExecuterWithBuffer(partInfo->PartitionIdx, /*isRetry*/ true);
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
                NYql::TIssues({NYql::TIssue(CurrentStateFuncName())}));
        }
    }

    void HandleAbort(TEvKqpExecuter::TEvTxResponse::TPtr& ev) {
        const auto& response = ev->Get()->Record.MutableResponse();

        PE_LOG_I("Got TEvKqpExecuter::TEvTxResponse from ActorId = " << ev->Sender << ", status = " << response->GetStatus());

        if (auto it = ExecuterPartition.find(ev->Sender); it != ExecuterPartition.end()) {
            auto& [_, partInfo] = *it;
            partInfo->Response = EExecuterResponse::ERROR;

            if (CheckExecutersAreFailed()) {
                PE_LOG_I("All executers are aborted. Abort partitioned executer.");
                RuntimeError(
                    Ydb::StatusIds::ABORTED,
                    NYql::TIssues({NYql::TIssue("Aborted.")}));
            }
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

    void FillTableMetaInfo(const NKqpProto::TKqpSink& sink) {
        NKikimrKqp::TKqpTableSinkSettings settings;
        YQL_ENSURE(sink.GetInternalSink().GetSettings().UnpackTo(&settings), "Failed to unpack settings");

        KeyColumnInfo.reserve(settings.GetKeyColumns().size());
        for (const auto& column : settings.GetKeyColumns()) {
            auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(column.GetTypeId(),
                column.HasTypeInfo() ? &column.GetTypeInfo() : nullptr);
            KeyColumnInfo.emplace_back(column.GetId(), typeInfoMod.TypeInfo);
        }

        TableId = MakeTableId(settings.GetTable());
        TablePath = settings.GetTable().GetPath();
    }

    void FillPhysicalRequest(IKqpGateway::TExecPhysicalRequest& physicalRequest, size_t partitionIdx) {
        IKqpGateway::TExecPhysicalRequest newLiteralRequest(LiteralRequest.TxAlloc);
        FillRequestWithParams(newLiteralRequest, partitionIdx, /* literal */ true);
        PrepareParameters(newLiteralRequest);

        auto ev = ExecuteLiteral(std::move(newLiteralRequest), RequestCounters, SelfId(), UserRequestContext);
        auto* response = ev->Record.MutableResponse();

        if (response->GetStatus() != Ydb::StatusIds::SUCCESS) {
            RuntimeError(
                Ydb::StatusIds::BAD_REQUEST,
                NYql::TIssues({NYql::TIssue(TStringBuilder() << CurrentStateFuncName()
                    << ", error status from literal.")}));
        }

        FillRequestWithParams(physicalRequest, partitionIdx, /* literal */ false);

        auto queryData = physicalRequest.Transactions.front().Params;
        queryData->ClearPrunedParams();

        DebugPrintRequest(queryData);

        if (!ev->GetTxResults().empty()) {
            queryData->AddTxResults(0, std::move(ev->GetTxResults()));
        }

        queryData->AddTxHolders(std::move(ev->GetTxHolders()));

        PrepareParameters(physicalRequest);
    }

    void FillRequestWithParams(IKqpGateway::TExecPhysicalRequest& newRequest, size_t partitionIdx, bool literal)
    {
        FillNewRequest(newRequest, literal);

        auto& queryData = newRequest.Transactions.front().Params;
        auto& partition = Partitions[partitionIdx];

        YQL_ENSURE(FillParamValue(queryData, NBatchParams::IsFirstQuery, partition->IsFirstQuery));
        YQL_ENSURE(FillParamValue(queryData, NBatchParams::IsLastQuery, partition->IsLastQuery));

        FillRequestRange(queryData, partition->BeginRange, /* isBegin */ true);
        FillRequestRange(queryData, partition->EndRange, /* isBegin */ false);
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

        newRequest.Transactions.emplace_back(PreparedQuery->GetTransactions()[static_cast<size_t>(!literal)], std::make_shared<TQueryData>(from.TxAlloc));

        auto newParams = newRequest.Transactions.front().Params;
        auto oldParams = LiteralRequest.Transactions.front().Params;
        for (auto& [name, _] : oldParams->GetParams()) {
            if (!name.StartsWith(NBatchParams::Header)) {
                TTypedUnboxedValue& typedValue = oldParams->GetParameterUnboxedValue(name);
                newParams->AddUVParam(name, typedValue.first, typedValue.second);
            }
        }
    }

    void FillRequestRange(TQueryData::TPtr queryData, const TMaybe<TKeyDesc::TPartitionRangeInfo>& range, bool isBegin) {
        YQL_ENSURE(FillParamValue(queryData,
            (isBegin) ? NBatchParams::IsInclusiveLeft : NBatchParams::IsInclusiveRight,
            (range) ? range->IsInclusive : false));

        for (size_t i = 0; i < KeyColumnInfo.size(); ++i) {
            auto paramName = ((isBegin) ? NBatchParams::Begin : NBatchParams::End) + ToString(i + 1);
            if (range && i < range->EndKeyPrefix.GetCells().size()) {
                auto cellValue = NMiniKQL::GetCellValue(range->EndKeyPrefix.GetCells()[i], KeyColumnInfo[i].Type);
                YQL_ENSURE(FillParamValue(queryData, paramName, cellValue));
            } else {
                YQL_ENSURE(FillParamValue(queryData, paramName, false, /* setDefault */ true));
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
                NYql::TIssues({NYql::TIssue(TStringBuilder() << CurrentStateFuncName()
                    << ", cannot prepare parameters for request.")}));
        }
    }

    bool CheckExecutersAreSuccess() const {
        return std::all_of(Partitions.cbegin(), Partitions.cend(),
            [](auto it) { return it->Response == EExecuterResponse::SUCCESS; });
    }

    bool CheckExecutersAreFailed() const {
        return std::all_of(Partitions.cbegin(), Partitions.cend(),
            [](auto it) { return it->Response == EExecuterResponse::ERROR; });
    }

    TSerializedCellVec ReorderRowKeyColumns(const TSerializedCellVec& row, const TVector<ui32>& keyIds) {
        if (row.GetCells().empty()) {
            return row;
        }

        TVector<TCell> newRow;
        for (auto keyId : keyIds) {
            for (size_t i = 0; i < KeyColumnInfo.size(); ++i) {
                const auto& [prevKeyId, _] = KeyColumnInfo[i];
                if (prevKeyId == keyId) {
                    newRow.push_back(row.GetCells()[i]);
                    break;
                }
            }
        }

        TConstArrayRef<TCell> rowRef(newRow);
        return TSerializedCellVec(rowRef);
    }

    void ReorderKeyColumnInfo(const TVector<ui32>& keyIds) {
        TVector<TKeyColumnInfo> newInfo;
        for (size_t i = 0; i < keyIds.size(); ++i) {
            for (const auto& [keyId, keyType] : KeyColumnInfo) {
                if (keyId == keyIds[i]) {
                    newInfo.emplace_back(keyId, keyType);
                    break;
                }
            }
        }

        KeyColumnInfo = std::move(newInfo);
    }

    void DebugPrintRequest(TQueryData::TPtr queryData) {
        TStringBuilder builder;
        builder << "Fill request with parameters: ";

        auto [isFirstQueryType, isFirstQueryValue] = queryData->GetParameterUnboxedValue(NBatchParams::IsFirstQuery);
        auto [isLastQueryType, isLastQueryValue] = queryData->GetParameterUnboxedValue(NBatchParams::IsLastQuery);

        builder << "IsFirstQuery = " << isFirstQueryValue.Get<bool>();
        builder << ", IsLastQuery = " << isLastQueryValue.Get<bool>() << ", ";

        auto [isInclusiveLeftType, isInclusiveLeftValue] = queryData->GetParameterUnboxedValue(NBatchParams::IsInclusiveLeft);
        auto [isInclusiveRightType, isInclusiveRightValue] = queryData->GetParameterUnboxedValue(NBatchParams::IsInclusiveRight);

        builder << "(";

        for (size_t i = 0; i < KeyColumnInfo.size(); ++i) {
            auto beginName = NBatchParams::Begin + ToString(i + 1);
            auto [beginType, beginValue] = queryData->GetParameterUnboxedValue(beginName);

            auto endName = NBatchParams::End + ToString(i + 1);
            auto [endType, endValue] = queryData->GetParameterUnboxedValue(endName);

            if (isFirstQueryValue.Get<bool>()) {
                builder << "-inf";
            } else {
                builder << "[" << beginValue << "]";
            }
            builder << ((isInclusiveLeftValue.Get<bool>()) ? " <= " : " < ");

            builder << ("Key" + ToString(i + 1));

            builder << ((isInclusiveRightValue.Get<bool>()) ? " <= " : " < ");
            if (isLastQueryValue.Get<bool>()) {
                builder << "+inf";
            } else {
                builder << "[" << endValue << "]";
            }

            if (i + 1 < KeyColumnInfo.size()) {
                builder << ", ";
            }
        }

        builder << ")";
        PE_LOG_I(builder);
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
    TVector<TKeyColumnInfo> KeyColumnInfo;
    TVector<TBatchPartitionInfo::TPtr> Partitions;
    THashMap<TActorId, TBatchPartitionInfo::TPtr> ExecuterPartition;
    THashMap<TActorId, TBatchPartitionInfo::TPtr> BufferPartition;
    TMaybe<bool> NeedReordering = Nothing();
    TTableId TableId;
    TString TablePath;
    IKqpGateway::TExecPhysicalRequest LiteralRequest;
    IKqpGateway::TExecPhysicalRequest PhysicalRequest;
    const TActorId SessionActorId;
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
