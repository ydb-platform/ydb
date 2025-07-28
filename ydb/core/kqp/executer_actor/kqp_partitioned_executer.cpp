#include "kqp_partitioned_executer.h"
#include "kqp_executer.h"

#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/batch/params.h>
#include <ydb/core/kqp/common/batch/batch_operation_settings.h>
#include <ydb/core/kqp/common/buffer/buffer.h>
#include <ydb/core/kqp/common/buffer/events.h>
#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_log.h>

namespace NKikimr {
namespace NKqp {

namespace {

#define PE_LOG_C(msg) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, LogPrefix() << msg)
#define PE_LOG_E(msg) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, LogPrefix() << msg)
#define PE_LOG_W(msg) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, LogPrefix() << msg)
#define PE_LOG_N(msg) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, LogPrefix() << msg)
#define PE_LOG_I(msg) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, LogPrefix() << msg)
#define PE_LOG_D(msg) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, LogPrefix() << msg)
#define PE_LOG_T(msg) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, LogPrefix() << msg)

/*
    TKqpPartitionedExecuter only executes BATCH UPDATE/DELETE queries
    with idempotent set of updates (except primary key), without RETURNING
    and without any joins or subqueries.

    Examples: ydb/core/kqp/ut/batch_operations
*/

class TKqpPartitionedExecuter : public TActorBootstrapped<TKqpPartitionedExecuter> {
    using TPartitionIndex = size_t;

    struct TBatchPartitionInfo {
        TMaybe<TKeyDesc::TPartitionRangeInfo> BeginRange;
        TMaybe<TKeyDesc::TPartitionRangeInfo> EndRange;
        TPartitionIndex PartitionIndex;

        TActorId ExecuterId;
        TActorId BufferId;

        ui64 LimitSize;
        ui64 RetryDelayMs;

        using TPtr = std::shared_ptr<TBatchPartitionInfo>;
    };

public:
    static constexpr char ActorName[] = "KQP_PARTITIONED_EXECUTER";

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_EXECUTER_ACTOR;
    }

    explicit TKqpPartitionedExecuter(TKqpPartitionedExecuterSettings settings)
        : LiteralRequest(std::move(settings.LiteralRequest))
        , PhysicalRequest(std::move(settings.PhysicalRequest))
        , SessionActorId(std::move(settings.SessionActorId))
        , FuncRegistry(std::move(settings.FuncRegistry))
        , TimeProvider(std::move(settings.TimeProvider))
        , RandomProvider(std::move(settings.RandomProvider))
        , Database(std::move(settings.Database))
        , UserToken(std::move(settings.UserToken))
        , RequestCounters(std::move(settings.RequestCounters))
        , TableServiceConfig(std::move(settings.TableServiceConfig))
        , UserRequestContext(std::move(settings.UserRequestContext))
        , StatementResultIndex(std::move(settings.StatementResultIndex))
        , AsyncIoFactory(std::move(std::move(settings.AsyncIoFactory)))
        , PreparedQuery(std::move(settings.PreparedQuery))
        , FederatedQuerySetup(std::move(settings.FederatedQuerySetup))
        , GUCSettings(std::move(settings.GUCSettings))
        , ShardIdToTableInfo(std::move(settings.ShardIdToTableInfo))
        , WriteBufferInitialMemoryLimit(std::move(settings.WriteBufferInitialMemoryLimit))
        , WriteBufferMemoryLimit(std::move(settings.WriteBufferMemoryLimit))
    {
        UseLiteral = PreparedQuery->GetTransactions().size() == 2;
        ResponseEv = std::make_unique<TEvKqpExecuter::TEvTxResponse>(PhysicalRequest.TxAlloc,
            TEvKqpExecuter::TEvTxResponse::EExecutionType::Data);

        if (TableServiceConfig.HasBatchOperationSettings()) {
            BatchOperationSettings = SetBatchOperationSettings(TableServiceConfig.GetBatchOperationSettings());
        }

        PE_LOG_I("Created " << ActorName << " with MaxBatchSize = " << BatchOperationSettings.MaxBatchSize
            << ", PartitionExecutionLimit = " << BatchOperationSettings.PartitionExecutionLimit);

        for (const auto& tx : PreparedQuery->GetTransactions()) {
            for (const auto& stage : tx->GetStages()) {
                for (const auto& sink : stage.GetSinks()) {
                    FillTableMetaInfo(sink);
                    if (!KeyColumnInfo.empty()) {
                        return;
                    }
                }
            }
        }
    }

    void Bootstrap() {
        SendRequestGetPartitions();

        Become(&TKqpPartitionedExecuter::PrepareState);
    }

    STFUNC(PrepareState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, HandlePrepare);
                hFunc(TEvKqp::TEvAbortExecution, HandlePrepare);
            default:
                AFL_ENSURE(false)("unknown message", ev->GetTypeRewrite());
            }
        } catch (...) {
            RuntimeError(
                Ydb::StatusIds::INTERNAL_ERROR,
                NYql::TIssues({NYql::TIssue(TStringBuilder()
                    << "from state handler = " << CurrentStateFuncName())}));
        }
    }

    void HandlePrepare(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        auto* request = ev->Get()->Request.Get();

        PE_LOG_D("Got TEvTxProxySchemeCache::TEvResolveKeySetResult from ActorId = " << ev->Sender);

        if (request->ErrorCount > 0) {
            return RuntimeError(
                Ydb::StatusIds::INTERNAL_ERROR,
                NYql::TIssues({NYql::TIssue(TStringBuilder() << CurrentStateFuncName()
                    << ", failed to get table")}));
        }

        YQL_ENSURE(request->ResultSet.size() == 1);

        FillTablePartitioning(request->ResultSet[0].KeyDescription->Partitioning);
        CreateExecutersWithBuffers();
    }

    void HandlePrepare(TEvKqp::TEvAbortExecution::TPtr& ev) {
        auto& msg = ev->Get()->Record;
        auto issues = ev->Get()->GetIssues();

        auto it = ExecuterToPartition.find(ev->Sender);
        if (it != ExecuterToPartition.end()) {
            PE_LOG_D("Got TEvKqp::EvAbortExecution from ActorId = " << ev->Sender
            << " , status: " << NYql::NDqProto::StatusIds_StatusCode_Name(msg.GetStatusCode())
            << ", message: " << issues.ToOneLineString() << ", abort child executers");

            ReturnIssues = issues;
            ReturnIssues.AddIssue(YqlIssue(NYql::TPosition(), NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                TStringBuilder() << "from PartitionedExecuterActor, state = " << CurrentStateFuncName()));

            auto [_, partInfo] = *it;
            AbortBuffer(partInfo->ExecuterId);
            ForgetExecuterAndBuffer(partInfo);
            ForgetPartition(partInfo);
        }

        Abort();
    }

    STFUNC(ExecuteState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpExecuter::TEvTxResponse, HandleExecute);
                hFunc(TEvKqpExecuter::TEvTxDelayedExecution, HandleExecute)
                hFunc(TEvKqp::TEvAbortExecution, HandlePrepare);
                hFunc(TEvKqpBuffer::TEvError, HandleExecute);
            default:
                AFL_ENSURE(false)("unknown message", ev->GetTypeRewrite());
            }
        } catch (...) {
            RuntimeError(
                Ydb::StatusIds::INTERNAL_ERROR,
                NYql::TIssues({NYql::TIssue(TStringBuilder()
                    << "from state handler = " << CurrentStateFuncName())}));
        }
    }

    void HandleExecute(TEvKqpExecuter::TEvTxResponse::TPtr& ev) {
        auto* response = ev->Get()->Record.MutableResponse();

        auto it = ExecuterToPartition.find(ev->Sender);
        if (it == ExecuterToPartition.end()) {
            PE_LOG_D("Got TEvKqpExecuter::TEvTxResponse from unknown actor with Id = " << ev->Sender
                << ", status = " << response->GetStatus() << ", ignore");
            return;
        }

        PE_LOG_I("Got TEvKqpExecuter::TEvTxResponse from ActorId = " << ev->Sender
            << ", status = " << response->GetStatus());

        auto [_, partInfo] = *it;
        AbortBuffer(partInfo->BufferId);
        ForgetExecuterAndBuffer(partInfo);

        switch (response->GetStatus()) {
            case Ydb::StatusIds::SUCCESS:
                partInfo->RetryDelayMs = BatchOperationSettings.StartRetryDelayMs;
                partInfo->LimitSize = std::min(partInfo->LimitSize * 2, BatchOperationSettings.MaxBatchSize);
                return OnSuccessResponse(partInfo, ev->Get());
            case Ydb::StatusIds::STATUS_CODE_UNSPECIFIED:
            case Ydb::StatusIds::ABORTED:
            case Ydb::StatusIds::UNAVAILABLE:
            case Ydb::StatusIds::OVERLOADED:
                return ScheduleRetryWithNewLimit(partInfo);
            default:
                IssuesFromMessage(response->GetIssues(), ReturnIssues);
        }

        ForgetPartition(partInfo);

        ReturnIssues.AddIssue(YqlIssue(NYql::TPosition(), NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
            TStringBuilder() << "from PartitionedExecuterActor, state = " << CurrentStateFuncName()));

        RuntimeError(response->GetStatus(), ReturnIssues);
    }

    void HandleExecute(TEvKqpExecuter::TEvTxDelayedExecution::TPtr& ev) {
        RequestCounters->Counters->BatchOperationRetries->Inc();

        auto& partInfo = StartedPartitions[ev->Get()->PartitionIdx];
        RetryPartExecution(partInfo);
    }

    void HandleExecute(TEvKqpBuffer::TEvError::TPtr& ev) {
        const auto& msg = *ev->Get();

        auto it = BufferToPartition.find(ev->Sender);
        if (it == BufferToPartition.end()) {
            PE_LOG_D("Got TEvKqpBuffer::TEvError from unknown actor with Id = " << ev->Sender << ", status = "
            << NYql::NDqProto::StatusIds_StatusCode_Name(msg.StatusCode) << ", ignore");
            return;
        }

        PE_LOG_D("Got TEvKqpBuffer::TEvError from ActorId = " << ev->Sender << ", status = "
            << NYql::NDqProto::StatusIds_StatusCode_Name(msg.StatusCode));

        auto [_, partInfo] = *it;
        AbortExecuter(partInfo->ExecuterId, "got error from BufferWriteActor");
        ForgetExecuterAndBuffer(partInfo);

        switch (msg.StatusCode) {
            case NYql::NDqProto::StatusIds::SUCCESS:
                YQL_ENSURE(false);
                break;
            case NYql::NDqProto::StatusIds::UNSPECIFIED:
            case NYql::NDqProto::StatusIds::ABORTED:
            case NYql::NDqProto::StatusIds::UNAVAILABLE:
            case NYql::NDqProto::StatusIds::OVERLOADED:
                return ScheduleRetryWithNewLimit(partInfo);
            default:
                ForgetPartition(partInfo);
                RuntimeError(
                    Ydb::StatusIds::INTERNAL_ERROR,
                    NYql::TIssues({NYql::TIssue(TStringBuilder() << CurrentStateFuncName()
                        << ", from BufferWriteActor by PartitionedExecuterActor")}));
        }
    }

    STFUNC(AbortState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpExecuter::TEvTxResponse, HandleAbort);
                hFunc(TEvKqpExecuter::TEvTxDelayedExecution, HandleExecute)
                hFunc(TEvKqp::TEvAbortExecution, HandleAbort);
                hFunc(TEvKqpBuffer::TEvError, HandleAbort);
            default:
                PE_LOG_W("unknown message from ActorId = " << ev->Sender);
            }
        } catch (...) {
            RuntimeError(
                Ydb::StatusIds::INTERNAL_ERROR,
                NYql::TIssues({NYql::TIssue(CurrentStateFuncName())}));
        }
    }

    void HandleAbort(TEvKqpExecuter::TEvTxResponse::TPtr& ev) {
        const auto& response = ev->Get()->Record.MutableResponse();
        auto it = ExecuterToPartition.find(ev->Sender);
        if (it == ExecuterToPartition.end()) {
            PE_LOG_D("Got TEvKqpExecuter::TEvTxResponse from unknown actor with Id = " << ev->Sender
                << ", status = " << response->GetStatus() << ", ignore");
            return;
        }

        PE_LOG_D("Got TEvKqpExecuter::TEvTxResponse from ActorId = " << ev->Sender
            << ", status = " << response->GetStatus());

        auto [_, partInfo] = *it;
        AbortBuffer(partInfo->BufferId);
        ForgetExecuterAndBuffer(partInfo);
        ForgetPartition(partInfo);

        if (CheckExecutersAreFinished()) {
            PE_LOG_I("All executers have been finished, abort PartitionedExecuterActor");
            RuntimeError(ReturnStatus, ReturnIssues);
        }
    }

    void HandleAbort(TEvKqp::TEvAbortExecution::TPtr& ev) {
        auto& msg = ev->Get()->Record;
        auto issues = ev->Get()->GetIssues();

        auto it = ExecuterToPartition.find(ev->Sender);
        if (it == ExecuterToPartition.end()) {
            PE_LOG_D("Got TEvKqp::EvAbortExecution from unknown actor with Id = " << ev->Sender
                << " , status: " << NYql::NDqProto::StatusIds_StatusCode_Name(msg.GetStatusCode())
                << ", message: " << issues.ToOneLineString() << ", ignore");
            return;
        }

        PE_LOG_D("Got TEvKqp::EvAbortExecution from ActorId = " << ev->Sender
            << " , status: " << NYql::NDqProto::StatusIds_StatusCode_Name(msg.GetStatusCode())
            << ", message: " << issues.ToOneLineString());

        auto [_, partInfo] = *it;
        AbortBuffer(partInfo->BufferId);
        ForgetExecuterAndBuffer(partInfo);
    }

    void HandleAbort(TEvKqpBuffer::TEvError::TPtr& ev) {
        const auto& msg = *ev->Get();
        PE_LOG_D("Got TEvError from BufferWriteActor with Id = " << ev->Sender << ", status = "
            << NYql::NDqProto::StatusIds_StatusCode_Name(msg.StatusCode) << ", ignore");
    }

    TString LogPrefix() const {
        TStringBuilder result = TStringBuilder()
            << "[PARTITIONED] ActorId: " << SelfId() << ", "
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

        switch (settings.GetType()) {
            case NKikimrKqp::TKqpTableSinkSettings::MODE_UPSERT:
                OperationType = TKeyDesc::ERowOperation::Update;
                break;
            case NKikimrKqp::TKqpTableSinkSettings::MODE_DELETE:
                OperationType = TKeyDesc::ERowOperation::Erase;
                break;
            default:
                YQL_ENSURE(false);
                break;
        }

        KeyColumnInfo.reserve(settings.GetKeyColumns().size());
        for (int i = 0; i < settings.GetKeyColumns().size(); ++i) {
            const auto& column = settings.GetKeyColumns()[i];
            auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(column.GetTypeId(),
                column.HasTypeInfo() ? &column.GetTypeInfo() : nullptr);
            KeyColumnInfo.emplace_back(column.GetId(), typeInfoMod.TypeInfo, i);
        }

        TableId = MakeTableId(settings.GetTable());
    }

    void SendRequestGetPartitions() {
        YQL_ENSURE(!KeyColumnInfo.empty());

        const TVector<TCell> minKey(KeyColumnInfo.size());
        const TTableRange range(minKey, true, {}, false, false);

        YQL_ENSURE(range.IsFullRange(KeyColumnInfo.size()));

        TVector<NScheme::TTypeInfo> keyColumnTypes;
        for (const auto& info : KeyColumnInfo) {
            keyColumnTypes.push_back(info.Type);
        }

        auto keyRange = MakeHolder<TKeyDesc>(TableId, range, OperationType, keyColumnTypes, TVector<TKeyDesc::TColumnOp>{});

        TAutoPtr<NSchemeCache::TSchemeCacheRequest> request(new NSchemeCache::TSchemeCacheRequest());
        request->ResultSet.emplace_back(std::move(keyRange));

        TAutoPtr<TEvTxProxySchemeCache::TEvResolveKeySet> resolveReq(new TEvTxProxySchemeCache::TEvResolveKeySet(request));

        Send(MakeSchemeCacheID(), resolveReq.Release());
    }

    void FillTablePartitioning(std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> partitioning) {
        TablePartitioning = std::move(partitioning);
    }

    void CreateExecutersWithBuffers() {
        Become(&TKqpPartitionedExecuter::ExecuteState);

        auto partCount = std::min(BatchOperationSettings.PartitionExecutionLimit, TablePartitioning->size());
        while (NextPartitionIndex < partCount) {
            CreateExecuterWithBuffer(NextPartitionIndex++, /* isRetry */ false);
        }
    }

    TBatchPartitionInfo::TPtr CreatePartition(TPartitionIndex idx) {
        YQL_ENSURE(idx < TablePartitioning->size());

        auto partition = std::make_shared<TBatchPartitionInfo>();
        StartedPartitions[idx] = partition;

        partition->EndRange = TablePartitioning->at(idx).Range;
        if (idx > 0 && !TablePartitioning->at(idx).Range.Empty()) {
            partition->BeginRange = TablePartitioning->at(idx - 1).Range;
            partition->BeginRange->IsInclusive = !partition->BeginRange->IsInclusive;
        }

        partition->PartitionIndex = idx;
        partition->LimitSize = BatchOperationSettings.MaxBatchSize;
        partition->RetryDelayMs = BatchOperationSettings.StartRetryDelayMs;

        ReorderPartitionRanges(idx);

        return partition;
    }

    void CreateExecuterWithBuffer(TPartitionIndex partitionIndex, bool isRetry) {
        auto partInfo = (isRetry) ? StartedPartitions[partitionIndex] : CreatePartition(partitionIndex);
        auto txAlloc = std::make_shared<TTxAllocatorState>(FuncRegistry, TimeProvider, RandomProvider);

        IKqpGateway::TExecPhysicalRequest request(txAlloc);
        FillPhysicalRequest(request, txAlloc, partitionIndex);

        auto txManager = CreateKqpTransactionManager();

        auto alloc = std::make_shared<NKikimr::NMiniKQL::TScopedAlloc>(
                __LOCATION__, NKikimr::TAlignedPagePoolCounters(), true, false);

        alloc->SetLimit(WriteBufferInitialMemoryLimit);
        alloc->Ref().SetIncreaseMemoryLimitCallback([this, alloc=alloc.get()](ui64 currentLimit, ui64 required) {
            if (required < WriteBufferMemoryLimit) {
                PE_LOG_D("Increase memory limit from " << currentLimit << " to " << required);
                alloc->SetLimit(required);
            }
        });

        TKqpBufferWriterSettings settings {
            .SessionActorId = SelfId(),
            .TxManager = txManager,
            .TraceId = PhysicalRequest.TraceId.GetTraceId(),
            .Counters = RequestCounters->Counters,
            .TxProxyMon = RequestCounters->TxProxyMon,
            .Alloc = std::move(alloc)
        };

        auto* bufferActor = CreateKqpBufferWriterActor(std::move(settings));
        auto bufferActorId = RegisterWithSameMailbox(bufferActor);

        auto batchSettings = TBatchOperationSettings(partInfo->LimitSize, BatchOperationSettings.MinBatchSize);
        auto executerActor = CreateKqpExecuter(std::move(request), Database, UserToken, TValueOutputFormat{}, RequestCounters,
            TableServiceConfig, AsyncIoFactory, PreparedQuery, SelfId(), UserRequestContext, StatementResultIndex,
            FederatedQuerySetup, GUCSettings, ShardIdToTableInfo, txManager, bufferActorId, std::move(batchSettings));
        auto exId = RegisterWithSameMailbox(executerActor);

        partInfo->ExecuterId = exId;
        partInfo->BufferId = bufferActorId;
        ExecuterToPartition[exId] = BufferToPartition[bufferActorId] = partInfo;

        PE_LOG_I("Create new KQP executer by PartitionedExecuterActor: ExecuterId = " << partInfo->ExecuterId
            << ", PartitionIndex = " << partitionIndex << ", LimitSize = " << partInfo->LimitSize
            << ", RetryDelayMs = " << partInfo->RetryDelayMs);

        auto ev = std::make_unique<TEvTxUserProxy::TEvProposeKqpTransaction>(exId);
        Send(MakeTxProxyID(), ev.release());
    }

    void Abort() {
        Become(&TKqpPartitionedExecuter::AbortState);

        if (CheckExecutersAreFinished()) {
            PE_LOG_I("All executers have been finished, abort PartitionedExecuterActor");
            return RuntimeError(ReturnStatus, ReturnIssues);
        }

        SendAbortToExecuters();
    }

    void SendAbortToExecuters() {
        PE_LOG_I("Send abort to executers");

        for (auto& [exId, partInfo] : ExecuterToPartition) {
            AbortExecuter(exId, "runtime error");
        }
    }

    void AbortExecuter(TActorId id, const TString& reason) {
        auto abortEv = TEvKqp::TEvAbortExecution::Aborted("Aborted by PartitionedExecuterActor, reason: " + reason);
        Send(id, abortEv.Release());
    }

    void AbortBuffer(TActorId id) {
        Send(id, new TEvKqpBuffer::TEvTerminate{});
    }

    void ForgetExecuterAndBuffer(const TBatchPartitionInfo::TPtr& partInfo) {
        YQL_ENSURE(ExecuterToPartition.erase(partInfo->ExecuterId) == 1);
        YQL_ENSURE(BufferToPartition.erase(partInfo->BufferId) == 1);
    }

    void ForgetPartition(const TBatchPartitionInfo::TPtr& partInfo) {
        YQL_ENSURE(StartedPartitions.erase(partInfo->PartitionIndex) == 1);
    }

    void OnSuccessResponse(TBatchPartitionInfo::TPtr& partInfo, TEvKqpExecuter::TEvTxResponse* ev) {
        const auto& maxReadKeys = ev->BatchOperationMaxKeys;
        const auto& keyIds = ev->BatchOperationKeyIds;

        TryReorderKeysByIds(keyIds);

        TSerializedCellVec maxKey = GetMaxCellVecKey(maxReadKeys);
        if (!maxKey.GetCells().empty()) {
            partInfo->BeginRange = TKeyDesc::TPartitionRangeInfo(maxKey,
                /* IsInclusive */ false,
                /* IsPoint */ false
            );
            return RetryPartExecution(partInfo);
        }

        ForgetPartition(partInfo);

        if (NextPartitionIndex < TablePartitioning->size()) {
            return CreateExecuterWithBuffer(NextPartitionIndex++, /* isRetry */ false);
        }

        if (CheckExecutersAreFinished()) {
            auto& response = *ResponseEv->Record.MutableResponse();
            response.SetStatus(ReturnStatus);

            PE_LOG_I("All executers have been finished. Send SUCCESS to SessionActor");

            Send(SessionActorId, ResponseEv.release());
            PassAway();
        }
    }

    void RetryPartExecution(const TBatchPartitionInfo::TPtr& partInfo) {
        PE_LOG_D("Retry query execution for PartitionIndex = " << partInfo->PartitionIndex
            << ", RetryDelayMs = " << partInfo->RetryDelayMs);

        if (this->CurrentStateFunc() != &TKqpPartitionedExecuter::AbortState) {
            return CreateExecuterWithBuffer(partInfo->PartitionIndex, /* isRetry */ true);
        }

        ForgetPartition(partInfo);

        if (CheckExecutersAreFinished()) {
            PE_LOG_I("All executers have been finished, abort PartitionedExecuterActor");
            RuntimeError(ReturnStatus, ReturnIssues);
        }
    }

    void ScheduleRetryWithNewLimit(TBatchPartitionInfo::TPtr& partInfo) {
        auto newLimit = std::max(partInfo->LimitSize / 2, BatchOperationSettings.MinBatchSize);
        partInfo->LimitSize = newLimit;

        auto ev = std::make_unique<TEvKqpExecuter::TEvTxDelayedExecution>(partInfo->PartitionIndex);
        Schedule(TDuration::MilliSeconds(partInfo->RetryDelayMs), ev.release());

        // We use the init delay value first and change it for the next attempt
        auto decJitterDelay = RandomProvider->Uniform(BatchOperationSettings.StartRetryDelayMs, partInfo->RetryDelayMs * 3ul);
        auto newDelay = std::min(BatchOperationSettings.MaxRetryDelayMs, decJitterDelay);
        partInfo->RetryDelayMs = newDelay;
    }

    void FillPhysicalRequest(IKqpGateway::TExecPhysicalRequest& physicalRequest,
        TTxAllocatorState::TPtr txAlloc, TPartitionIndex partitionIndex)
    {
        FillRequestByInitWithParams(physicalRequest, partitionIndex, /* literal */ false);

        auto queryData = physicalRequest.Transactions.front().Params;
        if (UseLiteral) {
            IKqpGateway::TExecPhysicalRequest literalRequest(txAlloc);
            FillRequestByInitWithParams(literalRequest, partitionIndex, /* literal */ true);
            PrepareParameters(literalRequest);

            auto ev = ExecuteLiteral(std::move(literalRequest), RequestCounters, SelfId(), UserRequestContext);
            auto* response = ev->Record.MutableResponse();

            if (response->GetStatus() != Ydb::StatusIds::SUCCESS) {
                return RuntimeError(
                    Ydb::StatusIds::BAD_REQUEST,
                    NYql::TIssues({NYql::TIssue(TStringBuilder() << CurrentStateFuncName()
                        << ", got error from KqpLiteralExecuter.")}));
            }

            if (!ev->GetTxResults().empty()) {
                queryData->AddTxResults(0, std::move(ev->GetTxResults()));
            }

            queryData->AddTxHolders(std::move(ev->GetTxHolders()));
        }

        PrepareParameters(physicalRequest);
        LogDebugRequest(queryData, partitionIndex);
    }

    void FillRequestByInitWithParams(IKqpGateway::TExecPhysicalRequest& request, TPartitionIndex partitionIndex, bool literal)
    {
        FillRequestByInit(request, literal);

        YQL_ENSURE(!request.Transactions.empty());

        auto& queryData = request.Transactions.front().Params;
        auto& partition = StartedPartitions[partitionIndex];

        FillRequestRange(queryData, partition->BeginRange, /* isBegin */ true);
        FillRequestRange(queryData, partition->EndRange, /* isBegin */ false);
    }

    void FillRequestByInit(IKqpGateway::TExecPhysicalRequest& newRequest, bool literal) {
        auto& from = (literal) ? LiteralRequest : PhysicalRequest;
        IKqpGateway::TExecPhysicalRequest::FillRequestFrom(newRequest, from);

        auto tx = PreparedQuery->GetTransactions()[(UseLiteral) ? 1 - static_cast<size_t>(literal) : 0];
        newRequest.Transactions.emplace_back(tx, std::make_shared<TQueryData>(newRequest.TxAlloc));
        newRequest.TraceId = NWilson::TTraceId();

        auto newData = newRequest.Transactions.front().Params;
        auto oldData = (UseLiteral) ? LiteralRequest.Transactions.front().Params : PhysicalRequest.Transactions.front().Params;
        for (auto& [name, _] : oldData->GetParams()) {
            if (!name.StartsWith(NBatchParams::Header)) {
                TTypedUnboxedValue& typedValue = oldData->GetParameterUnboxedValue(name);
                newData->AddUVParam(name, typedValue.first, typedValue.second);
            }
        }
    }

    void FillRequestRange(TQueryData::TPtr queryData, const TMaybe<TKeyDesc::TPartitionRangeInfo>& range, bool isBegin) {
        /*
            isBegin = true

            IsInclusiveLeft AND ((BeginPrefixSize = 0) OR ((BeginPrefixSize = 1) AND (Begin1 <= K1)) OR ((BeginPrefixSize = 2) AND ((Begin1, Begin2) <= (K1, K2)) OR ...)
            OR
            NOT IsInclusiveLeft AND ((BeginPrefixSize = 0) OR ((BeginPrefixSize = 1) AND (Begin1 < K1)) OR ((BeginPrefixSize = 2) AND ((Begin1, Begin2) < (K1, K2)) OR ...)
        */

        auto isInclusive = (isBegin) ? NBatchParams::IsInclusiveLeft : NBatchParams::IsInclusiveRight;
        auto rangeName = ((isBegin) ? NBatchParams::Begin : NBatchParams::End);
        auto prefixRangeName = ((isBegin) ? NBatchParams::BeginPrefixSize : NBatchParams::EndPrefixSize);

        FillRequestParameter(queryData, isInclusive, (!range.Empty()) ? range->IsInclusive : false);

        size_t firstEmpty = (range.Empty()) ? 0 : KeyColumnInfo.size();

        for (size_t i = 0; i < KeyColumnInfo.size(); ++i) {
            const auto& info = KeyColumnInfo[i];
            auto paramName = rangeName + ToString(info.ParamIndex + 1);

            if (range.Empty() || range->EndKeyPrefix.GetCells().size() <= i) {
                firstEmpty = std::min(firstEmpty, info.ParamIndex);
                FillRequestParameter(queryData, paramName, false, /* setDefault */ true);
                continue;
            }

            auto cellValue = NMiniKQL::GetCellValue(range->EndKeyPrefix.GetCells()[i], info.Type);
            if (!cellValue.HasValue()) {
                firstEmpty = std::min(firstEmpty, info.ParamIndex);
            }

            FillRequestParameter(queryData, paramName, cellValue, /* setDefault */ !cellValue.HasValue());
        }

        FillRequestParameter(queryData, prefixRangeName, firstEmpty);
    }

    template <typename T>
    void FillRequestParameter(TQueryData::TPtr queryData, const TString& name, T value, bool setDefault = false) {
        for (const auto& paramDesc : PreparedQuery->GetParameters()) {
            if (paramDesc.GetName() != name) {
                continue;
            }

            NKikimrMiniKQL::TType protoType = paramDesc.GetType();
            NKikimr::NMiniKQL::TType* paramType = ImportTypeFromProto(protoType, queryData->GetAllocState()->TypeEnv);

            if (setDefault) {
                auto defaultValue = MakeDefaultValueByType(paramType);
                queryData->AddUVParam(name, paramType, defaultValue);
                return;
            }

            queryData->AddUVParam(name, paramType, NUdf::TUnboxedValuePod(value));
            return;
        }

        YQL_ENSURE(false);
    }

    void PrepareParameters(IKqpGateway::TExecPhysicalRequest& request) {
        auto& queryData = request.Transactions.front().Params;
        TString paramName;

        try {
            for (const auto& paramDesc : PreparedQuery->GetParameters()) {
                paramName = paramDesc.GetName();
                queryData->ValidateParameter(paramDesc.GetName(), paramDesc.GetType(), request.TxAlloc->TypeEnv);
            }

            for(const auto& paramBinding: request.Transactions.front().Body->GetParamBindings()) {
                paramName = paramBinding.GetName();
                queryData->MaterializeParamValue(true, paramBinding);
            }
        } catch (const yexception& ex) {
            RuntimeError(
                Ydb::StatusIds::BAD_REQUEST,
                NYql::TIssues({NYql::TIssue(TStringBuilder() << CurrentStateFuncName()
                    << ", cannot prepare parameters for request, parameter name = " << paramName)}));
        }
    }

    bool CheckExecutersAreFinished() const {
        return StartedPartitions.empty();
    }

    // SchemeCache and ReadActor may have the different order of key columns,
    // so we need to reorder partition ranges for next compares.
    void TryReorderKeysByIds(const TVector<ui32>& keyIds) {
        if (keyIds.empty()) {
            return;
        }

        YQL_ENSURE(KeyColumnInfo.size() == keyIds.size());

        bool isEqual = true;
        for (size_t i = 0; i < KeyColumnInfo.size(); ++i) {
            if (KeyColumnInfo[i].Id != keyIds[i]) {
                isEqual = false;
                break;
            }
        }

        if (isEqual) {
            return;
        }

        ReorderKeyColumnInfo(keyIds);
        for (const auto& [partIdx, _] : StartedPartitions) {
            ReorderPartitionRanges(partIdx);
        }
    }

    void ReorderPartitionRanges(TPartitionIndex idx) {
        PE_LOG_D("Reorder KeyColumnInfo and partitioning ranges by keyIds from RA");

        auto& partInfo = StartedPartitions[idx];

        auto& beginRow = partInfo->BeginRange;
        if (!beginRow.Empty()) {
            beginRow->EndKeyPrefix = ReorderRangeByKeyColumnInfo(beginRow->EndKeyPrefix);
        }

        auto& endRow = partInfo->EndRange;
        if (!endRow.Empty()) {
            endRow->EndKeyPrefix = ReorderRangeByKeyColumnInfo(endRow->EndKeyPrefix);
        }
    }

    TSerializedCellVec ReorderRangeByKeyColumnInfo(const TSerializedCellVec& row) {
        if (row.GetCells().empty()) {
            return row;
        }

        TVector<TCell> newRow;
        auto cells = row.GetCells();
        for (const auto& info : KeyColumnInfo) {
            newRow.push_back(cells[info.ParamIndex]);
        }

        TConstArrayRef<TCell> rowRef(newRow);
        return TSerializedCellVec(rowRef);
    }

    void ReorderKeyColumnInfo(const TVector<ui32>& keyIds) {
        TVector<TKeyColumnInfo> newInfo;

        for (const auto& id : keyIds) {
            auto it = std::find_if(KeyColumnInfo.cbegin(), KeyColumnInfo.cend(), [&id] (const TKeyColumnInfo& info) {
                return info.Id == id;
            });

            YQL_ENSURE(it != KeyColumnInfo.cend());
            newInfo.push_back(*it);
        }

        KeyColumnInfo = std::move(newInfo);
    }

    TSerializedCellVec GetMaxCellVecKey(const TVector<TSerializedCellVec>& maxReadKeys) const {
        TSerializedCellVec maxKey;
        for (size_t i = 0; i < maxReadKeys.size(); ++i) {
            auto row = maxReadKeys[i];
            if (i == 0) {
                maxKey = row;
                continue;
            }

            auto max_cells = maxKey.GetCells();
            auto row_cells = row.GetCells();

            YQL_ENSURE(row_cells.size() == max_cells.size());

            for (size_t j = 0; j < KeyColumnInfo.size(); ++j) {
                NScheme::TTypeInfoOrder typeOrder(KeyColumnInfo[j].Type, NScheme::EOrder::Ascending);
                if (CompareTypedCells(max_cells[j], row_cells[j], typeOrder) < 0) {
                    maxKey = row;
                    break;
                }
            }
        }
        return maxKey;
    }

    void LogDebugRequest(TQueryData::TPtr queryData, TPartitionIndex partitionIndex) {
        TStringBuilder builder;
        builder << "Fill request with parameters, PartitionInddx = " << partitionIndex << ": ";

        auto [isInclusiveLeftType, isInclusiveLeftValue] = queryData->GetParameterUnboxedValue(NBatchParams::IsInclusiveLeft);
        auto [isInclusiveRightType, isInclusiveRightValue] = queryData->GetParameterUnboxedValue(NBatchParams::IsInclusiveRight);

        auto [beginPrefixSizeType, beginPrefixSizeValue] = queryData->GetParameterUnboxedValue(NBatchParams::BeginPrefixSize);
        auto [endPrefixSizeType, endPrefixSizeValue] = queryData->GetParameterUnboxedValue(NBatchParams::EndPrefixSize);

        builder << "(";

        for (size_t i = 0; i < KeyColumnInfo.size(); ++i) {
            auto paramIndex = KeyColumnInfo[i].ParamIndex;
            auto beginName = NBatchParams::Begin + ToString(paramIndex + 1);
            auto [beginType, beginValue] = queryData->GetParameterUnboxedValue(beginName);

            auto endName = NBatchParams::End + ToString(paramIndex + 1);
            auto [endType, endValue] = queryData->GetParameterUnboxedValue(endName);

            if (paramIndex >= beginPrefixSizeValue.Get<ui32>()) {
                builder << "-inf";
            } else {
                builder << "[" << beginValue << "]";
            }
            builder << ((isInclusiveLeftValue.Get<bool>()) ? " <= " : " < ");

            builder << ("Column" + ToString(paramIndex + 1));

            builder << ((isInclusiveRightValue.Get<bool>()) ? " <= " : " < ");
            if (paramIndex >= endPrefixSizeValue.Get<ui32>()) {
                builder << "+inf";
            } else {
                builder << "[" << endValue << "]";
            }

            if (i + 1 < KeyColumnInfo.size()) {
                builder << ", ";
            }
        }

        builder << ")";
        PE_LOG_D(builder);
    }

    void RuntimeError(Ydb::StatusIds::StatusCode code, const NYql::TIssues& issues) {
        PE_LOG_E(Ydb::StatusIds_StatusCode_Name(code) << ": " << issues.ToOneLineString());

        if (this->CurrentStateFunc() != &TKqpPartitionedExecuter::AbortState) {
            ReturnStatus = code;
            return Abort();
        }

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
    TBatchOperationSettings BatchOperationSettings;

    // for errors only
    Ydb::StatusIds::StatusCode ReturnStatus = Ydb::StatusIds::SUCCESS;
    NYql::TIssues ReturnIssues;

    struct TKeyColumnInfo {
        ui32 Id;
        NScheme::TTypeInfo Type;
        size_t ParamIndex;
    };

    // We have to save column ids and types for compare rows to start retry execution
    TVector<TKeyColumnInfo> KeyColumnInfo;

    std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> TablePartitioning;
    THashMap<TPartitionIndex, TBatchPartitionInfo::TPtr> StartedPartitions;
    TPartitionIndex NextPartitionIndex = 0;

    THashMap<TActorId, TBatchPartitionInfo::TPtr> ExecuterToPartition;
    THashMap<TActorId, TBatchPartitionInfo::TPtr> BufferToPartition;

    TKeyDesc::ERowOperation OperationType;
    TTableId TableId;

    IKqpGateway::TExecPhysicalRequest LiteralRequest;
    IKqpGateway::TExecPhysicalRequest PhysicalRequest;
    bool UseLiteral;

    const TActorId SessionActorId;
    const NMiniKQL::IFunctionRegistry* FuncRegistry;
    TIntrusivePtr<ITimeProvider> TimeProvider;
    TIntrusivePtr<IRandomProvider> RandomProvider;

    // The next variables are only for DEA and BWA
    TString Database;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TKqpRequestCounters::TPtr RequestCounters;
    NKikimrConfig::TTableServiceConfig TableServiceConfig;
    TIntrusivePtr<TUserRequestContext> UserRequestContext;
    ui32 StatementResultIndex;
    NYql::NDq::IDqAsyncIoFactory::TPtr AsyncIoFactory;
    TPreparedQueryHolder::TConstPtr PreparedQuery;
    const std::optional<TKqpFederatedQuerySetup> FederatedQuerySetup;
    const TGUCSettings::TPtr GUCSettings;
    TShardIdToTableInfoPtr ShardIdToTableInfo;

    const ui64 WriteBufferInitialMemoryLimit;
    const ui64 WriteBufferMemoryLimit;
};

} // namespace

NActors::IActor* CreateKqpPartitionedExecuter(TKqpPartitionedExecuterSettings settings)
{
    return new TKqpPartitionedExecuter(std::move(settings));
}

} // namespace NKqp
} // namespace NKikimr
