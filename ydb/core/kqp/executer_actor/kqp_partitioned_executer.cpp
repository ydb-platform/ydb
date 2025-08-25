#include "kqp_partitioned_executer.h"
#include "kqp_executer.h"

#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/kqp/common/kqp_batch_operations.h>
#include <ydb/core/kqp/common/buffer/buffer.h>
#include <ydb/core/kqp/common/buffer/events.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>

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
 * TKqpPartitionedExecuter only executes BATCH UPDATE/DELETE queries
 * with the idempotent set of updates (except primary key), without RETURNING,
 * only for row tables and without any joins or subqueries.
 *
 * Examples: ydb/core/kqp/ut/batch_operations
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
        : Request(std::move(settings.Request))
        , SessionActorId(std::move(settings.SessionActorId))
        , FuncRegistry(std::move(settings.FuncRegistry))
        , TimeProvider(std::move(settings.TimeProvider))
        , RandomProvider(std::move(settings.RandomProvider))
        , Database(std::move(settings.Database))
        , UserToken(std::move(settings.UserToken))
        , RequestCounters(std::move(settings.RequestCounters))
        , TableServiceConfig(std::move(settings.ExecuterConfig.TableServiceConfig))
        , MutableExecuterConfig(std::move(settings.ExecuterConfig.MutableConfig))
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
        ResponseEv = std::make_unique<TEvKqpExecuter::TEvTxResponse>(Request.TxAlloc, TEvKqpExecuter::TEvTxResponse::EExecutionType::Data);

        if (TableServiceConfig.HasBatchOperationSettings()) {
            Settings = NBatchOperations::ImportSettingsFromProto(TableServiceConfig.GetBatchOperationSettings());
        }

        PE_LOG_I("Created " << ActorName << " with MaxBatchSize = " << Settings.MaxBatchSize
            << ", PartitionExecutionLimit = " << Settings.PartitionExecutionLimit);

        FillTableMetaInfo();
    }

    void Bootstrap() {
        Become(&TKqpPartitionedExecuter::PrepareState);

        ResolvePartitioning();
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
                    << "KqpPartitionedExecuterActor got an unknown error, state = " << CurrentStateFuncName())}));
        }
    }

    void HandlePrepare(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        auto* request = ev->Get()->Request.Get();

        PE_LOG_D("Got TEvTxProxySchemeCache::TEvResolveKeySetResult from ActorId = " << ev->Sender);

        if (request->ErrorCount > 0) {
            return RuntimeError(
                Ydb::StatusIds::INTERNAL_ERROR,
                NYql::TIssues({NYql::TIssue(TStringBuilder()
                    << "KqpPartitionedExecuterActor could not resolve a partitioning of the table, state = " << CurrentStateFuncName())}));
        }

        YQL_ENSURE(request->ResultSet.size() == 1);

        TablePartitioning = request->ResultSet[0].KeyDescription->Partitioning;

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

            ReturnIssues.AddIssues(issues);
            ReturnIssues.AddIssue(NYql::TIssue(TStringBuilder()
                << "while preparing/executing by KqpPartitionedExecuterActor"));

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
                    << "KqpPartitionedExecuterActor got an unknown error, state = " << CurrentStateFuncName())}));
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
                partInfo->RetryDelayMs = Settings.StartRetryDelayMs;
                partInfo->LimitSize = std::min(partInfo->LimitSize * 2, Settings.MaxBatchSize);
                return OnSuccessResponse(partInfo, ev->Get());
            case Ydb::StatusIds::STATUS_CODE_UNSPECIFIED:
            case Ydb::StatusIds::ABORTED:
            case Ydb::StatusIds::UNAVAILABLE:
            case Ydb::StatusIds::OVERLOADED:
            case Ydb::StatusIds::UNDETERMINED:
                return ScheduleRetryWithNewLimit(partInfo);
            default:
                break;
        }

        ForgetPartition(partInfo);

        IssuesFromMessage(response->GetIssues(), ReturnIssues);
        ReturnIssues.AddIssue(NYql::TIssue(TStringBuilder()
            << "while executing by KqpPartitionedExecuterActor"));

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
        AbortExecuter(partInfo->ExecuterId, "got error from KqpBufferWriteActor");
        ForgetExecuterAndBuffer(partInfo);

        switch (msg.StatusCode) {
            case NYql::NDqProto::StatusIds::SUCCESS:
                YQL_ENSURE(false, "KqpBufferWriteActor should not return success by TEvKqpBuffer::TEvError");
                break;
            case NYql::NDqProto::StatusIds::UNSPECIFIED:
            case NYql::NDqProto::StatusIds::ABORTED:
            case NYql::NDqProto::StatusIds::UNAVAILABLE:
            case NYql::NDqProto::StatusIds::OVERLOADED:
            case NYql::NDqProto::StatusIds::UNDETERMINED:
                return ScheduleRetryWithNewLimit(partInfo);
            default:
                break;
        }

        ForgetPartition(partInfo);

        ReturnIssues.AddIssues(msg.Issues);

        RuntimeError(
            Ydb::StatusIds::INTERNAL_ERROR,
            NYql::TIssues({NYql::TIssue(TStringBuilder()
                << "while executing by KqpPartitionedExecuterActor")}));
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
                NYql::TIssues({NYql::TIssue(TStringBuilder()
                    << "KqpPartitionedExecuterActor got an unknown error, state = " << CurrentStateFuncName())}));
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
            PE_LOG_I("All executers have been finished, abort KqpPartitionedExecuterActor");
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
        PE_LOG_D("Got TEvError from KqpBufferWriteActor with Id = " << ev->Sender << ", status = "
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

    TMaybe<NKikimrKqp::TKqpTableSinkSettings> FillSinkSettings() {
        NKikimrKqp::TKqpTableSinkSettings settings;

        for (const auto& tx : PreparedQuery->GetTransactions()) {
            for (const auto& stage : tx->GetStages()) {
                for (const auto& sink : stage.GetSinks()) {
                    if (sink.GetTypeCase() == NKqpProto::TKqpSink::kInternalSink && sink.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
                        YQL_ENSURE(sink.GetInternalSink().GetSettings().UnpackTo(&settings), "Failed to unpack settings");
                        if (!settings.GetIsIndexImplTable()) {
                            return settings;
                        }
                    } 
                }
            }
        }

        return Nothing();
    }

    void FillTableMetaInfo() {
        auto settings = FillSinkSettings();
        if (!settings) {
            YQL_ENSURE(false, "Cannot execute a request without sinks");
        }

        TableId = MakeTableId(settings->GetTable());

        switch (settings->GetType()) {
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

        KeyIds.reserve(settings->GetKeyColumns().size());
        KeyColumnTypes.reserve(settings->GetKeyColumns().size());

        for (int i = 0; i < settings->GetKeyColumns().size(); ++i) {
            const auto& column = settings->GetKeyColumns()[i];
            const auto* typeInfo = column.HasTypeInfo() ? &column.GetTypeInfo() : nullptr;
            auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(column.GetTypeId(), typeInfo);

            KeyIds.emplace_back(column.GetId());
            KeyColumnTypes.emplace_back(typeInfoMod.TypeInfo);
        }

        YQL_ENSURE(!KeyIds.empty());
    }

    void ResolvePartitioning() {
        YQL_ENSURE(!KeyIds.empty());

        const TVector<TCell> minKey(KeyIds.size());
        const TTableRange range(minKey, true, {}, false, false);

        YQL_ENSURE(range.IsFullRange(KeyIds.size()));

        auto keyRange = MakeHolder<TKeyDesc>(TableId, range, OperationType, KeyColumnTypes, TVector<TKeyDesc::TColumnOp>{});

        TAutoPtr<NSchemeCache::TSchemeCacheRequest> request(new NSchemeCache::TSchemeCacheRequest());
        request->ResultSet.emplace_back(std::move(keyRange));

        TAutoPtr<TEvTxProxySchemeCache::TEvResolveKeySet> resolveReq(new TEvTxProxySchemeCache::TEvResolveKeySet(request));

        Send(MakeSchemeCacheID(), resolveReq.Release());
    }

    void CreateExecutersWithBuffers() {
        YQL_ENSURE(TablePartitioning);

        Become(&TKqpPartitionedExecuter::ExecuteState);

        auto partCount = std::min(Settings.PartitionExecutionLimit, TablePartitioning->size());
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
        partition->LimitSize = Settings.MaxBatchSize;
        partition->RetryDelayMs = Settings.StartRetryDelayMs;

        return partition;
    }

    void CreateExecuterWithBuffer(TPartitionIndex partitionIndex, bool isRetry) {
        auto partInfo = (isRetry) ? StartedPartitions[partitionIndex] : CreatePartition(partitionIndex);
        auto txAlloc = std::make_shared<TTxAllocatorState>(FuncRegistry, TimeProvider, RandomProvider);

        IKqpGateway::TExecPhysicalRequest newRequest(txAlloc);
        IKqpGateway::TExecPhysicalRequest::FillRequestFrom(newRequest, Request);
        for (auto& tx : Request.Transactions) {
            newRequest.Transactions.emplace_back(tx.Body, tx.Params);
        }

        auto txManager = CreateKqpTransactionManager();
        auto alloc = std::make_shared<NKikimr::NMiniKQL::TScopedAlloc>(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), true, false);
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
            .TraceId = Request.TraceId.GetTraceId(),
            .Counters = RequestCounters->Counters,
            .TxProxyMon = RequestCounters->TxProxyMon,
            .Alloc = std::move(alloc)
        };

        auto* bufferActor = CreateKqpBufferWriterActor(std::move(settings));
        auto bufferActorId = RegisterWithSameMailbox(bufferActor);
 
        TPartitionPruner::TConfig prunerConfig{
            .BatchOperationRange=NBatchOperations::MakePartitionRange(partInfo->BeginRange, partInfo->EndRange, KeyIds.size())
        };

        auto batchSettings = NBatchOperations::TSettings(partInfo->LimitSize, Settings.MinBatchSize);
        const auto executerConfig = TExecuterConfig(MutableExecuterConfig, TableServiceConfig);
        auto executerActor = CreateKqpExecuter(std::move(newRequest), Database, UserToken, TResultSetFormatSettings{}, RequestCounters,
            executerConfig, AsyncIoFactory, PreparedQuery, SelfId(), UserRequestContext, StatementResultIndex,
            FederatedQuerySetup, GUCSettings, prunerConfig, ShardIdToTableInfo, txManager, bufferActorId, std::move(batchSettings));
        auto exId = RegisterWithSameMailbox(executerActor);

        partInfo->ExecuterId = exId;
        partInfo->BufferId = bufferActorId;
        ExecuterToPartition[exId] = BufferToPartition[bufferActorId] = partInfo;

        PE_LOG_I("Create new KQP executer by KqpPartitionedExecuterActor: ExecuterId = " << partInfo->ExecuterId
            << ", PartitionIndex = " << partitionIndex << ", LimitSize = " << partInfo->LimitSize
            << ", RetryDelayMs = " << partInfo->RetryDelayMs);

        auto ev = std::make_unique<TEvTxUserProxy::TEvProposeKqpTransaction>(exId);
        Send(MakeTxProxyID(), ev.release());
    }

    void Abort() {
        Become(&TKqpPartitionedExecuter::AbortState);

        if (CheckExecutersAreFinished()) {
            PE_LOG_I("All executers have been finished, abort KqpPartitionedExecuterActor");
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
        auto abortEv = TEvKqp::TEvAbortExecution::Aborted("Aborted by KqpPartitionedExecuterActor, reason: " + reason);
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
        TSerializedCellVec minKey = GetMinCellVecKey(std::move(ev->BatchOperationMaxKeys), std::move(ev->BatchOperationKeyIds));
        if (minKey) {
            partInfo->BeginRange = TKeyDesc::TPartitionRangeInfo(minKey, /* IsInclusive */ false, /* IsPoint */ false);
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
            PE_LOG_I("All executers have been finished, abort KqpPartitionedExecuterActor");
            RuntimeError(ReturnStatus, ReturnIssues);
        }
    }

    void ScheduleRetryWithNewLimit(TBatchPartitionInfo::TPtr& partInfo) {
        if (partInfo->RetryDelayMs == Settings.MaxRetryDelayMs) {
            ForgetPartition(partInfo);

            if (this->CurrentStateFunc() != &TKqpPartitionedExecuter::AbortState) {
                RuntimeError(
                    Ydb::StatusIds::UNAVAILABLE,
                    NYql::TIssues({NYql::TIssue(TStringBuilder()
                        << "cannot retry query execution because the maximum retry delay has been reached")}));
            }

            return;
        }

        auto newLimit = std::max(partInfo->LimitSize / 2, Settings.MinBatchSize);
        partInfo->LimitSize = newLimit;

        auto ev = std::make_unique<TEvKqpExecuter::TEvTxDelayedExecution>(partInfo->PartitionIndex);
        Schedule(TDuration::MilliSeconds(partInfo->RetryDelayMs), ev.release());

        // We use the init delay value first and change it for the next attempt
        auto decJitterDelay = RandomProvider->Uniform(Settings.StartRetryDelayMs, partInfo->RetryDelayMs * 3ul);
        auto newDelay = std::min(Settings.MaxRetryDelayMs, decJitterDelay);

        partInfo->RetryDelayMs = newDelay;
    }

    bool CheckExecutersAreFinished() const {
        return StartedPartitions.empty();
    }

    bool IsColumnsNeedReorder(const TVector<ui32>& rowColumnIds) {
        if (KeyColumnIdToPos.empty()) {
            for (size_t i = 0; i < rowColumnIds.size(); ++i) {
                KeyColumnIdToPos[rowColumnIds[i]] = i;
            }
        }

        for (size_t i = 0; i < KeyIds.size(); ++i) {
            auto it = KeyColumnIdToPos.find(KeyIds[i]);
            YQL_ENSURE(it != KeyColumnIdToPos.end());

            if (it->second != i) {
                return true;
            }
        }

        return false;
    }

    TSerializedCellVec GetMinCellVecKey(TVector<TSerializedCellVec>&& rows, TVector<ui32>&& rowColumnIds) {
        if (!rowColumnIds.empty() && IsColumnsNeedReorder(rowColumnIds)) {
            std::transform(rows.begin(), rows.end(), rows.begin(), [&](TSerializedCellVec& key) {
                TVector<TCell> newKey;
                newKey.reserve(KeyIds.size());

                for (auto keyId : KeyIds) {
                    auto it = std::find(rowColumnIds.begin(), rowColumnIds.end(), keyId);
                    if (it != rowColumnIds.end()) {
                        newKey.emplace_back(key.GetCells()[it - rowColumnIds.begin()]);
                    } else {
                        YQL_ENSURE(false, "KeyId " << keyId << " not found in readKeyIds");
                    }
                }

                return TSerializedCellVec(std::move(newKey));
            });
        }

        TSerializedCellVec result;

        for (size_t i = 0; i < rows.size(); ++i) {
            const TSerializedCellVec& row = rows[i];
            if (i == 0) {
                result = row;
                continue;
            }

            TConstArrayRef<TCell> resultCells = result.GetCells();
            TConstArrayRef<TCell> cells = row.GetCells();

            YQL_ENSURE(cells.size() == resultCells.size());

            if (CompareTypedCellVectors(resultCells.data(), cells.data(), KeyColumnTypes.data(), KeyColumnTypes.size()) > 0) {
                result = row;
            }
        }

        return result;
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
    IKqpGateway::TExecPhysicalRequest Request;
    std::unique_ptr<TEvKqpExecuter::TEvTxResponse> ResponseEv;
    NBatchOperations::TSettings Settings;

    Ydb::StatusIds::StatusCode ReturnStatus = Ydb::StatusIds::SUCCESS;
    NYql::TIssues ReturnIssues;

    TVector<ui32> KeyIds;
    TVector<NScheme::TTypeInfo> KeyColumnTypes;
    THashMap<ui32, size_t> KeyColumnIdToPos;

    std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> TablePartitioning;
    THashMap<TPartitionIndex, TBatchPartitionInfo::TPtr> StartedPartitions;
    TPartitionIndex NextPartitionIndex = 0;

    THashMap<TActorId, TBatchPartitionInfo::TPtr> ExecuterToPartition;
    THashMap<TActorId, TBatchPartitionInfo::TPtr> BufferToPartition;

    TKeyDesc::ERowOperation OperationType;
    TTableId TableId;

    const TActorId SessionActorId;
    const NMiniKQL::IFunctionRegistry* FuncRegistry;
    TIntrusivePtr<ITimeProvider> TimeProvider;
    TIntrusivePtr<IRandomProvider> RandomProvider;

    // The next variables are only for creating KqpDataExecuterActor and KqpBufferWriteActor
    TString Database;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TKqpRequestCounters::TPtr RequestCounters;
    NKikimrConfig::TTableServiceConfig TableServiceConfig;
    TIntrusivePtr<TExecuterMutableConfig> MutableExecuterConfig;

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
