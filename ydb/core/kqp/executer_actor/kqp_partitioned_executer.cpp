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

/**
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

        PE_LOG_I("Created " << ActorName << " with maxBatchSize = " << Settings.MaxBatchSize
            << ", partitionExecutionLimit = " << Settings.PartitionExecutionLimit);

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
            AbortWithError(Ydb::StatusIds::INTERNAL_ERROR, NYql::TIssues({NYql::TIssue(TStringBuilder()
                << "KqpPartitionedExecuterActor got an unknown error, state = " << CurrentStateFuncName())}));
        }
    }

    void HandlePrepare(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        auto* request = ev->Get()->Request.Get();

        PE_LOG_D("Got TEvResolveKeySetResult from actorId = " << ev->Sender);

        if (request->ErrorCount > 0) {
            PE_LOG_E("Failed to resolve table partitioning, errorCount = " << request->ErrorCount);
            AbortWithError(Ydb::StatusIds::INTERNAL_ERROR, NYql::TIssues({NYql::TIssue(TStringBuilder()
                << "KqpPartitionedExecuterActor could not resolve a partitioning of the table, state = " << CurrentStateFuncName())}));
            return;
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
            PE_LOG_W("Got TEvAbortExecution from actorId = " << ev->Sender
            << ", status: " << NYql::NDqProto::StatusIds_StatusCode_Name(msg.GetStatusCode())
            << ", message: " << issues.ToOneLineString());

            auto [_, partInfo] = *it;
            AbortBuffer(partInfo->BufferId);
            ForgetExecuterAndBuffer(partInfo);
            ForgetPartition(partInfo);
        } else {
            PE_LOG_D("Got TEvAbortExecution from ActorId = " << ev->Sender
                << ", status: " << NYql::NDqProto::StatusIds_StatusCode_Name(msg.GetStatusCode())
                << ", message: " << issues.ToOneLineString()
                << ", isSessionActor: " << (ev->Sender == SessionActorId));
        }

        AbortWithError(NYql::NDq::DqStatusToYdbStatus(msg.GetStatusCode()), std::move(issues));
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
            AbortWithError(Ydb::StatusIds::INTERNAL_ERROR, NYql::TIssues({NYql::TIssue(TStringBuilder()
                << "KqpPartitionedExecuterActor got an unknown error, state = " << CurrentStateFuncName())}));
        }
    }

    void HandleExecute(TEvKqpExecuter::TEvTxResponse::TPtr& ev) {
        auto* response = ev->Get()->Record.MutableResponse();

        auto it = ExecuterToPartition.find(ev->Sender);
        if (it == ExecuterToPartition.end()) {
            PE_LOG_D("Got TEvTxResponse from unknown actor with actorId = " << ev->Sender
                << ", status = " << response->GetStatus() << ", ignore");
            return;
        }

        auto [_, partInfo] = *it;

        PE_LOG_D("Got TEvTxResponse from actorId = " << ev->Sender
            << ", partitionIndex = " << partInfo->PartitionIndex
            << ", status = " << response->GetStatus());

        AbortBuffer(partInfo->BufferId);
        ForgetExecuterAndBuffer(partInfo);

        switch (response->GetStatus()) {
            case Ydb::StatusIds::SUCCESS:
                PE_LOG_I("Partition " << partInfo->PartitionIndex << " completed successfully");
                partInfo->RetryDelayMs = Settings.StartRetryDelayMs;
                partInfo->LimitSize = std::min(partInfo->LimitSize * 2, Settings.MaxBatchSize);
                return OnSuccessResponse(partInfo, ev->Get());
            case Ydb::StatusIds::STATUS_CODE_UNSPECIFIED:
            case Ydb::StatusIds::ABORTED:
            case Ydb::StatusIds::UNAVAILABLE:
            case Ydb::StatusIds::OVERLOADED:
            case Ydb::StatusIds::UNDETERMINED:
                PE_LOG_N("Partition " << partInfo->PartitionIndex << " will be retried, status = " << response->GetStatus());
                return ScheduleRetryWithNewLimit(partInfo);
            default:
                break;
        }

        ForgetPartition(partInfo);

        NYql::TIssues issues;
        NYql::IssuesFromMessage(response->GetIssues(), issues);
        PE_LOG_W("Partition " << partInfo->PartitionIndex << " failed with status = " << response->GetStatus()
            << ", message: " << issues.ToOneLineString());
        AbortWithError(response->GetStatus(), std::move(issues));
    }

    void HandleExecute(TEvKqpExecuter::TEvTxDelayedExecution::TPtr& ev) {
        RequestCounters->Counters->BatchOperationRetries->Inc();

        auto& partInfo = StartedPartitions[ev->Get()->PartitionIdx];
        PE_LOG_D("Delayed execution timer fired for partitionIndex = " << ev->Get()->PartitionIdx);
        RetryPartExecution(partInfo);
    }

    void HandleExecute(TEvKqpBuffer::TEvError::TPtr& ev) {
        const auto& msg = *ev->Get();

        auto it = BufferToPartition.find(ev->Sender);
        if (it == BufferToPartition.end()) {
            PE_LOG_D("Got TEvError from unknown buffer with actorId = " << ev->Sender << ", status = "
                << NYql::NDqProto::StatusIds_StatusCode_Name(msg.StatusCode) << ", ignore");
            return;
        }

        auto [_, partInfo] = *it;

        PE_LOG_W("Got TEvError from buffer actorId = " << ev->Sender
            << ", partitionIndex = " << partInfo->PartitionIndex
            << ", status = " << NYql::NDqProto::StatusIds_StatusCode_Name(msg.StatusCode)
            << ", message: " << msg.Issues.ToOneLineString());

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
                PE_LOG_N("Partition " << partInfo->PartitionIndex << " buffer error, will retry");
                return ScheduleRetryWithNewLimit(partInfo);
            default:
                break;
        }

        ForgetPartition(partInfo);
        AbortWithError(NYql::NDq::DqStatusToYdbStatus(msg.StatusCode), msg.Issues);
    }

    STFUNC(AbortState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpExecuter::TEvTxResponse, HandleAbort);
                hFunc(TEvKqpExecuter::TEvTxDelayedExecution, HandleExecute)
                hFunc(TEvKqp::TEvAbortExecution, HandleAbort);
                hFunc(TEvKqpBuffer::TEvError, HandleAbort);
            default:
                PE_LOG_W("Got unknown message from actorId = " << ev->Sender);
            }
        } catch (...) {
            AbortWithError(Ydb::StatusIds::INTERNAL_ERROR, NYql::TIssues({NYql::TIssue(TStringBuilder()
                << "KqpPartitionedExecuterActor got an unknown error, state = " << CurrentStateFuncName())}));
        }
    }

    void HandleAbort(TEvKqpExecuter::TEvTxResponse::TPtr& ev) {
        const auto& response = ev->Get()->Record.MutableResponse();
        auto it = ExecuterToPartition.find(ev->Sender);
        if (it == ExecuterToPartition.end()) {
            PE_LOG_D("Got TEvTxResponse in AbortState from unknown actor with actorId = " << ev->Sender
                << ", status = " << response->GetStatus() << ", ignore");
            return;
        }

        auto [_, partInfo] = *it;

        PE_LOG_D("Got TEvTxResponse in AbortState from actorId = " << ev->Sender
            << ", partitionIndex = " << partInfo->PartitionIndex
            << ", status = " << response->GetStatus() << ", finishing partition");

        AbortBuffer(partInfo->BufferId);
        ForgetExecuterAndBuffer(partInfo);
        ForgetPartition(partInfo);

        if (CheckExecutersAreFinished()) {
            PE_LOG_N("All executers have been finished, replying with error: " << Ydb::StatusIds_StatusCode_Name(ReturnStatus));
            ReplyErrorAndDie(ReturnStatus, ReturnIssues);
        }
    }

    void HandleAbort(TEvKqp::TEvAbortExecution::TPtr& ev) {
        auto& msg = ev->Get()->Record;
        auto issues = ev->Get()->GetIssues();

        auto it = ExecuterToPartition.find(ev->Sender);
        if (it == ExecuterToPartition.end()) {
            PE_LOG_D("Got TEvAbortExecution in AbortState from unknown actor with actorId = " << ev->Sender
                << ", status: " << NYql::NDqProto::StatusIds_StatusCode_Name(msg.GetStatusCode())
                << ", message: " << issues.ToOneLineString() << ", ignore");
            return;
        }

        auto [_, partInfo] = *it;

        PE_LOG_D("Got TEvAbortExecution in AbortState from actorId = " << ev->Sender
            << ", partitionIndex = " << partInfo->PartitionIndex
            << ", status: " << NYql::NDqProto::StatusIds_StatusCode_Name(msg.GetStatusCode())
            << ", issues: " << issues.ToOneLineString() << ", finishing partition");

        AbortBuffer(partInfo->BufferId);
        ForgetExecuterAndBuffer(partInfo);
        ForgetPartition(partInfo);

        if (CheckExecutersAreFinished()) {
            PE_LOG_N("All executers have been finished, replying with error: " << Ydb::StatusIds_StatusCode_Name(ReturnStatus));
            ReplyErrorAndDie(ReturnStatus, ReturnIssues);
        }
    }

    void HandleAbort(TEvKqpBuffer::TEvError::TPtr& ev) {
        const auto& msg = *ev->Get();

        auto it = BufferToPartition.find(ev->Sender);
        if (it == BufferToPartition.end()) {
            PE_LOG_D("Got TEvError in AbortState from unknown buffer with actorId = " << ev->Sender << ", status = "
            << NYql::NDqProto::StatusIds_StatusCode_Name(msg.StatusCode) << ", ignore");
            return;
        }

        auto [_, partInfo] = *it;

        PE_LOG_D("Got TEvError in AbortState from buffer actorId = " << ev->Sender
            << ", partitionIndex = " << partInfo->PartitionIndex
            << ", status = " << NYql::NDqProto::StatusIds_StatusCode_Name(msg.StatusCode)
            << ", finishing partition");

        AbortExecuter(partInfo->ExecuterId, "got error from KqpBufferWriteActor");
        ForgetExecuterAndBuffer(partInfo);
        ForgetPartition(partInfo);

        if (CheckExecutersAreFinished()) {
            PE_LOG_N("All executers have been finished, replying with error: " << Ydb::StatusIds_StatusCode_Name(ReturnStatus));
            ReplyErrorAndDie(ReturnStatus, ReturnIssues);
        }
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
        const TVector<TCell> minKey(KeyIds.size());
        const TTableRange range(minKey, true, {}, false, false);

        YQL_ENSURE(range.IsFullRange(KeyIds.size()));

        auto keyRange = MakeHolder<TKeyDesc>(TableId, range, OperationType, KeyColumnTypes, TVector<TKeyDesc::TColumnOp>{});

        TAutoPtr<NSchemeCache::TSchemeCacheRequest> request(new NSchemeCache::TSchemeCacheRequest());
        request->DatabaseName = Database;
        request->ResultSet.emplace_back(std::move(keyRange));

        TAutoPtr<TEvTxProxySchemeCache::TEvResolveKeySet> resolveReq(new TEvTxProxySchemeCache::TEvResolveKeySet(request));

        Send(MakeSchemeCacheID(), resolveReq.Release());
    }

    void CreateExecutersWithBuffers() {
        YQL_ENSURE(TablePartitioning);

        PE_LOG_I("Resolved " << TablePartitioning->size() << " partitions, starting first "
            << std::min(Settings.PartitionExecutionLimit, TablePartitioning->size()) << " in parallel");

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

        TPartitionPrunerConfig prunerConfig{
            .BatchOperationRange = NBatchOperations::MakePartitionRange(partInfo->BeginRange, partInfo->EndRange, KeyIds.size())
        };

        std::optional<TLlvmSettings> llvmSettings;
        if (TableServiceConfig.GetEnableKqpScanQueryUseLlvm()) {
            llvmSettings = PreparedQuery->GetLlvmSettings();
        }

        auto batchSettings = NBatchOperations::TSettings(partInfo->LimitSize, Settings.MinBatchSize);
        const auto executerConfig = TExecuterConfig(MutableExecuterConfig, TableServiceConfig);
        auto executerActor = CreateKqpExecuter(std::move(newRequest), Database, UserToken, NFormats::TFormatsSettings{}, RequestCounters,
            executerConfig, AsyncIoFactory, SelfId(), UserRequestContext, StatementResultIndex,
            FederatedQuerySetup, GUCSettings, prunerConfig, ShardIdToTableInfo, txManager, bufferActorId, std::move(batchSettings),
            llvmSettings, {}, 0);
        auto exId = RegisterWithSameMailbox(executerActor);

        partInfo->ExecuterId = exId;
        partInfo->BufferId = bufferActorId;
        ExecuterToPartition[exId] = BufferToPartition[bufferActorId] = partInfo;

        PE_LOG_D("Created " << (isRetry ? "retry" : "new") << " executer for partitionIndex = " << partitionIndex
            << ", executerId = " << partInfo->ExecuterId
            << ", bufferId = " << bufferActorId
            << ", limitSize = " << partInfo->LimitSize);

        auto ev = std::make_unique<TEvTxUserProxy::TEvProposeKqpTransaction>(exId);
        Send(MakeTxProxyID(), ev.release());
    }

    void Abort() {
        PE_LOG_W("Entering AbortState, returnStatus = " << Ydb::StatusIds_StatusCode_Name(ReturnStatus)
            << ", active partitionsCount = " << StartedPartitions.size());

        Become(&TKqpPartitionedExecuter::AbortState);

        if (CheckExecutersAreFinished()) {
            PE_LOG_N("All executers have been finished, replying with error immediately");
            ReplyErrorAndDie(ReturnStatus, ReturnIssues);
            return;
        }

        PE_LOG_I("Sending abort to " << ExecuterToPartition.size() << " executers");
        for (auto& [exId, partInfo] : ExecuterToPartition) {
            AbortExecuter(exId, ReturnIssues.ToOneLineString());
        }
    }

    void AbortExecuter(TActorId id, const TString& reason) {
        auto abortEv = TEvKqp::TEvAbortExecution::Aborted("Aborted by KqpPartitionedExecuterActor: " + reason);
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
            if (!IsKeyInPartition(minKey.GetCells(), partInfo)) {
                PE_LOG_E("Partition " << partInfo->PartitionIndex << " returned key outside its range");
                ForgetPartition(partInfo);
                AbortWithError(Ydb::StatusIds::PRECONDITION_FAILED, NYql::TIssues({NYql::TIssue(TStringBuilder()
                    << "The next key from KqpReadActor does not belong to the partition with PartitionIndex = "
                    << partInfo->PartitionIndex)}));
                return;
            }

            PE_LOG_D("Partition " << partInfo->PartitionIndex << " has more data, continue processing");
            partInfo->BeginRange = TKeyDesc::TPartitionRangeInfo(minKey, /* IsInclusive */ false, /* IsPoint */ false);
            return RetryPartExecution(partInfo);
        }

        PE_LOG_D("Partition " << partInfo->PartitionIndex << " finished completely");
        ForgetPartition(partInfo);

        if (NextPartitionIndex < TablePartitioning->size()) {
            PE_LOG_D("Starting next partition " << NextPartitionIndex << " of " << TablePartitioning->size());
            return CreateExecuterWithBuffer(NextPartitionIndex++, /* isRetry */ false);
        }

        if (CheckExecutersAreFinished()) {
            if (ReturnStatus != Ydb::StatusIds::SUCCESS) {
                PE_LOG_N("All partitions processed, but have error: " << Ydb::StatusIds_StatusCode_Name(ReturnStatus));
                ReplyErrorAndDie(ReturnStatus, ReturnIssues);
            } else {
                PE_LOG_I("All partitions processed successfully");
                ReplySuccessAndDie();
            }
        }
    }

    bool IsKeyInPartition(const TConstArrayRef<TCell>& key, const TBatchPartitionInfo::TPtr& partInfo) {
        bool isGEThanBegin = !partInfo->BeginRange || CompareBorders<true, true>(key,
            partInfo->BeginRange->EndKeyPrefix.GetCells(), true, true, KeyColumnTypes) >= 0;
        bool isLEThanEnd = !partInfo->EndRange || CompareBorders<true, true>(key,
            partInfo->EndRange->EndKeyPrefix.GetCells(), true, true, KeyColumnTypes) <= 0;

        return isGEThanBegin && isLEThanEnd;
    }

    void RetryPartExecution(const TBatchPartitionInfo::TPtr& partInfo) {
        if (CurrentStateFunc() != &TKqpPartitionedExecuter::AbortState) {
            PE_LOG_D("Retrying partition " << partInfo->PartitionIndex
                << ", limitSize = " << partInfo->LimitSize
                << ", retryDelayMs = " << partInfo->RetryDelayMs);
            return CreateExecuterWithBuffer(partInfo->PartitionIndex, /* isRetry */ true);
        }

        PE_LOG_D("Partition " << partInfo->PartitionIndex << " retry cancelled due to AbortState");
        ForgetPartition(partInfo);

        if (CheckExecutersAreFinished()) {
            PE_LOG_N("All executers have been finished, replying with error: " << Ydb::StatusIds_StatusCode_Name(ReturnStatus));
            ReplyErrorAndDie(ReturnStatus, ReturnIssues);
        }
    }

    void ScheduleRetryWithNewLimit(TBatchPartitionInfo::TPtr& partInfo) {
        if (partInfo->RetryDelayMs == Settings.MaxRetryDelayMs) {
            PE_LOG_E("Partition " << partInfo->PartitionIndex << " reached maximum retry delay ("
                << Settings.MaxRetryDelayMs << " ms), giving up");
            ForgetPartition(partInfo);
            auto issues = NYql::TIssues({
                NYql::TIssue(TStringBuilder() << "Cannot retry query execution because the maximum retry delay has been reached"),
            });
            AbortWithError(Ydb::StatusIds::UNAVAILABLE, std::move(issues));
            return;
        }

        auto decJitterDelay = RandomProvider->Uniform(Settings.StartRetryDelayMs, partInfo->RetryDelayMs * 3ul);
        auto newDelay = std::min(Settings.MaxRetryDelayMs, decJitterDelay);
        auto oldLimit = partInfo->LimitSize;
        auto oldDelay = partInfo->RetryDelayMs;

        partInfo->RetryDelayMs = newDelay;
        partInfo->LimitSize = std::max(partInfo->LimitSize / 2, Settings.MinBatchSize);

        PE_LOG_N("Scheduling retry for partition " << partInfo->PartitionIndex
            << ", delay: " << oldDelay << " -> " << partInfo->RetryDelayMs << " ms"
            << ", batch size: " << oldLimit << " -> " << partInfo->LimitSize);

        auto ev = std::make_unique<TEvKqpExecuter::TEvTxDelayedExecution>(partInfo->PartitionIndex);
        Schedule(TDuration::MilliSeconds(partInfo->RetryDelayMs), ev.release());
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

    void AbortWithError(Ydb::StatusIds::StatusCode code, const NYql::TIssues& issues) {
        if (CurrentStateFunc() == &TKqpPartitionedExecuter::AbortState) {
            PE_LOG_D("Ignoring error " << Ydb::StatusIds_StatusCode_Name(code)
                << " because already in AbortState with error: " << Ydb::StatusIds_StatusCode_Name(ReturnStatus));

            if (CheckExecutersAreFinished()) {
                PE_LOG_N("All executers have been finished, replying with error immediately");
                ReplyErrorAndDie(ReturnStatus, ReturnIssues);
            }
            return;
        }

        PE_LOG_E("First error occurred: " << Ydb::StatusIds_StatusCode_Name(code)
            << ", issues: " << issues.ToOneLineString());

        ReturnStatus = code;
        ReturnIssues.AddIssues(issues);
        ReturnIssues.AddIssue(TStringBuilder() << "while executing BATCH UPDATE/DELETE by KqpPartitionedExecuterActor");
        Abort();
    }

    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues) {
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> protoIssues;
        IssuesToMessage(issues, &protoIssues);
        ReplyErrorAndDie(status, &protoIssues);
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

    void ReplySuccessAndDie() {
        auto& response = *ResponseEv->Record.MutableResponse();
        response.SetStatus(Ydb::StatusIds::SUCCESS);
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

NActors::IActor* CreateKqpPartitionedExecuter(TKqpPartitionedExecuterSettings settings) {
    return new TKqpPartitionedExecuter(std::move(settings));
}

} // namespace NKqp
} // namespace NKikimr
