#include "kqp_partitioned_executer.h"
#include "kqp_executer_stats.h"
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
#include <ydb/core/util/stlog.h>

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr {
namespace NKqp {

namespace {

#define PE_STLOG_T(MESSAGE, ...) STLOG(PRI_TRACE,  NKikimrServices::KQP_EXECUTER, KQPPEA, LogPrefix() << MESSAGE << '.', ##__VA_ARGS__)
#define PE_STLOG_D(MESSAGE, ...) STLOG(PRI_DEBUG,  NKikimrServices::KQP_EXECUTER, KQPPEA, LogPrefix() << MESSAGE << '.', ##__VA_ARGS__)
#define PE_STLOG_I(MESSAGE, ...) STLOG(PRI_INFO,   NKikimrServices::KQP_EXECUTER, KQPPEA, LogPrefix() << MESSAGE << '.', ##__VA_ARGS__)
#define PE_STLOG_N(MESSAGE, ...) STLOG(PRI_NOTICE, NKikimrServices::KQP_EXECUTER, KQPPEA, LogPrefix() << MESSAGE << '.', ##__VA_ARGS__)
#define PE_STLOG_W(MESSAGE, ...) STLOG(PRI_WARN,   NKikimrServices::KQP_EXECUTER, KQPPEA, LogPrefix() << MESSAGE << '.', ##__VA_ARGS__)
#define PE_STLOG_E(MESSAGE, ...) STLOG(PRI_ERROR,  NKikimrServices::KQP_EXECUTER, KQPPEA, LogPrefix() << MESSAGE << '.', ##__VA_ARGS__)
#define PE_STLOG_C(MESSAGE, ...) STLOG(PRI_CRIT,   NKikimrServices::KQP_EXECUTER, KQPPEA, LogPrefix() << MESSAGE << '.', ##__VA_ARGS__)

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

    explicit TKqpPartitionedExecuter(TKqpPartitionedExecuterSettings settings, std::shared_ptr<NYql::NDq::IDqChannelService> channelService)
        : Request(std::move(settings.Request))
        , Stats(Request.StatsMode)
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
        , ChannelService(channelService)
        , QuerySpanId(settings.QuerySpanId)
    {
        ResponseEv = std::make_unique<TEvKqpExecuter::TEvTxResponse>(Request.TxAlloc, TEvKqpExecuter::TEvTxResponse::EExecutionType::Data);

        if (TableServiceConfig.HasBatchOperationSettings()) {
            Settings = NBatchOperations::ImportSettingsFromProto(TableServiceConfig.GetBatchOperationSettings());
        }
    }

    TString LogPrefix() const {
        return TStringBuilder()
            << "ActorId: " << SelfId() << ", "
            << "ActorState: " << CurrentStateFuncName() << ", "
            << "Operation: " << OperationName() << ", "
            << "ActivePartitions: " << StartedPartitions.size() << ", "
            << "Message: ";
    }

    void Bootstrap() {
        Become(&TKqpPartitionedExecuter::PrepareState);

        PE_STLOG_I("Start resolving table partitions");
        Stats.StartTs = TInstant::Now();

        FillTableMetaInfo();
        ResolvePartitioning();
    }

    STFUNC(PrepareState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, HandlePrepare);
                hFunc(TEvKqp::TEvAbortExecution, HandleAbort);
            default:
                AbortWithError(Ydb::StatusIds::INTERNAL_ERROR, NYql::TIssues({NYql::TIssue(TStringBuilder()
                    << "Got an unknown event in PrepareState, "
                    << "ActorId = " << ev->Sender << ", "
                    << "EventType = " << ev->GetTypeRewrite())}));
            }
        } catch (...) {
            AbortWithError(Ydb::StatusIds::INTERNAL_ERROR, NYql::TIssues({NYql::TIssue(TStringBuilder()
                << "Got an unknown error in PrepareState")}));
        }
    }

    void HandlePrepare(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        const auto* request = ev->Get()->Request.Get();

        if (request->ErrorCount > 0) {
            return AbortWithError(Ydb::StatusIds::INTERNAL_ERROR, NYql::TIssues({NYql::TIssue(TStringBuilder()
                << "Could not resolve a partitioning of the table, "
                << "ErrorCount = " << request->ErrorCount)}));
        }

        if (request->ResultSet.size() != 1) {
            return AbortWithError(Ydb::StatusIds::INTERNAL_ERROR, NYql::TIssues({NYql::TIssue(TStringBuilder()
                << "Could not resolve a partitioning of the table, resultSet is empty")}));
        }

        const auto& result = request->ResultSet[0].KeyDescription;
        if (!result || !result->Partitioning) {
            return AbortWithError(Ydb::StatusIds::INTERNAL_ERROR, NYql::TIssues({NYql::TIssue(TStringBuilder()
                << "Could not resolve a partitioning of the table, partitioning is null")}));
        }

        if (result->Partitioning->empty()) {
            return AbortWithError(Ydb::StatusIds::INTERNAL_ERROR, NYql::TIssues({NYql::TIssue(TStringBuilder()
                << "Could not resolve a partitioning of the table, partitioning is empty, "
                << "TableId = " << result->TableId)}));
        }

        TablePartitioning = result->Partitioning;

        PE_STLOG_T("Partitions were resolved",
            (PartitionsCount, result->Partitioning->size()));

        CreateExecutersWithBuffers();
    }

    void HandleAbort(TEvKqp::TEvAbortExecution::TPtr& ev) {
        const auto& msg = ev->Get()->Record;
        auto issues = ev->Get()->GetIssues();

        PE_STLOG_E("Got abort execution",
            (Sender, ev->Sender),
            (FromSessionActor, ev->Sender == SessionActorId),
            (StatusCode, NYql::NDqProto::StatusIds_StatusCode_Name(msg.GetStatusCode())),
            (Issues, issues.ToOneLineString()));

        AbortWithError(NYql::NDq::DqStatusToYdbStatus(msg.GetStatusCode()), issues);
    }

    STFUNC(ExecuteState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpExecuter::TEvTxResponse, HandleExecute);
                hFunc(TEvKqpExecuter::TEvTxDelayedExecution, HandleExecute)
                hFunc(TEvKqp::TEvAbortExecution, HandleAbort);
                hFunc(TEvKqpBuffer::TEvError, HandleExecute);
            default:
                AbortWithError(Ydb::StatusIds::INTERNAL_ERROR, NYql::TIssues({NYql::TIssue(TStringBuilder()
                    << "Got an unknown event in ExecuteState, "
                    << "ActorId = " << ev->Sender << ", "
                    << "EventType = " << ev->GetTypeRewrite())}));
            }
        } catch (...) {
            AbortWithError(Ydb::StatusIds::INTERNAL_ERROR, NYql::TIssues({NYql::TIssue(TStringBuilder()
                << "Got an unknown error in ExecuteState")}));
        }
    }

    void HandleExecute(TEvKqpExecuter::TEvTxResponse::TPtr& ev) {
        auto* response = ev->Get()->Record.MutableResponse();

        NYql::TIssues issues;
        IssuesFromMessage(response->GetIssues(), issues);

        auto it = ExecuterToPartition.find(ev->Sender);
        if (it == ExecuterToPartition.end()) {
            PE_STLOG_W("Got tx response from an unknown executer",
                (Sender, ev->Sender),
                (Status, response->GetStatus()),
                (Issues, issues.ToOneLineString()));

            return TryFinishExecution();
        }

        auto [_, partInfo] = *it;

        PE_STLOG_T("Got tx response",
            (Sender, ev->Sender),
            (PartitionIndex, partInfo->PartitionIndex),
            (Status, response->GetStatus()));

        AbortBuffer(partInfo->BufferId);
        ForgetExecuterAndBuffer(partInfo);

        switch (response->GetStatus()) {
            case Ydb::StatusIds::SUCCESS:
                try {
                    partInfo->RetryDelayMs = Settings.StartRetryDelayMs;
                    partInfo->LimitSize = std::min(partInfo->LimitSize * 2, Settings.MaxBatchSize);
                    return OnSuccessResponse(partInfo, ev->Get());
                } catch (...) {
                    ForgetPartition(partInfo);
                    throw;
                }
            case Ydb::StatusIds::STATUS_CODE_UNSPECIFIED:
            case Ydb::StatusIds::ABORTED:
            case Ydb::StatusIds::UNAVAILABLE:
            case Ydb::StatusIds::OVERLOADED:
            case Ydb::StatusIds::UNDETERMINED:
                PE_STLOG_D("Executer retriable error, will retry",
                    (PartitionIndex, partInfo->PartitionIndex),
                    (Status, response->GetStatus()),
                    (Issues, issues.ToOneLineString()));

                return ScheduleRetryWithNewLimit(partInfo);
            default:
                break;
        }

        PE_STLOG_E("Executer unretriable error",
            (PartitionIndex, partInfo->PartitionIndex),
            (Status, response->GetStatus()),
            (Issues, issues.ToOneLineString()));

        ForgetPartition(partInfo);
        AbortWithError(response->GetStatus(), issues);
    }

    void HandleExecute(TEvKqpExecuter::TEvTxDelayedExecution::TPtr& ev) {
        RequestCounters->Counters->BatchOperationRetries->Inc();

        PE_STLOG_D("Delayed execution timer fired",
            (PartitionIndex, ev->Get()->PartitionIdx));

        auto it = StartedPartitions.find(ev->Get()->PartitionIdx);
        if (it != StartedPartitions.end()) {
            RetryPartExecution(it->second);
        }
    }

    void HandleExecute(TEvKqpBuffer::TEvError::TPtr& ev) {
        const auto& msg = *ev->Get();

        auto it = BufferToPartition.find(ev->Sender);
        if (it == BufferToPartition.end()) {
            PE_STLOG_W("Got error from an unknown buffer",
                (Sender, ev->Sender),
                (Status, NYql::NDqProto::StatusIds_StatusCode_Name(msg.StatusCode)),
                (Issues, msg.Issues.ToOneLineString()));

            return TryFinishExecution();
        }

        auto [_, partInfo] = *it;

        PE_STLOG_T("Got buffer error",
            (Sender, ev->Sender),
            (PartitionIndex, partInfo->PartitionIndex),
            (Status, NYql::NDqProto::StatusIds_StatusCode_Name(msg.StatusCode)));

        AbortExecuter(partInfo->ExecuterId, "got error from KqpBufferWriteActor");
        ForgetExecuterAndBuffer(partInfo);

        switch (msg.StatusCode) {
            case NYql::NDqProto::StatusIds::SUCCESS:
                ForgetPartition(partInfo);
                YQL_ENSURE(false, "Buffer should not return success in TEvError");
                break;
            case NYql::NDqProto::StatusIds::UNSPECIFIED:
            case NYql::NDqProto::StatusIds::ABORTED:
            case NYql::NDqProto::StatusIds::UNAVAILABLE:
            case NYql::NDqProto::StatusIds::OVERLOADED:
            case NYql::NDqProto::StatusIds::UNDETERMINED:
                PE_STLOG_D("Buffer retriable error, will retry",
                    (PartitionIndex, partInfo->PartitionIndex),
                    (Status, NYql::NDqProto::StatusIds_StatusCode_Name(msg.StatusCode)),
                    (Issues, msg.Issues.ToOneLineString()));

                return ScheduleRetryWithNewLimit(partInfo);
            default:
                break;
        }

        PE_STLOG_E("Buffer unretriable error",
            (PartitionIndex, partInfo->PartitionIndex),
            (Status, NYql::NDqProto::StatusIds_StatusCode_Name(msg.StatusCode)),
            (Issues, msg.Issues.ToOneLineString()));

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
                PE_STLOG_W("Got an unknown event",
                    (Sender, ev->Sender),
                    (EventType, ev->GetTypeRewrite()));

                return TryFinishExecution();
            }
        } catch (...) {
            AbortWithError(Ydb::StatusIds::INTERNAL_ERROR, NYql::TIssues({NYql::TIssue(TStringBuilder()
                << "Got an unknown error in AbortState")}));
        }
    }

    void HandleAbort(TEvKqpExecuter::TEvTxResponse::TPtr& ev) {
        const auto& response = ev->Get()->Record.MutableResponse();

        NYql::TIssues issues;
        IssuesFromMessage(response->GetIssues(), issues);

        auto it = ExecuterToPartition.find(ev->Sender);
        if (it == ExecuterToPartition.end()) {
            PE_STLOG_W("Got tx response from an unknown executer",
                (Sender, ev->Sender),
                (Status, response->GetStatus()),
                (Issues, issues.ToOneLineString()));

            return TryFinishExecution();
        }

        auto [_, partInfo] = *it;

        PE_STLOG_T("Got tx response",
            (Sender, ev->Sender),
            (PartitionIndex, partInfo->PartitionIndex),
            (Status, response->GetStatus()),
            (Issues, issues.ToOneLineString()));

        AbortBuffer(partInfo->BufferId);
        ForgetExecuterAndBuffer(partInfo);
        ForgetPartition(partInfo);

        TryFinishExecution();
    }

    void HandleAbort(TEvKqpBuffer::TEvError::TPtr& ev) {
        const auto& msg = *ev->Get();

        auto it = BufferToPartition.find(ev->Sender);
        if (it == BufferToPartition.end()) {
            PE_STLOG_W("Got error from an unknown buffer",
                (Sender, ev->Sender),
                (Status, NYql::NDqProto::StatusIds_StatusCode_Name(msg.StatusCode)),
                (Issues, msg.Issues.ToOneLineString()));

            return TryFinishExecution();
        }

        auto [_, partInfo] = *it;

        PE_STLOG_E("Got buffer error",
            (Sender, ev->Sender),
            (PartitionIndex, partInfo->PartitionIndex),
            (Status, NYql::NDqProto::StatusIds_StatusCode_Name(msg.StatusCode)),
            (Issues, msg.Issues.ToOneLineString()));

        AbortExecuter(partInfo->ExecuterId, "got error from KqpBufferWriteActor");
        ForgetExecuterAndBuffer(partInfo);
        ForgetPartition(partInfo);

        TryFinishExecution();
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
            return "UnknownState";
        }
    }

    TString OperationName() const {
        switch (OperationType) {
            case TKeyDesc::ERowOperation::Update:
                return "BATCH UPDATE";
            case TKeyDesc::ERowOperation::Erase:
                return "BATCH DELETE";
            case TKeyDesc::ERowOperation::Unknown:
                return "BATCH";
            default:
                return "";
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
            return AbortWithError(Ydb::StatusIds::INTERNAL_ERROR, NYql::TIssues({NYql::TIssue(TStringBuilder()
                << "Cannot execute a request without sinks")}));
        }

        TableId = MakeTableId(settings->GetTable());

        switch (settings->GetType()) {
            case NKikimrKqp::TKqpTableSinkSettings::MODE_UPSERT:
            case NKikimrKqp::TKqpTableSinkSettings::MODE_UPSERT_INCREMENT:
            case NKikimrKqp::TKqpTableSinkSettings::MODE_UPDATE_CONDITIONAL:
                OperationType = TKeyDesc::ERowOperation::Update;
                break;
            case NKikimrKqp::TKqpTableSinkSettings::MODE_DELETE:
                OperationType = TKeyDesc::ERowOperation::Erase;
                break;
            default:
                YQL_ENSURE(false, "Unknown operation type for BATCH query");
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

        PE_STLOG_D("Filling table meta info",
            (TableId, TableId),
            (KeyColumnsCount, KeyColumnTypes.size()));

        YQL_ENSURE(!KeyIds.empty());
    }

    void ResolvePartitioning() {
        const TVector<TCell> minKey(KeyIds.size());
        const TTableRange range(minKey, true, {}, false, false);

        YQL_ENSURE(range.IsFullRange(KeyIds.size()));

        PE_STLOG_D("Resolving table partitioning",
            (TableId, TableId),
            (KeyColumnsCount, KeyIds.size()));

        auto keyRange = MakeHolder<TKeyDesc>(TableId, range, OperationType, KeyColumnTypes, TVector<TKeyDesc::TColumnOp>{});

        TAutoPtr<NSchemeCache::TSchemeCacheRequest> request(new NSchemeCache::TSchemeCacheRequest());
        request->DatabaseName = Database;
        request->ResultSet.emplace_back(std::move(keyRange));

        TAutoPtr<TEvTxProxySchemeCache::TEvResolveKeySet> resolveReq(new TEvTxProxySchemeCache::TEvResolveKeySet(request));

        Send(MakeSchemeCacheID(), resolveReq.Release());
    }

    void CreateExecutersWithBuffers() {
        Become(&TKqpPartitionedExecuter::ExecuteState);

        YQL_ENSURE(TablePartitioning && !TablePartitioning->empty(), "No partitions to execute");
        auto partCount = std::min(Settings.PartitionExecutionLimit, TablePartitioning->size());

        PE_STLOG_I("Starting execution, creating executers with buffers",
            (PartitionsCount, TablePartitioning->size()),
            (InFlightPartitionsCount, partCount));

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

        PE_STLOG_D("Created partition",
            (PartitionIndex, idx),
            (HasBeginRange, partition->BeginRange.Defined()),
            (HasEndRange, partition->EndRange.Defined()),
            (InitialLimitSize, partition->LimitSize),
            (InitialRetryDelayMs, partition->RetryDelayMs));

        return partition;
    }

    void CreateExecuterWithBuffer(TPartitionIndex partitionIndex, bool isRetry) {
        TBatchPartitionInfo::TPtr partInfo;
        if (isRetry) {
            auto it = StartedPartitions.find(partitionIndex);
            if (it == StartedPartitions.end()) {
                return;
            }
            partInfo = it->second;
        } else {
            partInfo = CreatePartition(partitionIndex);
        }
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
                PE_STLOG_D("Increase memory limit",
                    (CurrentLimit, currentLimit),
                    (Required, required));
                alloc->SetLimit(required);
            }
        });

        TKqpBufferWriterSettings settings {
            .SessionActorId = SelfId(),
            .TxManager = txManager,
            .TraceId = Request.TraceId.GetTraceId(),
            .QuerySpanId = QuerySpanId,
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
            llvmSettings, {}, 0, ChannelService);
        auto exId = RegisterWithSameMailbox(executerActor);

        partInfo->ExecuterId = exId;
        partInfo->BufferId = bufferActorId;
        ExecuterToPartition[exId] = BufferToPartition[bufferActorId] = partInfo;

        PE_STLOG_D("Created executer",
            (PartitionIndex, partitionIndex),
            (ExecuterId, partInfo->ExecuterId),
            (BufferId, bufferActorId),
            (LimitSize, partInfo->LimitSize),
            (IsRetry, isRetry));

        auto ev = std::make_unique<TEvTxUserProxy::TEvProposeKqpTransaction>(exId);
        Send(MakeTxProxyID(), ev.release());
    }

    void Abort() {
        PE_STLOG_I("Entering AbortState, trying to finish execution",
            (ActivePartitionsCount, StartedPartitions.size()),
            (ReturnStatus, Ydb::StatusIds_StatusCode_Name(ReturnStatus)));

        Become(&TKqpPartitionedExecuter::AbortState);

        if (CheckExecutersAreFinished()) {
            return TryFinishExecution();
        }

        for (auto [exId, partInfo] : ExecuterToPartition) {
            AbortExecuter(exId, ReturnIssues.ToOneLineString());
        }
    }

    void AbortExecuter(TActorId id, const TString& reason) {
        auto abortEv = TEvKqp::TEvAbortExecution::Aborted("Aborted by PEA: " + reason);
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
        Stats.TakeExecStats(std::move(*ev->Record.MutableResponse()->MutableResult()->MutableStats()));
        Stats.AffectedPartitions.insert(partInfo->PartitionIndex);

        TSerializedCellVec minKey = GetMinCellVecKey(std::move(ev->BatchOperationMaxKeys), std::move(ev->BatchOperationKeyIds));
        if (minKey) {
            if (!IsKeyInPartition(minKey.GetCells(), partInfo)) {
                ForgetPartition(partInfo);
                return AbortWithError(Ydb::StatusIds::PRECONDITION_FAILED, NYql::TIssues({NYql::TIssue(TStringBuilder()
                    << "The next key from ReadActor does not belong to the partition, "
                    << "PartitionIndex = " << partInfo->PartitionIndex)}));
            }

            PE_STLOG_D("Partition has more data, continue processing",
                (PartitionIndex, partInfo->PartitionIndex),
                (NextKeyCellsCount, minKey.GetCells().size()));

            partInfo->BeginRange = TKeyDesc::TPartitionRangeInfo(minKey, /* IsInclusive */ false, /* IsPoint */ false);
            return RetryPartExecution(partInfo);
        }

        PE_STLOG_D("Partition finished completely",
            (PartitionIndex, partInfo->PartitionIndex));

        ForgetPartition(partInfo);

        if (NextPartitionIndex < TablePartitioning->size()) {
            return CreateExecuterWithBuffer(NextPartitionIndex++, /* isRetry */ false);
        }

        TryFinishExecution();
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
            PE_STLOG_D("Retrying partition",
                (PartitionIndex, partInfo->PartitionIndex),
                (LimitSize, partInfo->LimitSize),
                (RetryDelayMs, partInfo->RetryDelayMs));

            return CreateExecuterWithBuffer(partInfo->PartitionIndex, /* isRetry */ true);
        }

        PE_STLOG_D("Partition retry cancelled due to AbortState",
            (PartitionIndex, partInfo->PartitionIndex));

        ForgetPartition(partInfo);
        TryFinishExecution();
    }

    void ScheduleRetryWithNewLimit(TBatchPartitionInfo::TPtr& partInfo) {
        if (partInfo->RetryDelayMs == Settings.MaxRetryDelayMs) {
            PE_STLOG_E("Partition reached maximum retry delay",
                (PartitionIndex, partInfo->PartitionIndex),
                (MaxRetryDelayMs, Settings.MaxRetryDelayMs));

            ForgetPartition(partInfo);
            return AbortWithError(Ydb::StatusIds::UNAVAILABLE, NYql::TIssues({NYql::TIssue(TStringBuilder()
                << "Cannot retry query execution because the maximum retry delay has been reached")}));
        }

        const auto decJitterDelay = RandomProvider->Uniform(Settings.StartRetryDelayMs, partInfo->RetryDelayMs * 3ul);
        const auto newDelay = std::min(Settings.MaxRetryDelayMs, decJitterDelay);
        const auto oldLimit = partInfo->LimitSize;
        const auto oldDelay = partInfo->RetryDelayMs;

        partInfo->RetryDelayMs = newDelay;
        partInfo->LimitSize = std::max(partInfo->LimitSize / 2, Settings.MinBatchSize);

        PE_STLOG_D("Scheduling retry for partition",
            (PartitionIndex, partInfo->PartitionIndex),
            (OldDelay, oldDelay),
            (NewDelay, partInfo->RetryDelayMs),
            (OldLimit, oldLimit),
            (NewLimit, partInfo->LimitSize));

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
                PE_STLOG_D("Key columns need reorder to continue processing",
                    (KeyColumnsCount, KeyIds.size()));

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
                        const auto pos = static_cast<size_t>(it - rowColumnIds.begin());
                        YQL_ENSURE(pos < key.GetCells().size(), "Column with KeyId = " << keyId << " not found in the key row");
                        newKey.emplace_back(key.GetCells()[pos]);
                    } else {
                        YQL_ENSURE(false, "KeyId " << keyId << " not found in readKeyIds");
                    }
                }

                return TSerializedCellVec(std::move(newKey));
            });
        }

        YQL_ENSURE(!rowColumnIds.empty() || rows.empty(), "No column ids for key extraction");

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

    void TryFinishExecution() {
        if (CheckExecutersAreFinished()) {
            PE_STLOG_I("All partitions processed, finish execution",
                (Status, Ydb::StatusIds_StatusCode_Name(ReturnStatus)),
                (Issues, ReturnIssues.ToOneLineString()));

            Stats.FinishTs = TInstant::Now();
            Stats.ExportExecStats(*ResponseEv->Record.MutableResponse()->MutableResult()->MutableStats());

            if (ReturnStatus != Ydb::StatusIds::SUCCESS) {
                return ReplyErrorAndDie(ReturnStatus, ReturnIssues);
            }

            return ReplySuccessAndDie();
        }

        PE_STLOG_D("Not all partitions have been processed, cannot finish execution",
            (RemainingPartitionsCount, StartedPartitions.size()),
            (TotalPartitions, TablePartitioning ? TablePartitioning->size() : 0));
    }

    void AbortWithError(Ydb::StatusIds::StatusCode code, const NYql::TIssues& issues) {
        if (CurrentStateFunc() == &TKqpPartitionedExecuter::AbortState) {
            PE_STLOG_N("Ignoring error because already in AbortState",
                (Status, Ydb::StatusIds_StatusCode_Name(code)),
                (Issues, issues.ToOneLineString()));

            return TryFinishExecution();
        }

        PE_STLOG_E("First error occurred",
            (Status, Ydb::StatusIds_StatusCode_Name(code)),
            (Issues, issues.ToOneLineString()));

        ReturnStatus = code;
        ReturnIssues.AddIssues(issues);
        ReturnIssues.AddIssue(TStringBuilder() << "while executing " << OperationName() << " query");

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
        response.SetStatus(ReturnStatus);

        Send(SessionActorId, ResponseEv.release());
        PassAway();
    }

private:
    IKqpGateway::TExecPhysicalRequest Request;
    std::unique_ptr<TEvKqpExecuter::TEvTxResponse> ResponseEv;
    NBatchOperations::TSettings Settings;

    TBatchOperationExecutionStats Stats;
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

    TKeyDesc::ERowOperation OperationType = TKeyDesc::ERowOperation::Unknown;
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
    std::shared_ptr<NYql::NDq::IDqChannelService> ChannelService;
    ui64 QuerySpanId = 0;
};

} // namespace

NActors::IActor* CreateKqpPartitionedExecuter(TKqpPartitionedExecuterSettings settings, std::shared_ptr<NYql::NDq::IDqChannelService> channelService) {
    return new TKqpPartitionedExecuter(std::move(settings), channelService);
}

} // namespace NKqp
} // namespace NKikimr
