#include "kqp_executer.h"
#include "kqp_executer_impl.h"
#include "kqp_locks_helper.h"
#include "kqp_planner.h"
#include "kqp_tasks_validate.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/client/minikql_compile/db_key_resolver.h>
#include <ydb/core/fq/libs/checkpointing/checkpoint_coordinator.h>
#include <ydb/core/kqp/common/buffer/events.h>
#include <ydb/core/kqp/common/kqp_data_integrity_trails.h>
#include <ydb/core/kqp/common/kqp_tx_manager.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/common/simple/reattach.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_actor.h>
#include <ydb/core/kqp/common/kqp_tx.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/opt/kqp_query_plan.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/data_events/common/error_codes.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/tx/long_tx_service/public/lock_handle.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/persqueue/events/global.h>

#include <ydb/library/yql/dq/actors/compute/dq_checkpoints.h>
#include <ydb/library/yql/dq/runtime/dq_columns_resolve.h>
#include <ydb/library/yql/dq/tasks/dq_connection_builder.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/wilson_ids/wilson.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NDq;
using namespace NLongTxService;

namespace {

static constexpr ui32 ReplySizeLimit = 48 * 1024 * 1024; // 48 MB

class TKqpDataExecuter : public TKqpExecuterBase<TKqpDataExecuter, EExecType::Data> {
    using TBase = TKqpExecuterBase<TKqpDataExecuter, EExecType::Data>;
    using TKqpSnapshot = IKqpGateway::TKqpSnapshot;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_DATA_EXECUTER_ACTOR;
    }

    TKqpDataExecuter(IKqpGateway::TExecPhysicalRequest&& request, const TString& database,
        const TIntrusiveConstPtr<NACLib::TUserToken>& userToken,
        NFormats::TFormatsSettings formatsSettings, TKqpRequestCounters::TPtr counters,
        const TExecuterConfig& executerConfig,
        NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
        const TActorId& creator, const TIntrusivePtr<TUserRequestContext>& userRequestContext,
        ui32 statementResultIndex, const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup,
        const TGUCSettings::TPtr& GUCSettings,
        TPartitionPrunerConfig partitionPrunerConfig,
        const TShardIdToTableInfoPtr& shardIdToTableInfo,
        const IKqpTransactionManagerPtr& txManager,
        const TActorId bufferActorId,
        TMaybe<NBatchOperations::TSettings> batchOperationSettings,
        const NKikimrConfig::TQueryServiceConfig& queryServiceConfig,
        ui64 generation,
        std::shared_ptr<NYql::NDq::IDqChannelService> channelService,
        TVector<NKikimr::TTableId> tableIdsForSnapshot)
        : TBase(std::move(request), std::move(asyncIoFactory), federatedQuerySetup, GUCSettings, std::move(partitionPrunerConfig),
            database, userToken, std::move(formatsSettings), counters,
            executerConfig, userRequestContext, statementResultIndex, TWilsonKqp::DataExecuter,
            "DataExecuter", bufferActorId, txManager, std::move(batchOperationSettings), channelService)
        , ShardIdToTableInfo(shardIdToTableInfo)
        , TableIdsForSnapshot(std::move(tableIdsForSnapshot))
        , ReadOnlyTx(IsReadOnlyTx())
        , WaitCAStatsTimeout(TDuration::MilliSeconds(executerConfig.TableServiceConfig.GetQueryLimits().GetWaitCAStatsTimeoutMs()))
        , QueryServiceConfig(queryServiceConfig)
        , Generation(generation)
    {
        TasksGraph.GetMeta().AllowOlapDataQuery = executerConfig.TableServiceConfig.GetAllowOlapDataQuery();
        Target = creator;

        YQL_ENSURE(Request.IsolationLevel != NKqpProto::ISOLATION_LEVEL_UNDEFINED);

        if (Request.AcquireLocksTxId || Request.LocksOp == ELocksOp::Commit || Request.LocksOp == ELocksOp::Rollback) {
            YQL_ENSURE(Request.IsolationLevel == NKqpProto::ISOLATION_LEVEL_SERIALIZABLE
                || Request.IsolationLevel == NKqpProto::ISOLATION_LEVEL_SNAPSHOT_RW);
        }
    }

    bool CheckExecutionComplete() {
        if (TBase::CheckExecutionComplete()) {
            return true;
        }

        if (IsDebugLogEnabled()) {
            auto sb = TStringBuilder() << "ActorState: " << CurrentStateFuncName()
                << ", waiting for " << (Planner ? Planner->GetPendingComputeActors().size() : 0) << " compute actor(s): ";
            if (Planner) {
                for (const auto& shardId : Planner->GetPendingComputeActors()) {
                    sb << "CA " << shardId.first << ", ";
                }
            }
            KQP_STLOG_D(KQPDATA, sb,
                (trace_id, TraceId()));
        }
        return false;
    }

    bool ForceAcquireSnapshot() const {
        const bool forceSnapshot = (
            !GetSnapshot().IsValid() &&
            ReadOnlyTx &&
            !ImmediateTx &&
            !HasPersistentChannels &&
            !HasOlapTable &&
            (!Database.empty() || AppData()->EnableMvccSnapshotWithLegacyDomainRoot)
        );

        return forceSnapshot;
    }

    // TODO: simplified way to detect if we should use followers - should be refactored away.
    bool GetSimplifiedUseFollowers() const {
        size_t sourceScanPartitionsCount = 0;
        bool unknownAffectedShardCount = false;

        for (auto& [_, stageInfo] : TasksGraph.GetStagesInfo()) {
            const auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

            const bool isComputeTask = !stageInfo.Meta.IsSysView() && stageInfo.Meta.ShardOperations.empty()
                || (stage.SinksSize() + stage.OutputTransformsSize() > 0 && (
                    !(TasksGraph.GetMeta().AllowOlapDataQuery || TasksGraph.GetMeta().StreamResult)
                    || !stageInfo.Meta.IsOlap()
                    || !stageInfo.Meta.HasReads())
                );

            if (stage.SourcesSize() > 0 && stage.GetSources(0).GetTypeCase() == NKqpProto::TKqpSource::kReadRangesSource) {
                const auto& source = stage.GetSources(0).GetReadRangesSource();
                const auto& partitions = stageInfo.Meta.PrunedPartitions.at(0);
                bool isSequentialInFlight = source.GetSequentialInFlightShards() > 0 && partitions.size() > source.GetSequentialInFlightShards();

                if (partitions.empty()) {
                    continue;
                }

                if (isSequentialInFlight) {
                    unknownAffectedShardCount = true;
                } else {
                    sourceScanPartitionsCount += partitions.size();
                }
            } else if (stage.SourcesSize() > 0 && stage.GetSources(0).GetTypeCase() == NKqpProto::TKqpSource::kFullTextSource) {
                unknownAffectedShardCount = true;
            } else if (stage.SourcesSize() > 0 && stage.GetSources(0).GetTypeCase() == NKqpProto::TKqpSource::kVectorIndexSource) {
                unknownAffectedShardCount = true;
            } else if (isComputeTask) {
                for (ui32 inputIndex = 0; inputIndex < stage.InputsSize(); ++inputIndex) {
                    const auto& input = stage.GetInputs(inputIndex);
                    if (input.GetTypeCase() == NKqpProto::TKqpPhyConnection::kStreamLookup ||
                        input.GetTypeCase() == NKqpProto::TKqpPhyConnection::kVectorResolve)
                    {
                        unknownAffectedShardCount = true;
                    }
                }
            }
        }

        bool isSingleShardRead = sourceScanPartitionsCount <= 1 && !unknownAffectedShardCount && !HasOlapTable;

        return
            Request.IsolationLevel == NKqpProto::ISOLATION_LEVEL_READ_STALE &&
            !GetSnapshot().IsValid() &&
            ReadOnlyTx && (
                isSingleShardRead ||
                HasPersistentChannels ||
                HasOlapTable ||
                (Database.empty() && !AppData()->EnableMvccSnapshotWithLegacyDomainRoot)
            );
    }

    bool GetUseFollowers() const {
        return (
            // first, we must specify read stale flag.
            Request.IsolationLevel == NKqpProto::ISOLATION_LEVEL_READ_STALE &&
            // next, if snapshot is already defined, so in this case followers are not allowed.
            !GetSnapshot().IsValid() &&
            // ensure that followers are allowed only for read only transactions.
            ReadOnlyTx &&
            // if we are forced to acquire snapshot by some reason, so we cannot use followers.
            !ForceAcquireSnapshot()
        );
    }

    void Finalize() {
        Y_ABORT_UNLESS(!AlreadyReplied);

        FillLocksFromExtraData();
        TxManager->SetHasSnapshot(GetSnapshot().IsValid());

        if (!BufferActorId || (ReadOnlyTx && Request.LocksOp != ELocksOp::Rollback)) {
            Become(&TKqpDataExecuter::FinalizeState);
            MakeResponseAndPassAway();
            return;
        }  else if (Request.LocksOp == ELocksOp::Commit && !ReadOnlyTx) {
            Become(&TKqpDataExecuter::FinalizeState);
            KQP_STLOG_D(KQPDATA, "Send Commit to BufferActor",
                (buffer_actor_id, BufferActorId),
                (trace_id, TraceId()));

            auto event = std::make_unique<NKikimr::NKqp::TEvKqpBuffer::TEvCommit>();
            event->ExecuterActorId = SelfId();
            event->TxId = TxId;
            Send<ESendingType::Tail>(
                BufferActorId,
                event.release(),
                IEventHandle::FlagTrackDelivery,
                0,
                ExecuterSpan.GetTraceId());
            return;
        } else if (Request.LocksOp == ELocksOp::Rollback) {
            Become(&TKqpDataExecuter::FinalizeState);
            KQP_STLOG_D(KQPDATA, "Send Rollback to BufferActor",
                (buffer_actor_id, BufferActorId),
                (trace_id, TraceId()));

            auto event = std::make_unique<NKikimr::NKqp::TEvKqpBuffer::TEvRollback>();
            event->ExecuterActorId = SelfId();
            Send<ESendingType::Tail>(
                BufferActorId,
                event.release(),
                IEventHandle::FlagTrackDelivery,
                0,
                ExecuterSpan.GetTraceId());
            return;
        } else if (Request.UseImmediateEffects) {
            Become(&TKqpDataExecuter::FinalizeState);
            KQP_STLOG_D(KQPDATA, "Send Flush to BufferActor",
                (buffer_actor_id, BufferActorId),
                (trace_id, TraceId()));

            auto event = std::make_unique<NKikimr::NKqp::TEvKqpBuffer::TEvFlush>();
            event->ExecuterActorId = SelfId();
            Send<ESendingType::Tail>(
                BufferActorId,
                event.release(),
                IEventHandle::FlagTrackDelivery,
                0,
                ExecuterSpan.GetTraceId());
            return;
        } else {
            Become(&TKqpDataExecuter::FinalizeState);
            MakeResponseAndPassAway();
            return;
        }
    }

    STATEFN(FinalizeState) {
        try {
            switch(ev->GetTypeRewrite()) {
                hFunc(TEvKqp::TEvAbortExecution, HandleFinalize);
                hFunc(TEvKqpBuffer::TEvError, Handle);
                hFunc(TEvKqpBuffer::TEvResult, HandleFinalize);
                hFunc(TEvents::TEvUndelivered, HandleFinalize);

                IgnoreFunc(TEvDqCompute::TEvState);
                IgnoreFunc(TEvDqCompute::TEvNodeState);
                IgnoreFunc(TEvDqCompute::TEvChannelData);
                IgnoreFunc(TEvDqCompute::TEvResumeExecution);
                IgnoreFunc(TEvKqpExecuter::TEvStreamDataAck);
                IgnoreFunc(TEvInterconnect::TEvNodeDisconnected);
                IgnoreFunc(TEvKqpNode::TEvStartKqpTasksResponse);
                IgnoreFunc(TEvInterconnect::TEvNodeConnected);
                IgnoreFunc(NFq::TEvCheckpointCoordinator::TEvZeroCheckpointDone);
                IgnoreFunc(NFq::TEvCheckpointCoordinator::TEvRaiseTransientIssues);
                default:
                    UnexpectedEvent("FinalizeState", ev->GetTypeRewrite());
            }
        } catch (const yexception& e) {
            InternalError(e.what());
        }
        ReportEventElapsedTime();
    }

    void HandleFinalize(TEvKqp::TEvAbortExecution::TPtr& ev) {
        if (IsCancelAfterAllowed(ev)) {
            TBase::HandleAbortExecution(ev);
        } else {
            KQP_STLOG_D(KQPDATA, "Got TEvAbortExecution, but cancellation is not allowed",
                (sender, ev->Sender),
                (trace_id, TraceId()));
        }
    }

    void HandleFinalize(TEvKqpBuffer::TEvResult::TPtr& ev) {
        if (ev->Get()->Stats && Stats) {
            Stats->AddBufferStats(std::move(*ev->Get()->Stats));
        }
        MakeResponseAndPassAway();
    }

    void HandleFinalize(TEvents::TEvUndelivered::TPtr& ev) {
        AFL_ENSURE(ev->Sender == BufferActorId);
        KQP_STLOG_W(KQPDATA, "Got Undelivered from BufferActor",
            (sender, ev->Sender),
            (trace_id, TraceId()));
    }

    void MakeResponseAndPassAway() {
        ResponseEv->Record.MutableResponse()->SetStatus(Ydb::StatusIds::SUCCESS);
        Counters->TxProxyMon->ReportStatusOK->Inc();

        ResponseEv->Snapshot = GetSnapshot();

        if (LockHandle) {
            // Keep LockHandle even if locks are empty.
            ResponseEv->LockHandle = std::move(LockHandle);
        }

        for (const ui64& shardId : TxManager->GetShards()) {
            Stats->AffectedShards.insert(shardId);
        }

        auto resultSize = ResponseEv->GetByteSize();
        if (resultSize > (int)ReplySizeLimit) {
            TString message;
            if (ResponseEv->TxResults.size() == 1 && !ResponseEv->TxResults[0].QueryResultIndex.Defined()) {
                message = TStringBuilder() << "Intermediate data materialization exceeded size limit"
                    << " (" << resultSize << " > " << ReplySizeLimit << ")."
                    << " This usually happens when trying to write large amounts of data or to perform lookup"
                    << " by big collection of keys in single query. Consider using smaller batches of data.";
            } else {
                message = TStringBuilder() << "Query result size limit exceeded. ("
                    << resultSize << " > " << ReplySizeLimit << ")";
            }

            auto issue = YqlIssue({}, TIssuesIds::KIKIMR_RESULT_UNAVAILABLE, message);
            ReplyErrorAndDie(Ydb::StatusIds::PRECONDITION_FAILED, issue);
            Counters->Counters->TxReplySizeExceededError->Inc();
            return;
        }

        LWTRACK(KqpDataExecuterFinalize, ResponseEv->Orbit, TxId, ResponseEv->ResultsSize(), ResponseEv->GetByteSize());

        ExecuterSpan.EndOk();

        AlreadyReplied = true;
        PassAway();
    }

    STATEFN(WaitResolveState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpExecuter::TEvTableResolveStatus, HandleResolve);
                hFunc(NShardResolver::TEvShardsResolveStatus, HandleResolve);
                hFunc(TEvPrivate::TEvResourcesSnapshot, HandleResolve);
                hFunc(TEvSaveScriptExternalEffectResponse, HandleResolve);
                hFunc(TEvSaveScriptPhysicalGraphResponse, HandleResolve);
                hFunc(TEvDescribeSecretsResponse, HandleResolve);
                hFunc(TEvKqp::TEvAbortExecution, HandleAbortExecution);
                hFunc(TEvKqpBuffer::TEvError, Handle);
                default:
                    UnexpectedEvent("WaitResolveState", ev->GetTypeRewrite());
            }

        } catch (const yexception& e) {
            InternalError(e.what());
        } catch (const TMemoryLimitExceededException&) {
            RuntimeError(Ydb::StatusIds::PRECONDITION_FAILED, NYql::TIssues({NYql::TIssue(BuildMemoryLimitExceptionMessage())}));
        }
        ReportEventElapsedTime();
    }

private:
    bool IsCancelAfterAllowed(const TEvKqp::TEvAbortExecution::TPtr& ev) const {
        return ReadOnlyTx || ev->Get()->Record.GetStatusCode() != NYql::NDqProto::StatusIds::CANCELLED;
    }

    TString CurrentStateFuncName() const override {
        const auto& func = CurrentStateFunc();
        if (func == &TThis::ExecuteState) {
            return "ExecuteState";
        } else if (func == &TThis::WaitSnapshotState) {
            return "WaitSnapshotState";
        } else if (func == &TThis::WaitResolveState) {
            return "WaitResolveState";
        } else if (func == &TThis::WaitShutdownState) {
            return "WaitShutdownState";
        } else if (func == &TThis::FinalizeState) {
            return "FinalizeState";
        } else {
            return TBase::CurrentStateFuncName();
        }
    }

private:
    STATEFN(ExecuteState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvents::TEvUndelivered, HandleUndelivered);
                hFunc(TEvPrivate::TEvRetry, HandleRetry);
                hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
                hFunc(TEvKqpNode::TEvStartKqpTasksResponse, HandleStartKqpTasksResponse);
                hFunc(TEvDqCompute::TEvState, HandleComputeState);
                hFunc(TEvDqCompute::TEvNodeState, HandleNodeState);
                hFunc(TEvDqCompute::TEvChannelData, HandleChannelData);
                hFunc(TEvDqCompute::TEvResumeExecution, HandleResultData); // from Fast Channels
                hFunc(TEvKqpExecuter::TEvStreamDataAck, HandleStreamAck);
                hFunc(TEvKqp::TEvAbortExecution, HandleExecute);
                hFunc(TEvKqpBuffer::TEvError, Handle);
                hFunc(NFq::TEvCheckpointCoordinator::TEvZeroCheckpointDone, Handle);
                hFunc(NFq::TEvCheckpointCoordinator::TEvRaiseTransientIssues, Handle);
                hFunc(NActors::NMon::TEvHttpInfo, HandleHttpInfo);
                IgnoreFunc(TEvInterconnect::TEvNodeConnected);
                default:
                    UnexpectedEvent("ExecuteState", ev->GetTypeRewrite());
            }
        } catch (const yexception& e) {
            InternalError(e.what());
        } catch (const TMemoryLimitExceededException) {
            if (ReadOnlyTx) {
                RuntimeError(Ydb::StatusIds::PRECONDITION_FAILED, NYql::TIssues({NYql::TIssue(BuildMemoryLimitExceptionMessage())}));
            } else {
                RuntimeError(Ydb::StatusIds::UNDETERMINED, NYql::TIssues({NYql::TIssue(BuildMemoryLimitExceptionMessage())}));
            }
        }
        ReportEventElapsedTime();
    }

    void HandleExecute(TEvKqp::TEvAbortExecution::TPtr& ev) {
        if (IsCancelAfterAllowed(ev)) {
            TBase::HandleAbortExecution(ev);
        } else {
            KQP_STLOG_D(KQPDATA, "Got TEvAbortExecution, but cancellation is not allowed",
                (sender, ev->Sender),
                (trace_id, TraceId()));
        }
    }

    void Handle(TEvKqpBuffer::TEvError::TPtr& ev) {
        auto& msg = *ev->Get();
        if (msg.Stats && Stats) {
            Stats->AddBufferStats(std::move(*msg.Stats));
        }
        TBase::HandleAbortExecution(msg.StatusCode, msg.Issues, false);
    }

private:
    bool IsReadOnlyTx() const {
        if (TxManager->GetTopicOperations().HasOperations()) {
            YQL_ENSURE(!Request.UseImmediateEffects);
            return false;
        }

        if (Request.LocksOp == ELocksOp::Commit) {
            YQL_ENSURE(!Request.UseImmediateEffects);
            return false;
        }

        for (const auto& tx : Request.Transactions) {
            for (const auto& stage : tx.Body->GetStages()) {
                if (stage.GetIsEffectsStage()) {
                    return false;
                }
            }
        }

        return true;
    }

    bool WaitRequired() const {
        return SecretSnapshotRequired || ResourceSnapshotRequired || SaveScriptExternalEffectRequired;
    }

    void HandleResolve(TEvDescribeSecretsResponse::TPtr& ev) {
        YQL_ENSURE(ev->Get()->Description.Status == Ydb::StatusIds::SUCCESS, "failed to get secrets snapshot with issues: " << ev->Get()->Description.Issues.ToOneLineString());

        for (size_t i = 0; i < SecretNames.size(); ++i) {
            TasksGraph.GetMeta().SecureParams.emplace(SecretNames[i], ev->Get()->Description.SecretValues[i]);
        }

        SecretSnapshotRequired = false;
        if (!WaitRequired()) {
            Execute();
        }
    }

    void HandleResolve(TEvPrivate::TEvResourcesSnapshot::TPtr& ev) {
        if (ev->Get()->Snapshot.empty()) {
            KQP_STLOG_E(KQPDATA, "Can not find default state storage group for database",
                (database, Database),
                (trace_id, TraceId()));
        }
        ResourcesSnapshot = std::move(ev->Get()->Snapshot);
        ResourceSnapshotRequired = false;
        if (!WaitRequired()) {
            Execute();
        }
    }

    void HandleResolve(TEvSaveScriptExternalEffectResponse::TPtr& ev) {
        YQL_ENSURE(ev->Get()->Status == Ydb::StatusIds::SUCCESS, "failed to save script external effect with issues: " << ev->Get()->Issues.ToOneLineString());

        SaveScriptExternalEffectRequired = false;
        if (!WaitRequired()) {
            Execute();
        }
    }

    void DoExecute() {
        const auto& requestContext = GetUserRequestContext();
        auto scriptExternalEffect = std::make_unique<TEvSaveScriptExternalEffectRequest>(
            requestContext->CurrentExecutionId, requestContext->Database,
            requestContext->CustomerSuppliedId
        );
        for (const auto& transaction : Request.Transactions) {
            for (const auto& secretName : transaction.Body->GetSecretNames()) {
                SecretSnapshotRequired = true;
                SecretNames.push_back(secretName);
            }
            for (const auto& stage : transaction.Body->GetStages()) {
                if (stage.SourcesSize() > 0 && stage.GetSources(0).GetTypeCase() == NKqpProto::TKqpSource::kExternalSource) {
                    ResourceSnapshotRequired = true;
                    HasExternalSources = true;
                }
                if (requestContext->CurrentExecutionId) {
                    for (const auto& sink : stage.GetSinks()) {
                        if (sink.GetTypeCase() == NKqpProto::TKqpSink::kExternalSink) {
                            SaveScriptExternalEffectRequired = true;
                            scriptExternalEffect->Description.Sinks.push_back(sink.GetExternalSink());
                        }
                    }
                }
            }
        }
        scriptExternalEffect->Description.SecretNames = SecretNames;

        if (!WaitRequired()) {
            return Execute();
        }
        if (SecretSnapshotRequired) {
            GetSecretsSnapshot();
        }
        if (ResourceSnapshotRequired) {
            GetResourcesSnapshot();
        }
        if (SaveScriptExternalEffectRequired) {
            SaveScriptExternalEffect(std::move(scriptExternalEffect));
        }
    }

    bool HasDmlOperationOnOlap(NKqpProto::TKqpPhyTx_EType queryType, const NKqpProto::TKqpPhyStage& stage) {
        if (queryType == NKqpProto::TKqpPhyTx::TYPE_DATA && !TasksGraph.GetMeta().AllowOlapDataQuery) {
            return true;
        }

        for (const auto& input : stage.GetInputs()) {
            if (input.GetTypeCase() == NKqpProto::TKqpPhyConnection::kStreamLookup) {
                return true;
            }
        }

        for (const auto &tableOp : stage.GetTableOps()) {
            if (tableOp.GetTypeCase() != NKqpProto::TKqpPhyTableOperation::kReadOlapRange) {
                return true;
            }
        }

        return false;
    }

    void Execute() {
        LWTRACK(KqpDataExecuterStartExecute, ResponseEv->Orbit, TxId);

        // TODO: move graph restoration outside of executer
        const bool graphRestored = RestoreTasksGraph();

        NDq::TTxId dqTxId = TxId;
        if (GetUserRequestContext() && GetUserRequestContext()->StreamingQueryPath) {
            dqTxId = GetUserRequestContext()->StreamingQueryPath;
        }

        for (ui32 txIdx = 0; txIdx < Request.Transactions.size(); ++txIdx) {
            const auto& tx = Request.Transactions[txIdx];
            for (ui32 stageIdx = 0; stageIdx < tx.Body->StagesSize(); ++stageIdx) {
                const auto& stage = tx.Body->GetStages(stageIdx);
                auto& stageInfo = TasksGraph.GetStageInfo(TStageId(txIdx, stageIdx));

                if (stageInfo.Meta.ShardKind == NSchemeCache::ETableKind::KindAsyncIndexTable) {
                    TMaybe<TString> error;

                    if (stageInfo.Meta.ShardKey->RowOperation != TKeyDesc::ERowOperation::Read) {
                        error = TStringBuilder() << "Non-read operations can't be performed on async index table"
                            << ": " << stageInfo.Meta.ShardKey->TableId;
                    } else if (Request.IsolationLevel != NKqpProto::ISOLATION_LEVEL_READ_STALE) {
                        error = TStringBuilder() << "Read operation can be performed on async index table"
                            << ": " << stageInfo.Meta.ShardKey->TableId << " only with StaleRO isolation level";
                    }

                    if (error) {
                        KQP_STLOG_E(KQPDATA, *error,
                            (trace_id, TraceId()));
                        ReplyErrorAndDie(Ydb::StatusIds::PRECONDITION_FAILED,
                            YqlIssue({}, NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED, *error));
                        return;
                    }
                }

                if ((stageInfo.Meta.IsOlap() && HasDmlOperationOnOlap(tx.Body->GetType(), stage))) {
                    auto error = TStringBuilder()
                        << "Data manipulation queries with column-oriented tables are supported only by API QueryService.";
                    KQP_STLOG_E(KQPDATA, error,
                        (trace_id, TraceId()));
                    ReplyErrorAndDie(Ydb::StatusIds::PRECONDITION_FAILED,
                        YqlIssue({}, NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED, error));
                    return;
                }

                for (const auto& [taskParam, controlPlaneSettings] : stage.GetStageControlPlaneActors()) {
                    YQL_ENSURE(stageInfo.Meta.ControlPlaneActors.emplace(taskParam, Register(AsyncIoFactory->CreateDqControlPlane({.Type = controlPlaneSettings.GetType(), .TxId = dqTxId}))).second);
                }
            }
        }

        size_t sourceScanPartitionsCount = 0;

        if (!graphRestored) {
            sourceScanPartitionsCount = TasksGraph.BuildAllTasks({}, ResourcesSnapshot, Stats.get());
        }

        TIssue validateIssue;
        if (!ValidateTasks(TasksGraph, EExecType::Data, TasksGraph.GetMeta().AllowWithSpilling, validateIssue)) {
            ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, validateIssue);
            return;
        }

        if (Stats) {
            Stats->Prepare();
        }

        TVector<ui64> computeTasks;

        for (const auto& task : TasksGraph.GetTasks()) {
            const auto& stageInfo = TasksGraph.GetStageInfo(task.StageId);
            if (stageInfo.Meta.IsSysView() || !task.Meta.ShardId) {
                computeTasks.emplace_back(task.Id);
            }
        }

        if (computeTasks.size() > Request.MaxComputeActors) {
            KQP_STLOG_N(KQPDATA, "Too many compute actors",
                (count, computeTasks.size()),
                (trace_id, TraceId()));
            ReplyErrorAndDie(Ydb::StatusIds::PRECONDITION_FAILED,
                YqlIssue({}, TIssuesIds::KIKIMR_PRECONDITION_FAILED, TStringBuilder()
                    << "Requested too many execution units: " << computeTasks.size()));
            return;
        }

        ui32 shardsLimit = Request.MaxAffectedShards;
        if (i64 msc = (i64) Request.MaxShardCount; msc > 0) {
            shardsLimit = std::min(shardsLimit, (ui32) msc);
        }

        // Even if total number of affected shards may be unknown - check the known shards count against limit.
        const size_t shards = sourceScanPartitionsCount;

        if (shardsLimit > 0 && shards > shardsLimit) {
            KQP_STLOG_W(KQPDATA, "Too many affected shards",
                (datashard_tasks, shards),
                (limit, shardsLimit),
                (trace_id, TraceId()));
            Counters->TxProxyMon->TxResultError->Inc();
            ReplyErrorAndDie(Ydb::StatusIds::PRECONDITION_FAILED,
                YqlIssue({}, TIssuesIds::KIKIMR_PRECONDITION_FAILED, TStringBuilder()
                    << "Affected too many shards: " << shards));
            return;
        }

        // Single-shard datashard transactions are always immediate
        ImmediateTx = (TxManager->GetTopicOperations().GetSize() + sourceScanPartitionsCount) <= 1
                    && !TasksGraph.GetMeta().UnknownAffectedShardCount
                    && !HasOlapTable;

        switch (Request.IsolationLevel) {
            // OnlineRO with AllowInconsistentReads = true
            case NKqpProto::ISOLATION_LEVEL_READ_UNCOMMITTED:
                YQL_ENSURE(ReadOnlyTx);
                TasksGraph.GetMeta().AllowInconsistentReads = true;
                ImmediateTx = true;
                break;

            default:
                break;
        }

        if ((ReadOnlyTx || Request.UseImmediateEffects) && GetSnapshot().IsValid()) {
            // Snapshot reads are always immediate
            // Uncommitted writes are executed without coordinators, so they can be immediate
            ImmediateTx = true;
        }

        ComputeTasks = std::move(computeTasks);

        TasksGraph.GetMeta().UseFollowers = GetUseFollowers();

        // TODO: use coroutines here.
        if (Request.SaveQueryPhysicalGraph) {
            SavePhysicalGraph();
        } else {
            OnShardsResolve();
        }
    }

    void SavePhysicalGraph() {
        YQL_ENSURE(Request.Transactions.size() == 1);

        if (!Request.QueryPhysicalGraph) {
            const auto preparedQuery = Request.Transactions[0].Body->GetPreparedQuery();
            YQL_ENSURE(preparedQuery);
            NKikimrKqp::TQueryPhysicalGraph physicalGraph;
            *physicalGraph.MutablePreparedQuery() = *preparedQuery;
            TasksGraph.PersistTasksGraphInfo(physicalGraph);
            Request.QueryPhysicalGraph = std::make_shared<NKikimrKqp::TQueryPhysicalGraph>(std::move(physicalGraph));
        }

        const auto runScriptActorId = GetUserRequestContext()->RunScriptActorId;
        Y_ENSURE(runScriptActorId);
        this->Send(runScriptActorId, new TEvSaveScriptPhysicalGraphRequest(*Request.QueryPhysicalGraph));
        Become(&TKqpDataExecuter::WaitResolveState);
    }

    void HandleResolve(TEvSaveScriptPhysicalGraphResponse::TPtr& ev) {
        // TODO: replace with coroutine to flawlessly break executer workflow without additional states.
        YQL_ENSURE(ev->Get()->Status == Ydb::StatusIds::SUCCESS, "failed to save script physical graph with issues: " << ev->Get()->Issues.ToOneLineString());
        OnShardsResolve();
    }

    void HandleResolve(TEvKqpExecuter::TEvTableResolveStatus::TPtr& ev) {
        if (TasksGraph.GetMeta().StreamResult || TasksGraph.GetMeta().AllowOlapDataQuery) {
            for (const auto& [stageId, stageInfo] : TasksGraph.GetStagesInfo()) {
                if (stageInfo.Meta.IsOlap()) {
                    HasOlapTable = true;
                    ResourceSnapshotRequired = true;
                }
            }
        }

        auto resolveStatus = TBase::HandleResolve(ev);

        if (resolveStatus == ERROR) {
            return;
        }

        if (IsEnabledReadsMerge()) {
            for (const auto& [stageId, stageInfo] : TasksGraph.GetStagesInfo()) {
                const auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);
                if (stage.SourcesSize() > 0 && stage.GetSources(0).GetTypeCase() == NKqpProto::TKqpSource::kReadRangesSource) {
                    HasDatashardSourceScan = true;
                }

                if (stage.SourcesSize() > 0 && stage.GetSources(0).GetTypeCase() == NKqpProto::TKqpSource::kFullTextSource) {
                    HasDatashardSourceScan = true;
                }

                if (stage.SourcesSize() > 0 && stage.GetSources(0).GetTypeCase() == NKqpProto::TKqpSource::kVectorIndexSource) {
                    HasDatashardSourceScan = true;
                }
            }

            if (HasDatashardSourceScan) {
                TSet<ui64> shardIds;

                for (const auto& [stageId, stageInfo] : TasksGraph.GetStagesInfo()) {
                    YQL_ENSURE(stageId == stageInfo.Id);
                    const auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);
                    if (stage.SourcesSize() > 0 && stage.GetSources(0).GetTypeCase() == NKqpProto::TKqpSource::kReadRangesSource) {
                        const auto& partitions = stageInfo.Meta.PrunedPartitions.at(0);
                        for (const auto& [shardId, _] : partitions) {
                            shardIds.insert(shardId);
                        }
                    }
                }

                if (shardIds.size() <= 1) {
                    HasDatashardSourceScan = false; // nothing to merge
                }
            }
        }

        if (resolveStatus == CONTINUE) {
            DoExecute();
        }
    }

    void HandleResolve(NShardResolver::TEvShardsResolveStatus::TPtr& ev) {
        if (TBase::HandleResolve(ev)) {
            DoExecute();
        }
    }

    void OnShardsResolve() {
        if (ForceAcquireSnapshot()) {
            auto longTxService = NLongTxService::MakeLongTxServiceID(SelfId().NodeId());
            Send(longTxService, new NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot(Database, TableIdsForSnapshot));

            KQP_STLOG_T(KQPDATA, "Create temporary mvcc snapshot, become WaitSnapshotState",
                (trace_id, TraceId()));
            Become(&TKqpDataExecuter::WaitSnapshotState);
            ExecuterStateSpan = NWilson::TSpan(TWilsonKqp::DataExecuterAcquireSnapshot, ExecuterSpan.GetTraceId(), "WaitForSnapshot");

            return;
        }

        ContinueExecute();
    }

private:
    STATEFN(WaitSnapshotState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult, Handle);
                hFunc(TEvKqp::TEvAbortExecution, HandleAbortExecution);
                hFunc(TEvKqpBuffer::TEvError, Handle);
                default:
                    UnexpectedEvent("WaitSnapshotState", ev->GetTypeRewrite());
            }
        } catch (const yexception& e) {
            InternalError(e.what());
        }
        ReportEventElapsedTime();
    }

    void Handle(NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult::TPtr& ev) {
        auto* msg = ev->Get();

        KQP_STLOG_T(KQPDATA, "Read snapshot result",
            (status, msg->Status),
            (step, msg->Snapshot.Step),
            (tx_id, msg->Snapshot.TxId),
            (trace_id, TraceId()));

        if (msg->Status != Ydb::StatusIds::SUCCESS) {
            ExecuterStateSpan.EndError(TStringBuilder() << Ydb::StatusIds::StatusCode_Name(msg->Status));
            ReplyErrorAndDie(msg->Status, msg->Issues);
            return;
        }
        ExecuterStateSpan.EndOk();

        SetSnapshot(msg->Snapshot.Step, msg->Snapshot.TxId);
        ImmediateTx = true;

        ContinueExecute();
    }

    using TDatashardTxs = THashMap<ui64, NKikimrTxDataShard::TKqpTransaction*>;
    using TEvWriteTxs = THashMap<ui64, NKikimrDataEvents::TEvWrite*>;
    using TTopicTabletTxs = NTopic::TTopicOperationTransactions;

    void ContinueExecute() {
        OnEmptyResult();

        StartCheckpointCoordinator();
        ExecuteTasks();

        if (CheckExecutionComplete()) {
            return;
        }

        ExecuterStateSpan = NWilson::TSpan(TWilsonKqp::DataExecuterRunTasks, ExecuterSpan.GetTraceId(), "RunTasks", NWilson::EFlags::AUTO_END);
        KQP_STLOG_D(KQPDATA, "become ExecuteState",
            (current_state, CurrentStateFuncName()),
            (immediate, true),
            (trace_id, TraceId()));
        Become(&TKqpDataExecuter::ExecuteState);
    }

    void ExecuteTasks() {
        auto lockTxId = Request.AcquireLocksTxId;
        if (lockTxId.Defined() && *lockTxId == 0) {
            lockTxId = TxId;
            LockHandle = TLockHandle(TxId, TActivationContext::ActorSystem());
        }

        LWTRACK(KqpDataExecuterStartTasksAndTxs, ResponseEv->Orbit, TxId, ComputeTasks.size());

        TasksGraph.GetMeta().SinglePartitionOptAllowed = !HasOlapTable && !TasksGraph.GetMeta().UnknownAffectedShardCount && !HasExternalSources;
        TasksGraph.GetMeta().MayRunTasksLocally = !HasExternalSources && !HasOlapTable && !HasDatashardSourceScan;

        bool isSubmitSuccessful = BuildPlannerAndSubmitTasks();
        if (!isSubmitSuccessful)
            return;

        KQP_STLOG_I(KQPDATA, "Total tasks",
            (total_tasks, TasksGraph.GetTasks().size()),
            (read_only, ReadOnlyTx),
            (immediate, ImmediateTx),
            (pending_compute_tasks, Planner ? Planner->GetPendingComputeTasks().size() : 0),
            (use_followers, GetUseFollowers()),
            (trace_id, TraceId()));

        // error
        KQP_STLOG_T(KQPDATA, "Updating channels after the creation of compute actors",
            (trace_id, TraceId()));
        Y_ENSURE(Planner);
        THashMap<TActorId, THashSet<ui64>> updates;
        for (ui64 taskId : ComputeTasks) {
            const auto& task = TasksGraph.GetTask(taskId);
            if (task.ComputeActorId)
                Planner->CollectTaskChannelsUpdates(task, updates);
        }
        Planner->PropagateChannelsUpdates(updates);
    }

    void Shutdown() override {
        if (Planner) {
            if (Planner->GetPendingComputeTasks().empty() && Planner->GetPendingComputeActors().empty()) {
                KQP_STLOG_I(KQPDATA, "Shutdown immediately - nothing to wait",
                    (trace_id, TraceId()));
                PassAway();
            } else {
                this->Become(&TThis::WaitShutdownState);
                KQP_STLOG_I(KQPDATA, "Waiting for shutdown",
                    (pending_tasks, Planner->GetPendingComputeTasks().size()),
                    (pending_compute_actors, Planner->GetPendingComputeActors().size()),
                    (trace_id, TraceId()));
                TActivationContext::Schedule(WaitCAStatsTimeout, new IEventHandle(SelfId(), SelfId(), new TEvents::TEvPoison));
            }
        } else {
            PassAway();
        }
    }

    void PassAway() override {
        auto totalTime = TInstant::Now() - StartTime;
        Counters->Counters->DataTxTotalTimeHistogram->Collect(totalTime.MilliSeconds());

        // TxProxyMon compatibility
        Counters->TxProxyMon->TxTotalTimeHgram->Collect(totalTime.MilliSeconds());
        Counters->TxProxyMon->TxExecuteTimeHgram->Collect(totalTime.MilliSeconds());

        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));

        if (GetUseFollowers()) {
            Send(MakePipePerNodeCacheID(true), new TEvPipeCache::TEvUnlink(0));
        }

        if (CheckpointCoordinatorId) {
            Send(CheckpointCoordinatorId, new NActors::TEvents::TEvPoisonPill());
            CheckpointCoordinatorId = TActorId{};
            const auto context = TasksGraph.GetMeta().UserRequestContext;
            if (AppData()->FeatureFlags.GetEnableStreamingQueriesCounters() && context && !context->StreamingQueryPath.empty()) {
                auto counters = Counters->Counters->GetKqpCounters();
                counters = counters->GetSubgroup("host", "");
                counters->RemoveSubgroup("path", context->StreamingQueryPath);
            }
        }
        TBase::PassAway();
    }

    STATEFN(WaitShutdownState) {
        switch(ev->GetTypeRewrite()) {
            hFunc(TEvDqCompute::TEvState, HandleShutdown);
            hFunc(TEvDqCompute::TEvNodeState, HandleShutdown);
            hFunc(TEvInterconnect::TEvNodeDisconnected, HandleShutdown);
            hFunc(TEvents::TEvPoison, HandleShutdown);
            hFunc(TEvDq::TEvAbortExecution, HandleShutdown);
            default:
                KQP_STLOG_E(KQPDATA, "Unexpected event while waiting for shutdown",
                    (event_type, ev->GetTypeName()), // ignore all other events
                    (trace_id, TraceId()));
        }
    }

    void HandleShutdown(TEvDqCompute::TEvState::TPtr& ev) {
        HandleComputeStats(ev);

        if (Planner->GetPendingComputeTasks().empty() && Planner->GetPendingComputeActors().empty()) {
            PassAway();
        }
    }

    void HandleShutdown(TEvDqCompute::TEvNodeState::TPtr&) {
        if (Planner->GetPendingComputeTasks().empty() && Planner->GetPendingComputeActors().empty()) {
            PassAway();
        }
    }

    void HandleShutdown(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        const auto nodeId = ev->Get()->NodeId;
        KQP_STLOG_N(KQPDATA, "Node has disconnected while shutdown",
            (node_id, nodeId),
            (trace_id, TraceId()));

        YQL_ENSURE(Planner);

        for (const auto& task : TasksGraph.GetTasks()) {
            // TODO: is Meta.NodeId assigned real NodeId where task is executed?
            if (task.Meta.NodeId == nodeId && !task.Meta.Completed) {
                if (task.ComputeActorId) {
                    Planner->CompletedCA(task.Id, task.ComputeActorId);
                } else {
                    Planner->TaskNotStarted(task.Id);
                }
            }
        }

        if (Planner->GetPendingComputeTasks().empty() && Planner->GetPendingComputeActors().empty()) {
            FillLocksFromExtraData();
            PassAway();
        }
    }

    void HandleShutdown(TEvents::TEvPoison::TPtr& ev) {
        // Self-poison means timeout - don't wait anymore.
        KQP_STLOG_I(KQPDATA, "Timed out on waiting for Compute Actors to finish - forcing shutdown",
            (Sender, ev->Sender),
            (trace_id, TraceId()));

        if (ev->Sender == SelfId()) {
            FillLocksFromExtraData();
            PassAway();
        }
    }

    void HandleShutdown(TEvDq::TEvAbortExecution::TPtr& ev) {
        auto statusCode = NYql::NDq::DqStatusToYdbStatus(ev->Get()->Record.GetStatusCode());

        // TODO(ISSUE-12128): if wait stats timeout is less than 1% of the original query timeout, then still wait for stats.

        // In case of external timeout the response is already sent to the client - no need to wait for stats.
        if (statusCode == Ydb::StatusIds::TIMEOUT) {
            FillLocksFromExtraData();
            KQP_STLOG_I(KQPDATA, "External timeout while waiting for Compute Actors to finish - forcing shutdown",
                (Sender, ev->Sender),
                (trace_id, TraceId()));
            PassAway();
        }
    }

    void Handle(NFq::TEvCheckpointCoordinator::TEvZeroCheckpointDone::TPtr& ev) {
        KQP_STLOG_D(KQPDATA, "Coordinator saved zero checkpoint",
            (trace_id, TraceId()));
        Send(CheckpointCoordinatorId, new NFq::TEvCheckpointCoordinator::TEvRunGraph());

        if (const auto context = GetUserRequestContext()) {
            Send(ev->Forward(context->RunScriptActorId));
        }
    }

    void Handle(NFq::TEvCheckpointCoordinator::TEvRaiseTransientIssues::TPtr& ev) {
        KQP_STLOG_N(KQPDATA, "TEvRaiseTransientIssues from checkpoint coordinator",
            (TransientIssues, ev->Get()->TransientIssues.ToOneLineString()),
            (trace_id, TraceId()));
    }

    void StartCheckpointCoordinator() {
        const auto context = TasksGraph.GetMeta().UserRequestContext;
        bool disableCheckpoints = Request.QueryPhysicalGraph && Request.QueryPhysicalGraph->GetPreparedQuery().GetPhysicalQuery().GetDisableCheckpoints();

        bool enableCheckpointCoordinator = AppData()->FeatureFlags.GetEnableStreamingQueries()
            && (Request.SaveQueryPhysicalGraph || Request.QueryPhysicalGraph != nullptr)
            && context && context->CheckpointId && !disableCheckpoints;
        if (!enableCheckpointCoordinator) {
            return;
        }

        FederatedQuery::StreamingDisposition streamingDisposition;
        if (const auto disposition = context->StreamingDisposition) {
            switch (disposition->GetDispositionCase()) {
                case NYql::NPq::NProto::StreamingDisposition::kOldest:
                    *streamingDisposition.mutable_oldest() = disposition->oldest();
                    break;
                case NYql::NPq::NProto::StreamingDisposition::kFresh:
                    *streamingDisposition.mutable_fresh() = disposition->fresh();
                    break;
                case NYql::NPq::NProto::StreamingDisposition::kFromTime:
                    *streamingDisposition.mutable_from_time()->mutable_timestamp() = disposition->from_time().timestamp();
                    break;
                case NYql::NPq::NProto::StreamingDisposition::kTimeAgo:
                    *streamingDisposition.mutable_time_ago()->mutable_duration() = disposition->time_ago().duration();
                    break;
                case NYql::NPq::NProto::StreamingDisposition::kFromLastCheckpoint:
                    streamingDisposition.mutable_from_last_checkpoint()->set_force(disposition->from_last_checkpoint().force());
                    break;
                case NYql::NPq::NProto::StreamingDisposition::DISPOSITION_NOT_SET:
                    break;
            }
        } else {
            streamingDisposition.mutable_from_last_checkpoint()->set_force(true);
        }

        const auto stateLoadMode = Request.QueryPhysicalGraph && Request.QueryPhysicalGraph->GetZeroCheckpointSaved()
            ? FederatedQuery::FROM_LAST_CHECKPOINT
            : FederatedQuery::EMPTY;

        NFq::NProto::TGraphParams graphParams;
        if (Request.QueryPhysicalGraph) {
            for (const auto& task : Request.QueryPhysicalGraph->GetTasks()) {
                *graphParams.AddTasks() = task.GetDqTask();
            }
        }

        auto counters = Counters->Counters->GetKqpCounters();
        if (AppData()->FeatureFlags.GetEnableStreamingQueriesCounters() && !context->StreamingQueryPath.empty()) {
            counters = counters->GetSubgroup("host", "");
            counters = counters->GetSubgroup("path", context->StreamingQueryPath);
        }
        const auto& checkpointId = context->CheckpointId;
        CheckpointCoordinatorId = Register(MakeCheckpointCoordinator(
            ::NFq::TCoordinatorId(checkpointId, Generation),
            NYql::NDq::MakeCheckpointStorageID(),
            SelfId(),
            {},
            counters,
            graphParams,
            stateLoadMode,
            streamingDisposition).Release());
        KQP_STLOG_D(KQPDATA, "Created new CheckpointCoordinator",
            (CheckpointCoordinatorId, CheckpointCoordinatorId),
            (ExecutionId, context->CurrentExecutionId),
            (CheckpointId, checkpointId),
            (Generation, Generation),
            (StateLoadMode, FederatedQuery::StateLoadMode_Name(stateLoadMode)),
            (StreamingDisposition, streamingDisposition.ShortDebugString()),
            (HasQueryPhysicalGraph, Request.QueryPhysicalGraph != nullptr),
            (EnableWatermarks, Request.QueryPhysicalGraph ? Request.QueryPhysicalGraph->GetPreparedQuery().GetPhysicalQuery().GetEnableWatermarks() : false),
            (trace_id, TraceId()));
    }

private:
    void ReplyTxStateUnknown(ui64 shardId) {
        auto message = TStringBuilder() << "Tx state unknown for shard " << shardId << ", txid " << TxId;
        if (ReadOnlyTx) {
            auto issue = YqlIssue({}, TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE);
            issue.AddSubIssue(new TIssue(message));
            issue.GetSubIssues()[0]->SetCode(NKikimrIssues::TIssuesIds::TX_STATE_UNKNOWN, TSeverityIds::S_ERROR);
            ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, issue);
        } else {
            auto issue = YqlIssue({}, TIssuesIds::KIKIMR_OPERATION_STATE_UNKNOWN);
            issue.AddSubIssue(new TIssue(message));
            issue.GetSubIssues()[0]->SetCode(NKikimrIssues::TIssuesIds::TX_STATE_UNKNOWN, TSeverityIds::S_ERROR);
            ReplyErrorAndDie(Ydb::StatusIds::UNDETERMINED, issue);
        }
    }

    void FillLocksFromExtraData() {
        auto addLocks = [this](const ui64 taskId, const auto& data) {
            if (data.GetData().template Is<NKikimrTxDataShard::TEvKqpInputActorResultInfo>()) {
                NKikimrTxDataShard::TEvKqpInputActorResultInfo info;
                YQL_ENSURE(data.GetData().UnpackTo(&info), "Failed to unpack settings");
                NDataIntegrity::LogIntegrityTrails("InputActorResult", Request.UserTraceId, TxId, info, TlsActivationContext->AsActorContext());
                ui64 deferredVictimSpanId = info.HasDeferredVictimQuerySpanId()
                    ? info.GetDeferredVictimQuerySpanId() : 0;
                for (auto& lock : info.GetLocks()) {
                    const auto& task = TasksGraph.GetTask(taskId);
                    const auto& stageInfo = TasksGraph.GetStageInfo(task.StageId);
                    ShardIdToTableInfo->Add(lock.GetDataShard(), stageInfo.Meta.TableKind == ETableKind::Olap, stageInfo.Meta.TablePath);

                    TxManager->AddShard(lock.GetDataShard(), stageInfo.Meta.TableKind == ETableKind::Olap, stageInfo.Meta.TablePath);
                    TxManager->AddAction(lock.GetDataShard(), IKqpTransactionManager::EAction::READ);
                    TxManager->AddLock(lock.GetDataShard(), lock, Request.QuerySpanId, deferredVictimSpanId);
                }

                if (!BatchOperationSettings.Empty() && info.HasBatchOperationMaxKey()) {
                    if (ResponseEv->BatchOperationMaxKeys.empty()) {
                        for (auto keyId : info.GetBatchOperationKeyIds()) {
                            ResponseEv->BatchOperationKeyIds.push_back(keyId);
                        }
                    }

                    ResponseEv->BatchOperationMaxKeys.emplace_back(info.GetBatchOperationMaxKey());
                }
                // Collect deferred breaker info for TLI logging at SessionActor level
                {
                    const auto& traceIds = info.GetDeferredBreakerQuerySpanIds();
                    const auto& nodeIds = info.GetDeferredBreakerNodeIds();
                    for (int i = 0; i < traceIds.size(); ++i) {
                        ResponseEv->DeferredBreakers.push_back({
                            traceIds[i],
                            i < nodeIds.size() ? nodeIds[i] : 0u
                        });
                    }
                }
            } else if (data.GetData().template Is<NKikimrKqp::TEvKqpOutputActorResultInfo>()) {
                NKikimrKqp::TEvKqpOutputActorResultInfo info;
                YQL_ENSURE(data.GetData().UnpackTo(&info), "Failed to unpack settings");
                NDataIntegrity::LogIntegrityTrails("OutputActorResult", Request.UserTraceId, TxId, info, TlsActivationContext->AsActorContext());
                for (auto& lock : info.GetLocks()) {
                    const auto& task = TasksGraph.GetTask(taskId);
                    const auto& stageInfo = TasksGraph.GetStageInfo(task.StageId);
                    ShardIdToTableInfo->Add(lock.GetDataShard(), stageInfo.Meta.TableKind == ETableKind::Olap, stageInfo.Meta.TablePath);
                    YQL_ENSURE(stageInfo.Meta.TableKind == ETableKind::Olap);
                    IKqpTransactionManager::TActionFlags flags = IKqpTransactionManager::EAction::WRITE;
                    if (info.GetHasRead()) {
                        flags |= IKqpTransactionManager::EAction::READ;
                    }

                    TxManager->AddShard(lock.GetDataShard(), stageInfo.Meta.TableKind == ETableKind::Olap, stageInfo.Meta.TablePath);
                    TxManager->AddAction(lock.GetDataShard(), flags, Request.QuerySpanId);
                    TxManager->AddLock(lock.GetDataShard(), lock, Request.QuerySpanId);
                }
            }
        };

        for (auto& [_, extraData] : ExtraData) {
            for (const auto& source : extraData.Data.GetSourcesExtraData()) {
                addLocks(extraData.TaskId, source);
            }
            for (const auto& transform : extraData.Data.GetInputTransformsData()) {
                addLocks(extraData.TaskId, transform);
            }
            for (const auto& sink : extraData.Data.GetSinksExtraData()) {
                addLocks(extraData.TaskId, sink);
            }
            if (extraData.Data.HasComputeExtraData()) {
                addLocks(extraData.TaskId, extraData.Data.GetComputeExtraData());
            }
        }
    }

private:
    TShardIdToTableInfoPtr ShardIdToTableInfo;
    TVector<NKikimr::TTableId> TableIdsForSnapshot;

    bool HasExternalSources = false;
    bool SecretSnapshotRequired = false;
    bool ResourceSnapshotRequired = false;
    bool SaveScriptExternalEffectRequired = false;

    const bool ReadOnlyTx;
    bool ImmediateTx = false;

    TInstant FirstPrepareReply;
    TInstant LastPrepareReply;

    bool HasPersistentChannels = false;

    TVector<ui64> ComputeTasks;

    // Lock handle for a newly acquired lock
    TLockHandle LockHandle;

    const TDuration WaitCAStatsTimeout;

    NKikimrConfig::TQueryServiceConfig QueryServiceConfig;
    ui64 Generation = 0;
};

} // namespace

IActor* CreateKqpDataExecuter(IKqpGateway::TExecPhysicalRequest&& request, const TString& database, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken,
    NFormats::TFormatsSettings formatsSettings, TKqpRequestCounters::TPtr counters, const TExecuterConfig& executerConfig,
    NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory, const TActorId& creator,
    const TIntrusivePtr<TUserRequestContext>& userRequestContext, ui32 statementResultIndex,
    const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup, const TGUCSettings::TPtr& GUCSettings,
    TPartitionPrunerConfig partitionPrunerConfig, const TShardIdToTableInfoPtr& shardIdToTableInfo,
    const IKqpTransactionManagerPtr& txManager, const TActorId bufferActorId,
    TMaybe<NBatchOperations::TSettings> batchOperationSettings, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, ui64 generation,
    std::shared_ptr<NYql::NDq::IDqChannelService> channelService,
    TVector<NKikimr::TTableId> tableIdsForSnapshot)
{
    return new TKqpDataExecuter(std::move(request), database, userToken, std::move(formatsSettings), counters, executerConfig,
        std::move(asyncIoFactory), creator, userRequestContext, statementResultIndex, federatedQuerySetup, GUCSettings,
        std::move(partitionPrunerConfig), shardIdToTableInfo, txManager, bufferActorId, std::move(batchOperationSettings), queryServiceConfig, generation,
        channelService, std::move(tableIdsForSnapshot));
}

} // namespace NKqp
} // namespace NKikimr
