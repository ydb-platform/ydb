#include "kqp_executer.h"
#include "kqp_executer_impl.h"
#include "kqp_partition_helper.h"
#include "kqp_tasks_graph.h"
#include "kqp_tasks_validate.h"
#include "kqp_shards_resolver.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/client/minikql_compile/db_key_resolver.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_actor.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/query_data/kqp_predictor.h>
#include <ydb/core/kqp/node_service/kqp_node_service.h>
#include <ydb/core/kqp/runtime/kqp_transport.h>
#include <ydb/core/kqp/opt/kqp_query_plan.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/library/yql/dq/runtime/dq_columns_resolve.h>
#include <ydb/library/yql/dq/tasks/dq_connection_builder.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NDq;

namespace {

class TKqpScanExecuter : public TKqpExecuterBase<TKqpScanExecuter, EExecType::Scan> {
    using TBase = TKqpExecuterBase<TKqpScanExecuter, EExecType::Scan>;
    TPreparedQueryHolder::TConstPtr PreparedQuery;
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_EXECUTER_ACTOR;
    }

    TKqpScanExecuter(IKqpGateway::TExecPhysicalRequest&& request, const TString& database,
        const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TKqpRequestCounters::TPtr counters,
        const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
        TPreparedQueryHolder::TConstPtr preparedQuery,
        TDuration maximalSecretsSnapshotWaitTime, const TIntrusivePtr<TUserRequestContext>& userRequestContext)
        : TBase(std::move(request), database, userToken, counters,
            maximalSecretsSnapshotWaitTime, userRequestContext, TWilsonKqp::ScanExecuter, "ScanExecuter",
            false
        )
        , PreparedQuery(preparedQuery)
    {
        YQL_ENSURE(Request.Transactions.size() == 1);
        YQL_ENSURE(Request.DataShardLocks.empty());
        YQL_ENSURE(Request.LocksOp == ELocksOp::Unspecified);
        YQL_ENSURE(Request.IsolationLevel == NKikimrKqp::ISOLATION_LEVEL_UNDEFINED);
        YQL_ENSURE(Request.Snapshot.IsValid());

        size_t resultsSize = Request.Transactions[0].Body->ResultsSize();
        YQL_ENSURE(resultsSize != 0);

        StreamResult = Request.Transactions[0].Body->GetResults(0).GetIsStream();

        if (StreamResult) {
            YQL_ENSURE(resultsSize == 1);
        } else {
            for (size_t i = 1; i < resultsSize; ++i) {
                YQL_ENSURE(Request.Transactions[0].Body->GetResults(i).GetIsStream() == StreamResult);
            }
        }
    }

public:
    STATEFN(WaitResolveState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpExecuter::TEvTableResolveStatus, HandleResolve);
                hFunc(TEvKqpExecuter::TEvShardsResolveStatus, HandleResolve);
                hFunc(TEvPrivate::TEvResourcesSnapshot, HandleResolve);
                hFunc(TEvKqp::TEvAbortExecution, HandleAbortExecution);
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
    TString CurrentStateFuncName() const override {
        const auto& func = CurrentStateFunc();
        if (func == &TThis::ExecuteState) {
            return "ExecuteState";
        } else if (func == &TThis::WaitResolveState) {
            return "WaitResolveState";
        } else {
            return TBase::CurrentStateFuncName();
        }
    }

    STATEFN(ExecuteState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvDqCompute::TEvState, HandleComputeStats);
                hFunc(TEvDqCompute::TEvChannelData, HandleChannelData); // from CA
                hFunc(TEvKqpExecuter::TEvStreamDataAck, HandleStreamAck);
                hFunc(TEvKqp::TEvAbortExecution, HandleAbortExecution);
                hFunc(TEvents::TEvUndelivered, HandleUndelivered);
                hFunc(TEvPrivate::TEvRetry, HandleRetry);
                hFunc(TEvKqpNode::TEvStartKqpTasksResponse, HandleStartKqpTasksResponse);
                IgnoreFunc(TEvKqpNode::TEvCancelKqpTasksResponse);
                hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
                IgnoreFunc(TEvInterconnect::TEvNodeConnected);
                default:
                    UnexpectedEvent("ExecuteState", ev->GetTypeRewrite());
            }
        } catch (const yexception& e) {
            InternalError(e.what());
        } catch (const TMemoryLimitExceededException&) {
            RuntimeError(Ydb::StatusIds::PRECONDITION_FAILED, NYql::TIssues({NYql::TIssue(BuildMemoryLimitExceptionMessage())}));
        }
        ReportEventElapsedTime();
    }

private:
    void HandleResolve(TEvKqpExecuter::TEvTableResolveStatus::TPtr& ev) {
        if (!TBase::HandleResolve(ev)) return;
        TSet<ui64> shardIds;
        for (auto& [stageId, stageInfo] : TasksGraph.GetStagesInfo()) {
            if (stageInfo.Meta.ShardKey) {
                for (auto& partition : stageInfo.Meta.ShardKey->GetPartitions()) {
                    shardIds.insert(partition.ShardId);
                }
            }
        }
        if (shardIds) {
            LOG_D("Start resolving tablets nodes... (" << shardIds.size() << ")");
            ExecuterStateSpan = NWilson::TSpan(TWilsonKqp::ExecuterShardsResolve, ExecuterSpan.GetTraceId(), "WaitForShardsResolve", NWilson::EFlags::AUTO_END);
            auto kqpShardsResolver = CreateKqpShardsResolver(
                this->SelfId(), TxId, false, std::move(shardIds));
            KqpShardsResolverId = this->RegisterWithSameMailbox(kqpShardsResolver);
        } else {
            GetResourcesSnapshot();
        }
    }

    void HandleResolve(TEvKqpExecuter::TEvShardsResolveStatus::TPtr& ev) {
        if (!TBase::HandleResolve(ev)) return;
        GetResourcesSnapshot();
    }

    void HandleResolve(TEvPrivate::TEvResourcesSnapshot::TPtr& ev) {
        if (ev->Get()->Snapshot.empty()) {
            LOG_E("Can not find default state storage group for database " << Database);
        }

        ResourcesSnapshot = std::move(ev->Get()->Snapshot);
        Execute();
    }

    void Execute() {
        LWTRACK(KqpScanExecuterStartExecute, ResponseEv->Orbit, TxId);

        auto& tx = Request.Transactions[0];
        for (ui32 stageIdx = 0; stageIdx < tx.Body->StagesSize(); ++stageIdx) {
            auto& stage = tx.Body->GetStages(stageIdx);
            auto& stageInfo = TasksGraph.GetStageInfo(TStageId(0, stageIdx));

            LOG_D("Stage " << stageInfo.Id << " AST: " << stage.GetProgramAst());

            Y_DEBUG_ABORT_UNLESS(!stage.GetIsEffectsStage());

            if (stage.SourcesSize() > 0) {
                switch (stage.GetSources(0).GetTypeCase()) {
                    case NKqpProto::TKqpSource::kReadRangesSource:
                        BuildScanTasksFromSource(
                            stageInfo,
                            /* shardsResolved */ true,
                            /* limitTasksPerNode */ false);
                        break;
                    default:
                        YQL_ENSURE(false, "unknown source type");
                }
            } else if (stageInfo.Meta.ShardOperations.empty()) {
                BuildComputeTasks(stageInfo, ShardsOnNode.size());
            } else if (stageInfo.Meta.IsSysView()) {
                BuildSysViewScanTasks(stageInfo);
            } else if (stageInfo.Meta.IsOlap() || stageInfo.Meta.IsDatashard()) {
                HasOlapTable = true;
                BuildScanTasksFromShards(stageInfo);
            } else {
                YQL_ENSURE(false, "Unexpected stage type " << (int) stageInfo.Meta.TableKind);
            }

            {
                const NKqpProto::TKqpPhyStage& stage = stageInfo.Meta.GetStage(stageInfo.Id);
                const bool useLlvm = PreparedQuery ? PreparedQuery->GetLlvmSettings().GetUseLlvm(stage.GetProgram().GetSettings()) : false;
                for (auto& taskId : stageInfo.Tasks) {
                    auto& task = TasksGraph.GetTask(taskId);
                    task.SetUseLlvm(useLlvm);
                }
                if (Stats && CollectProfileStats(Request.StatsMode)) {
                    Stats->SetUseLlvm(stageInfo.Id.StageId, useLlvm);
                }

            }

            if (stage.GetIsSinglePartition()) {
                YQL_ENSURE(stageInfo.Tasks.size() == 1, "Unexpected multiple tasks in single-partition stage");
            }

            BuildKqpStageChannels(TasksGraph, stageInfo, TxId, AppData()->EnableKqpSpilling);
        }

        ResponseEv->InitTxResult(tx.Body);
        BuildKqpTaskGraphResultChannels(TasksGraph, tx.Body, 0);

        TIssue validateIssue;
        if (!ValidateTasks(TasksGraph, EExecType::Scan, AppData()->EnableKqpSpilling, validateIssue)) {
            TBase::ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, validateIssue);
            return;
        }

        ui32 nShardScans = 0;
        TVector<ui64> computeTasks;

        // calc stats
        for (auto& task : TasksGraph.GetTasks()) {
            auto& stageInfo = TasksGraph.GetStageInfo(task.StageId);

            if (task.Meta.NodeId || stageInfo.Meta.IsSysView()) {
                // Task with source
                if (!task.Meta.Reads) {
                    continue;
                }

                nShardScans += task.Meta.Reads->size();
                if (Stats) {
                    for(const auto& read: *task.Meta.Reads) {
                        Stats->AffectedShards.insert(read.ShardId);
                    }
                }

            }
        }

        if (TasksGraph.GetTasks().size() > Request.MaxComputeActors) {
            // LOG_N("Too many compute actors: computeTasks=" << computeTasks.size() << ", scanTasks=" << nScanTasks);
            LOG_N("Too many compute actors: totalTasks=" << TasksGraph.GetTasks().size());
            TBase::ReplyErrorAndDie(Ydb::StatusIds::PRECONDITION_FAILED,
                YqlIssue({}, TIssuesIds::KIKIMR_PRECONDITION_FAILED, TStringBuilder()
                    << "Requested too many execution units: " << TasksGraph.GetTasks().size()));
            return;
        }

        LOG_D("TotalShardScans: " << nShardScans);

        ExecuterStateSpan = NWilson::TSpan(TWilsonKqp::ScanExecuterRunTasks, ExecuterSpan.GetTraceId(), "RunTasks", NWilson::EFlags::AUTO_END);
        ExecuteScanTx();

        Become(&TKqpScanExecuter::ExecuteState);
    }

public:

    void FillResponseStats(Ydb::StatusIds::StatusCode status) {
        auto& response = *ResponseEv->Record.MutableResponse();

        response.SetStatus(status);

        if (Stats) {
            ReportEventElapsedTime();

            Stats->FinishTs = TInstant::Now();
            Stats->Finish();

            if (Stats->CollectStatsByLongTasks || CollectFullStats(Request.StatsMode)) {
                const auto& tx = Request.Transactions[0].Body;
                auto planWithStats = AddExecStatsToTxPlan(tx->GetPlan(), response.GetResult().GetStats());
                response.MutableResult()->MutableStats()->AddTxPlansWithStats(planWithStats);
            }

            if (Stats->CollectStatsByLongTasks) {
                const auto& txPlansWithStats = response.GetResult().GetStats().GetTxPlansWithStats();
                if (!txPlansWithStats.empty()) {
                    LOG_N("Full stats: " << txPlansWithStats);
                }
            }
        }
    }

    void Finalize() {
        FillResponseStats(Ydb::StatusIds::SUCCESS);

        LWTRACK(KqpScanExecuterFinalize, ResponseEv->Orbit, TxId, LastTaskId, LastComputeActorId, ResponseEv->ResultsSize());

        if (ExecuterSpan) {
            ExecuterSpan.EndOk();
        }

        LOG_D("Sending response to: " << Target);
        Send(Target, ResponseEv.release());
        PassAway();
    }

private:
    void ExecuteScanTx() {

        Planner = CreateKqpPlanner(TasksGraph, TxId, SelfId(), GetSnapshot(),
            Database, UserToken, Deadline.GetOrElse(TInstant::Zero()), Request.StatsMode, AppData()->EnableKqpSpilling,
            Request.RlPath, ExecuterSpan, std::move(ResourcesSnapshot), ExecuterRetriesConfig, /* useDataQueryPool */ false, /* localComputeTasks */ false,
            Request.MkqlMemoryLimit, nullptr, false, GetUserRequestContext());

        LOG_D("Execute scan tx, PendingComputeTasks: " << TasksGraph.GetTasks().size());
        auto err = Planner->PlanExecution();
        if (err) {
            TlsActivationContext->Send(err.release());
            return;
        }

        LWTRACK(KqpScanExecuterStartTasksAndTxs, ResponseEv->Orbit, TxId, Planner->GetnComputeTasks(), Planner->GetnComputeTasks());

        Planner->Submit();
    }

private:
    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status,
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>* issues) override
    {
        if (Planner) {
            if (!Planner->GetPendingComputeTasks().empty()) {
                LOG_D("terminate pending resources request: " << Ydb::StatusIds::StatusCode_Name(status));

                auto ev = MakeHolder<TEvKqpNode::TEvCancelKqpTasksRequest>();
                ev->Record.SetTxId(TxId);
                ev->Record.SetReason(Ydb::StatusIds::StatusCode_Name(status));

                Send(MakeKqpNodeServiceID(SelfId().NodeId()), ev.Release());
            }
        }

        TBase::ReplyErrorAndDie(status, issues);
    }

    void PassAway() override {
        auto totalTime = TInstant::Now() - StartTime;
        Counters->Counters->ScanTxTotalTimeHistogram->Collect(totalTime.MilliSeconds());

        TBase::PassAway();
    }
};

} // namespace

IActor* CreateKqpScanExecuter(IKqpGateway::TExecPhysicalRequest&& request, const TString& database,
    const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TKqpRequestCounters::TPtr counters,
    const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
    TPreparedQueryHolder::TConstPtr preparedQuery,
    TDuration maximalSecretsSnapshotWaitTime, const TIntrusivePtr<TUserRequestContext>& userRequestContext)
{
    return new TKqpScanExecuter(std::move(request), database, userToken, counters, tableServiceConfig,
        preparedQuery, maximalSecretsSnapshotWaitTime, userRequestContext);
}

} // namespace NKqp
} // namespace NKikimr
