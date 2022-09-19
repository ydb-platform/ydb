#pragma once

#include "kqp_executer.h"
#include "kqp_executer_stats.h"
#include "kqp_partition_helper.h"
#include "kqp_table_resolver.h"

#include <ydb/core/kqp/common/kqp_ru_calc.h>
#include <ydb/core/kqp/common/kqp_lwtrace_probes.h>

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/wilson.h>
#include <ydb/core/base/kikimr_issue.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/kqp/executer/kqp_tasks_graph.h>
#include <ydb/core/kqp/kqp.h>
#include <ydb/core/grpc_services/local_rate_limiter.h>

#include <ydb/library/mkql_proto/mkql_proto.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/dq/proto/dq_transport.pb.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <ydb/library/yql/dq/runtime/dq_transport.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/wilson/wilson_span.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#include <util/generic/size_literals.h>

LWTRACE_USING(KQP_PROVIDER);

namespace NKikimr {
namespace NKqp {

#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext,  NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext,  NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext,   NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext,   NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext,  NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)
#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext,   NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)

enum class EExecType {
    Data,
    Scan
};

const ui64 MaxTaskSize = 48_MB;

struct TKqpExecuterTxResult {
    NKikimrMiniKQL::TType ItemType;
    TMaybe<NKikimrMiniKQL::TType> ResultItemType;
    TVector<NYql::NDqProto::TData> Data; // used in KqpDataExecuter
    NMiniKQL::TUnboxedValueVector Rows;  // used in KqpLiteralExecuter
    bool IsStream = true;
};

void BuildKqpExecuterResult(const NKqpProto::TKqpPhyResult& txResult, TKqpExecuterTxResult& result);
void BuildKqpExecuterResults(const NKqpProto::TKqpPhyTx& tx, TVector<TKqpExecuterTxResult>& results);

void PrepareKqpTaskParameters(const NKqpProto::TKqpPhyStage& stage, const TStageInfo& stageInfo, const TTask& task,
    NYql::NDqProto::TDqTask& dqTask, const NMiniKQL::TTypeEnvironment& typeEnv, const NMiniKQL::THolderFactory& holderFactory);

std::pair<TString, TString> SerializeKqpTasksParametersForOlap(const NKqpProto::TKqpPhyStage& stage,
    const TStageInfo& stageInfo, const TTask& task, const NMiniKQL::THolderFactory& holderFactory,
    const NMiniKQL::TTypeEnvironment& typeEnv);

inline bool IsDebugLogEnabled() {
    return TlsActivationContext->LoggerSettings() &&
           TlsActivationContext->LoggerSettings()->Satisfies(NActors::NLog::PRI_DEBUG, NKikimrServices::KQP_EXECUTER);
}

TActorId ReportToRl(ui64 ru, const TString& database, const TString& userToken,
    const NKikimrKqp::TRlPath& path);

template <class TDerived, EExecType ExecType>
class TKqpExecuterBase : public TActorBootstrapped<TDerived> {
public:
    TKqpExecuterBase(IKqpGateway::TExecPhysicalRequest&& request, const TString& database, const TMaybe<TString>& userToken,
        TKqpRequestCounters::TPtr counters, ui64 spanVerbosity = 0, TString spanName = "no_name")
        : Request(std::move(request))
        , Database(database)
        , UserToken(userToken)
        , Counters(counters)
        , ExecuterSpan(spanVerbosity, std::move(Request.TraceId), spanName)
    {
        ResponseEv = std::make_unique<TEvKqpExecuter::TEvTxResponse>();
        ResponseEv->Orbit = std::move(Request.Orbit);
        Stats = std::make_unique<TQueryExecutionStats>(Request.StatsMode, &TasksGraph,
            ResponseEv->Record.MutableResponse()->MutableResult()->MutableStats());
    }

    void Bootstrap() {
        StartTime = TAppData::TimeProvider->Now();
        if (Request.Timeout) {
            Deadline = StartTime + Request.Timeout;
        }
        if (Request.CancelAfter) {
            CancelAt = StartTime + *Request.CancelAfter;
        }

        LOG_T("Bootstrap done, become ReadyState");
        this->Become(&TKqpExecuterBase::ReadyState);
        ExecuterStateSpan = NWilson::TSpan(TWilsonKqp::ExecuterReadyState, ExecuterSpan.GetTraceId(), "ReadyState", NWilson::EFlags::AUTO_END);
    }

    void ReportEventElapsedTime() {
        if (Stats) {
            ui64 elapsedMicros = TlsActivationContext->GetCurrentEventTicksAsSeconds() * 1'000'000;
            Stats->ExecuterCpuTime += TDuration::MicroSeconds(elapsedMicros);
        }
    }

protected:
    STATEFN(ReadyState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKqpExecuter::TEvTxRequest, HandleReady);
            default: {
                UnexpectedEvent("ReadyState", ev->GetTypeRewrite());
            }
        }
        ReportEventElapsedTime();
    }

    void HandleReady(TEvKqpExecuter::TEvTxRequest::TPtr& ev) {
        TxId = ev->Get()->Record.GetRequest().GetTxId();
        Target = ActorIdFromProto(ev->Get()->Record.GetTarget());

        LWTRACK(KqpBaseExecuterHandleReady, ResponseEv->Orbit, TxId);

        LOG_D("Report self actorId " << this->SelfId() << " to " << Target);
        auto progressEv = MakeHolder<TEvKqpExecuter::TEvExecuterProgress>();
        ActorIdToProto(this->SelfId(), progressEv->Record.MutableExecuterActorId());
        this->Send(Target, progressEv.Release());

        const auto now = TAppData::TimeProvider->Now();
        const auto& ctx = TlsActivationContext->AsActorContext();
        TMaybe<TDuration> timeout;
        if (Deadline) {
            timeout = *Deadline - now;
            DeadlineActor = CreateLongTimer(ctx, *timeout,
                new IEventHandle(this->SelfId(), this->SelfId(), new TEvents::TEvWakeup(0)));
        }

        TMaybe<TDuration> cancelAfter;
        if (CancelAt) {
            cancelAfter = *CancelAt - now;
            CancelAtActor = CreateLongTimer(ctx, *cancelAfter,
                new IEventHandle(this->SelfId(), this->SelfId(), new TEvents::TEvWakeup(1)));
        }

        LOG_I("Begin execution. Operation timeout: " << timeout << ", cancelAfter: " << cancelAfter
            << ", txs: " << Request.Transactions.size());

        if (IsDebugLogEnabled()) {
            for (auto& tx : Request.Transactions) {
                LOG_D("Executing physical tx, type: " << (ui32) tx.Body->GetType() << ", stages: " << tx.Body->StagesSize());
            }
        }

        auto kqpTableResolver = CreateKqpTableResolver(this->SelfId(), TxId, UserToken, Request.Transactions,
            TableKeys, TasksGraph);
        KqpTableResolverId = this->RegisterWithSameMailbox(kqpTableResolver);

        LOG_T("Got request, become WaitResolveState");
        this->Become(&TDerived::WaitResolveState);
        if (ExecuterStateSpan) {
            ExecuterStateSpan.End();
            ExecuterStateSpan = NWilson::TSpan(TWilsonKqp::ExecuterWaitResolveState, ExecuterSpan.GetTraceId(), "WaitResolveState", NWilson::EFlags::AUTO_END);
        }

        ExecuterTableResolveSpan = NWilson::TSpan(TWilsonKqp::ExecuterTableResolve, ExecuterStateSpan.GetTraceId(), "ExecuterTableResolve", NWilson::EFlags::AUTO_END);

        StartResolveTime = now;

        if (Stats) {
            Stats->StartTs = now;
        }
    }

protected:
    void HandleAbortExecution(TEvKqp::TEvAbortExecution::TPtr& ev) {
        auto& msg = ev->Get()->Record;
        NYql::TIssues issues = ev->Get()->GetIssues();
        LOG_D("Got EvAbortExecution, status: " << NYql::NDqProto::StatusIds_StatusCode_Name(msg.GetStatusCode())
            << ", message: " << issues.ToOneLineString());
        auto statusCode = NYql::NDq::DqStatusToYdbStatus(msg.GetStatusCode());
        if (statusCode == Ydb::StatusIds::INTERNAL_ERROR) {
            InternalError(issues);
        } else if (statusCode == Ydb::StatusIds::TIMEOUT) {
            auto abortEv = MakeHolder<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::TIMEOUT, "Request timeout exceeded");
            
            if (ExecuterSpan) {
                ExecuterSpan.EndError("timeout");
            }

            this->Send(Target, abortEv.Release());

            TerminateComputeActors(Ydb::StatusIds::TIMEOUT, "timeout");
            this->PassAway();
        } else {
            RuntimeError(NYql::NDq::DqStatusToYdbStatus(msg.GetStatusCode()), issues);
        }
    }

    void HandleTimeout(TEvents::TEvWakeup::TPtr& ev) {
        bool cancel = ev->Get()->Tag == 1;
        LOG_I((cancel ? "CancelAt" : "Timeout") << " exceeded. Send timeout event to the rpc actor " << Target);

        if (cancel) {
            auto abortEv = MakeHolder<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::CANCELLED, "Request timeout exceeded");
            
            if (ExecuterSpan) {
                ExecuterSpan.EndError("timeout");
            }

            this->Send(Target, abortEv.Release());
            CancelAtActor = {};
        } else {
            auto abortEv = MakeHolder<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::TIMEOUT, "Request timeout exceeded");
            
            if (ExecuterSpan) {
                ExecuterSpan.EndError("timeout");
            }

            this->Send(Target, abortEv.Release());
            DeadlineActor = {};
        }

        TerminateComputeActors(Ydb::StatusIds::TIMEOUT, "timeout");
        this->PassAway();
    }

protected:
    void CollectTaskChannelsUpdates(const TKqpTasksGraph::TTaskType& task, THashMap<TActorId, THashSet<ui64>>& updates) {
        YQL_ENSURE(task.ComputeActorId);

        LOG_T("Collect channels updates for task: " << task.Id << " at actor " << task.ComputeActorId);

        // N.B. update only output channels

        for (auto& input : task.Inputs) {
            for (auto channelId : input.Channels) {
                auto& channel = TasksGraph.GetChannel(channelId);
                YQL_ENSURE(channel.DstTask == task.Id);
                YQL_ENSURE(channel.SrcTask);

                auto& srcTask = TasksGraph.GetTask(channel.SrcTask);
                if (srcTask.ComputeActorId) {
                    updates[srcTask.ComputeActorId].emplace(channelId);
                }

                LOG_T("Task: " << task.Id << ", input channelId: " << channelId << ", src task: " << channel.SrcTask
                    << ", at actor " << srcTask.ComputeActorId);
            }
        }

        for (auto& output : task.Outputs) {
            for (auto channelId : output.Channels) {
                updates[task.ComputeActorId].emplace(channelId);
            }
        }
    }

    void PropagateChannelsUpdates(const THashMap<TActorId, THashSet<ui64>>& updates) {
        for (auto& pair : updates) {
            auto computeActorId = pair.first;
            auto& channelIds = pair.second;

            auto channelsInfoEv = MakeHolder<NYql::NDq::TEvDqCompute::TEvChannelsInfo>();
            auto& record = channelsInfoEv->Record;

            for (auto& channelId : channelIds) {
                static_cast<TDerived*>(this)->FillChannelDesc(*record.AddUpdate(), TasksGraph.GetChannel(channelId));
            }

            LOG_T("Sending channels info to compute actor: " << computeActorId << ", channels: " << channelIds.size());
            bool sent = this->Send(computeActorId, channelsInfoEv.Release());
            YQL_ENSURE(sent, "Failed to send event to " << computeActorId.ToString());
        }
    }

    void UpdateResourcesUsage(bool force) {
        TInstant now = TActivationContext::Now();
        if ((now - LastResourceUsageUpdate < ResourceUsageUpdateInterval) && !force)
            return;

        LastResourceUsageUpdate = now;

        TProgressStat::TEntry consumption;
        for (const auto& p : PendingComputeActors) {
            const auto& t = p.second.GetLastUsage();
            consumption += t;
        }

        for (const auto& p : LastStats) {
            const auto& t = p.GetLastUsage();
            consumption += t;
        }

        auto ru = NRuCalc::CalcRequestUnit(consumption);

        // Some heuristic to reduce overprice due to round part stats
        if (ru <= 100 && !force)
            return;

        for (auto& p : PendingComputeActors) {
            p.second.Update();
        }

        for (auto& p : LastStats) {
            p.Update();
        }

        if (Request.RlPath) {
            auto actorId = ReportToRl(ru, Database, UserToken.GetOrElse(""),
                Request.RlPath.GetRef());

            LOG_D("Resource usage for last stat interval: " << consumption
                  << " ru: " << ru << " rl path: " << Request.RlPath.GetRef()
                  << " rl actor: " << actorId
                  << " force flag: " << force);
        } else {
            LOG_D("Resource usage for last stat interval: " << consumption
                  << " ru: " << ru << " rate limiter was not found"
                  << " force flag: " << force);
        }
    }



protected:
    void BuildSysViewScanTasks(TStageInfo& stageInfo, const NMiniKQL::THolderFactory& holderFactory,
        const NMiniKQL::TTypeEnvironment& typeEnv)
    {
        Y_VERIFY_DEBUG(stageInfo.Meta.IsSysView());

        auto& stage = GetStage(stageInfo);

        const auto& table = TableKeys.GetTable(stageInfo.Meta.TableId);
        const auto& keyTypes = table.KeyColumnTypes;

        for (auto& op : stage.GetTableOps()) {
            Y_VERIFY_DEBUG(stageInfo.Meta.TablePath == op.GetTable().GetPath());

            auto& task = TasksGraph.AddTask(stageInfo);
            TShardKeyRanges keyRanges;

            switch (op.GetTypeCase()) {
                case NKqpProto::TKqpPhyTableOperation::kReadRange:
                    stageInfo.Meta.SkipNullKeys.assign(
                        op.GetReadRange().GetSkipNullKeys().begin(),
                        op.GetReadRange().GetSkipNullKeys().end()
                    );
                    keyRanges.Add(MakeKeyRange(
                        keyTypes, op.GetReadRange().GetKeyRange(),
                        stageInfo, holderFactory, typeEnv)
                    );
                    break;
                case NKqpProto::TKqpPhyTableOperation::kReadRanges:
                    keyRanges.CopyFrom(FillReadRanges(keyTypes, op.GetReadRanges(), stageInfo, holderFactory, typeEnv));
                    break;
                default:
                    YQL_ENSURE(false, "Unexpected table scan operation: " << (ui32) op.GetTypeCase());
            }

            TTaskMeta::TShardReadInfo readInfo = {
                .Ranges = std::move(keyRanges),
                .Columns = BuildKqpColumns(op, table),
            };

            task.Meta.Reads.ConstructInPlace();
            task.Meta.Reads->emplace_back(std::move(readInfo));
            task.Meta.ReadInfo.Reverse = op.GetReadRange().GetReverse();

            LOG_D("Stage " << stageInfo.Id << " create sysview scan task: " << task.Id);
        }
    }

protected:
    // in derived classes
    // void FillEndpointDesc(NYql::NDqProto::TEndpoint& endpoint, const TTask& task);
    // void FillChannelDesc(NYql::NDqProto::TChannel& channelDesc, const TChannel& channel);

    void FillInputDesc(NYql::NDqProto::TTaskInput& inputDesc, const TTaskInput& input) {
        switch (input.Type()) {
            case NYql::NDq::TTaskInputType::UnionAll: {
                inputDesc.MutableUnionAll();
                break;
            }
            case NYql::NDq::TTaskInputType::Merge: {
                auto& mergeProto = *inputDesc.MutableMerge();
                YQL_ENSURE(std::holds_alternative<NYql::NDq::TMergeTaskInput>(input.ConnectionInfo));
                auto& sortColumns = std::get<NYql::NDq::TMergeTaskInput>(input.ConnectionInfo).SortColumns;
                for (const auto& sortColumn : sortColumns) {
                    auto newSortCol = mergeProto.AddSortColumns();
                    newSortCol->SetColumn(sortColumn.Column.c_str());
                    newSortCol->SetAscending(sortColumn.Ascending);
                }
                break;
            }
            default:
                YQL_ENSURE(false, "Unexpected task input type: " << (int) input.Type() << Endl << this->DebugString());
        }

        for (ui64 channel : input.Channels) {
            auto& channelDesc = *inputDesc.AddChannels();
            static_cast<TDerived*>(this)->FillChannelDesc(channelDesc, TasksGraph.GetChannel(channel));
        }

        if (input.Transform) {
            auto* transformProto = inputDesc.MutableTransform();
            transformProto->SetType(input.Transform->Type);
            transformProto->SetInputType(input.Transform->InputType);
            transformProto->SetOutputType(input.Transform->OutputType);
            *transformProto->MutableSettings() = input.Transform->Settings;
        }
    }

    void FillOutputDesc(NYql::NDqProto::TTaskOutput& outputDesc, const TTaskOutput& output) {
        switch (output.Type) {
            case TTaskOutputType::Map:
                YQL_ENSURE(output.Channels.size() == 1, "" << this->DebugString());
                outputDesc.MutableMap();
                break;

            case TTaskOutputType::HashPartition: {
                auto& hashPartitionDesc = *outputDesc.MutableHashPartition();
                for (auto& column : output.KeyColumns) {
                    hashPartitionDesc.AddKeyColumns(column);
                }
                hashPartitionDesc.SetPartitionsCount(output.PartitionsCount);
                break;
            }

            case TKqpTaskOutputType::ShardRangePartition: {
                auto& rangePartitionDesc = *outputDesc.MutableRangePartition();
                auto& columns = *rangePartitionDesc.MutableKeyColumns();
                for (auto& column : output.KeyColumns) {
                    *columns.Add() = column;
                }

                auto& partitionsDesc = *rangePartitionDesc.MutablePartitions();
                for (auto& pair : output.Meta.ShardPartitions) {
                    auto& range = *pair.second->Range;
                    auto& partitionDesc = *partitionsDesc.Add();
                    partitionDesc.SetEndKeyPrefix(range.EndKeyPrefix.GetBuffer());
                    partitionDesc.SetIsInclusive(range.IsInclusive);
                    partitionDesc.SetIsPoint(range.IsPoint);
                    partitionDesc.SetChannelId(pair.first);
                }
                break;
            }

            case TTaskOutputType::Broadcast: {
                outputDesc.MutableBroadcast();
                break;
            }

            case TTaskOutputType::Effects: {
                outputDesc.MutableEffects();
                break;
            }

            default: {
                YQL_ENSURE(false, "Unexpected task output type " << output.Type << Endl << this->DebugString());
            }
        }

        for (auto& channel : output.Channels) {
            auto& channelDesc = *outputDesc.AddChannels();
            static_cast<TDerived*>(this)->FillChannelDesc(channelDesc, TasksGraph.GetChannel(channel));
        }
    }

    void FillTableMeta(const TStageInfo& stageInfo, NKikimrTxDataShard::TKqpTransaction_TTableMeta* meta) {
        meta->SetTablePath(stageInfo.Meta.TablePath);
        meta->MutableTableId()->SetTableId(stageInfo.Meta.TableId.PathId.LocalPathId);
        meta->MutableTableId()->SetOwnerId(stageInfo.Meta.TableId.PathId.OwnerId);
        meta->SetSchemaVersion(stageInfo.Meta.TableId.SchemaVersion);
        meta->SetSysViewInfo(stageInfo.Meta.TableId.SysViewInfo);
        meta->SetTableKind((ui32)stageInfo.Meta.TableKind);
    }

    void ExtractItemsLimit(const TStageInfo& stageInfo, const NKqpProto::TKqpPhyValue& protoItemsLimit,
        const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv,
        ui64& itemsLimit, TString& itemsLimitParamName, NYql::NDqProto::TData& itemsLimitBytes,
        NKikimr::NMiniKQL::TType*& itemsLimitType)
    {
        switch (protoItemsLimit.GetKindCase()) {
            case NKqpProto::TKqpPhyValue::kLiteralValue: {
                const auto& literalValue = protoItemsLimit.GetLiteralValue();

                auto [type, value] = NMiniKQL::ImportValueFromProto(
                    literalValue.GetType(), literalValue.GetValue(), typeEnv, holderFactory);

                YQL_ENSURE(type->GetKind() == NMiniKQL::TType::EKind::Data, "" << this->DebugString());
                itemsLimit = value.Get<ui64>();
                itemsLimitType = type;

                return;
            }

            case NKqpProto::TKqpPhyValue::kParamValue: {
                itemsLimitParamName = protoItemsLimit.GetParamValue().GetParamName();
                if (!itemsLimitParamName) {
                    return;
                }

                auto* itemsLimitParam = stageInfo.Meta.Tx.Params.Values.FindPtr(itemsLimitParamName);
                YQL_ENSURE(itemsLimitParam);

                auto [type, value] = NMiniKQL::ImportValueFromProto(
                    itemsLimitParam->GetType(), itemsLimitParam->GetValue(), typeEnv, holderFactory);

                YQL_ENSURE(type->GetKind() == NMiniKQL::TType::EKind::Data, "" << this->DebugString());
                itemsLimit = value.Get<ui64>();

                NYql::NDq::TDqDataSerializer dataSerializer(typeEnv, holderFactory, NYql::NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0);
                itemsLimitBytes = dataSerializer.Serialize(value, type);
                itemsLimitType = type;

                return;
            }

            case NKqpProto::TKqpPhyValue::kParamElementValue:
            case NKqpProto::TKqpPhyValue::kRowsList:
                YQL_ENSURE(false, "Unexpected ItemsLimit kind " << protoItemsLimit.DebugString());

            case NKqpProto::TKqpPhyValue::KIND_NOT_SET:
                return;
        }
    }

protected:
    void TerminateComputeActors(Ydb::StatusIds::StatusCode code, const NYql::TIssues& issues) {
        for (const auto& task : this->TasksGraph.GetTasks()) {
            if (task.ComputeActorId) {
                LOG_I("aborting compute actor execution, message: " << issues.ToOneLineString()
                    << ", compute actor: " << task.ComputeActorId << ", task: " << task.Id);

                auto ev = MakeHolder<TEvKqp::TEvAbortExecution>(NYql::NDq::YdbStatusToDqStatus(code), issues);
                this->Send(task.ComputeActorId, ev.Release());
            } else {
                LOG_I("task: " << task.Id << ", does not have Compute ActorId yet");
            }
        }
    }

    void TerminateComputeActors(Ydb::StatusIds::StatusCode code, const TString& message) {
        TerminateComputeActors(code, NYql::TIssues({NYql::TIssue(message)}));
    }

protected:
    void UnexpectedEvent(const TString& state, ui32 eventType) {
        LOG_C("TKqpExecuter, unexpected event: " << eventType << ", at state:" << state << ", selfID: " << this->SelfId());
        InternalError(TStringBuilder() << "Unexpected event at TKqpScanExecuter, state: " << state
            << ", event: " << eventType);
    }

    void InternalError(const NYql::TIssues& issues) {
        LOG_E(issues.ToOneLineString());
        TerminateComputeActors(Ydb::StatusIds::INTERNAL_ERROR, issues);
        auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::UNEXPECTED, "Internal error while executing transaction.");
        for (const NYql::TIssue& i : issues) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
        }
        ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, issue);
    }

    void InternalError(const TString& message) {
        InternalError(NYql::TIssues({NYql::TIssue(message)}));
    }

    void ReplyUnavailable(const TString& message) {
        LOG_E("UNAVAILABLE: " << message);
        TerminateComputeActors(Ydb::StatusIds::UNAVAILABLE, message);
        auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE);
        issue.AddSubIssue(new NYql::TIssue(message));
        ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, issue);
    }

    void RuntimeError(Ydb::StatusIds::StatusCode code, const NYql::TIssues& issues) {
        LOG_E(Ydb::StatusIds_StatusCode_Name(code) << ": " << issues.ToOneLineString());
        TerminateComputeActors(code, issues);
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

    virtual void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status,
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>* issues)
    {
        for (auto computeActor : PendingComputeActors) {
            LOG_D("terminate compute actor " << computeActor.first);

            auto ev = MakeHolder<TEvKqp::TEvAbortExecution>(NYql::NDq::YdbStatusToDqStatus(status), "Terminate execution");
            this->Send(computeActor.first, ev.Release());
        }

        auto& response = *ResponseEv->Record.MutableResponse();

        response.SetStatus(status);
        response.MutableIssues()->Swap(issues);

        LOG_T("ReplyErrorAndDie. " << response.DebugString());

        if constexpr (ExecType == EExecType::Data) {
            if (status != Ydb::StatusIds::SUCCESS) {
                Counters->TxProxyMon->ReportStatusNotOK->Inc();
            } else {
                Counters->TxProxyMon->ReportStatusOK->Inc();
            }
        }

        LWTRACK(KqpBaseExecuterReplyErrorAndDie, ResponseEv->Orbit, TxId);

        if (ExecuterSpan) {
            ExecuterSpan.EndError(response.DebugString());
        }

        this->Send(Target, ResponseEv.release());
        this->PassAway();
    }

protected:
    template <class TCollection>
    bool ValidateTaskSize(const TCollection& tasks) {
        for (const auto& task : tasks) {
            if (ui32 size = task.ByteSize(); size > MaxTaskSize) {
                LOG_E("Abort execution. Task #" << task.GetId() << " size is too big: " << size << " > " << MaxTaskSize);
                ReplyErrorAndDie(Ydb::StatusIds::ABORTED,
                    MakeIssue(NKikimrIssues::TIssuesIds::SHARD_PROGRAM_SIZE_EXCEEDED, TStringBuilder() <<
                        "Datashard program size limit exceeded (" << size << " > " << MaxTaskSize << ")"));
                return false;
            }
        }
        return true;
    }

protected:
    void PassAway() override {
        LOG_D("terminate execution.");
        if (DeadlineActor) {
            this->Send(DeadlineActor, new TEvents::TEvPoison);
        }
        if (CancelAtActor) {
            this->Send(CancelAtActor, new TEvents::TEvPoison);
        }
        if (KqpTableResolverId) {
            this->Send(KqpTableResolverId, new TEvents::TEvPoison);
            this->Send(this->SelfId(), new TEvents::TEvPoison);
            LOG_T("Terminate, become ZombieState");
            this->Become(&TKqpExecuterBase::ZombieState);
            if (ExecuterStateSpan) {
                ExecuterStateSpan.End();
                ExecuterStateSpan = NWilson::TSpan(TWilsonKqp::ExecuterZombieState, ExecuterSpan.GetTraceId(), "ZombieState", NWilson::EFlags::AUTO_END);
            }
        } else {
            IActor::PassAway();
        }
    }

    STATEFN(ZombieState) {
        if (ev->GetTypeRewrite() == TEvents::TEvPoison::EventType) {
            IActor::PassAway();
        }
    }

protected:
    TString DebugString() const {
        TStringBuilder sb;
        sb << "[KqpExecuter], type: " << (ExecType == EExecType::Data ? "Data" : "Scan")
           << ", Database: " << Database << ", TxId: " << TxId << ", TxCnt: " << Request.Transactions.size()
           << ", Transactions: " << Endl;
        for (const auto& tx : Request.Transactions) {
            sb << "tx: " << tx.Body->DebugString() << Endl;
        }
        return std::move(sb);
    }

protected:
    IKqpGateway::TExecPhysicalRequest Request;
    const TString Database;
    const TMaybe<TString> UserToken;
    TKqpRequestCounters::TPtr Counters;
    std::unique_ptr<TQueryExecutionStats> Stats;
    TInstant StartTime;
    TMaybe<TInstant> Deadline;
    TActorId DeadlineActor;
    TMaybe<TInstant> CancelAt;
    TActorId CancelAtActor;
    TActorId Target;
    ui64 TxId = 0;

    TKqpTasksGraph TasksGraph;
    TKqpTableKeys TableKeys;

    TActorId KqpTableResolverId;
    THashMap<TActorId, TProgressStat> PendingComputeActors; // Running compute actors (pure and DS)
    TVector<TProgressStat> LastStats;

    TInstant StartResolveTime;
    TInstant LastResourceUsageUpdate;

    std::unique_ptr<TEvKqpExecuter::TEvTxResponse> ResponseEv;
    NWilson::TSpan ExecuterSpan;
    NWilson::TSpan ExecuterStateSpan;
    NWilson::TSpan ExecuterTableResolveSpan;
private:
    static constexpr TDuration ResourceUsageUpdateInterval = TDuration::MilliSeconds(100);
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

IActor* CreateKqpLiteralExecuter(IKqpGateway::TExecPhysicalRequest&& request, TKqpRequestCounters::TPtr counters);

IActor* CreateKqpDataExecuter(IKqpGateway::TExecPhysicalRequest&& request, const TString& database,
    const TMaybe<TString>& userToken, TKqpRequestCounters::TPtr counters);

IActor* CreateKqpScanExecuter(IKqpGateway::TExecPhysicalRequest&& request, const TString& database,
    const TMaybe<TString>& userToken, TKqpRequestCounters::TPtr counters);

} // namespace NKqp
} // namespace NKikimr
