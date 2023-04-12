#include "kqp_executer.h"
#include "kqp_executer_impl.h"
#include "kqp_locks_helper.h"
#include "kqp_partition_helper.h"
#include "kqp_planner.h"
#include "kqp_result_channel.h"
#include "kqp_table_resolver.h"
#include "kqp_tasks_validate.h"
#include "kqp_shards_resolver.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/base/wilson.h>
#include <ydb/core/client/minikql_compile/db_key_resolver.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_actor.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/runtime/kqp_transport.h>
#include <ydb/core/kqp/opt/kqp_query_plan.h>
#include <ydb/core/tx/coordinator/coordinator_impl.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/tx/long_tx_service/public/lock_handle.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/persqueue/events/global.h>

#include <ydb/library/yql/dq/runtime/dq_columns_resolve.h>
#include <ydb/library/yql/dq/tasks/dq_connection_builder.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <library/cpp/containers/absl_flat_hash/flat_hash_set.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NDq;
using namespace NLongTxService;

namespace {

static constexpr TDuration MinReattachDelay = TDuration::MilliSeconds(10);
static constexpr TDuration MaxReattachDelay = TDuration::MilliSeconds(100);
static constexpr TDuration MaxReattachDuration = TDuration::Seconds(4);
static constexpr ui32 ReplySizeLimit = 48 * 1024 * 1024; // 48 MB

class TKqpDataExecuter : public TKqpExecuterBase<TKqpDataExecuter, EExecType::Data> {
    using TBase = TKqpExecuterBase<TKqpDataExecuter, EExecType::Data>;
    using TKqpSnapshot = IKqpGateway::TKqpSnapshot;

    struct TReattachState {
        TDuration Delay;
        TInstant Deadline;
        ui64 Cookie = 0;
        bool Reattaching = false;

        bool ShouldReattach(TInstant now) {
            ++Cookie; // invalidate any previous cookie

            if (!Reattaching) {
                Deadline = now + MaxReattachDuration;
                Delay = TDuration::Zero();
                Reattaching = true;
                return true;
            }

            TDuration left = Deadline - now;
            if (!left) {
                Reattaching = false;
                return false;
            }

            Delay *= 2.0;
            if (Delay < MinReattachDelay) {
                Delay = MinReattachDelay;
            } else if (Delay > MaxReattachDelay) {
                Delay = MaxReattachDelay;
            }

            // Add ±10% jitter
            Delay *= 0.9 + 0.2 * TAppData::RandomProvider->GenRandReal4();
            if (Delay > left) {
                Delay = left;
            }

            return true;
        }

        void Reattached() {
            Reattaching = false;
        }
    };

    struct TShardState {
        enum class EState {
            Initial,
            Preparing,      // planned tx only
            Prepared,       // planned tx only
            Executing,
            Finished
        };

        EState State = EState::Initial;
        TSet<ui64> TaskIds;

        struct TDatashardState {
            ui64 ShardMinStep = 0;
            ui64 ShardMaxStep = 0;
            ui64 ReadSize = 0;
            bool ShardReadLocks = false;
            bool Follower = false;
        };
        TMaybe<TDatashardState> DatashardState;

        TReattachState ReattachState;
        ui32 RestartCount = 0;
        bool Restarting = false;
    };

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_DATA_EXECUTER_ACTOR;
    }

    TKqpDataExecuter(IKqpGateway::TExecPhysicalRequest&& request, const TString& database,
        const TIntrusiveConstPtr<NACLib::TUserToken>& userToken,
        TKqpRequestCounters::TPtr counters, bool streamResult,
        const NKikimrConfig::TTableServiceConfig::TExecuterRetriesConfig& executerRetriesConfig,
        NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory)
        : TBase(std::move(request), database, userToken, counters, executerRetriesConfig, TWilsonKqp::DataExecuter, "DataExecuter")
        , AsyncIoFactory(std::move(asyncIoFactory))
        , StreamResult(streamResult)
    {
        YQL_ENSURE(Request.IsolationLevel != NKikimrKqp::ISOLATION_LEVEL_UNDEFINED);

        if (Request.AcquireLocksTxId || Request.ValidateLocks || Request.EraseLocks) {
            YQL_ENSURE(Request.IsolationLevel == NKikimrKqp::ISOLATION_LEVEL_SERIALIZABLE);
        }

        if (GetSnapshot().IsValid()) {
            YQL_ENSURE(Request.IsolationLevel == NKikimrKqp::ISOLATION_LEVEL_SERIALIZABLE);
        }

        ReadOnlyTx = IsReadOnlyTx();
    }

    void CheckExecutionComplete() {
        ui32 notFinished = 0;
        for (const auto& x : ShardStates) {
            if (x.second.State != TShardState::EState::Finished) {
                notFinished++;
                LOG_D("ActorState: " << CurrentStateFuncName()
                    << ", datashard " << x.first << " not finished yet: " << ToString(x.second.State));
            }
        }
        for (const auto& x : TopicTabletStates) {
            if (x.second.State != TShardState::EState::Finished) {
                ++notFinished;
                LOG_D("ActorState: " << CurrentStateFuncName()
                    << ", topicTablet " << x.first << " not finished yet: " << ToString(x.second.State));
            }
        }
        if (notFinished == 0 && TBase::CheckExecutionComplete()) {
            return;
        }

        if (IsDebugLogEnabled()) {
            auto sb = TStringBuilder() << "ActorState: " << CurrentStateFuncName()
                << ", waiting for " << PendingComputeActors.size() << " compute actor(s) and "
                << notFinished << " datashard(s): ";
            for (const auto& shardId : PendingComputeActors) {
                sb << "CA " << shardId.first << ", ";
            }
            for (const auto& [shardId, shardState] : ShardStates) {
                if (shardState.State != TShardState::EState::Finished) {
                    sb << "DS " << shardId << " (" << ToString(shardState.State) << "), ";
                }
            }
            for (const auto& [tabletId, tabletState] : TopicTabletStates) {
                if (tabletState.State != TShardState::EState::Finished) {
                    sb << "PQ " << tabletId << " (" << ToString(tabletState.State) << "), ";
                }
            }
            LOG_D(sb);
        }
    }

    void Finalize() {
        if (LocksBroken) {
            TString message = "Transaction locks invalidated.";

            return ReplyErrorAndDie(Ydb::StatusIds::ABORTED,
                YqlIssue({}, TIssuesIds::KIKIMR_LOCKS_INVALIDATED, message));
        }

        auto& response = *ResponseEv->Record.MutableResponse();

        response.SetStatus(Ydb::StatusIds::SUCCESS);
        Counters->TxProxyMon->ReportStatusOK->Inc();

        auto addLocks = [&](const NYql::NDqProto::TExtraInputData& data) {
            if (data.GetData().Is<NKikimrTxDataShard::TEvKqpInputActorResultInfo>()) {
                NKikimrTxDataShard::TEvKqpInputActorResultInfo info;
                YQL_ENSURE(data.GetData().UnpackTo(&info), "Failed to unpack settings");
                for (auto& lock : info.GetLocks()) {
                    Locks.push_back(lock);
                }
            }
        };
        for (auto& [_, data] : ExtraData) {
            for (auto& source : data.GetSourcesExtraData()) {
                addLocks(source);
            }
            for (auto& transform : data.GetInputTransformsData()) {
                addLocks(transform);
            }
        }

        if (!Locks.empty()) {
            if (LockHandle) {
                ResponseEv->LockHandle = std::move(LockHandle);
            }
            BuildLocks(*response.MutableResult()->MutableLocks(), Locks);
        }

        if (Stats) {
            ReportEventElapsedTime();

            Stats->FinishTs = TInstant::Now();
            Stats->Finish();

            if (CollectFullStats(Request.StatsMode)) {
                for (ui32 txId = 0; txId < Request.Transactions.size(); ++txId) {
                    const auto& tx = Request.Transactions[txId].Body;
                    auto planWithStats = AddExecStatsToTxPlan(tx->GetPlan(), response.GetResult().GetStats());
                    response.MutableResult()->MutableStats()->AddTxPlansWithStats(planWithStats);
                }
            }

            Stats.reset();
        }

        auto resultSize = ResponseEv->GetByteSize();
        if (resultSize > (int)ReplySizeLimit) {
            TString message = TStringBuilder() << "Query result size limit exceeded. ("
                << resultSize << " > " << ReplySizeLimit << ")";

            auto issue = YqlIssue({}, TIssuesIds::KIKIMR_RESULT_UNAVAILABLE, message);
            ReplyErrorAndDie(Ydb::StatusIds::PRECONDITION_FAILED, issue);
            return;
        }

        LWTRACK(KqpDataExecuterFinalize, ResponseEv->Orbit, TxId, LastShard, ResponseEv->ResultsSize(), ResponseEv->GetByteSize());

        if (ExecuterStateSpan) {
            ExecuterStateSpan.End();
            ExecuterStateSpan = {};
        }

        if (ExecuterSpan) {
            ExecuterSpan.EndOk();
        }

        Request.Transactions.crop(0);
        LOG_D("Sending response to: " << Target << ", results: " << ResponseEv->ResultsSize());
        Send(Target, ResponseEv.release());
        PassAway();
    }

    STATEFN(WaitResolveState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpExecuter::TEvTableResolveStatus, HandleResolve);
                hFunc(TEvKqpExecuter::TEvShardsResolveStatus, HandleResolve);
                hFunc(TEvKqp::TEvAbortExecution, HandleAbortExecution);
                default:
                    UnexpectedEvent("WaitResolveState", ev->GetTypeRewrite());
            }

        } catch (const yexception& e) {
            InternalError(e.what());
        }
        ReportEventElapsedTime();
    }

private:
    TString CurrentStateFuncName() const override {
        const auto& func = CurrentStateFunc();
        if (func == &TThis::PrepareState) {
            return "PrepareState";
        } else if (func == &TThis::ExecuteState) {
            return "ExecuteState";
        } else if (func == &TThis::WaitSnapshotState) {
            return "WaitSnapshotState";
        } else if (func == &TThis::WaitResolveState) {
            return "WaitResolveState";
        } else {
            return TBase::CurrentStateFuncName();
        }
    }

    STATEFN(PrepareState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvColumnShard::TEvProposeTransactionResult, HandlePrepare);
                hFunc(TEvDataShard::TEvProposeTransactionResult, HandlePrepare);
                hFunc(TEvDataShard::TEvProposeTransactionRestart, HandleExecute);
                hFunc(TEvDataShard::TEvProposeTransactionAttachResult, HandlePrepare);
                hFunc(TEvPersQueue::TEvProposeTransactionResult, HandlePrepare);
                hFunc(TEvPrivate::TEvReattachToShard, HandleExecute);
                hFunc(TEvDqCompute::TEvState, HandlePrepare); // from CA
                hFunc(TEvDqCompute::TEvChannelData, HandleExecute); // from CA
                hFunc(TEvPipeCache::TEvDeliveryProblem, HandlePrepare);
                hFunc(TEvKqp::TEvAbortExecution, HandlePrepare);
                hFunc(TEvents::TEvUndelivered, HandleUndelivered);
                hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
                hFunc(TEvKqpNode::TEvStartKqpTasksResponse, HandleStartKqpTasksResponse);
                IgnoreFunc(TEvInterconnect::TEvNodeConnected);
                default: {
                    CancelProposal(0);
                    UnexpectedEvent("PrepareState", ev->GetTypeRewrite());
                }
            }
        } catch (const yexception& e) {
            CancelProposal(0);
            InternalError(e.what());
        }
        ReportEventElapsedTime();
    }

    void HandlePrepare(TEvPersQueue::TEvProposeTransactionResult::TPtr& ev) {
        NKikimrPQ::TEvProposeTransactionResult& event = ev->Get()->Record;

        LOG_D("Got propose result, topic tablet: " << event.GetOrigin() << ", status: "
            << NKikimrPQ::TEvProposeTransactionResult_EStatus_Name(event.GetStatus()));

        TShardState *state = TopicTabletStates.FindPtr(event.GetOrigin());
        YQL_ENSURE(state);

        YQL_ENSURE(event.GetStatus() == NKikimrPQ::TEvProposeTransactionResult::ERROR);

        auto issue = YqlIssue({}, TIssuesIds::KIKIMR_OPERATION_ABORTED);
        ReplyErrorAndDie(Ydb::StatusIds::ABORTED, issue);
    }

    void HandlePrepare(TEvDataShard::TEvProposeTransactionResult::TPtr& ev) {
        TEvDataShard::TEvProposeTransactionResult* res = ev->Get();
        const ui64 shardId = res->GetOrigin();
        TShardState* shardState = ShardStates.FindPtr(shardId);
        YQL_ENSURE(shardState, "Unexpected propose result from unknown tabletId " << shardId);

        LOG_D("Got propose result, shard: " << shardId << ", status: "
            << NKikimrTxDataShard::TEvProposeTransactionResult_EStatus_Name(res->GetStatus())
            << ", error: " << res->GetError());

        if (Stats) {
            Stats->AddDatashardPrepareStats(std::move(*res->Record.MutableTxStats()));
        }

        switch (res->GetStatus()) {
            case NKikimrTxDataShard::TEvProposeTransactionResult::PREPARED: {
                if (!ShardPrepared(*shardState, res->Record)) {
                    return CancelProposal(shardId);
                }
                return CheckPrepareCompleted();
            }
            case NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE: {
                YQL_ENSURE(false);
            }
            default: {
                CancelProposal(shardId);
                return ShardError(res->Record);
            }
        }
    }

    void HandlePrepare(TEvColumnShard::TEvProposeTransactionResult::TPtr& ev) {
        TEvColumnShard::TEvProposeTransactionResult* res = ev->Get();
        const ui64 shardId = res->Record.GetOrigin();
        TShardState* shardState = ShardStates.FindPtr(shardId);
        YQL_ENSURE(shardState, "Unexpected propose result from unknown tabletId " << shardId);

        LOG_D("Got propose result, shard: " << shardId << ", status: "
            << NKikimrTxColumnShard::EResultStatus_Name(res->Record.GetStatus())
            << ", error: " << res->Record.GetStatusMessage());

//        if (Stats) {
//            Stats->AddDatashardPrepareStats(std::move(*res->Record.MutableTxStats()));
//        }

        switch (res->Record.GetStatus()) {
            case NKikimrTxColumnShard::EResultStatus::PREPARED:
            {
                if (!ShardPrepared(*shardState, res->Record)) {
                    return CancelProposal(shardId);
                }
                return CheckPrepareCompleted();
            }
            case NKikimrTxColumnShard::EResultStatus::SUCCESS:
            {
                YQL_ENSURE(false);
            }
            default:
            {
                CancelProposal(shardId);
                return ShardError(res->Record);
            }
        }
    }

    void HandlePrepare(TEvDataShard::TEvProposeTransactionAttachResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const ui64 tabletId = record.GetTabletId();

        auto* shardState = ShardStates.FindPtr(tabletId);
        YQL_ENSURE(shardState, "Unknown tablet " << tabletId);

        if (ev->Cookie != shardState->ReattachState.Cookie) {
            return;
        }

        switch (shardState->State) {
            case TShardState::EState::Preparing:
            case TShardState::EState::Prepared:
                break;
            case TShardState::EState::Initial:
            case TShardState::EState::Executing:
            case TShardState::EState::Finished:
                YQL_ENSURE(false, "Unexpected shard " << tabletId << " state " << ToString(shardState->State));
        }

        if (record.GetStatus() == NKikimrProto::OK) {
            // Transaction still exists at this shard
            LOG_D("Reattached to shard " << tabletId << ", state was: " << ToString(shardState->State));
            shardState->State = TShardState::EState::Prepared;
            shardState->ReattachState.Reattached();
            return CheckPrepareCompleted();
        }

        LOG_E("Shard " << tabletId << " transaction lost during reconnect: " << record.GetStatus());

        CancelProposal(tabletId);
        ReplyTxStateUnknown(tabletId);
    }

    void HandlePrepare(TEvDqCompute::TEvState::TPtr& ev) {
        if (ev->Get()->Record.GetState() == NDqProto::COMPUTE_STATE_FAILURE) {
            CancelProposal(0);
        }
        HandleComputeStats(ev);
    }

    void HandlePrepare(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        TEvPipeCache::TEvDeliveryProblem* msg = ev->Get();
        auto* shardState = ShardStates.FindPtr(msg->TabletId);
        YQL_ENSURE(shardState, "EvDeliveryProblem from unknown tablet " << msg->TabletId);

        bool wasRestarting = std::exchange(shardState->Restarting, false);

        // We can only be sure tx was not prepared if initial propose was not delivered
        bool notPrepared = msg->NotDelivered && (shardState->RestartCount == 0);

        switch (shardState->State) {
            case TShardState::EState::Preparing: {
                // Disconnected while waiting for initial propose response

                LOG_I("Shard " << msg->TabletId << " propose error, notDelivered: " << msg->NotDelivered
                    << ", notPrepared: " << notPrepared << ", wasRestart: " << wasRestarting);

                if (notPrepared) {
                    CancelProposal(msg->TabletId);
                    return ReplyUnavailable(TStringBuilder() << "Could not deliver program to shard " << msg->TabletId);
                }

                CancelProposal(0);

                if (wasRestarting) {
                    // We are waiting for propose and have a restarting flag, which means shard was
                    // persisting our tx. We did not receive a reply, so we cannot be sure if it
                    // succeeded or not, but we know that it could not apply any side effects, since
                    // we don't start transaction planning until prepare phase is complete.
                    return ReplyUnavailable(TStringBuilder() << "Could not prepare program on shard " << msg->TabletId);
                }

                return ReplyTxStateUnknown(msg->TabletId);
            }

            case TShardState::EState::Prepared: {
                // Disconnected while waiting for other shards to prepare

                if ((wasRestarting || shardState->ReattachState.Reattaching) &&
                    shardState->ReattachState.ShouldReattach(TlsActivationContext->Now()))
                {
                    LOG_N("Shard " << msg->TabletId << " delivery problem (already prepared, reattaching in "
                        << shardState->ReattachState.Delay << ")");

                    Schedule(shardState->ReattachState.Delay, new TEvPrivate::TEvReattachToShard(msg->TabletId));
                    ++shardState->RestartCount;
                    return;
                }

                LOG_N("Shard " << msg->TabletId << " delivery problem (already prepared)"
                    << (msg->NotDelivered ? ", last message not delivered" : ""));

                CancelProposal(0);
                return ReplyTxStateUnknown(msg->TabletId);
            }

            case TShardState::EState::Initial:
            case TShardState::EState::Executing:
            case TShardState::EState::Finished:
                YQL_ENSURE(false, "Unexpected shard " << msg->TabletId << " state " << ToString(shardState->State));
        }
    }

    void HandlePrepare(TEvKqp::TEvAbortExecution::TPtr& ev) {
        CancelProposal(0);
        TBase::HandleAbortExecution(ev);
    }

    void CancelProposal(ui64 exceptShardId) {
        for (auto& [shardId, state] : ShardStates) {
            if (shardId != exceptShardId &&
                (state.State == TShardState::EState::Preparing
                 || state.State == TShardState::EState::Prepared
                 || (state.State == TShardState::EState::Executing && ImmediateTx)))
            {
                ui64 id = shardId;
                LOG_D("Send CancelTransactionProposal to shard: " << id);

                state.State = TShardState::EState::Finished;

                YQL_ENSURE(!state.DatashardState->Follower);

                Send(MakePipePeNodeCacheID(/* allowFollowers */ false), new TEvPipeCache::TEvForward(
                    new TEvDataShard::TEvCancelTransactionProposal(TxId), shardId, /* subscribe */ false));
            }
        }
    }

    bool ShardPrepared(TShardState& state, const NKikimrTxDataShard::TEvProposeTransactionResult& result) {
        YQL_ENSURE(state.State == TShardState::EState::Preparing);
        state.State = TShardState::EState::Prepared;

        state.DatashardState->ShardMinStep = result.GetMinStep();
        state.DatashardState->ShardMaxStep = result.GetMaxStep();
        state.DatashardState->ReadSize += result.GetReadSize();

        ui64 coordinator = 0;
        if (result.DomainCoordinatorsSize()) {
            auto domainCoordinators = TCoordinators(TVector<ui64>(result.GetDomainCoordinators().begin(),
                                                                  result.GetDomainCoordinators().end()));
            coordinator = domainCoordinators.Select(TxId);
        }

        if (coordinator && !TxCoordinator) {
            TxCoordinator = coordinator;
        }

        if (!TxCoordinator || TxCoordinator != coordinator) {
            LOG_E("Handle TEvProposeTransactionResult: unable to select coordinator. Tx canceled, actorId: " << SelfId()
                << ", previously selected coordinator: " << TxCoordinator
                << ", coordinator selected at propose result: " << coordinator);

            Counters->TxProxyMon->TxResultAborted->Inc();
            ReplyErrorAndDie(Ydb::StatusIds::CANCELLED, MakeIssue(
                NKikimrIssues::TIssuesIds::TX_DECLINED_IMPLICIT_COORDINATOR, "Unable to choose coordinator."));
            return false;
        }

        LastPrepareReply = TInstant::Now();
        if (!FirstPrepareReply) {
            FirstPrepareReply = LastPrepareReply;
        }

        return true;
    }

    bool ShardPrepared(TShardState& state, const NKikimrTxColumnShard::TEvProposeTransactionResult& result) {
        YQL_ENSURE(state.State == TShardState::EState::Preparing);
        state.State = TShardState::EState::Prepared;

        state.DatashardState->ShardMinStep = result.GetMinStep();
        state.DatashardState->ShardMaxStep = result.GetMaxStep();
//        state.DatashardState->ReadSize += result.GetReadSize();

        ui64 coordinator = 0;
        if (result.DomainCoordinatorsSize()) {
            auto domainCoordinators = TCoordinators(TVector<ui64>(result.GetDomainCoordinators().begin(),
                result.GetDomainCoordinators().end()));
            coordinator = domainCoordinators.Select(TxId);
        }

        if (coordinator && !TxCoordinator) {
            TxCoordinator = coordinator;
        }

        if (!TxCoordinator || TxCoordinator != coordinator) {
            LOG_E("Handle TEvProposeTransactionResult: unable to select coordinator. Tx canceled, actorId: " << SelfId()
                << ", previously selected coordinator: " << TxCoordinator
                << ", coordinator selected at propose result: " << coordinator);

            Counters->TxProxyMon->TxResultAborted->Inc();
            ReplyErrorAndDie(Ydb::StatusIds::CANCELLED, MakeIssue(
                NKikimrIssues::TIssuesIds::TX_DECLINED_IMPLICIT_COORDINATOR, "Unable to choose coordinator."));
            return false;
        }

        LastPrepareReply = TInstant::Now();
        if (!FirstPrepareReply) {
            FirstPrepareReply = LastPrepareReply;
        }

        return true;
    }

    void ShardError(const NKikimrTxDataShard::TEvProposeTransactionResult& result) {
        if (result.ErrorSize() != 0) {
            TStringBuilder message;
            message << NKikimrTxDataShard::TEvProposeTransactionResult_EStatus_Name(result.GetStatus()) << ": ";
            for (const auto &err : result.GetError()) {
                message << "[" << NKikimrTxDataShard::TError_EKind_Name(err.GetKind()) << "] " << err.GetReason() << "; ";
            }
            LOG_E(message);
        }

        switch (result.GetStatus()) {
            case NKikimrTxDataShard::TEvProposeTransactionResult::OVERLOADED: {
                Counters->TxProxyMon->TxResultShardOverloaded->Inc();
                auto issue = YqlIssue({}, TIssuesIds::KIKIMR_OVERLOADED);
                AddDataShardErrors(result, issue);
                return ReplyErrorAndDie(Ydb::StatusIds::OVERLOADED, issue);
            }
            case NKikimrTxDataShard::TEvProposeTransactionResult::ABORTED: {
                Counters->TxProxyMon->TxResultAborted->Inc();
                auto issue = YqlIssue({}, TIssuesIds::KIKIMR_OPERATION_ABORTED);
                AddDataShardErrors(result, issue);
                return ReplyErrorAndDie(Ydb::StatusIds::ABORTED, issue);
            }
            case NKikimrTxDataShard::TEvProposeTransactionResult::TRY_LATER: {
                Counters->TxProxyMon->TxResultShardTryLater->Inc();
                auto issue = YqlIssue({}, TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE);
                AddDataShardErrors(result, issue);
                return ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, issue);
            }
            case NKikimrTxDataShard::TEvProposeTransactionResult::RESULT_UNAVAILABLE: {
                Counters->TxProxyMon->TxResultResultUnavailable->Inc();
                auto issue = YqlIssue({}, TIssuesIds::KIKIMR_RESULT_UNAVAILABLE);
                AddDataShardErrors(result, issue);
                return ReplyErrorAndDie(Ydb::StatusIds::UNDETERMINED, issue);
            }
            case NKikimrTxDataShard::TEvProposeTransactionResult::CANCELLED: {
                Counters->TxProxyMon->TxResultCancelled->Inc();
                auto issue = YqlIssue({}, TIssuesIds::KIKIMR_OPERATION_CANCELLED);
                AddDataShardErrors(result, issue);
                return ReplyErrorAndDie(Ydb::StatusIds::CANCELLED, issue);
            }
            case NKikimrTxDataShard::TEvProposeTransactionResult::BAD_REQUEST: {
                Counters->TxProxyMon->TxResultCancelled->Inc();
                if (HasMissingSnapshotError(result)) {
                    auto issue = YqlIssue({}, TIssuesIds::KIKIMR_PRECONDITION_FAILED);
                    AddDataShardErrors(result, issue);
                    return ReplyErrorAndDie(Ydb::StatusIds::PRECONDITION_FAILED, issue);
                }
                auto issue = YqlIssue({}, TIssuesIds::KIKIMR_BAD_REQUEST);
                AddDataShardErrors(result, issue);
                return ReplyErrorAndDie(Ydb::StatusIds::BAD_REQUEST, issue);
            }
            case NKikimrTxDataShard::TEvProposeTransactionResult::EXEC_ERROR: {
                Counters->TxProxyMon->TxResultExecError->Inc();
                for (auto& er : result.GetError()) {
                    if (er.GetKind() == NKikimrTxDataShard::TError::PROGRAM_ERROR) {
                        auto issue = YqlIssue({}, TIssuesIds::KIKIMR_PRECONDITION_FAILED);
                        issue.AddSubIssue(new TIssue(TStringBuilder() << "Data shard error: [PROGRAM_ERROR] " << er.GetReason()));
                        return ReplyErrorAndDie(Ydb::StatusIds::PRECONDITION_FAILED, issue);
                    }
                }
                auto issue = YqlIssue({}, TIssuesIds::DEFAULT_ERROR, "Error executing transaction (ExecError): Execution failed");
                AddDataShardErrors(result, issue);
                return ReplyErrorAndDie(Ydb::StatusIds::GENERIC_ERROR, issue);
            }
            case NKikimrTxDataShard::TEvProposeTransactionResult::ERROR: {
                Counters->TxProxyMon->TxResultError->Inc();
                for (auto& er : result.GetError()) {
                    switch (er.GetKind()) {
                        case NKikimrTxDataShard::TError::SCHEME_CHANGED:
                        case NKikimrTxDataShard::TError::SCHEME_ERROR:
                            return ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR, YqlIssue({},
                                TIssuesIds::KIKIMR_SCHEME_MISMATCH, er.GetReason()));

                        default:
                            break;
                    }
                }
                auto issue = YqlIssue({}, TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE);
                AddDataShardErrors(result, issue);
                return ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, issue);
            }
            default: {
                Counters->TxProxyMon->TxResultFatal->Inc();
                auto issue = YqlIssue({}, TIssuesIds::DEFAULT_ERROR, "Error executing transaction: transaction failed.");
                AddDataShardErrors(result, issue);
                return ReplyErrorAndDie(Ydb::StatusIds::GENERIC_ERROR, issue);
            }
        }
    }

    void ShardError(const NKikimrTxColumnShard::TEvProposeTransactionResult& result) {
        if (!!result.GetStatusMessage()) {
            TStringBuilder message;
            message << NKikimrTxColumnShard::EResultStatus_Name(result.GetStatus()) << ": ";
            message << "[" << result.GetStatusMessage() << "]" << "; ";
            LOG_E(message);
        }

        switch (result.GetStatus()) {
            case NKikimrTxColumnShard::EResultStatus::OVERLOADED:
            {
                Counters->TxProxyMon->TxResultShardOverloaded->Inc();
                auto issue = YqlIssue({}, TIssuesIds::KIKIMR_OVERLOADED);
                AddColumnShardErrors(result, issue);
                return ReplyErrorAndDie(Ydb::StatusIds::OVERLOADED, issue);
            }
            case NKikimrTxColumnShard::EResultStatus::ABORTED:
            {
                Counters->TxProxyMon->TxResultAborted->Inc();
                auto issue = YqlIssue({}, TIssuesIds::KIKIMR_OPERATION_ABORTED);
                AddColumnShardErrors(result, issue);
                return ReplyErrorAndDie(Ydb::StatusIds::ABORTED, issue);
            }
            case NKikimrTxColumnShard::EResultStatus::TIMEOUT:
            {
                Counters->TxProxyMon->TxResultShardTryLater->Inc();
                auto issue = YqlIssue({}, TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE);
                AddColumnShardErrors(result, issue);
                return ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, issue);
            }
            case NKikimrTxColumnShard::EResultStatus::ERROR:
            {
                Counters->TxProxyMon->TxResultError->Inc();
                auto issue = YqlIssue({}, TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE);
                AddColumnShardErrors(result, issue);
                return ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, issue);
            }
            default:
            {
                Counters->TxProxyMon->TxResultFatal->Inc();
                auto issue = YqlIssue({}, TIssuesIds::DEFAULT_ERROR, "Error executing transaction: transaction failed." + NKikimrTxColumnShard::EResultStatus_Name(result.GetStatus()));
                AddColumnShardErrors(result, issue);
                return ReplyErrorAndDie(Ydb::StatusIds::GENERIC_ERROR, issue);
            }
        }
    }

    void PQTabletError(const NKikimrPQ::TEvProposeTransactionResult& result) {
        NYql::TIssuesIds::EIssueCode issueCode;
        Ydb::StatusIds::StatusCode statusCode;

        switch (result.GetStatus()) {
        default: {
            issueCode = TIssuesIds::DEFAULT_ERROR;
            statusCode = Ydb::StatusIds::GENERIC_ERROR;
            break;
        }
        case NKikimrPQ::TEvProposeTransactionResult::ABORTED: {
            issueCode = TIssuesIds::KIKIMR_OPERATION_ABORTED;
            statusCode = Ydb::StatusIds::ABORTED;
            break;
        }
        case NKikimrPQ::TEvProposeTransactionResult::BAD_REQUEST: {
            issueCode = TIssuesIds::KIKIMR_BAD_REQUEST;
            statusCode = Ydb::StatusIds::BAD_REQUEST;
            break;
        }
        case NKikimrPQ::TEvProposeTransactionResult::CANCELLED: {
            issueCode = TIssuesIds::KIKIMR_OPERATION_CANCELLED;
            statusCode = Ydb::StatusIds::CANCELLED;
            break;
        }
        case NKikimrPQ::TEvProposeTransactionResult::OVERLOADED: {
            issueCode = TIssuesIds::KIKIMR_OVERLOADED;
            statusCode = Ydb::StatusIds::OVERLOADED;
            break;
        }
        }

        auto issue = YqlIssue({}, issueCode);
        ReplyErrorAndDie(statusCode, issue);
    }

    void CheckPrepareCompleted() {
        for (const auto& [_, state] : ShardStates) {
            if (state.State != TShardState::EState::Prepared) {
                LOG_D("Not all shards are prepared, waiting...");
                return;
            }
        }

        Counters->TxProxyMon->TxPrepareSpreadHgram->Collect((LastPrepareReply - FirstPrepareReply).MilliSeconds());

        LOG_D("All shards prepared, become ExecuteState.");
        Become(&TKqpDataExecuter::ExecuteState);
        if (ExecuterStateSpan) {
            ExecuterStateSpan.End();
            ExecuterStateSpan = NWilson::TSpan(TWilsonKqp::DataExecuterExecuteState, ExecuterSpan.GetTraceId(), "ExecuteState", NWilson::EFlags::AUTO_END);
        }

        ExecutePlanned();
    }

    void ExecutePlanned() {
        YQL_ENSURE(TxCoordinator);

        auto ev = MakeHolder<TEvTxProxy::TEvProposeTransaction>();
        ev->Record.SetCoordinatorID(TxCoordinator);

        auto& transaction = *ev->Record.MutableTransaction();
        auto& affectedSet = *transaction.MutableAffectedSet();
        affectedSet.Reserve(static_cast<int>(ShardStates.size()));

        //
        // TODO(abcdef): учесть таблетки топиков
        //

        ui64 aggrMinStep = 0;
        ui64 aggrMaxStep = Max<ui64>();
        ui64 totalReadSize = 0;

        for (auto& [shardId, state] : ShardStates) {
            YQL_ENSURE(state.State == TShardState::EState::Prepared);
            state.State = TShardState::EState::Executing;

            YQL_ENSURE(state.DatashardState.Defined());
            YQL_ENSURE(!state.DatashardState->Follower);

            aggrMinStep = Max(aggrMinStep, state.DatashardState->ShardMinStep);
            aggrMaxStep = Min(aggrMaxStep, state.DatashardState->ShardMaxStep);
            totalReadSize += state.DatashardState->ReadSize;

            auto& item = *affectedSet.Add();
            item.SetTabletId(shardId);

            ui32 affectedFlags = 0;
            if (state.DatashardState->ShardReadLocks) {
                affectedFlags |= NFlatTxCoordinator::TTransactionProposal::TAffectedEntry::AffectedRead;
            }

            for (auto taskId : state.TaskIds) {
                auto& task = TasksGraph.GetTask(taskId);
                auto& stageInfo = TasksGraph.GetStageInfo(task.StageId);

                if (stageInfo.Meta.HasReads()) {
                    affectedFlags |= NFlatTxCoordinator::TTransactionProposal::TAffectedEntry::AffectedRead;
                }
                if (stageInfo.Meta.HasWrites()) {
                    affectedFlags |= NFlatTxCoordinator::TTransactionProposal::TAffectedEntry::AffectedWrite;
                }
            }

            //
            // TODO(abcdef): учесть таблетки топиков
            //

            item.SetFlags(affectedFlags);
        }

        ui64 sizeLimit = Request.PerRequestDataSizeLimit;
        if (Request.TotalReadSizeLimitBytes > 0) {
            sizeLimit = sizeLimit
                ? std::min(sizeLimit, Request.TotalReadSizeLimitBytes)
                : Request.TotalReadSizeLimitBytes;
        }

        if (totalReadSize > sizeLimit) {
            auto msg = TStringBuilder() << "Transaction total read size " << totalReadSize << " exceeded limit " << sizeLimit;
            LOG_N(msg);
            ReplyErrorAndDie(Ydb::StatusIds::PRECONDITION_FAILED,
                YqlIssue({}, NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED, msg));
            return;
        }

        transaction.SetTxId(TxId);
        transaction.SetMinStep(aggrMinStep);
        transaction.SetMaxStep(aggrMaxStep);

        LOG_T("Execute planned transaction, coordinator: " << TxCoordinator);
        Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvForward(ev.Release(), TxCoordinator, /* subscribe */ true));
    }

private:
    STATEFN(ExecuteState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvColumnShard::TEvProposeTransactionResult, HandleExecute);
                hFunc(TEvDataShard::TEvProposeTransactionResult, HandleExecute);
                hFunc(TEvDataShard::TEvProposeTransactionRestart, HandleExecute);
                hFunc(TEvDataShard::TEvProposeTransactionAttachResult, HandleExecute);
                hFunc(TEvPersQueue::TEvProposeTransactionResult, HandleExecute);
                hFunc(TEvPrivate::TEvReattachToShard, HandleExecute);
                hFunc(TEvPipeCache::TEvDeliveryProblem, HandleExecute);
                hFunc(TEvents::TEvUndelivered, HandleUndelivered);
                hFunc(TEvPrivate::TEvRetry, HandleRetry);
                hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
                hFunc(TEvKqpNode::TEvStartKqpTasksResponse, HandleStartKqpTasksResponse);
                hFunc(TEvTxProxy::TEvProposeTransactionStatus, HandleExecute);
                hFunc(TEvDqCompute::TEvState, HandleComputeStats);
                hFunc(TEvDqCompute::TEvChannelData, HandleExecute);
                hFunc(TEvKqp::TEvAbortExecution, HandleExecute);
                IgnoreFunc(TEvInterconnect::TEvNodeConnected);
                default:
                    UnexpectedEvent("ExecuteState", ev->GetTypeRewrite());
            }
        } catch (const yexception& e) {
            InternalError(e.what());
        }
        ReportEventElapsedTime();
    }

    void HandleExecute(TEvPersQueue::TEvProposeTransactionResult::TPtr& ev) {
        NKikimrPQ::TEvProposeTransactionResult& event = ev->Get()->Record;

        LOG_D("Got propose result, topic tablet: " << event.GetOrigin() << ", status: "
            << NKikimrPQ::TEvProposeTransactionResult_EStatus_Name(event.GetStatus()));

        TShardState *state = TopicTabletStates.FindPtr(event.GetOrigin());
        YQL_ENSURE(state);

        switch (event.GetStatus()) {
            case NKikimrPQ::TEvProposeTransactionResult::COMPLETE: {
                YQL_ENSURE(state->State == TShardState::EState::Executing);

                state->State = TShardState::EState::Finished;
                CheckExecutionComplete();

                return;
            }
            case NKikimrPQ::TEvProposeTransactionResult::PREPARED: {
                YQL_ENSURE(false);
            }
            default: {
                PQTabletError(event);
                return;
            }
        }
    }

    void HandleExecute(TEvKqp::TEvAbortExecution::TPtr& ev) {
        if (ImmediateTx) {
            CancelProposal(0);
        }
        TBase::HandleAbortExecution(ev);
    }

    void HandleExecute(TEvColumnShard::TEvProposeTransactionResult::TPtr& ev) {
        TEvColumnShard::TEvProposeTransactionResult* res = ev->Get();
        const ui64 shardId = res->Record.GetOrigin();
        LastShard = shardId;

        TShardState* shardState = ShardStates.FindPtr(shardId);
        YQL_ENSURE(shardState);

        LOG_D("Got propose result, shard: " << shardId << ", status: "
            << NKikimrTxColumnShard::EResultStatus_Name(res->Record.GetStatus())
            << ", error: " << res->Record.GetStatusMessage());

//        if (Stats) {
//            Stats->AddDatashardStats(std::move(*res->Record.MutableComputeActorStats()),
//                std::move(*res->Record.MutableTxStats()));
//        }

        switch (res->Record.GetStatus()) {
            case NKikimrTxColumnShard::EResultStatus::SUCCESS:
            {
                YQL_ENSURE(shardState->State == TShardState::EState::Executing);
                shardState->State = TShardState::EState::Finished;

                Counters->TxProxyMon->ResultsReceivedCount->Inc();
//                Counters->TxProxyMon->ResultsReceivedSize->Add(res->GetTxResult().size());

//                for (auto& lock : res->Record.GetTxLocks()) {
//                    LOG_D("Shard " << shardId << " completed, store lock " << lock.ShortDebugString());
//                    Locks.emplace_back(std::move(lock));
//                }

                Counters->TxProxyMon->TxResultComplete->Inc();

                CheckExecutionComplete();
                return;
            }
            case NKikimrTxColumnShard::EResultStatus::PREPARED:
            {
                YQL_ENSURE(false);
            }
            default:
            {
                return ShardError(res->Record);
            }
        }
    }

    void HandleExecute(TEvDataShard::TEvProposeTransactionResult::TPtr& ev) {
        TEvDataShard::TEvProposeTransactionResult* res = ev->Get();
        const ui64 shardId = res->GetOrigin();
        LastShard = shardId;

        TShardState* shardState = ShardStates.FindPtr(shardId);
        YQL_ENSURE(shardState);

        LOG_D("Got propose result, shard: " << shardId << ", status: "
            << NKikimrTxDataShard::TEvProposeTransactionResult_EStatus_Name(res->GetStatus())
            << ", error: " << res->GetError());

        if (Stats) {
            Stats->AddDatashardStats(std::move(*res->Record.MutableComputeActorStats()),
                std::move(*res->Record.MutableTxStats()));
        }

        switch (res->GetStatus()) {
            case NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE: {
                YQL_ENSURE(shardState->State == TShardState::EState::Executing);
                shardState->State = TShardState::EState::Finished;

                Counters->TxProxyMon->ResultsReceivedCount->Inc();
                Counters->TxProxyMon->ResultsReceivedSize->Add(res->GetTxResult().size());

                for (auto& lock : res->Record.GetTxLocks()) {
                    LOG_D("Shard " << shardId << " completed, store lock " << lock.ShortDebugString());
                    Locks.emplace_back(std::move(lock));
                }

                Counters->TxProxyMon->TxResultComplete->Inc();

                CheckExecutionComplete();
                return;
            }
            case NKikimrTxDataShard::TEvProposeTransactionResult::LOCKS_BROKEN: {
                LOG_D("Broken locks: " << res->Record.DebugString());

                YQL_ENSURE(shardState->State == TShardState::EState::Executing);
                shardState->State = TShardState::EState::Finished;

                Counters->TxProxyMon->TxResultAborted->Inc(); // TODO: dedicated counter?

                LocksBroken = true;

                TMaybe<TString> tableName;
                if (!res->Record.GetTxLocks().empty()) {
                    auto& lock = res->Record.GetTxLocks(0);
                    auto tableId = TTableId(lock.GetSchemeShard(), lock.GetPathId());
                    auto it = FindIf(GetTableKeys().Get(), [tableId](const auto& x){ return x.first.HasSamePath(tableId); });
                    if (it != GetTableKeys().Get().end()) {
                        tableName = it->second.Path;
                    }
                }

                // Reply as soon as we know which table had locks invalidated
                if (tableName) {
                    auto message = TStringBuilder()
                        << "Transaction locks invalidated. Table: " << *tableName;

                    return ReplyErrorAndDie(Ydb::StatusIds::ABORTED,
                        YqlIssue({}, TIssuesIds::KIKIMR_LOCKS_INVALIDATED, message));
                }

                // Receive more replies from other shards
                CheckExecutionComplete();
                return;
            }
            case NKikimrTxDataShard::TEvProposeTransactionResult::PREPARED: {
                YQL_ENSURE(false);
            }
            default: {
                return ShardError(res->Record);
            }
        }
    }

    void HandleExecute(TEvDataShard::TEvProposeTransactionRestart::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const ui64 shardId = record.GetTabletId();

        auto* shardState = ShardStates.FindPtr(shardId);
        YQL_ENSURE(shardState, "restart tx event from unknown tabletId: " << shardId << ", tx: " << TxId);

        LOG_D("Got transaction restart event from tabletId: " << shardId << ", state: " << ToString(shardState->State)
            << ", txPlanned: " << TxPlanned);

        switch (shardState->State) {
            case TShardState::EState::Preparing:
            case TShardState::EState::Prepared:
            case TShardState::EState::Executing: {
                shardState->Restarting = true;
                return;
            }
            case TShardState::EState::Finished: {
                return;
            }
            case TShardState::EState::Initial: {
                YQL_ENSURE(false);
            }
        }
    }

    void HandleExecute(TEvDataShard::TEvProposeTransactionAttachResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const ui64 tabletId = record.GetTabletId();

        auto* shardState = ShardStates.FindPtr(tabletId);
        YQL_ENSURE(shardState, "Unknown tablet " << tabletId);

        if (ev->Cookie != shardState->ReattachState.Cookie) {
            return;
        }

        switch (shardState->State) {
            case TShardState::EState::Executing:
                break;
            case TShardState::EState::Initial:
            case TShardState::EState::Preparing:
            case TShardState::EState::Prepared:
            case TShardState::EState::Finished:
                return;
        }

        if (record.GetStatus() == NKikimrProto::OK) {
            // Transaction still exists at this shard
            LOG_N("Reattached to shard " << tabletId << ", state was: " << ToString(shardState->State));
            shardState->ReattachState.Reattached();

            CheckExecutionComplete();
            return;
        }

        LOG_E("Shard " << tabletId << " transaction lost during reconnect: " << record.GetStatus());

        ReplyTxStateUnknown(tabletId);
    }

    void HandleExecute(TEvPrivate::TEvReattachToShard::TPtr& ev) {
        const ui64 tabletId = ev->Get()->TabletId;
        auto* shardState = ShardStates.FindPtr(tabletId);
        YQL_ENSURE(shardState);

        LOG_I("Reattach to shard " << tabletId);

        Send(MakePipePeNodeCacheID(UseFollowers), new TEvPipeCache::TEvForward(
            new TEvDataShard::TEvProposeTransactionAttach(tabletId, TxId),
            tabletId, /* subscribe */ true), 0, ++shardState->ReattachState.Cookie);
    }

    void HandleExecute(TEvTxProxy::TEvProposeTransactionStatus::TPtr &ev) {
        TEvTxProxy::TEvProposeTransactionStatus* res = ev->Get();
        LOG_D("Got transaction status, status: " << res->GetStatus());

        switch (res->GetStatus()) {
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusAccepted:
                Counters->TxProxyMon->ClientTxStatusAccepted->Inc();
                break;
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusProcessed:
                Counters->TxProxyMon->ClientTxStatusProcessed->Inc();
                break;
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusConfirmed:
                Counters->TxProxyMon->ClientTxStatusConfirmed->Inc();
                break;

            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusPlanned:
                Counters->TxProxyMon->ClientTxStatusPlanned->Inc();
                TxPlanned = true;
                break;

            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusOutdated:
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusDeclined:
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusDeclinedNoSpace:
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusRestarting:
                Counters->TxProxyMon->ClientTxStatusCoordinatorDeclined->Inc();
                CancelProposal(0);
                ReplyUnavailable(TStringBuilder() << "Failed to plan transaction, status: " << res->GetStatus());
                break;

            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusUnknown:
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusAborted:
                Counters->TxProxyMon->ClientTxStatusCoordinatorDeclined->Inc();
                InternalError(TStringBuilder() << "Unexpected TEvProposeTransactionStatus status: " << res->GetStatus());
                break;
        }
    }

    void HandleExecute(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        TEvPipeCache::TEvDeliveryProblem* msg = ev->Get();

        LOG_D("DeliveryProblem to shard " << msg->TabletId << ", notDelivered: " << msg->NotDelivered
            << ", txPlanned: " << TxPlanned << ", coordinator: " << TxCoordinator);

        if (msg->TabletId == TxCoordinator) {
            if (msg->NotDelivered) {
                LOG_E("Not delivered to coordinator " << msg->TabletId << ", abort execution");
                CancelProposal(0);
                return ReplyUnavailable("Delivery problem: could not plan transaction.");
            }

            if (TxPlanned) {
                // We lost pipe to coordinator, but we already know tx is planned
                return;
            }

            LOG_E("Delivery problem to coordinator " << msg->TabletId << ", abort execution");
            return ReplyTxStateUnknown(msg->TabletId);
        }

        auto* shardState = ShardStates.FindPtr(msg->TabletId);
        YQL_ENSURE(shardState, "EvDeliveryProblem from unknown shard " << msg->TabletId);

        bool wasRestarting = std::exchange(shardState->Restarting, false);

        switch (shardState->State) {
            case TShardState::EState::Prepared: // is it correct?
                LOG_E("DeliveryProblem to shard " << msg->TabletId << ", notDelivered: " << msg->NotDelivered
                    << ", txPlanned: " << TxPlanned << ", coordinator: " << TxCoordinator);
                Y_VERIFY_DEBUG(false);
                // Proceed with query processing
                [[fallthrough]];
            case TShardState::EState::Executing: {
                if ((wasRestarting || shardState->ReattachState.Reattaching) &&
                     shardState->ReattachState.ShouldReattach(TlsActivationContext->Now()))
                {
                    LOG_N("Shard " << msg->TabletId << " lost pipe while waiting for reply (reattaching in "
                        << shardState->ReattachState.Delay << ")");

                    Schedule(shardState->ReattachState.Delay, new TEvPrivate::TEvReattachToShard(msg->TabletId));
                    ++shardState->RestartCount;
                    return;
                }

                LOG_N("Shard " << msg->TabletId << " lost pipe while waiting for reply"
                    << (msg->NotDelivered ? " (last message not delivered)" : ""));

                return ReplyTxStateUnknown(msg->TabletId);
            }

            case TShardState::EState::Finished: {
                return;
            }

            case TShardState::EState::Initial:
            case TShardState::EState::Preparing:
                YQL_ENSURE(false, "Unexpected shard " << msg->TabletId << " state " << ToString(shardState->State));
        }
    }

    void HandleExecute(TEvDqCompute::TEvChannelData::TPtr& ev) {
        auto& record = ev->Get()->Record;
        auto& channelData = record.GetChannelData();

        auto& channel = TasksGraph.GetChannel(channelData.GetChannelId());
        YQL_ENSURE(channel.DstTask == 0);
        auto shardId = TasksGraph.GetTask(channel.SrcTask).Meta.ShardId;

        if (Stats) {
            Stats->ResultBytes += channelData.GetData().GetRaw().size();
            Stats->ResultRows += channelData.GetData().GetRows();
        }

        LOG_T("Got result, channelId: " << channel.Id << ", shardId: " << shardId
            << ", inputIndex: " << channel.DstInputIndex << ", from: " << ev->Sender
            << ", finished: " << channelData.GetFinished());

        ResponseEv->TakeResult(channel.DstInputIndex, std::move(*record.MutableChannelData()->MutableData()));
        {
            LOG_T("Send ack to channelId: " << channel.Id << ", seqNo: " << record.GetSeqNo() << ", to: " << ev->Sender);

            auto ackEv = MakeHolder<TEvDqCompute::TEvChannelDataAck>();
            ackEv->Record.SetSeqNo(record.GetSeqNo());
            ackEv->Record.SetChannelId(channel.Id);
            ackEv->Record.SetFreeSpace(50_MB);
            Send(ev->Sender, ackEv.Release(), /* TODO: undelivery */ 0, /* cookie */ channel.Id);
        }
    }

private:
    bool IsReadOnlyTx() const {
        if (Request.TopicOperations.HasOperations()) {
            YQL_ENSURE(!Request.UseImmediateEffects);
            return false;
        }

        if (Request.ValidateLocks && Request.EraseLocks) {
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

    void FillGeneralReadInfo(TTaskMeta& taskMeta, ui64 itemsLimit, bool reverse) {
        if (taskMeta.Reads && !taskMeta.Reads.GetRef().empty()) {
            // Validate parameters
            YQL_ENSURE(taskMeta.ReadInfo.ItemsLimit == itemsLimit);
            YQL_ENSURE(taskMeta.ReadInfo.Reverse == reverse);
            return;
        }

        taskMeta.ReadInfo.ItemsLimit = itemsLimit;
        taskMeta.ReadInfo.Reverse = reverse;
    };

    void BuildDatashardTasks(TStageInfo& stageInfo) {
        THashMap<ui64, ui64> shardTasks; // shardId -> taskId

        auto getShardTask = [&](ui64 shardId) -> TTask& {
            auto it  = shardTasks.find(shardId);
            if (it != shardTasks.end()) {
                return TasksGraph.GetTask(it->second);
            }
            auto& task = TasksGraph.AddTask(stageInfo);
            task.Meta.ExecuterId = SelfId();
            task.Meta.ShardId = shardId;
            shardTasks.emplace(shardId, task.Id);
            return task;
        };

        auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

        const auto& table = GetTableKeys().GetTable(stageInfo.Meta.TableId);
        const auto& keyTypes = table.KeyColumnTypes;;

        for (auto& op : stage.GetTableOps()) {
            Y_VERIFY_DEBUG(stageInfo.Meta.TablePath == op.GetTable().GetPath());
            auto columns = BuildKqpColumns(op, table);
            switch (op.GetTypeCase()) {
                case NKqpProto::TKqpPhyTableOperation::kReadRanges:
                case NKqpProto::TKqpPhyTableOperation::kReadRange:
                case NKqpProto::TKqpPhyTableOperation::kLookup: {
                    auto partitions = PrunePartitions(GetTableKeys(), op, stageInfo, HolderFactory(), TypeEnv());
                    auto readSettings = ExtractReadSettings(op, stageInfo, HolderFactory(), TypeEnv());

                    for (auto& [shardId, shardInfo] : partitions) {
                        YQL_ENSURE(!shardInfo.KeyWriteRanges);

                        auto& task = getShardTask(shardId);
                        for (auto& [name, value] : shardInfo.Params) {
                            task.Meta.Params.emplace(name, std::move(value));
                        }

                        FillGeneralReadInfo(task.Meta, readSettings.ItemsLimit, readSettings.Reverse);

                        TTaskMeta::TShardReadInfo readInfo;
                        readInfo.Ranges = std::move(*shardInfo.KeyReadRanges);
                        readInfo.Columns = columns;

                        if (readSettings.ItemsLimitParamName) {
                            task.Meta.Params.emplace(readSettings.ItemsLimitParamName, readSettings.ItemsLimitBytes);
                        }

                        if (!task.Meta.Reads) {
                            task.Meta.Reads.ConstructInPlace();
                        }
                        task.Meta.Reads->emplace_back(std::move(readInfo));
                    }

                    break;
                }

                case NKqpProto::TKqpPhyTableOperation::kUpsertRows:
                case NKqpProto::TKqpPhyTableOperation::kDeleteRows: {
                    YQL_ENSURE(stage.InputsSize() <= 1, "Effect stage with multiple inputs: " << stage.GetProgramAst());

                    if (stage.InputsSize() == 1 && stage.GetInputs(0).GetTypeCase() == NKqpProto::TKqpPhyConnection::kMapShard) {
                        const auto& inputStageInfo = TasksGraph.GetStageInfo(
                            TStageId(stageInfo.Id.TxId, stage.GetInputs(0).GetStageIndex()));

                        for (ui64 inputTaskId : inputStageInfo.Tasks) {
                            auto& task = getShardTask(TasksGraph.GetTask(inputTaskId).Meta.ShardId);

                            auto& inputTask = TasksGraph.GetTask(inputTaskId);
                            YQL_ENSURE(inputTask.Meta.Reads, "" << inputTask.Meta.ToString(keyTypes, *AppData()->TypeRegistry));
                            for (auto& read : *inputTask.Meta.Reads) {
                                if (!task.Meta.Writes) {
                                    task.Meta.Writes.ConstructInPlace();
                                    task.Meta.Writes->Ranges = read.Ranges;
                                } else {
                                    task.Meta.Writes->Ranges.MergeWritePoints(TShardKeyRanges(read.Ranges), keyTypes);
                                }

                                if (op.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kDeleteRows) {
                                    task.Meta.Writes->AddEraseOp();
                                } else {
                                    task.Meta.Writes->AddUpdateOp();
                                }

                            }

                            ShardsWithEffects.insert(task.Meta.ShardId);
                        }
                    } else {
                        auto result = PruneEffectPartitions(GetTableKeys(), op, stageInfo, HolderFactory(), TypeEnv());
                        for (auto& [shardId, shardInfo] : result) {
                            YQL_ENSURE(!shardInfo.KeyReadRanges);
                            YQL_ENSURE(shardInfo.KeyWriteRanges);

                            auto& task = getShardTask(shardId);
                            task.Meta.Params = std::move(shardInfo.Params);

                            if (!task.Meta.Writes) {
                                task.Meta.Writes.ConstructInPlace();
                                task.Meta.Writes->Ranges = std::move(*shardInfo.KeyWriteRanges);
                            } else {
                                task.Meta.Writes->Ranges.MergeWritePoints(std::move(*shardInfo.KeyWriteRanges), keyTypes);
                            }

                            if (op.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kDeleteRows) {
                                task.Meta.Writes->AddEraseOp();
                            } else {
                                task.Meta.Writes->AddUpdateOp();
                            }

                            for (const auto& [name, info] : shardInfo.ColumnWrites) {
                                auto& column = table.Columns.at(name);

                                auto& taskColumnWrite = task.Meta.Writes->ColumnWrites[column.Id];
                                taskColumnWrite.Column.Id = column.Id;
                                taskColumnWrite.Column.Type = column.Type;
                                taskColumnWrite.Column.Name = name;
                                taskColumnWrite.MaxValueSizeBytes = std::max(taskColumnWrite.MaxValueSizeBytes,
                                    info.MaxValueSizeBytes);
                            }

                            ShardsWithEffects.insert(shardId);
                        }
                    }
                    break;
                }

                case NKqpProto::TKqpPhyTableOperation::kReadOlapRange: {
                    YQL_ENSURE(false, "The previous check did not work! Data query read does not support column shard tables." << Endl
                        << this->DebugString());
                }

                default: {
                    YQL_ENSURE(false, "Unexpected table operation: " << (ui32) op.GetTypeCase() << Endl
                        << this->DebugString());
                }
            }
        }

        LOG_D("Stage " << stageInfo.Id << " will be executed on " << shardTasks.size() << " shards.");

        for (auto& shardTask : shardTasks) {
            auto& task = TasksGraph.GetTask(shardTask.second);
            LOG_D("ActorState: " << CurrentStateFuncName()
                << ", stage: " << stageInfo.Id << " create datashard task: " << shardTask.second
                << ", shard: " << shardTask.first
                << ", meta: " << task.Meta.ToString(keyTypes, *AppData()->TypeRegistry));
        }
    }

    void BuildComputeTasks(TStageInfo& stageInfo) {
        auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

        ui32 partitionsCount = 1;
        for (ui32 inputIndex = 0; inputIndex < stage.InputsSize(); ++inputIndex) {
            const auto& input = stage.GetInputs(inputIndex);

            // Current assumptions:
            // 1. `Broadcast` can not be the 1st stage input unless it's a single input
            // 2. All stage's inputs, except 1st one, must be a `Broadcast` or `UnionAll`
            if (inputIndex == 0) {
                if (stage.InputsSize() > 1) {
                    YQL_ENSURE(input.GetTypeCase() != NKqpProto::TKqpPhyConnection::kBroadcast);
                }
            } else {
                switch (input.GetTypeCase()) {
                    case NKqpProto::TKqpPhyConnection::kBroadcast:
                    case NKqpProto::TKqpPhyConnection::kHashShuffle:
                    case NKqpProto::TKqpPhyConnection::kUnionAll:
                    case NKqpProto::TKqpPhyConnection::kMerge:
                        break;
                    default:
                        YQL_ENSURE(false, "Unexpected connection type: " << (ui32)input.GetTypeCase() << Endl
                            << this->DebugString());
                }
            }

            auto& originStageInfo = TasksGraph.GetStageInfo(TStageId(stageInfo.Id.TxId, input.GetStageIndex()));

            switch (input.GetTypeCase()) {
                case NKqpProto::TKqpPhyConnection::kHashShuffle: {
                    partitionsCount = std::max(partitionsCount, (ui32)originStageInfo.Tasks.size() / 2);
                    partitionsCount = std::min(partitionsCount, 24u);
                    break;
                }

                case NKqpProto::TKqpPhyConnection::kStreamLookup:
                    HasStreamLookup = true;
                case NKqpProto::TKqpPhyConnection::kMap: {
                    partitionsCount = originStageInfo.Tasks.size();
                    break;
                }

                default: {
                    break;
                }
            }
        }

        for (ui32 i = 0; i < partitionsCount; ++i) {
            auto& task = TasksGraph.AddTask(stageInfo);
            task.Meta.ExecuterId = SelfId();
            LOG_D("Stage " << stageInfo.Id << " create compute task: " << task.Id);
        }
    }

    void ExecuteDatashardTransaction(ui64 shardId, NKikimrTxDataShard::TKqpTransaction& kqpTx,
        const TMaybe<ui64> lockTxId, const bool isOlap)
    {
        TShardState shardState;
        shardState.State = ImmediateTx ? TShardState::EState::Executing : TShardState::EState::Preparing;
        shardState.DatashardState.ConstructInPlace();
        shardState.DatashardState->Follower = UseFollowers;

        if (Deadline) {
            TDuration timeout = *Deadline - TAppData::TimeProvider->Now();
            kqpTx.MutableRuntimeSettings()->SetTimeoutMs(timeout.MilliSeconds());
        }
        kqpTx.MutableRuntimeSettings()->SetExecType(NDqProto::TComputeRuntimeSettings::DATA);
        kqpTx.MutableRuntimeSettings()->SetStatsMode(GetDqStatsModeShard(Request.StatsMode));

        kqpTx.MutableRuntimeSettings()->SetUseLLVM(false);
        kqpTx.MutableRuntimeSettings()->SetUseSpilling(false);

        NKikimrTxDataShard::TDataTransaction dataTransaction;
        dataTransaction.MutableKqpTransaction()->Swap(&kqpTx);
        dataTransaction.SetImmediate(ImmediateTx);
        dataTransaction.SetReadOnly(ReadOnlyTx);
        if (CancelAt) {
            dataTransaction.SetCancelAfterMs((*CancelAt - AppData()->TimeProvider->Now()).MilliSeconds());
        }
        if (Request.PerShardKeysSizeLimitBytes) {
            YQL_ENSURE(!ReadOnlyTx);
            dataTransaction.SetPerShardKeysSizeLimitBytes(Request.PerShardKeysSizeLimitBytes);
        }

        if (lockTxId) {
            dataTransaction.SetLockTxId(*lockTxId);
            dataTransaction.SetLockNodeId(SelfId().NodeId());
        }

        for (auto& task : dataTransaction.GetKqpTransaction().GetTasks()) {
            shardState.TaskIds.insert(task.GetId());
        }

        auto locksCount = dataTransaction.GetKqpTransaction().GetLocks().LocksSize();
        shardState.DatashardState->ShardReadLocks = locksCount > 0;

        LOG_D("State: " << CurrentStateFuncName()
            << ", Executing KQP transaction on shard: " << shardId
            << ", tasks: [" << JoinStrings(shardState.TaskIds.begin(), shardState.TaskIds.end(), ",") << "]"
            << ", lockTxId: " << lockTxId
            << ", locks: " << dataTransaction.GetKqpTransaction().GetLocks().ShortDebugString());

        std::unique_ptr<IEventBase> ev;
        if (isOlap) {
            const ui32 flags =
                (ImmediateTx ? NKikimrTxColumnShard::ETransactionFlag::TX_FLAG_IMMEDIATE: 0);
            ev.reset(new TEvColumnShard::TEvProposeTransaction(
                NKikimrTxColumnShard::TX_KIND_DATA,
                SelfId(),
                TxId,
                dataTransaction.SerializeAsString(),
                flags));
        } else {
            const ui32 flags =
                (ImmediateTx ? NTxDataShard::TTxFlags::Immediate : 0) |
                (VolatileTx ? NTxDataShard::TTxFlags::VolatilePrepare : 0);
            if (GetSnapshot().IsValid() && (ReadOnlyTx || Request.UseImmediateEffects)) {
                ev.reset(new TEvDataShard::TEvProposeTransaction(
                    NKikimrTxDataShard::TX_KIND_DATA,
                    SelfId(),
                    TxId,
                    dataTransaction.SerializeAsString(),
                    GetSnapshot().Step,
                    GetSnapshot().TxId,
                    flags));
            } else {
                ev.reset(new TEvDataShard::TEvProposeTransaction(
                    NKikimrTxDataShard::TX_KIND_DATA,
                    SelfId(),
                    TxId,
                    dataTransaction.SerializeAsString(),
                    flags));
            }
        }
        auto traceId = ExecuterSpan.GetTraceId();

        LOG_D("ExecuteDatashardTransaction traceId.verbosity: " << std::to_string(traceId.GetVerbosity()));

        Send(MakePipePeNodeCacheID(UseFollowers), new TEvPipeCache::TEvForward(ev.release(), shardId, true), 0, 0, std::move(traceId));

        auto result = ShardStates.emplace(shardId, std::move(shardState));
        YQL_ENSURE(result.second);
    }

    void ExecuteDataComputeTask(NDqProto::TDqTask&& taskDesc, bool shareMailbox) {
        auto taskId = taskDesc.GetId();
        auto& task = TasksGraph.GetTask(taskId);

        TComputeRuntimeSettings settings;
        if (Deadline) {
            settings.Timeout = *Deadline - TAppData::TimeProvider->Now();
        }
        //settings.ExtraMemoryAllocationPool = NRm::EKqpMemoryPool::DataQuery;
        settings.ExtraMemoryAllocationPool = NRm::EKqpMemoryPool::Unspecified;
        settings.FailOnUndelivery = true;
        settings.StatsMode = GetDqStatsMode(Request.StatsMode);
        settings.UseLLVM = false;
        settings.UseSpilling = false;

        TComputeMemoryLimits limits;
        limits.ChannelBufferSize = 50_MB;
        limits.MkqlLightProgramMemoryLimit = Request.MkqlMemoryLimit > 0 ? std::min(500_MB, Request.MkqlMemoryLimit) : 500_MB;
        limits.MkqlHeavyProgramMemoryLimit = Request.MkqlMemoryLimit > 0 ? std::min(2_GB, Request.MkqlMemoryLimit) : 2_GB;
        limits.AllocateMemoryFn = [TxId = TxId](auto /* txId */, ui64 taskId, ui64 memory) {
            LOG_E("Data query task cannot allocate additional memory during executing."
                      << " Task: " << taskId << ", memory: " << memory);
            return false;
        };

        auto computeActor = CreateKqpComputeActor(SelfId(), TxId, std::move(taskDesc), AsyncIoFactory,
            AppData()->FunctionRegistry, settings, limits);

        auto computeActorId = shareMailbox ? RegisterWithSameMailbox(computeActor) : Register(computeActor);
        task.ComputeActorId = computeActorId;

        LOG_D("Executing task: " << taskId << " on compute actor: " << task.ComputeActorId);

        auto result = PendingComputeActors.emplace(task.ComputeActorId, TProgressStat());
        YQL_ENSURE(result.second);
    }

    void Execute() {
        LockTxId = Request.AcquireLocksTxId;
        if (LockTxId.Defined() && *LockTxId == 0) {
            LockTxId = TxId;
        }

        NWilson::TSpan prepareTasksSpan(TWilsonKqp::DataExecuterPrepateTasks, ExecuterStateSpan.GetTraceId(), "PrepateTasks", NWilson::EFlags::AUTO_END);
        LWTRACK(KqpDataExecuterStartExecute, ResponseEv->Orbit, TxId);

        size_t readActors = 0;
        for (ui32 txIdx = 0; txIdx < Request.Transactions.size(); ++txIdx) {
            auto& tx = Request.Transactions[txIdx];

            for (ui32 stageIdx = 0; stageIdx < tx.Body->StagesSize(); ++stageIdx) {
                auto& stage = tx.Body->GetStages(stageIdx);
                auto& stageInfo = TasksGraph.GetStageInfo(TStageId(txIdx, stageIdx));

                if (stageInfo.Meta.ShardKind == NSchemeCache::TSchemeCacheRequest::KindAsyncIndexTable) {
                    TMaybe<TString> error;

                    if (stageInfo.Meta.ShardKey->RowOperation != TKeyDesc::ERowOperation::Read) {
                        error = TStringBuilder() << "Non-read operations can't be performed on async index table"
                            << ": " << stageInfo.Meta.ShardKey->TableId;
                    } else if (Request.IsolationLevel != NKikimrKqp::ISOLATION_LEVEL_READ_STALE) {
                        error = TStringBuilder() << "Read operation can be performed on async index table"
                            << ": " << stageInfo.Meta.ShardKey->TableId << " only with StaleRO isolation level";
                    }

                    if (error) {
                        LOG_E(*error);
                        ReplyErrorAndDie(Ydb::StatusIds::PRECONDITION_FAILED,
                            YqlIssue({}, NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED, *error));
                        return;
                    }
                }

                if (stageInfo.Meta.IsOlap() && tx.Body->GetType() == NKqpProto::TKqpPhyTx::TYPE_DATA) {
                    auto error = TStringBuilder() << "Data manipulation queries do not support column shard tables.";
                    LOG_E(error);
                    ReplyErrorAndDie(Ydb::StatusIds::PRECONDITION_FAILED,
                        YqlIssue({}, NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED, error));
                    return;
                }

                LOG_D("Stage " << stageInfo.Id << " AST: " << stage.GetProgramAst());

                if (stage.SourcesSize() > 0) {
                    switch (stage.GetSources(0).GetTypeCase()) {
                        case NKqpProto::TKqpSource::kReadRangesSource:
                            readActors += BuildScanTasksFromSource(stageInfo, LockTxId);
                            break;
                        case NKqpProto::TKqpSource::kExternalSource:
                            BuildReadTasksFromSource(stageInfo);
                            break;
                        default:
                            YQL_ENSURE(false, "unknown source type");
                    }
                } else if (stageInfo.Meta.ShardOperations.empty()) {
                    BuildComputeTasks(stageInfo);
                } else if (stageInfo.Meta.IsSysView()) {
                    BuildSysViewScanTasks(stageInfo);
                } else {
                    BuildDatashardTasks(stageInfo);
                }

                if (stage.GetIsSinglePartition()) {
                    YQL_ENSURE(stageInfo.Tasks.size() == 1, "Unexpected multiple tasks in single-partition stage");
                }

                BuildKqpStageChannels(TasksGraph, GetTableKeys(), stageInfo, TxId, /* enableSpilling */ false);
            }

            ResponseEv->InitTxResult(tx.Body);
            BuildKqpTaskGraphResultChannels(TasksGraph, tx.Body, txIdx);
        }

        TIssue validateIssue;
        if (!ValidateTasks(TasksGraph, EExecType::Data, /* enableSpilling */ false, validateIssue)) {
            ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, validateIssue);
            return;
        }

        THashMap<ui64, TVector<NDqProto::TDqTask>> datashardTasks;  // shardId -> [task]
        THashMap<ui64, TVector<ui64>> remoteComputeTasks;  // shardId -> [task]
        TVector<ui64> computeTasks;

        if (StreamResult) {
            InitializeChannelProxies();
        }

        for (auto& task : TasksGraph.GetTasks()) {
            auto& stageInfo = TasksGraph.GetStageInfo(task.StageId);
            if (task.Meta.ShardId && (task.Meta.Reads || task.Meta.Writes)) {
                auto protoTask = SerializeTaskToProto(TasksGraph, task);
                datashardTasks[task.Meta.ShardId].emplace_back(std::move(protoTask));
            } else if (stageInfo.Meta.IsSysView()) {
                computeTasks.emplace_back(task.Id);
            } else {
                if (task.Meta.ShardId) {
                    remoteComputeTasks[task.Meta.ShardId].emplace_back(task.Id);
                } else {
                    computeTasks.emplace_back(task.Id);
                }
            }
        }

        for(const auto& channel: TasksGraph.GetChannels()) {
            if (IsCrossShardChannel(TasksGraph, channel)) {
                HasPersistentChannels = true;
                break;
            }
        }

        if (computeTasks.size() > Request.MaxComputeActors) {
            LOG_N("Too many compute actors: " << computeTasks.size());
            ReplyErrorAndDie(Ydb::StatusIds::PRECONDITION_FAILED,
                YqlIssue({}, TIssuesIds::KIKIMR_PRECONDITION_FAILED, TStringBuilder()
                    << "Requested too many execution units: " << computeTasks.size()));
            return;
        }

        ui32 shardsLimit = Request.MaxAffectedShards;
        if (i64 msc = (i64) Request.MaxShardCount; msc > 0) {
            shardsLimit = std::min(shardsLimit, (ui32) msc);
        }
        size_t shards = datashardTasks.size() + remoteComputeTasks.size();
        if (shardsLimit > 0 && shards > shardsLimit) {
            LOG_W("Too many affected shards: datashardTasks=" << shards << ", limit: " << shardsLimit);
            Counters->TxProxyMon->TxResultError->Inc();
            ReplyErrorAndDie(Ydb::StatusIds::PRECONDITION_FAILED,
                YqlIssue({}, TIssuesIds::KIKIMR_PRECONDITION_FAILED, TStringBuilder()
                    << "Affected too many shards: " << datashardTasks.size()));
            return;
        }

        bool fitSize = AllOf(datashardTasks, [this](const auto& x){ return ValidateTaskSize(x.second); });
        if (!fitSize) {
            Counters->TxProxyMon->TxResultError->Inc();
            return;
        }

        TTopicTabletTxs topicTxs;
        auto datashardTxs = BuildDatashardTxs(datashardTasks, topicTxs);

        // Single-shard transactions are always immediate
        ImmediateTx = (datashardTxs.size() + Request.TopicOperations.GetSize() + readActors) <= 1 && !HasStreamLookup;

        if (ImmediateTx) {
            // Transaction cannot be both immediate and volatile
            YQL_ENSURE(!VolatileTx);
        }

        switch (Request.IsolationLevel) {
            // OnlineRO with AllowInconsistentReads = true
            case NKikimrKqp::ISOLATION_LEVEL_READ_UNCOMMITTED:
            // StaleRO transactions always execute as immediate
            // (legacy behavior, for compatibility with current execution engine)
            case NKikimrKqp::ISOLATION_LEVEL_READ_STALE:
                YQL_ENSURE(ReadOnlyTx);
                YQL_ENSURE(!VolatileTx);
                ImmediateTx = true;
                break;

            default:
                break;
        }

        if ((ReadOnlyTx || Request.UseImmediateEffects) && GetSnapshot().IsValid()) {
            // Snapshot reads are always immediate
            // Uncommitted writes are executed without coordinators, so they can be immediate
            YQL_ENSURE(!VolatileTx);
            ImmediateTx = true;
        }

        ComputeTasks = std::move(computeTasks);
        DatashardTxs = std::move(datashardTxs);
        TopicTxs = std::move(topicTxs);
        RemoteComputeTasks = std::move(remoteComputeTasks);

        if (prepareTasksSpan) {
            prepareTasksSpan.End();
        }

        if (RemoteComputeTasks) {
            TSet<ui64> shardIds;
            for (const auto& [shardId, _] : RemoteComputeTasks) {
                shardIds.insert(shardId);
            }

            auto kqpShardsResolver = CreateKqpShardsResolver(SelfId(), TxId, std::move(shardIds));
            RegisterWithSameMailbox(kqpShardsResolver);
            Become(&TKqpDataExecuter::WaitResolveState);
        } else {
            OnShardsResolve();
        }
    }

    void HandleResolve(TEvKqpExecuter::TEvTableResolveStatus::TPtr& ev) {
        if (!TBase::HandleResolve(ev)) return;
        Execute();
    }

    void HandleResolve(TEvKqpExecuter::TEvShardsResolveStatus::TPtr& ev) {
        if (!TBase::HandleResolve(ev)) return;
        OnShardsResolve();
    }

    void OnShardsResolve() {
        const bool forceSnapshot = (
                ReadOnlyTx &&
                !ImmediateTx &&
                !HasPersistentChannels &&
                (!Database.empty() || AppData()->EnableMvccSnapshotWithLegacyDomainRoot) &&
                AppData()->FeatureFlags.GetEnableMvccSnapshotReads());

        if (forceSnapshot) {
            YQL_ENSURE(!VolatileTx);
            auto longTxService = NLongTxService::MakeLongTxServiceID(SelfId().NodeId());
            Send(longTxService, new NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot(Database));

            LOG_T("Create temporary mvcc snapshot, ebcome WaitSnapshotState");
            Become(&TKqpDataExecuter::WaitSnapshotState);
            if (ExecuterStateSpan) {
                ExecuterStateSpan.End();
                ExecuterStateSpan = NWilson::TSpan(TWilsonKqp::DataExecuterWaitSnapshotState, ExecuterSpan.GetTraceId(), "WaitSnapshotState", NWilson::EFlags::AUTO_END);
            }

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
                default:
                    UnexpectedEvent("WaitSnapshotState", ev->GetTypeRewrite());
            }
        } catch (const yexception& e) {
            InternalError(e.what());
        }
        ReportEventElapsedTime();
    }

    void Handle(NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult::TPtr& ev) {
        auto& record = ev->Get()->Record;

        if (record.GetStatus() != Ydb::StatusIds::SUCCESS) {
            ReplyErrorAndDie(record.GetStatus(), record.MutableIssues());
            return;
        }

        TasksGraph.GetMeta().Snapshot = TKqpSnapshot(record.GetSnapshotStep(), record.GetSnapshotTxId());
        ImmediateTx = true;

        ContinueExecute();
    }

    using TDatashardTxs = THashMap<ui64, NKikimrTxDataShard::TKqpTransaction>;
    using TTopicTabletTxs = THashMap<ui64, NKikimrPQ::TDataTransaction>;

    void ContinueExecute() {
        UseFollowers = Request.IsolationLevel == NKikimrKqp::ISOLATION_LEVEL_READ_STALE;

        if (!ImmediateTx) {
            // Followers only allowed for single shard transactions.
            // (legacy behaviour, for compatibility with current execution engine)
            UseFollowers = false;
        }
        if (GetSnapshot().IsValid()) {
            // TODO: KIKIMR-11912
            UseFollowers = false;
        }
        if (UseFollowers) {
            YQL_ENSURE(ReadOnlyTx);
        }

        if (Stats) {
            //Stats->AffectedShards = datashardTxs.size();
            Stats->DatashardStats.reserve(DatashardTxs.size());
            //Stats->ComputeStats.reserve(computeTasks.size());
        }

        ExecuteTasks();

        if (ImmediateTx) {
            LOG_D("ActorState: " << CurrentStateFuncName()
                << ", immediate tx, become ExecuteState");
            Become(&TKqpDataExecuter::ExecuteState);
            if (ExecuterStateSpan) {
                ExecuterStateSpan.End();
                ExecuterStateSpan = NWilson::TSpan(TWilsonKqp::DataExecuterExecuteState, ExecuterSpan.GetTraceId(), "ExecuteState", NWilson::EFlags::AUTO_END);
            }
        } else {
            LOG_D("ActorState: " << CurrentStateFuncName()
                << ", not immediate tx, become PrepareState");
            Become(&TKqpDataExecuter::PrepareState);
            if (ExecuterStateSpan) {
                ExecuterStateSpan.End();
                ExecuterStateSpan = NWilson::TSpan(TWilsonKqp::DataExecuterPrepareState, ExecuterSpan.GetTraceId(), "PrepareState", NWilson::EFlags::AUTO_END);
            }
        }
    }

    TDatashardTxs BuildDatashardTxs(const THashMap<ui64, TVector<NDqProto::TDqTask>>& datashardTasks,
                                    TTopicTabletTxs& topicTxs) {
        TDatashardTxs datashardTxs;

        for (auto& [shardId, tasks]: datashardTasks) {
            auto& dsTxs = datashardTxs[shardId];
            for (auto& task: tasks) {
                dsTxs.AddTasks()->CopyFrom(task);
            }
        }

        Request.TopicOperations.BuildTopicTxs(topicTxs);

        const bool needRollback = Request.EraseLocks && !Request.ValidateLocks;

        VolatileTx = (
            // We want to use volatile transactions only when the feature is enabled
            AppData()->FeatureFlags.GetEnableDataShardVolatileTransactions() &&
            // We don't want volatile tx when acquiring locks (including write locks for uncommitted writes)
            !Request.AcquireLocksTxId &&
            // We don't want readonly volatile transactions
            !ReadOnlyTx &&
            // We only want to use volatile transactions with side-effects
            !ShardsWithEffects.empty() &&
            // We don't want to use volatile transactions when doing a rollback
            !needRollback &&
            // We cannot use volatile transactions with topics
            // TODO: add support in the future
            topicTxs.empty() &&
            // We only want to use volatile transactions for multiple shards
            (datashardTasks.size() + topicTxs.size()) > 1 &&
            // We cannot use volatile transactions with persistent channels
            // Note: currently persistent channels are never used
            !HasPersistentChannels);

        const bool useGenericReadSets = (
            // Use generic readsets when feature is explicitly enabled
            AppData()->FeatureFlags.GetEnableDataShardGenericReadSets() ||
            // Volatile transactions must always use generic readsets
            VolatileTx ||
            // Transactions with topics must always use generic readsets
            !topicTxs.empty());

        if (auto locksMap = Request.DataShardLocks;
            !locksMap.empty() ||
            VolatileTx ||
            Request.TopicOperations.HasReadOperations())
        {
            YQL_ENSURE(Request.ValidateLocks || Request.EraseLocks || VolatileTx);

            bool needCommit = (
                (Request.ValidateLocks && Request.EraseLocks) ||
                VolatileTx);

            auto locksOp = needCommit
                ? NKikimrTxDataShard::TKqpLocks::Commit
                : (Request.ValidateLocks
                        ? NKikimrTxDataShard::TKqpLocks::Validate
                        : NKikimrTxDataShard::TKqpLocks::Rollback);

            absl::flat_hash_set<ui64> sendingShardsSet;
            absl::flat_hash_set<ui64> receivingShardsSet;

            // Gather shards that need to send/receive readsets (shards with effects)
            if (needCommit) {
                for (auto& [shardId, _] : datashardTasks) {
                    if (ShardsWithEffects.contains(shardId)) {
                        // Volatile transactions may abort effects, so they send readsets
                        if (VolatileTx) {
                            sendingShardsSet.insert(shardId);
                        }
                        receivingShardsSet.insert(shardId);
                    }
                }

                if (auto tabletIds = Request.TopicOperations.GetSendingTabletIds()) {
                    sendingShardsSet.insert(tabletIds.begin(), tabletIds.end());
                }

                if (auto tabletIds = Request.TopicOperations.GetReceivingTabletIds()) {
                    receivingShardsSet.insert(tabletIds.begin(), tabletIds.end());
                }
            }

            // Gather locks that need to be committed or erased
            for (auto& [shardId, locksList] : locksMap) {
                auto& tx = datashardTxs[shardId];
                tx.MutableLocks()->SetOp(locksOp);

                if (!locksList.empty()) {
                    auto* protoLocks = tx.MutableLocks()->MutableLocks();
                    protoLocks->Reserve(locksList.size());
                    bool hasWrites = false;
                    for (auto& lock : locksList) {
                        hasWrites = hasWrites || lock.GetHasWrites();
                        protoLocks->Add()->Swap(&lock);
                    }

                    if (needCommit) {
                        // We also send the result on commit
                        sendingShardsSet.insert(shardId);

                        if (hasWrites) {
                            // Tx with uncommitted changes can be aborted due to conflicts,
                            // so shards with write locks should receive readsets
                            receivingShardsSet.insert(shardId);
                        }
                    }
                }
            }

            if (Request.ValidateLocks || needCommit) {
                NProtoBuf::RepeatedField<ui64> sendingShards(sendingShardsSet.begin(), sendingShardsSet.end());
                NProtoBuf::RepeatedField<ui64> receivingShards(receivingShardsSet.begin(), receivingShardsSet.end());

                std::sort(sendingShards.begin(), sendingShards.end());
                std::sort(receivingShards.begin(), receivingShards.end());

                for (auto& [shardId, shardTx] : datashardTxs) {
                    shardTx.MutableLocks()->SetOp(locksOp);
                    *shardTx.MutableLocks()->MutableSendingShards() = sendingShards;
                    *shardTx.MutableLocks()->MutableReceivingShards() = receivingShards;
                }

                for (auto& [_, tx] : topicTxs) {
                    switch (locksOp) {
                    case NKikimrTxDataShard::TKqpLocks::Commit:
                        tx.SetOp(NKikimrPQ::TDataTransaction::Commit);
                        break;
                    case NKikimrTxDataShard::TKqpLocks::Validate:
                        tx.SetOp(NKikimrPQ::TDataTransaction::Validate);
                        break;
                    case NKikimrTxDataShard::TKqpLocks::Rollback:
                        tx.SetOp(NKikimrPQ::TDataTransaction::Rollback);
                        break;
                    case NKikimrTxDataShard::TKqpLocks::Unspecified:
                        break;
                    }

                    *tx.MutableSendingShards() = sendingShards;
                    *tx.MutableReceivingShards() = receivingShards;
                }
            }
        }

        if (useGenericReadSets) {
            // Make sure datashards use generic readsets
            for (auto& pr : datashardTxs) {
                pr.second.SetUseGenericReadSets(true);
            }
        }

        return datashardTxs;
    }

    void ExecuteTasks() {
        {
            auto lockTxId = Request.AcquireLocksTxId;
            if (lockTxId.Defined() && *lockTxId == 0) {
                lockTxId = TxId;
                LockHandle = TLockHandle(TxId, TActivationContext::ActorSystem());
            }
        }

        NWilson::TSpan sendTasksSpan(TWilsonKqp::DataExecuterSendTasksAndTxs, ExecuterStateSpan.GetTraceId(), "SendTasksAndTxs", NWilson::EFlags::AUTO_END);
        LWTRACK(KqpDataExecuterStartTasksAndTxs, ResponseEv->Orbit, TxId, ComputeTasks.size(), DatashardTxs.size());

        // first, start compute tasks
        bool shareMailbox = (ComputeTasks.size() <= 1);
        for (ui64 taskId : ComputeTasks) {
            const auto& task = TasksGraph.GetTask(taskId);
            auto taskDesc = SerializeTaskToProto(TasksGraph, task);
            ExecuteDataComputeTask(std::move(taskDesc), shareMailbox);
        }

        size_t remoteComputeTasksCnt = 0;
        THashMap<ui64, TVector<NDqProto::TDqTask>> tasksPerNode;
        for (auto& [shardId, tasks] : RemoteComputeTasks) {
            auto it = ShardIdToNodeId.find(shardId);
            YQL_ENSURE(it != ShardIdToNodeId.end());

            for (ui64 taskId : tasks) {
                const auto& task = TasksGraph.GetTask(taskId);
                remoteComputeTasksCnt += 1;
                PendingComputeTasks.insert(taskId);
                auto taskDesc = SerializeTaskToProto(TasksGraph, task);
                tasksPerNode[it->second].emplace_back(std::move(taskDesc));
            }
        }

        Planner = CreateKqpPlanner(TxId, SelfId(), {}, std::move(tasksPerNode), GetSnapshot(),
            Database, UserToken, Deadline.GetOrElse(TInstant::Zero()), Request.StatsMode,
            Request.DisableLlvmForUdfStages, Request.LlvmEnabled, false, Nothing(),
            ExecuterSpan, {}, ExecuterRetriesConfig);
        Planner->ProcessTasksForDataExecuter();

        // then start data tasks with known actor ids of compute tasks
        for (auto& [shardId, shardTx] : DatashardTxs) {
            shardTx.SetType(NKikimrTxDataShard::KQP_TX_TYPE_DATA);
            std::optional<bool> isOlap;
            for (auto& protoTask : *shardTx.MutableTasks()) {
                ui64 taskId = protoTask.GetId();
                auto& task = TasksGraph.GetTask(taskId);
                auto& stageInfo = TasksGraph.GetStageInfo(task.StageId);
                Y_ENSURE(!isOlap || *isOlap == stageInfo.Meta.IsOlap());
                isOlap = stageInfo.Meta.IsOlap();

                for (ui64 outputIndex = 0; outputIndex < task.Outputs.size(); ++outputIndex) {
                    auto& output = task.Outputs[outputIndex];
                    auto* protoOutput = protoTask.MutableOutputs(outputIndex);

                    for (ui64 outputChannelIndex = 0; outputChannelIndex < output.Channels.size(); ++outputChannelIndex) {
                        ui64 outputChannelId = output.Channels[outputChannelIndex];
                        auto* protoChannel = protoOutput->MutableChannels(outputChannelIndex);

                        ui64 dstTaskId = TasksGraph.GetChannel(outputChannelId).DstTask;

                        if (dstTaskId == 0) {
                            continue;
                        }

                        auto& dstTask = TasksGraph.GetTask(dstTaskId);
                        if (dstTask.ComputeActorId) {
                            protoChannel->MutableDstEndpoint()->Clear();
                            ActorIdToProto(dstTask.ComputeActorId, protoChannel->MutableDstEndpoint()->MutableActorId());
                        } else {
                            if (protoChannel->HasDstEndpoint() && protoChannel->GetDstEndpoint().HasTabletId()) {
                                if (protoChannel->GetDstEndpoint().GetTabletId() == shardId) {
                                    // inplace update
                                } else {
                                    // TODO: send data via executer?
                                    // but we don't have such examples...
                                    YQL_ENSURE(false, "not implemented yet: " << protoTask.DebugString());
                                }
                            } else {
                                YQL_ENSURE(!protoChannel->GetDstEndpoint().IsInitialized());
                                // effects-only stage
                            }
                        }
                    }
                }

                LOG_D("datashard task: " << taskId << ", proto: " << protoTask.ShortDebugString());
            }

            ExecuteDatashardTransaction(shardId, shardTx, LockTxId, isOlap.value_or(false));
        }

        ExecuteTopicTabletTransactions(TopicTxs);

        if (sendTasksSpan) {
            sendTasksSpan.End();
        }

        LOG_I("Total tasks: " << TasksGraph.GetTasks().size()
            << ", readonly: " << ReadOnlyTx
            << ", datashardTxs: " << DatashardTxs.size()
            << ", topicTxs: " << Request.TopicOperations.GetSize()
            << ", volatile: " << VolatileTx
            << ", immediate: " << ImmediateTx
            << ", remote tasks" << remoteComputeTasksCnt
            << ", useFollowers: " << UseFollowers);

        LOG_T("Updating channels after the creation of compute actors");
        THashMap<TActorId, THashSet<ui64>> updates;
        for (ui64 taskId : ComputeTasks) {
            auto& task = TasksGraph.GetTask(taskId);
            CollectTaskChannelsUpdates(task, updates);
        }
        PropagateChannelsUpdates(updates);

        CheckExecutionComplete();
    }

    void ExecuteTopicTabletTransactions(TTopicTabletTxs& topicTxs) {
        auto lockTxId = Request.AcquireLocksTxId;
        if (lockTxId.Defined() && *lockTxId == 0) {
            lockTxId = TxId;
            LockHandle = TLockHandle(TxId, TActivationContext::ActorSystem());
        }

        for (auto& tx : topicTxs) {
            auto tabletId = tx.first;
            auto& transaction = tx.second;

            auto ev = std::make_unique<TEvPersQueue::TEvProposeTransaction>();

            if (lockTxId) {
                transaction.SetLockTxId(*lockTxId);
            }
            transaction.SetImmediate(ImmediateTx);

            ActorIdToProto(SelfId(), ev->Record.MutableSource());
            ev->Record.MutableData()->Swap(&transaction);
            ev->Record.SetTxId(TxId);

            auto traceId = ExecuterSpan.GetTraceId();
            LOG_D("ExecuteTopicTabletTransaction traceId.verbosity: " << std::to_string(traceId.GetVerbosity()));

            LOG_D("Executing KQP transaction on topic tablet: " << tabletId
                  << ", lockTxId: " << lockTxId);

            Send(MakePipePeNodeCacheID(UseFollowers),
                 new TEvPipeCache::TEvForward(ev.release(), tabletId, true),
                 0,
                 0,
                 std::move(traceId));

            TShardState state;
            state.State =
                ImmediateTx ? TShardState::EState::Executing : TShardState::EState::Preparing;
            state.DatashardState.ConstructInPlace();
            state.DatashardState->Follower = UseFollowers;

            state.DatashardState->ShardReadLocks = Request.TopicOperations.TabletHasReadOperations(tabletId);

            auto result = TopicTabletStates.emplace(tabletId, std::move(state));
            YQL_ENSURE(result.second);
        }
    }

    void PassAway() override {
        auto totalTime = TInstant::Now() - StartTime;
        Counters->Counters->DataTxTotalTimeHistogram->Collect(totalTime.MilliSeconds());

        // TxProxyMon compatibility
        Counters->TxProxyMon->TxTotalTimeHgram->Collect(totalTime.MilliSeconds());
        Counters->TxProxyMon->TxExecuteTimeHgram->Collect(totalTime.MilliSeconds());

        Send(MakePipePeNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));

        if (UseFollowers) {
            Send(MakePipePeNodeCacheID(true), new TEvPipeCache::TEvUnlink(0));
        }

        TBase::PassAway();
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

    static bool HasMissingSnapshotError(const NKikimrTxDataShard::TEvProposeTransactionResult& result) {
        for (const auto& err : result.GetError()) {
            if (err.GetKind() == NKikimrTxDataShard::TError::SNAPSHOT_NOT_EXIST) {
                return true;
            }
        }
        return false;
    }

    static void AddDataShardErrors(const NKikimrTxDataShard::TEvProposeTransactionResult& result, TIssue& issue) {
        for (const auto& err : result.GetError()) {
            issue.AddSubIssue(new TIssue(TStringBuilder()
                << "[" << NKikimrTxDataShard::TError_EKind_Name(err.GetKind()) << "] " << err.GetReason()));
        }
    }

    static void AddColumnShardErrors(const NKikimrTxColumnShard::TEvProposeTransactionResult& result, TIssue& issue) {
        issue.AddSubIssue(new TIssue(TStringBuilder() << result.GetStatusMessage()));
    }

    static std::string_view ToString(TShardState::EState state) {
        switch (state) {
            case TShardState::EState::Initial:   return "Initial"sv;
            case TShardState::EState::Preparing: return "Preparing"sv;
            case TShardState::EState::Prepared:  return "Prepared"sv;
            case TShardState::EState::Executing: return "Executing"sv;
            case TShardState::EState::Finished:  return "Finished"sv;
        }
    }

private:
    NYql::NDq::IDqAsyncIoFactory::TPtr AsyncIoFactory;
    bool StreamResult = false;

    bool HasStreamLookup = false;

    ui64 TxCoordinator = 0;
    THashMap<ui64, TShardState> ShardStates;
    THashMap<ui64, TShardState> TopicTabletStates;
    TVector<NKikimrTxDataShard::TLock> Locks;
    bool ReadOnlyTx = true;
    bool VolatileTx = false;
    bool ImmediateTx = false;
    bool UseFollowers = false;
    bool TxPlanned = false;
    bool LocksBroken = false;

    TInstant FirstPrepareReply;
    TInstant LastPrepareReply;

    // Tracks which shards are expected to have effects
    THashSet<ui64> ShardsWithEffects;
    bool HasPersistentChannels = false;

    THashSet<ui64> SubscribedNodes;
    THashMap<ui64, TVector<ui64>> RemoteComputeTasks;

    TVector<ui64> ComputeTasks;
    TDatashardTxs DatashardTxs;
    TTopicTabletTxs TopicTxs;

    TMaybe<ui64> LockTxId;
    // Lock handle for a newly acquired lock
    TLockHandle LockHandle;
    ui64 LastShard = 0;
};

} // namespace

IActor* CreateKqpDataExecuter(IKqpGateway::TExecPhysicalRequest&& request, const TString& database, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken,
    TKqpRequestCounters::TPtr counters, bool streamResult, const NKikimrConfig::TTableServiceConfig::TExecuterRetriesConfig& executerRetriesConfig, NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory)
{
    return new TKqpDataExecuter(std::move(request), database, userToken, counters, streamResult, executerRetriesConfig, std::move(asyncIoFactory));
}

} // namespace NKqp
} // namespace NKikimr
