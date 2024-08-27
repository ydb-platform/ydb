#include "kqp_executer.h"
#include "kqp_executer_impl.h"
#include "kqp_locks_helper.h"
#include "kqp_partition_helper.h"
#include "kqp_planner.h"
#include "kqp_table_resolver.h"
#include "kqp_tasks_validate.h"
#include "kqp_shards_resolver.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/core/client/minikql_compile/db_key_resolver.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_actor.h>
#include <ydb/core/kqp/common/kqp_tx.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/runtime/kqp_transport.h>
#include <ydb/core/kqp/opt/kqp_query_plan.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/tx/long_tx_service/public/lock_handle.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/persqueue/events/global.h>

#include <ydb/library/yql/dq/runtime/dq_columns_resolve.h>
#include <ydb/library/yql/dq/tasks/dq_connection_builder.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>


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
        const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
        NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
        const TActorId& creator, const TIntrusivePtr<TUserRequestContext>& userRequestContext,
        const bool enableOlapSink, const bool useEvWrite, ui32 statementResultIndex, const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup,
        const TGUCSettings::TPtr& GUCSettings)
        : TBase(std::move(request), database, userToken, counters, tableServiceConfig,
            userRequestContext, statementResultIndex, TWilsonKqp::DataExecuter, "DataExecuter", streamResult)
        , AsyncIoFactory(std::move(asyncIoFactory))
        , EnableOlapSink(enableOlapSink)
        , UseEvWrite(useEvWrite)
        , FederatedQuerySetup(federatedQuerySetup)
        , GUCSettings(GUCSettings)
    {
        Target = creator;

        YQL_ENSURE(Request.IsolationLevel != NKikimrKqp::ISOLATION_LEVEL_UNDEFINED);

        if (Request.AcquireLocksTxId || Request.LocksOp == ELocksOp::Commit || Request.LocksOp == ELocksOp::Rollback) {
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
        if (notFinished == 0 && TBase::CheckExecutionComplete()) {
            return;
        }

        if (IsDebugLogEnabled()) {
            auto sb = TStringBuilder() << "ActorState: " << CurrentStateFuncName()
                << ", waiting for " << (Planner ? Planner->GetPendingComputeActors().size() : 0) << " compute actor(s) and "
                << notFinished << " datashard(s): ";
            if (Planner) {
                for (const auto& shardId : Planner->GetPendingComputeActors()) {
                    sb << "CA " << shardId.first << ", ";
                }
            }
            for (const auto& [shardId, shardState] : ShardStates) {
                if (shardState.State != TShardState::EState::Finished) {
                    sb << "DS " << shardId << " (" << ToString(shardState.State) << "), ";
                }
            }
            LOG_D(sb);
        }
    }

    bool ForceAcquireSnapshot() const {
        const bool forceSnapshot = (
            ReadOnlyTx &&
            !ImmediateTx &&
            !HasPersistentChannels &&
            !HasOlapTable &&
            (!Database.empty() || AppData()->EnableMvccSnapshotWithLegacyDomainRoot)
        );

        return forceSnapshot;
    }

    bool GetUseFollowers() const {
        return (
            // first, we must specify read stale flag.
            Request.IsolationLevel == NKikimrKqp::ISOLATION_LEVEL_READ_STALE &&
            // next, if snapshot is already defined, so in this case followers are not allowed.
            !GetSnapshot().IsValid() &&
            // ensure that followers are allowed only for read only transactions.
            ReadOnlyTx &&
            // if we are forced to acquire snapshot by some reason, so we cannot use followers.
            !ForceAcquireSnapshot()
        );
    }

    void Finalize() {
        YQL_ENSURE(!AlreadyReplied);

        if (LocksBroken) {
            TString message = "Transaction locks invalidated.";

            return ReplyErrorAndDie(Ydb::StatusIds::ABORTED,
                YqlIssue({}, TIssuesIds::KIKIMR_LOCKS_INVALIDATED, message));
        }

        auto& response = *ResponseEv->Record.MutableResponse();

        FillResponseStats(Ydb::StatusIds::SUCCESS);
        Counters->TxProxyMon->ReportStatusOK->Inc();

        auto addLocks = [this](const auto& data) {
            if (data.GetData().template Is<NKikimrTxDataShard::TEvKqpInputActorResultInfo>()) {
                NKikimrTxDataShard::TEvKqpInputActorResultInfo info;
                YQL_ENSURE(data.GetData().UnpackTo(&info), "Failed to unpack settings");
                for (auto& lock : info.GetLocks()) {
                    Locks.push_back(lock);
                }
            } else if (data.GetData().template Is<NKikimrKqp::TEvKqpOutputActorResultInfo>()) {
                NKikimrKqp::TEvKqpOutputActorResultInfo info;
                YQL_ENSURE(data.GetData().UnpackTo(&info), "Failed to unpack settings");
                for (auto& lock : info.GetLocks()) {
                    Locks.push_back(lock);
                }
            }
        };

        for (auto& [_, data] : ExtraData) {
            for (const auto& source : data.GetSourcesExtraData()) {
                addLocks(source);
            }
            for (const auto& transform : data.GetInputTransformsData()) {
                addLocks(transform);
            }
            for (const auto& sink : data.GetSinksExtraData()) {
                addLocks(sink);
            }
        }

        ResponseEv->Snapshot = GetSnapshot();

        if (!Locks.empty()) {
            if (LockHandle) {
                ResponseEv->LockHandle = std::move(LockHandle);
            }
            BuildLocks(*response.MutableResult()->MutableLocks(), Locks);
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

        LWTRACK(KqpDataExecuterFinalize, ResponseEv->Orbit, TxId, LastShard, ResponseEv->ResultsSize(), ResponseEv->GetByteSize());

        ExecuterSpan.EndOk();

        Request.Transactions.crop(0);
        AlreadyReplied = true;
        PassAway();
    }

    STATEFN(WaitResolveState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpExecuter::TEvTableResolveStatus, HandleResolve);
                hFunc(TEvKqpExecuter::TEvShardsResolveStatus, HandleResolve);
                hFunc(TEvPrivate::TEvResourcesSnapshot, HandleResolve);
                hFunc(TEvSaveScriptExternalEffectResponse, HandleResolve);
                hFunc(TEvDescribeSecretsResponse, HandleResolve);
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
    bool IsCancelAfterAllowed(const TEvKqp::TEvAbortExecution::TPtr& ev) const {
        return ReadOnlyTx || ev->Get()->Record.GetStatusCode() != NYql::NDqProto::StatusIds::CANCELLED;
    }

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
        } else if (func == &TThis::WaitShutdownState) {
            return "WaitShutdownState";
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
                hFunc(NKikimr::NEvents::TDataEvents::TEvWriteResult, HandlePrepare);
                hFunc(TEvPrivate::TEvReattachToShard, HandleExecute);
                hFunc(TEvDqCompute::TEvState, HandlePrepare); // from CA
                hFunc(TEvDqCompute::TEvChannelData, HandleChannelData); // from CA
                hFunc(TEvKqpExecuter::TEvStreamDataAck, HandleStreamAck);
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
        } catch (const TMemoryLimitExceededException& e) {
            CancelProposal(0);
            RuntimeError(Ydb::StatusIds::PRECONDITION_FAILED, NYql::TIssues({NYql::TIssue(BuildMemoryLimitExceptionMessage())}));
        }

        ReportEventElapsedTime();
    }

    void HandlePrepare(TEvPersQueue::TEvProposeTransactionResult::TPtr& ev) {
        auto& event = ev->Get()->Record;
        const ui64 tabletId = event.GetOrigin();

        LOG_D("Got propose result" <<
              ", PQ tablet: " << tabletId <<
              ", status: " << NKikimrPQ::TEvProposeTransactionResult_EStatus_Name(event.GetStatus()));

        TShardState* state = ShardStates.FindPtr(tabletId);
        YQL_ENSURE(state, "Unexpected propose result from unknown PQ tablet " << tabletId);

        switch (event.GetStatus()) {
        case NKikimrPQ::TEvProposeTransactionResult::PREPARED:
            if (!ShardPrepared(*state, event)) {
                return CancelProposal(tabletId);
            }
            return CheckPrepareCompleted();
        case NKikimrPQ::TEvProposeTransactionResult::COMPLETE:
            YQL_ENSURE(false);
        default:
            CancelProposal(tabletId);
            return PQTabletError(event);
        }
    }

    void HandlePrepare(TEvDataShard::TEvProposeTransactionResult::TPtr& ev) {
        TEvDataShard::TEvProposeTransactionResult* res = ev->Get();
        ResponseEv->Orbit.Join(res->Orbit);
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

    void HandlePrepare(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        auto* res = ev->Get();

        const ui64 shardId = res->Record.GetOrigin();
        TShardState* shardState = ShardStates.FindPtr(shardId);
        YQL_ENSURE(shardState, "Unexpected propose result from unknown tabletId " << shardId);

        NYql::TIssues issues;
        NYql::IssuesFromMessage(res->Record.GetIssues(), issues);
        LOG_D("Got evWrite result, shard: " << shardId << ", status: "
            << NKikimrDataEvents::TEvWriteResult::EStatus_Name(res->Record.GetStatus())
            << ", error: " << issues.ToString());

        if (Stats) {
            Stats->AddDatashardPrepareStats(std::move(*res->Record.MutableTxStats()));
        }

        switch (ev->Get()->GetStatus()) {
            case NKikimrDataEvents::TEvWriteResult::STATUS_UNSPECIFIED: {
                YQL_ENSURE(false);
            }
            case NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED: {
                if (!ShardPrepared(*shardState, res->Record)) {
                    return;
                }
                return CheckPrepareCompleted();
            }
            case NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED: {
                YQL_ENSURE(false);
            }
            default:
            {
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
        if (IsCancelAfterAllowed(ev)) {
            CancelProposal(0);
            TBase::HandleAbortExecution(ev);
        } else {
            LOG_D("Got TEvAbortExecution from : " << ev->Sender << " but cancelation is not alowed");
        }
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

                YQL_ENSURE(state.DatashardState.Defined());
                YQL_ENSURE(!state.DatashardState->Follower);

                Send(MakePipePerNodeCacheID(/* allowFollowers */ false), new TEvPipeCache::TEvForward(
                    new TEvDataShard::TEvCancelTransactionProposal(TxId), shardId, /* subscribe */ false));
            }
        }
    }

    template<class E>
    bool ShardPreparedImpl(TShardState& state, const E& result) {
        YQL_ENSURE(state.State == TShardState::EState::Preparing);
        state.State = TShardState::EState::Prepared;

        state.DatashardState->ShardMinStep = result.GetMinStep();
        state.DatashardState->ShardMaxStep = result.GetMaxStep();

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

    bool ShardPrepared(TShardState& state, const NKikimrTxDataShard::TEvProposeTransactionResult& result) {
        bool success = ShardPreparedImpl(state, result);
        if (success) {
            state.DatashardState->ReadSize += result.GetReadSize();
        }
        return success;
    }

    bool ShardPrepared(TShardState& state, const NKikimrTxColumnShard::TEvProposeTransactionResult& result) {
        return ShardPreparedImpl(state, result);
    }

    bool ShardPrepared(TShardState& state, const NKikimrPQ::TEvProposeTransactionResult& result) {
        return ShardPreparedImpl(state, result);
    }

    bool ShardPrepared(TShardState& state, const NKikimrDataEvents::TEvWriteResult& result) {
        return ShardPreparedImpl(state, result);
    }

    void ShardError(const NKikimrTxDataShard::TEvProposeTransactionResult& result) {
        if (result.ErrorSize() != 0) {
            TStringBuilder message;
            message << NKikimrTxDataShard::TEvProposeTransactionResult_EStatus_Name(result.GetStatus()) << ": ";
            for (const auto &err : result.GetError()) {
                if (err.GetKind() == NKikimrTxDataShard::TError::REPLY_SIZE_EXCEEDED) {
                    Counters->Counters->DataShardTxReplySizeExceededError->Inc();
                }
                message << "[" << err.GetKind() << "] " << err.GetReason() << "; ";
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
                        case NKikimrTxDataShard::TError::OUT_OF_SPACE:
                        case NKikimrTxDataShard::TError::DISK_SPACE_EXHAUSTED: {
                            auto issue = YqlIssue({}, TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE);
                            AddDataShardErrors(result, issue);
                            return ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, issue);
                        }
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

    void ShardError(const NKikimrDataEvents::TEvWriteResult& result) {
        NYql::TIssues issues;
        NYql::IssuesFromMessage(result.GetIssues(), issues);

        switch (result.GetStatus()) {
            case NKikimrDataEvents::TEvWriteResult::STATUS_UNSPECIFIED:
            case NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED:
            case NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED: {
                YQL_ENSURE(false);
            }
            case NKikimrDataEvents::TEvWriteResult::STATUS_ABORTED: {
                return ReplyErrorAndDie(Ydb::StatusIds::ABORTED, issues);
            }
            case NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR: {
                return ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, issues);
            }
            case NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED: {
                return ReplyErrorAndDie(Ydb::StatusIds::OVERLOADED, issues);
            }
            case NKikimrDataEvents::TEvWriteResult::STATUS_CANCELLED: {
                return ReplyErrorAndDie(Ydb::StatusIds::CANCELLED, issues);
            }
            case NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST: {
                return ReplyErrorAndDie(Ydb::StatusIds::BAD_REQUEST, issues);
            }
            case NKikimrDataEvents::TEvWriteResult::STATUS_SCHEME_CHANGED: {
                return ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR, issues);
            }
            case NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN: {
                return ReplyErrorAndDie(Ydb::StatusIds::ABORTED, issues);
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

        if (result.ErrorsSize()) {
            ReplyErrorAndDie(statusCode, YqlIssue({}, issueCode, result.GetErrors(0).GetReason()));
        } else {
            ReplyErrorAndDie(statusCode, YqlIssue({}, issueCode));
        }
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
        ExecutePlanned();
    }

    void ExecutePlanned() {
        YQL_ENSURE(TxCoordinator);
        auto ev = MakeHolder<TEvTxProxy::TEvProposeTransaction>();
        ev->Record.SetCoordinatorID(TxCoordinator);

        auto& transaction = *ev->Record.MutableTransaction();
        auto& affectedSet = *transaction.MutableAffectedSet();
        affectedSet.Reserve(static_cast<int>(ShardStates.size()));

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
                affectedFlags |= TEvTxProxy::TEvProposeTransaction::AffectedRead;
            }

            for (auto taskId : state.TaskIds) {
                auto& task = TasksGraph.GetTask(taskId);
                auto& stageInfo = TasksGraph.GetStageInfo(task.StageId);

                if (stageInfo.Meta.HasReads()) {
                    affectedFlags |= TEvTxProxy::TEvProposeTransaction::AffectedRead;
                }
                if (stageInfo.Meta.HasWrites()) {
                    affectedFlags |= TEvTxProxy::TEvProposeTransaction::AffectedWrite;
                }
            }

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

        if (VolatileTx) {
            transaction.SetFlags(TEvTxProxy::TEvProposeTransaction::FlagVolatile);
        }

        LOG_T("Execute planned transaction, coordinator: " << TxCoordinator);
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(ev.Release(), TxCoordinator, /* subscribe */ true));
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
                hFunc(NKikimr::NEvents::TDataEvents::TEvWriteResult, HandleExecute);
                hFunc(TEvPrivate::TEvReattachToShard, HandleExecute);
                hFunc(TEvPipeCache::TEvDeliveryProblem, HandleExecute);
                hFunc(TEvents::TEvUndelivered, HandleUndelivered);
                hFunc(TEvPrivate::TEvRetry, HandleRetry);
                hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
                hFunc(TEvKqpNode::TEvStartKqpTasksResponse, HandleStartKqpTasksResponse);
                hFunc(TEvTxProxy::TEvProposeTransactionStatus, HandleExecute);
                hFunc(TEvDqCompute::TEvState, HandleComputeStats);
                hFunc(NYql::NDq::TEvDqCompute::TEvChannelData, HandleChannelData);
                hFunc(TEvKqpExecuter::TEvStreamDataAck, HandleStreamAck);
                hFunc(TEvKqp::TEvAbortExecution, HandleExecute);
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

    void HandleExecute(TEvPersQueue::TEvProposeTransactionResult::TPtr& ev) {
        NKikimrPQ::TEvProposeTransactionResult& event = ev->Get()->Record;

        LOG_D("Got propose result" <<
              ", topic tablet: " << event.GetOrigin() <<
              ", status: " << NKikimrPQ::TEvProposeTransactionResult_EStatus_Name(event.GetStatus()));

        TShardState *state = ShardStates.FindPtr(event.GetOrigin());
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
        if (IsCancelAfterAllowed(ev)) {
            if (ImmediateTx) {
                CancelProposal(0);
            }
            TBase::HandleAbortExecution(ev);
        } else {
            LOG_D("Got TEvAbortExecution from : " << ev->Sender << " but cancelation is not alowed");
        }
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

    void HandleExecute(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        auto* res = ev->Get();
        const ui64 shardId = res->Record.GetOrigin();
        LastShard = shardId;

        TShardState* shardState = ShardStates.FindPtr(shardId);
        YQL_ENSURE(shardState);

        NYql::TIssues issues;
        NYql::IssuesFromMessage(res->Record.GetIssues(), issues);
        LOG_D("Got evWrite result, shard: " << shardId << ", status: "
            << NKikimrDataEvents::TEvWriteResult::EStatus_Name(res->Record.GetStatus())
            << ", error: " << issues.ToString());

        if (Stats) {
            Stats->AddDatashardStats(std::move(*res->Record.MutableTxStats()));
        }

        switch (ev->Get()->GetStatus()) {
            case NKikimrDataEvents::TEvWriteResult::STATUS_UNSPECIFIED: {
                YQL_ENSURE(false);
            }
            case NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED: {
                YQL_ENSURE(false);
            }
            case NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED: {
                YQL_ENSURE(shardState->State == TShardState::EState::Executing);
                shardState->State = TShardState::EState::Finished;

                Counters->TxProxyMon->ResultsReceivedCount->Inc();
                Counters->TxProxyMon->TxResultComplete->Inc();

                CheckExecutionComplete();
                return;
            }
            default:
            {
                return ShardError(res->Record);
            }
        }
    }

    void HandleExecute(TEvDataShard::TEvProposeTransactionResult::TPtr& ev) {
        TEvDataShard::TEvProposeTransactionResult* res = ev->Get();
        ResponseEv->Orbit.Join(res->Orbit);
        const ui64 shardId = res->GetOrigin();
        LastShard = shardId;

        TShardState* shardState = ShardStates.FindPtr(shardId);
        YQL_ENSURE(shardState);

        LOG_D("Got propose result, shard: " << shardId << ", status: "
            << NKikimrTxDataShard::TEvProposeTransactionResult_EStatus_Name(res->GetStatus())
            << ", error: " << res->GetError());

        if (Stats) {
            Stats->AddDatashardStats(
                std::move(*res->Record.MutableComputeActorStats()),
                std::move(*res->Record.MutableTxStats()),
                TDuration::MilliSeconds(AggregationSettings.GetCollectLongTasksStatsTimeoutMs()));
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
                    auto it = FindIf(TasksGraph.GetStagesInfo(), [tableId](const auto& x){ return x.second.Meta.TableId.HasSamePath(tableId); });
                    if (it != TasksGraph.GetStagesInfo().end()) {
                        tableName = it->second.Meta.TableConstInfo->Path;
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

        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(
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
                Y_DEBUG_ABORT_UNLESS(false);
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

private:
    bool IsReadOnlyTx() const {
        if (Request.TopicOperations.HasOperations()) {
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

    void BuildDatashardTasks(TStageInfo& stageInfo) {
        THashMap<ui64, ui64> shardTasks; // shardId -> taskId
        auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

        auto getShardTask = [&](ui64 shardId) -> TTask& {
            YQL_ENSURE(!UseEvWrite);
            auto it  = shardTasks.find(shardId);
            if (it != shardTasks.end()) {
                return TasksGraph.GetTask(it->second);
            }
            auto& task = TasksGraph.AddTask(stageInfo);
            task.Meta.Type = TTaskMeta::TTaskType::DataShard;
            task.Meta.ExecuterId = SelfId();
            task.Meta.ShardId = shardId;
            shardTasks.emplace(shardId, task.Id);

            FillSecureParamsFromStage(task.Meta.SecureParams, stage);
            BuildSinks(stage, task);

            return task;
        };

        const auto& tableInfo = stageInfo.Meta.TableConstInfo;
        const auto& keyTypes = tableInfo->KeyColumnTypes;

        for (auto& op : stage.GetTableOps()) {
            Y_DEBUG_ABORT_UNLESS(stageInfo.Meta.TablePath == op.GetTable().GetPath());
            auto columns = BuildKqpColumns(op, tableInfo);
            switch (op.GetTypeCase()) {
                case NKqpProto::TKqpPhyTableOperation::kReadRanges:
                case NKqpProto::TKqpPhyTableOperation::kReadRange:
                case NKqpProto::TKqpPhyTableOperation::kLookup: {
                    bool isFullScan = false;
                    auto partitions = PrunePartitions(op, stageInfo, HolderFactory(), TypeEnv(), isFullScan);
                    auto readSettings = ExtractReadSettings(op, stageInfo, HolderFactory(), TypeEnv());

                    if (!readSettings.ItemsLimit && isFullScan) {
                        Counters->Counters->FullScansExecuted->Inc();
                    }

                    for (auto& [shardId, shardInfo] : partitions) {
                        YQL_ENSURE(!shardInfo.KeyWriteRanges);

                        auto& task = getShardTask(shardId);
                        MergeReadInfoToTaskMeta(task.Meta, shardId, shardInfo.KeyReadRanges, readSettings,
                            columns, op, /*isPersistentScan*/ false);
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
                        auto result = PruneEffectPartitions(op, stageInfo, HolderFactory(), TypeEnv());
                        for (auto& [shardId, shardInfo] : result) {
                            YQL_ENSURE(!shardInfo.KeyReadRanges);
                            YQL_ENSURE(shardInfo.KeyWriteRanges);

                            auto& task = getShardTask(shardId);

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
                                auto& column = tableInfo->Columns.at(name);

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

    void ExecuteDatashardTransaction(ui64 shardId, NKikimrTxDataShard::TKqpTransaction& kqpTx, const bool isOlap)
    {
        YQL_ENSURE(!UseEvWrite);
        TShardState shardState;
        shardState.State = ImmediateTx ? TShardState::EState::Executing : TShardState::EState::Preparing;
        shardState.DatashardState.ConstructInPlace();
        shardState.DatashardState->Follower = GetUseFollowers();

        if (Deadline) {
            TDuration timeout = *Deadline - TAppData::TimeProvider->Now();
            kqpTx.MutableRuntimeSettings()->SetTimeoutMs(timeout.MilliSeconds());
        }
        kqpTx.MutableRuntimeSettings()->SetExecType(NDqProto::TComputeRuntimeSettings::DATA);
        kqpTx.MutableRuntimeSettings()->SetStatsMode(GetDqStatsModeShard(Request.StatsMode));

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

        auto& lockTxId = TasksGraph.GetMeta().LockTxId;
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
            std::unique_ptr<TEvDataShard::TEvProposeTransaction> evData;
            if (GetSnapshot().IsValid() && (ReadOnlyTx || Request.UseImmediateEffects)) {
                evData.reset(new TEvDataShard::TEvProposeTransaction(
                    NKikimrTxDataShard::TX_KIND_DATA,
                    SelfId(),
                    TxId,
                    dataTransaction.SerializeAsString(),
                    GetSnapshot().Step,
                    GetSnapshot().TxId,
                    flags));
            } else {
                evData.reset(new TEvDataShard::TEvProposeTransaction(
                    NKikimrTxDataShard::TX_KIND_DATA,
                    SelfId(),
                    TxId,
                    dataTransaction.SerializeAsString(),
                    flags));
            }
            ResponseEv->Orbit.Fork(evData->Orbit);
            ev = std::move(evData);
        }
        auto traceId = ExecuterSpan.GetTraceId();

        LOG_D("ExecuteDatashardTransaction traceId.verbosity: " << std::to_string(traceId.GetVerbosity()));

        Send(MakePipePerNodeCacheID(GetUseFollowers()), new TEvPipeCache::TEvForward(ev.release(), shardId, true), 0, 0, std::move(traceId));

        auto result = ShardStates.emplace(shardId, std::move(shardState));
        YQL_ENSURE(result.second);
    }

    void ExecuteEvWriteTransaction(ui64 shardId, NKikimrDataEvents::TEvWrite& evWrite) {
        YQL_ENSURE(!ImmediateTx);
        TShardState shardState;
        shardState.State = TShardState::EState::Preparing;
        shardState.DatashardState.ConstructInPlace();

        auto evWriteTransaction = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>();
        evWriteTransaction->Record = evWrite;
        evWriteTransaction->Record.SetTxMode(NKikimrDataEvents::TEvWrite::MODE_PREPARE);
        evWriteTransaction->Record.SetTxId(TxId);

        evWriteTransaction->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);

        auto locksCount = evWriteTransaction->Record.GetLocks().LocksSize();
        shardState.DatashardState->ShardReadLocks = locksCount > 0;

        LOG_D("State: " << CurrentStateFuncName()
            << ", Executing EvWrite (PREPARE) on shard: " << shardId
            << ", TxId: " << TxId
            << ", locks: " << evWriteTransaction->Record.GetLocks().ShortDebugString());

        auto traceId = ExecuterSpan.GetTraceId();

        LOG_D("ExecuteEvWriteTransaction traceId.verbosity: " << std::to_string(traceId.GetVerbosity()));

        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(evWriteTransaction.release(), shardId, true), 0, 0, std::move(traceId));

        auto result = ShardStates.emplace(shardId, std::move(shardState));
        YQL_ENSURE(result.second);
    }

    bool WaitRequired() const {
        return SecretSnapshotRequired || ResourceSnapshotRequired || SaveScriptExternalEffectRequired;
    }

    void HandleResolve(TEvDescribeSecretsResponse::TPtr& ev) {
        YQL_ENSURE(ev->Get()->Description.Status == Ydb::StatusIds::SUCCESS, "failed to get secrets snapshot with issues: " << ev->Get()->Description.Issues.ToOneLineString());

        for (size_t i = 0; i < SecretNames.size(); ++i) {
            SecureParams.emplace(SecretNames[i], ev->Get()->Description.SecretValues[i]);
        }

        SecretSnapshotRequired = false;
        if (!WaitRequired()) {
            Execute();
        }
    }

    void HandleResolve(TEvPrivate::TEvResourcesSnapshot::TPtr& ev) {
        if (ev->Get()->Snapshot.empty()) {
            LOG_E("Can not find default state storage group for database " << Database);
        }
        ResourceSnapshot = std::move(ev->Get()->Snapshot);
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
            requestContext->CustomerSuppliedId, UserToken ? UserToken->GetUserSID() : ""
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
        if (queryType == NKqpProto::TKqpPhyTx::TYPE_DATA) {
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

    bool HasOlapSink(const NKqpProto::TKqpPhyStage& stage, const google::protobuf::RepeatedPtrField< ::NKqpProto::TKqpPhyTable>& tables) {
        return NKqp::HasOlapTableWriteInStage(stage, tables);
    }

    void Execute() {
        LWTRACK(KqpDataExecuterStartExecute, ResponseEv->Orbit, TxId);

        size_t sourceScanPartitionsCount = 0;
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

                const bool hasOlapSink = HasOlapSink(stage, tx.Body->GetTables());
                if ((stageInfo.Meta.IsOlap() && HasDmlOperationOnOlap(tx.Body->GetType(), stage))
                    || (!EnableOlapSink && hasOlapSink)) {
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
                            if (auto partitionsCount = BuildScanTasksFromSource(
                                    stageInfo,
                                    /* shardsResolved */ StreamResult,
                                    /* limitTasksPerNode */ StreamResult)) {
                                sourceScanPartitionsCount += *partitionsCount;
                            } else {
                                UnknownAffectedShardCount = true;
                            }
                            break;
                        case NKqpProto::TKqpSource::kExternalSource:
                            BuildReadTasksFromSource(stageInfo, ResourceSnapshot);
                            break;
                        default:
                            YQL_ENSURE(false, "unknown source type");
                    }
                } else if (StreamResult && stageInfo.Meta.IsOlap() && stage.SinksSize() == 0) {
                    BuildScanTasksFromShards(stageInfo);
                } else if (stageInfo.Meta.IsSysView()) {
                    BuildSysViewScanTasks(stageInfo);
                } else if (stageInfo.Meta.ShardOperations.empty() || stage.SinksSize() > 0) {
                    BuildComputeTasks(stageInfo, std::max<ui32>(ShardsOnNode.size(), ResourceSnapshot.size()));
                } else {
                    BuildDatashardTasks(stageInfo);
                }

                if (stage.GetIsSinglePartition()) {
                    YQL_ENSURE(stageInfo.Tasks.size() == 1, "Unexpected multiple tasks in single-partition stage");
                }

                TasksGraph.GetMeta().AllowWithSpilling |= stage.GetAllowWithSpilling();
                BuildKqpStageChannels(TasksGraph, stageInfo, TxId, /* enableSpilling */ TasksGraph.GetMeta().AllowWithSpilling);
            }

            ResponseEv->InitTxResult(tx.Body);
            BuildKqpTaskGraphResultChannels(TasksGraph, tx.Body, txIdx);
        }

        TIssue validateIssue;
        if (!ValidateTasks(TasksGraph, EExecType::Data, /* enableSpilling */ TasksGraph.GetMeta().AllowWithSpilling, validateIssue)) {
            ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, validateIssue);
            return;
        }

        THashMap<ui64, TVector<NDqProto::TDqTask*>> datashardTasks;  // shardId -> [task]
        THashMap<ui64, TVector<ui64>> remoteComputeTasks;  // shardId -> [task]
        TVector<ui64> computeTasks;

        if (StreamResult) {
            InitializeChannelProxies();
        }

        for (auto& task : TasksGraph.GetTasks()) {
            auto& stageInfo = TasksGraph.GetStageInfo(task.StageId);
            if (task.Meta.ShardId && (task.Meta.Reads || task.Meta.Writes)) {
                NYql::NDqProto::TDqTask* protoTask = ArenaSerializeTaskToProto(TasksGraph, task, true);
                datashardTasks[task.Meta.ShardId].emplace_back(protoTask);
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

        // For generic query all shards are already resolved
        YQL_ENSURE(!StreamResult || remoteComputeTasks.empty());

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
        size_t shards = datashardTasks.size() + sourceScanPartitionsCount;
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
        TDatashardTxs datashardTxs;
        TEvWriteTxs evWriteTxs;
        BuildDatashardTxs(datashardTasks, datashardTxs, evWriteTxs, topicTxs);
        YQL_ENSURE(evWriteTxs.empty() || datashardTxs.empty());

        // Single-shard datashard transactions are always immediate
        ImmediateTx = (datashardTxs.size() + evWriteTxs.size() + Request.TopicOperations.GetSize() + sourceScanPartitionsCount) <= 1
                    && !UnknownAffectedShardCount
                    && evWriteTxs.empty()
                    && !HasOlapTable;

        switch (Request.IsolationLevel) {
            // OnlineRO with AllowInconsistentReads = true
            case NKikimrKqp::ISOLATION_LEVEL_READ_UNCOMMITTED:
                YQL_ENSURE(ReadOnlyTx);
                YQL_ENSURE(!VolatileTx);
                TasksGraph.GetMeta().AllowInconsistentReads = true;
                ImmediateTx = true;
                break;

            default:
                break;
        }

        if (ImmediateTx) {
            // Transaction cannot be both immediate and volatile
            YQL_ENSURE(!VolatileTx);
        }

        if ((ReadOnlyTx || Request.UseImmediateEffects) && GetSnapshot().IsValid()) {
            // Snapshot reads are always immediate
            // Uncommitted writes are executed without coordinators, so they can be immediate
            YQL_ENSURE(!VolatileTx);
            ImmediateTx = true;
        }

        ComputeTasks = std::move(computeTasks);
        TopicTxs = std::move(topicTxs);
        DatashardTxs = std::move(datashardTxs);
        EvWriteTxs = std::move(evWriteTxs);
        RemoteComputeTasks = std::move(remoteComputeTasks);

        TasksGraph.GetMeta().UseFollowers = GetUseFollowers();

        if (RemoteComputeTasks) {
            YQL_ENSURE(!StreamResult);
            TSet<ui64> shardIds;
            for (const auto& [shardId, _] : RemoteComputeTasks) {
                shardIds.insert(shardId);
            }

            ExecuterStateSpan = NWilson::TSpan(TWilsonKqp::ExecuterShardsResolve, ExecuterSpan.GetTraceId(), "WaitForShardsResolve", NWilson::EFlags::AUTO_END);
            auto kqpShardsResolver = CreateKqpShardsResolver(
                SelfId(), TxId, TasksGraph.GetMeta().UseFollowers, std::move(shardIds));
            RegisterWithSameMailbox(kqpShardsResolver);
            Become(&TKqpDataExecuter::WaitResolveState);
        } else {
            OnShardsResolve();
        }
    }

    void HandleResolve(TEvKqpExecuter::TEvTableResolveStatus::TPtr& ev) {
        if (!TBase::HandleResolve(ev)) return;

        if (StreamResult) {
            TSet<ui64> shardIds;
            for (auto& [stageId, stageInfo] : TasksGraph.GetStagesInfo()) {
                if (stageInfo.Meta.IsOlap()) {
                    HasOlapTable = true;
                }
                const auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);
                if (stage.SourcesSize() > 0 && stage.GetSources(0).GetTypeCase() == NKqpProto::TKqpSource::kReadRangesSource) {
                    YQL_ENSURE(stage.SourcesSize() == 1, "multiple sources in one task are not supported");
                    HasDatashardSourceScan = true;
                }
            }
            if (HasOlapTable) {
                for (auto& [stageId, stageInfo] : TasksGraph.GetStagesInfo()) {
                    if (stageInfo.Meta.ShardKey) {
                        for (auto& partition : stageInfo.Meta.ShardKey->GetPartitions()) {
                            shardIds.insert(partition.ShardId);
                        }
                    }
                }
            }
            if (HasDatashardSourceScan) {
                for (auto& [stageId, stageInfo] : TasksGraph.GetStagesInfo()) {
                    YQL_ENSURE(stageId == stageInfo.Id);
                    const auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);
                    if (stage.SourcesSize() > 0 && stage.GetSources(0).GetTypeCase() == NKqpProto::TKqpSource::kReadRangesSource) {
                        const auto& source = stage.GetSources(0).GetReadRangesSource();
                        bool isFullScan;
                        SourceScanStageIdToParititions[stageInfo.Id] = PrunePartitions(source, stageInfo, HolderFactory(), TypeEnv(), isFullScan);
                        if (isFullScan && !source.HasItemsLimit()) {
                            Counters->Counters->FullScansExecuted->Inc();
                        }
                        for (const auto& [shardId, _] : SourceScanStageIdToParititions.at(stageId)) {
                            shardIds.insert(shardId);
                        }
                    }
                }
            }

            if ((HasOlapTable || HasDatashardSourceScan) && shardIds) {
                LOG_D("Start resolving tablets nodes... (" << shardIds.size() << ")");
                ExecuterStateSpan = NWilson::TSpan(TWilsonKqp::ExecuterShardsResolve, ExecuterSpan.GetTraceId(), "WaitForShardsResolve", NWilson::EFlags::AUTO_END);
                auto kqpShardsResolver = CreateKqpShardsResolver(
                    this->SelfId(), TxId, false, std::move(shardIds));
                KqpShardsResolverId = this->RegisterWithSameMailbox(kqpShardsResolver);
                return;
            } else if (HasOlapTable) {
                ResourceSnapshotRequired = true;
            }
        }
        DoExecute();
    }

    void HandleResolve(TEvKqpExecuter::TEvShardsResolveStatus::TPtr& ev) {
        if (!TBase::HandleResolve(ev)) {
            return;
        }
        if (HasOlapTable || HasDatashardSourceScan) {
            ResourceSnapshotRequired = ResourceSnapshotRequired || HasOlapTable;
            DoExecute();
            return;
        }

        OnShardsResolve();
    }

    void OnShardsResolve() {
        if (ForceAcquireSnapshot()) {
            YQL_ENSURE(!VolatileTx);
            auto longTxService = NLongTxService::MakeLongTxServiceID(SelfId().NodeId());
            Send(longTxService, new NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot(Database));

            LOG_T("Create temporary mvcc snapshot, become WaitSnapshotState");
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

        LOG_T("read snapshot result: " << record.GetStatus() << ", step: " << record.GetSnapshotStep()
            << ", tx id: " << record.GetSnapshotTxId());

        if (record.GetStatus() != Ydb::StatusIds::SUCCESS) {
            ExecuterStateSpan.EndError(TStringBuilder() << Ydb::StatusIds::StatusCode_Name(record.GetStatus()));
            ReplyErrorAndDie(record.GetStatus(), record.MutableIssues());
            return;
        }
        ExecuterStateSpan.EndOk();

        SetSnapshot(record.GetSnapshotStep(), record.GetSnapshotTxId());
        ImmediateTx = true;

        ContinueExecute();
    }

    using TDatashardTxs = THashMap<ui64, NKikimrTxDataShard::TKqpTransaction*>;
    using TEvWriteTxs = THashMap<ui64, NKikimrDataEvents::TEvWrite*>;
    using TTopicTabletTxs = THashMap<ui64, NKikimrPQ::TDataTransaction>;

    void ContinueExecute() {
        if (Stats) {
            //Stats->AffectedShards = datashardTxs.size();
            Stats->DatashardStats.reserve(DatashardTxs.size());
            //Stats->ComputeStats.reserve(computeTasks.size());
        }

        ExecuteTasks();

        ExecuterStateSpan = NWilson::TSpan(TWilsonKqp::DataExecuterRunTasks, ExecuterSpan.GetTraceId(), "RunTasks", NWilson::EFlags::AUTO_END);
        if (ImmediateTx) {
            LOG_D("ActorState: " << CurrentStateFuncName()
                << ", immediate tx, become ExecuteState");
            Become(&TKqpDataExecuter::ExecuteState);
        } else {
            LOG_D("ActorState: " << CurrentStateFuncName()
                << ", not immediate tx, become PrepareState");
            Become(&TKqpDataExecuter::PrepareState);
        }
    }

    void BuildDatashardTxs(
            THashMap<ui64, TVector<NDqProto::TDqTask*>>& datashardTasks,
            TDatashardTxs& datashardTxs,
            TEvWriteTxs& evWriteTxs,
            TTopicTabletTxs& topicTxs) {
        for (auto& [shardId, tasks]: datashardTasks) {
            auto [it, success] = datashardTxs.emplace(
                shardId,
                TasksGraph.GetMeta().Allocate<NKikimrTxDataShard::TKqpTransaction>());

            YQL_ENSURE(success, "unexpected duplicates in datashard transactions");
            NKikimrTxDataShard::TKqpTransaction* dsTxs = it->second;
            dsTxs->MutableTasks()->Reserve(tasks.size());
            for (auto& task: tasks) {
                dsTxs->AddTasks()->Swap(task);
            }
        }

        // Note: when locks map is present it will be mutated to avoid copying data
        auto& locksMap = Request.DataShardLocks;
        if (!locksMap.empty()) {
            YQL_ENSURE(Request.LocksOp == ELocksOp::Commit || Request.LocksOp == ELocksOp::Rollback);
        }

        // Materialize (possibly empty) txs for all shards with locks (either commit or rollback)
        for (auto& [shardId, locksList] : locksMap) {
            YQL_ENSURE(!locksList.empty(), "unexpected empty locks list in DataShardLocks");
            NKikimrDataEvents::TKqpLocks* locks = nullptr;

            if (UseEvWrite) {
                if (auto it = evWriteTxs.find(shardId); it != evWriteTxs.end()) {
                    locks = it->second->MutableLocks();
                } else {
                    auto [eIt, success] = evWriteTxs.emplace(
                        shardId,
                        TasksGraph.GetMeta().Allocate<NKikimrDataEvents::TEvWrite>());
                    locks = eIt->second->MutableLocks();
                }
            } else {
                if (auto it = datashardTxs.find(shardId); it != datashardTxs.end()) {
                    locks = it->second->MutableLocks();
                } else {
                    auto [eIt, success] = datashardTxs.emplace(
                        shardId,
                        TasksGraph.GetMeta().Allocate<NKikimrTxDataShard::TKqpTransaction>());
                    locks = eIt->second->MutableLocks();
                }
            }

            switch (Request.LocksOp) {
                case ELocksOp::Commit:
                    locks->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
                    break;
                case ELocksOp::Rollback:
                    locks->SetOp(NKikimrDataEvents::TKqpLocks::Rollback);
                    break;
                case ELocksOp::Unspecified:
                    break;
            }

            // Move lock descriptions to the datashard tx
            auto* protoLocks = locks->MutableLocks();
            protoLocks->Reserve(locksList.size());
            bool hasWrites = false;
            for (auto& lock : locksList) {
                hasWrites = hasWrites || lock.GetHasWrites();
                protoLocks->Add(std::move(lock));
            }
            locksList.clear();

            // When locks with writes are committed this commits accumulated effects
            if (Request.LocksOp == ELocksOp::Commit && hasWrites) {
                ShardsWithEffects.insert(shardId);
                YQL_ENSURE(!ReadOnlyTx);
            }
        }

        Request.TopicOperations.BuildTopicTxs(topicTxs);

        const bool needRollback = Request.LocksOp == ELocksOp::Rollback;

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
            (datashardTxs.size() + topicTxs.size()) > 1 &&
            // We cannot use volatile transactions with persistent channels
            // Note: currently persistent channels are never used
            !HasPersistentChannels &&
            // Can't use volatile transactions for EvWrite at current time
            !UseEvWrite);

        const bool useGenericReadSets = (
            // Use generic readsets when feature is explicitly enabled
            AppData()->FeatureFlags.GetEnableDataShardGenericReadSets() ||
            // Volatile transactions must always use generic readsets
            VolatileTx ||
            // Transactions with topics must always use generic readsets
            !topicTxs.empty());

        if (!locksMap.empty() || VolatileTx ||
            Request.TopicOperations.HasReadOperations() || Request.TopicOperations.HasWriteOperations())
        {
            YQL_ENSURE(Request.LocksOp == ELocksOp::Commit || Request.LocksOp == ELocksOp::Rollback || VolatileTx);

            bool needCommit = Request.LocksOp == ELocksOp::Commit || VolatileTx;

            absl::flat_hash_set<ui64> sendingShardsSet;
            absl::flat_hash_set<ui64> receivingShardsSet;
            ui64 arbiter = 0;

            // Gather shards that need to send/receive readsets (shards with effects)
            if (needCommit) {
                for (auto& [shardId, tx] : datashardTxs) {
                    if (tx->HasLocks()) {
                        // Locks may be broken so shards with locks need to send readsets
                        sendingShardsSet.insert(shardId);
                    }
                    if (ShardsWithEffects.contains(shardId)) {
                        // Volatile transactions may abort effects, so they send readsets
                        if (VolatileTx) {
                            sendingShardsSet.insert(shardId);
                        }
                        // Effects are only applied when all locks are valid
                        receivingShardsSet.insert(shardId);
                    }
                }

                for (auto& [shardId, tx] : evWriteTxs) {
                    if (tx->HasLocks()) {
                        // Locks may be broken so shards with locks need to send readsets
                        sendingShardsSet.insert(shardId);
                    }
                    if (ShardsWithEffects.contains(shardId)) {
                        // Volatile transactions may abort effects, so they send readsets
                        if (VolatileTx) {
                            sendingShardsSet.insert(shardId);
                        }
                        // Effects are only applied when all locks are valid
                        receivingShardsSet.insert(shardId);
                    }
                }

                if (auto tabletIds = Request.TopicOperations.GetSendingTabletIds()) {
                    sendingShardsSet.insert(tabletIds.begin(), tabletIds.end());
                    receivingShardsSet.insert(tabletIds.begin(), tabletIds.end());
                }

                if (auto tabletIds = Request.TopicOperations.GetReceivingTabletIds()) {
                    sendingShardsSet.insert(tabletIds.begin(), tabletIds.end());
                    receivingShardsSet.insert(tabletIds.begin(), tabletIds.end());
                }

                // The current value of 5 is arbitrary. Writing to 5 shards in
                // a single transaction is unusual enough, and having latency
                // regressions is unlikely. Full mesh readset count grows like
                // 2n(n-1), and arbiter reduces it to 4(n-1). Here's a readset
                // count table for various small `n`:
                //
                // n = 2: 4 -> 4
                // n = 3: 12 -> 8
                // n = 4: 24 -> 12
                // n = 5: 40 -> 16
                // n = 6: 60 -> 20
                // n = 7: 84 -> 24
                //
                // The ideal crossover is at n = 4, since the readset count
                // doesn't change when going from 3 to 4 shards, but the
                // increase in latency may not really be worth it. With n = 5
                // the readset count lowers from 24 to 16 readsets when going
                // from 4 to 5 shards. This makes 5 shards potentially cheaper
                // than 4 shards when readsets dominate the workload, but at
                // the price of possible increase in latency. Too many readsets
                // cause interconnect overload and reduce throughput however,
                // so we don't want to use a crossover value that is too high.
                const size_t minArbiterMeshSize = 5; // TODO: make configurable?
                if (VolatileTx &&
                    receivingShardsSet.size() >= minArbiterMeshSize &&
                    AppData()->FeatureFlags.GetEnableVolatileTransactionArbiters())
                {
                    std::vector<ui64> candidates;
                    candidates.reserve(receivingShardsSet.size());
                    for (ui64 candidate : receivingShardsSet) {
                        // Note: all receivers are also senders in volatile transactions
                        if (Y_LIKELY(sendingShardsSet.contains(candidate))) {
                            candidates.push_back(candidate);
                        }
                    }
                    if (candidates.size() >= minArbiterMeshSize) {
                        // Select a random arbiter
                        ui32 index = RandomNumber<ui32>(candidates.size());
                        arbiter = candidates.at(index);
                    }
                }
            }


            // Encode sending/receiving shards in tx bodies
            if (needCommit) {
                NProtoBuf::RepeatedField<ui64> sendingShards(sendingShardsSet.begin(), sendingShardsSet.end());
                NProtoBuf::RepeatedField<ui64> receivingShards(receivingShardsSet.begin(), receivingShardsSet.end());

                std::sort(sendingShards.begin(), sendingShards.end());
                std::sort(receivingShards.begin(), receivingShards.end());

                for (auto& [shardId, shardTx] : datashardTxs) {
                    shardTx->MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
                    *shardTx->MutableLocks()->MutableSendingShards() = sendingShards;
                    *shardTx->MutableLocks()->MutableReceivingShards() = receivingShards;
                    if (arbiter) {
                        shardTx->MutableLocks()->SetArbiterShard(arbiter);
                    }
                }

                for (auto& [_, tx] : evWriteTxs) {
                    tx->MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
                    *tx->MutableLocks()->MutableSendingShards() = sendingShards;
                    *tx->MutableLocks()->MutableReceivingShards() = receivingShards;
                    if (arbiter) {
                        tx->MutableLocks()->SetArbiterShard(arbiter);
                    }
                }

                for (auto& [_, tx] : topicTxs) {
                    tx.SetOp(NKikimrPQ::TDataTransaction::Commit);
                    *tx.MutableSendingShards() = sendingShards;
                    *tx.MutableReceivingShards() = receivingShards;
                    YQL_ENSURE(!arbiter);
                }
            }
        }

        if (useGenericReadSets) {
            // Make sure datashards use generic readsets
            for (auto& pr : datashardTxs) {
                pr.second->SetUseGenericReadSets(true);
            }
        }
    }

    void ExecuteTasks() {
        auto lockTxId = Request.AcquireLocksTxId;
        if (lockTxId.Defined() && *lockTxId == 0) {
            lockTxId = TxId;
            LockHandle = TLockHandle(TxId, TActivationContext::ActorSystem());
        }

        LWTRACK(KqpDataExecuterStartTasksAndTxs, ResponseEv->Orbit, TxId, ComputeTasks.size(), DatashardTxs.size() + EvWriteTxs.size());

        for (auto& [shardId, tasks] : RemoteComputeTasks) {
            auto it = ShardIdToNodeId.find(shardId);
            YQL_ENSURE(it != ShardIdToNodeId.end());
            for (ui64 taskId : tasks) {
                auto& task = TasksGraph.GetTask(taskId);
                task.Meta.NodeId = it->second;
            }
        }

        const bool singlePartitionOptAllowed = !HasOlapTable && !UnknownAffectedShardCount && !HasExternalSources && DatashardTxs.empty() && EvWriteTxs.empty();
        const bool useDataQueryPool = !(HasExternalSources && DatashardTxs.empty() && EvWriteTxs.empty());
        const bool localComputeTasks = !DatashardTxs.empty();
        const bool mayRunTasksLocally = !((HasExternalSources || HasOlapTable || HasDatashardSourceScan) && DatashardTxs.empty());

        Planner = CreateKqpPlanner({
            .TasksGraph = TasksGraph,
            .TxId = TxId,
            .LockTxId = lockTxId.GetOrElse(0),
            .LockNodeId = SelfId().NodeId(),
            .Executer = SelfId(),
            .Snapshot = GetSnapshot(),
            .Database = Database,
            .UserToken = UserToken,
            .Deadline = Deadline.GetOrElse(TInstant::Zero()),
            .StatsMode = Request.StatsMode,
            .WithSpilling = TasksGraph.GetMeta().AllowWithSpilling,
            .RlPath = Nothing(),
            .ExecuterSpan =  ExecuterSpan,
            .ResourcesSnapshot = std::move(ResourceSnapshot),
            .ExecuterRetriesConfig = ExecuterRetriesConfig,
            .UseDataQueryPool = useDataQueryPool,
            .LocalComputeTasks = localComputeTasks,
            .MkqlMemoryLimit = Request.MkqlMemoryLimit,
            .AsyncIoFactory = AsyncIoFactory,
            .AllowSinglePartitionOpt = singlePartitionOptAllowed,
            .UserRequestContext = GetUserRequestContext(),
            .FederatedQuerySetup = FederatedQuerySetup,
            .OutputChunkMaxSize = Request.OutputChunkMaxSize,
            .GUCSettings = GUCSettings,
            .MayRunTasksLocally = mayRunTasksLocally,
            .ResourceManager_ = Request.ResourceManager_,
            .CaFactory_ = Request.CaFactory_
        });

        auto err = Planner->PlanExecution();
        if (err) {
            TlsActivationContext->Send(err.release());
            return;
        }

        Planner->Submit();

        // then start data tasks with known actor ids of compute tasks
        for (auto& [shardId, shardTx] : DatashardTxs) {
            shardTx->SetType(NKikimrTxDataShard::KQP_TX_TYPE_DATA);
            std::optional<bool> isOlap;
            for (auto& protoTask : *shardTx->MutableTasks()) {
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

            ExecuteDatashardTransaction(shardId, *shardTx, isOlap.value_or(false));
        }

        for (auto& [shardId, shardTx] : EvWriteTxs) {
            ExecuteEvWriteTransaction(shardId, *shardTx);
        }

        ExecuteTopicTabletTransactions(TopicTxs);

        LOG_I("Total tasks: " << TasksGraph.GetTasks().size()
            << ", readonly: " << ReadOnlyTx
            << ", datashardTxs: " << DatashardTxs.size()
            << ", evWriteTxs: " << EvWriteTxs.size()
            << ", topicTxs: " << Request.TopicOperations.GetSize()
            << ", volatile: " << VolatileTx
            << ", immediate: " << ImmediateTx
            << ", pending compute tasks" << (Planner ? Planner->GetPendingComputeTasks().size() : 0)
            << ", useFollowers: " << GetUseFollowers());

        // error
        LOG_T("Updating channels after the creation of compute actors");
        THashMap<TActorId, THashSet<ui64>> updates;
        for (ui64 taskId : ComputeTasks) {
            auto& task = TasksGraph.GetTask(taskId);
            if (task.ComputeActorId)
                CollectTaskChannelsUpdates(task, updates);
        }
        PropagateChannelsUpdates(updates);

        CheckExecutionComplete();
    }

    void ExecuteTopicTabletTransactions(TTopicTabletTxs& topicTxs) {
        TMaybe<ui64> writeId;
        if (Request.TopicOperations.HasWriteId()) {
            writeId = Request.TopicOperations.GetWriteId();
        }

        for (auto& tx : topicTxs) {
            auto tabletId = tx.first;
            auto& transaction = tx.second;

            auto ev = std::make_unique<TEvPersQueue::TEvProposeTransaction>();

            if (writeId.Defined()) {
                auto* w = transaction.MutableWriteId();
                w->SetNodeId(SelfId().NodeId());
                w->SetKeyId(*writeId);
            }
            transaction.SetImmediate(ImmediateTx);

            ActorIdToProto(SelfId(), ev->Record.MutableSourceActor());
            ev->Record.MutableData()->Swap(&transaction);
            ev->Record.SetTxId(TxId);

            auto traceId = ExecuterSpan.GetTraceId();
            LOG_D("ExecuteTopicTabletTransaction traceId.verbosity: " << std::to_string(traceId.GetVerbosity()));

            LOG_D("Executing KQP transaction on topic tablet: " << tabletId
                  << ", writeId: " << writeId);

            Send(MakePipePerNodeCacheID(false),
                 new TEvPipeCache::TEvForward(ev.release(), tabletId, true),
                 0,
                 0,
                 std::move(traceId));

            TShardState state;
            state.State =
                ImmediateTx ? TShardState::EState::Executing : TShardState::EState::Preparing;
            state.DatashardState.ConstructInPlace();
            state.DatashardState->Follower = false;

            state.DatashardState->ShardReadLocks = Request.TopicOperations.TabletHasReadOperations(tabletId);

            auto result = ShardStates.emplace(tabletId, std::move(state));
            YQL_ENSURE(result.second);
        }
    }

    void Shutdown() override {
        if (Planner) {
            if (Planner->GetPendingComputeTasks().empty() && Planner->GetPendingComputeActors().empty()) {
                LOG_I("Shutdown immediately - nothing to wait");
                PassAway();
            } else {
                this->Become(&TThis::WaitShutdownState);
                LOG_I("Waiting for shutdown of " << Planner->GetPendingComputeTasks().size() << " tasks and "
                    << Planner->GetPendingComputeActors().size() << " compute actors");
                TActivationContext::Schedule(TDuration::Seconds(10), new IEventHandle(SelfId(), SelfId(), new TEvents::TEvPoison));
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

        TBase::PassAway();
    }

    STATEFN(WaitShutdownState) {
        switch(ev->GetTypeRewrite()) {
            hFunc(TEvDqCompute::TEvState, HandleShutdown);
            hFunc(TEvInterconnect::TEvNodeDisconnected, HandleShutdown);
            hFunc(TEvents::TEvPoison, HandleShutdown);
            default:
                LOG_E("Unexpected event: " << ev->GetTypeName()); // ignore all other events
        }
    }

    void HandleShutdown(TEvDqCompute::TEvState::TPtr& ev) {
        if (ev->Get()->Record.GetState() == NDqProto::COMPUTE_STATE_FAILURE) {
            YQL_ENSURE(Planner);

            TActorId actor = ev->Sender;
            ui64 taskId = ev->Get()->Record.GetTaskId();

            Planner->CompletedCA(taskId, actor);

            if (Planner->GetPendingComputeTasks().empty() && Planner->GetPendingComputeActors().empty()) {
                PassAway();
            }
        }
    }

    void HandleShutdown(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        const auto nodeId = ev->Get()->NodeId;
        LOG_N("Node has disconnected while shutdown: " << nodeId);

        YQL_ENSURE(Planner);

        for (const auto& task : TasksGraph.GetTasks()) {
            if (task.Meta.NodeId == nodeId && !task.Meta.Completed) {
                if (task.ComputeActorId) {
                    Planner->CompletedCA(task.Id, task.ComputeActorId);
                } else {
                    Planner->TaskNotStarted(task.Id);
                }
            }
        }

        if (Planner->GetPendingComputeTasks().empty() && Planner->GetPendingComputeActors().empty()) {
            PassAway();
        }
    }

    void HandleShutdown(TEvents::TEvPoison::TPtr& ev) {
        // Self-poison means timeout - don't wait anymore.
        LOG_I("Timed out on waiting for Compute Actors to finish - forcing shutdown");

        if (ev->Sender == SelfId()) {
            PassAway();
        }
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
                << "[" << err.GetKind() << "] " << err.GetReason()));
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
    bool EnableOlapSink = false;
    bool UseEvWrite = false;
    const std::optional<TKqpFederatedQuerySetup> FederatedQuerySetup;
    const TGUCSettings::TPtr GUCSettings;

    bool HasExternalSources = false;
    bool SecretSnapshotRequired = false;
    bool ResourceSnapshotRequired = false;
    bool SaveScriptExternalEffectRequired = false;
    TVector<NKikimrKqp::TKqpNodeResources> ResourceSnapshot;

    ui64 TxCoordinator = 0;
    THashMap<ui64, TShardState> ShardStates;
    TVector<NKikimrDataEvents::TLock> Locks;
    bool ReadOnlyTx = true;
    bool VolatileTx = false;
    bool ImmediateTx = false;
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
    TEvWriteTxs EvWriteTxs;
    TTopicTabletTxs TopicTxs;

    // Lock handle for a newly acquired lock
    TLockHandle LockHandle;
    ui64 LastShard = 0;
};

} // namespace

IActor* CreateKqpDataExecuter(IKqpGateway::TExecPhysicalRequest&& request, const TString& database, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken,
    TKqpRequestCounters::TPtr counters, bool streamResult, const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
    NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory, const TActorId& creator,
    const TIntrusivePtr<TUserRequestContext>& userRequestContext,
    const bool enableOlapSink, const bool useEvWrite, ui32 statementResultIndex,
    const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup, const TGUCSettings::TPtr& GUCSettings)
{
    return new TKqpDataExecuter(std::move(request), database, userToken, counters, streamResult, tableServiceConfig,
        std::move(asyncIoFactory), creator, userRequestContext,
        enableOlapSink, useEvWrite, statementResultIndex, federatedQuerySetup, GUCSettings);
}

} // namespace NKqp
} // namespace NKikimr
