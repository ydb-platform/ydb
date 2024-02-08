#include "proxy.h"
#include "resolvereq.h"

#include <ydb/core/tx/datashard/datashard.h>

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr {
namespace NTxProxy {

////////////////////////////////////////////////////////////////////////////////

class TCommitWritesReq : public TActorBootstrapped<TCommitWritesReq> {
private:
    struct TPerShardState {
        enum {
            AffectedRead = 1 << 0,
            AffectedWrite = 1 << 1,
        };

        enum class EStatus {
            Unknown = 0,
            Wait = 1,
            Prepared = 2,
            Error = 4,
            Complete = 3,
        };

        friend inline IOutputStream& operator<<(IOutputStream& stream, EStatus status) {
            switch (status) {
#define STATUS_CASE(x)            \
                case EStatus::x:  \
                    stream << #x; \
                    break;
                STATUS_CASE(Unknown)
                STATUS_CASE(Wait)
                STATUS_CASE(Prepared)
                STATUS_CASE(Error)
                STATUS_CASE(Complete)
#undef STATUS_CASE
            }

            return stream;
        }

        ui64 MinStep = 0;
        ui64 MaxStep = 0;
        ui32 AffectedFlags = 0;
        EStatus Status = EStatus::Unknown;

        TTablePathHashSet Tables;
    };

private:
    void Die(const TActorContext &ctx) override {
        Send(Services.LeaderPipeCache, new TEvPipeCache::TEvUnlink(0));

        TActor::Die(ctx);
    }

    static TInstant Now() {
        return AppData()->TimeProvider->Now();
    }

public:
    TCommitWritesReq(const TTxProxyServices& services, const ui64 txid, TEvTxUserProxy::TEvProposeTransaction::TPtr&& ev, const TIntrusivePtr<TTxProxyMon>& mon)
        : Services(services)
        , TxId(txid)
        , Sender(ev->Sender)
        , Cookie(ev->Cookie)
        , Request(ev->Release())
        , TxProxyMon(mon)
        , DefaultTimeoutMs(60000, 0, 360000)
    { }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_REQ_PROXY;
    }

    void Bootstrap(const TActorContext& ctx) {
        AppData(ctx)->Icb->RegisterSharedControl(DefaultTimeoutMs,
                                                "TxLimitControls.DefaultTimeoutMs");

        WallClockAccepted = Now();

        const auto& record = Request->Record;
        Y_ABORT_UNLESS(record.HasTransaction());

        if (record.HasProxyFlags()) {
            ProxyFlags = record.GetProxyFlags();
        }

        ExecTimeoutPeriod = record.HasExecTimeoutPeriod()
            ? TDuration::MilliSeconds(record.GetExecTimeoutPeriod())
            : TDuration::MilliSeconds(DefaultTimeoutMs);
        if (ExecTimeoutPeriod.Minutes() > 60) {
            LOG_WARN_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
                            "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
                            << " huge ExecTimeoutPeriod requested " << ExecTimeoutPeriod.ToString()
                            << ", trimming to 30 min");
            ExecTimeoutPeriod = TDuration::Minutes(30);
        }

        // Schedule execution timeout
        {
            THolder<IEventHandle> wakeupEv = MakeHolder<IEventHandle>(ctx.SelfID, ctx.SelfID, new TEvents::TEvWakeup());
            ExecTimeoutCookieHolder.Reset(ISchedulerCookie::Make2Way());

            CreateLongTimer(ctx, ExecTimeoutPeriod, wakeupEv, AppData(ctx)->SystemPoolId, ExecTimeoutCookieHolder.Get());
        }

        if (!record.GetUserToken().empty()) {
            UserToken = MakeHolder<NACLib::TUserToken>(record.GetUserToken());
        }

        const auto& tx = record.GetTransaction();
        Y_ABORT_UNLESS(tx.HasCommitWrites());

        TxFlags = tx.GetFlags() & ~NTxDataShard::TTxFlags::Immediate;

        const auto& params = tx.GetCommitWrites();

        TVector<TResolveTableRequest> requests;
        requests.reserve(params.TablesSize());

        for (const auto& proto : params.GetTables()) {
            auto& table = requests.emplace_back();
            table.TablePath = proto.GetTablePath();
            table.KeyRange = proto.GetKeyRange();
        }

        ResolveActorID = ctx.RegisterWithSameMailbox(CreateResolveTablesActor(ctx.SelfID, TxId, Services, std::move(requests), record.GetDatabaseName()));
        Become(&TThis::StateWaitResolve);
    }

private:
    STFUNC(StateWaitResolve) {
        TRACE_EVENT(NKikimrServices::TX_PROXY);
        switch (ev->GetTypeRewrite()) {
            HFuncTraced(TEvResolveTablesResponse, HandleResolve);
            CFunc(TEvents::TSystem::Wakeup, HandleResolveTimeout);
        }
    }

    void HandleResolveTimeout(const TActorContext& ctx) {
        LOG_ERROR_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
            "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " HANDLE ResolveTimeout TCommitWritesReq");
        ctx.Send(ResolveActorID, new TEvents::TEvPoison());
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecTimeout, NKikimrIssues::TStatusIds::TIMEOUT, true, ctx);
        return Die(ctx);
    }

    void HandleResolve(TEvResolveTablesResponse::TPtr& ev, const TActorContext& ctx) {
        Y_DEBUG_ABORT_UNLESS(ev->Sender == ResolveActorID);
        ResolveActorID = { };

        auto* msg = ev->Get();

        WallClockResolveStarted = msg->WallClockResolveStarted;
        WallClockResolved = msg->WallClockResolved;
        if (msg->UnresolvedKeys) {
            UnresolvedKeys.insert(UnresolvedKeys.end(), msg->UnresolvedKeys.begin(), msg->UnresolvedKeys.end());
        }
        if (msg->Issues) {
            IssueManager.RaiseIssues(msg->Issues);
        }

        if (WallClockResolved) {
            TxProxyMon->CacheRequestLatency->Collect((WallClockResolved - WallClockAccepted).MilliSeconds());
        }

        if (msg->Status != TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyResolved) {
            if (msg->StatusCode == NKikimrIssues::TStatusIds::SCHEME_ERROR ||
                msg->StatusCode == NKikimrIssues::TStatusIds::QUERY_ERROR)
            {
                TxProxyMon->ResolveKeySetWrongRequest->Inc();
            }

            ReportStatus(msg->Status, msg->StatusCode, true, ctx);
            return Die(ctx);
        }

        if (ProxyFlags & TEvTxUserProxy::TEvProposeTransaction::ProxyReportResolved) {
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyResolved, NKikimrIssues::TStatusIds::TRANSIENT, false, ctx);
        }

        TxProxyMon->TxPrepareResolveHgram->Collect((WallClockResolved - WallClockResolveStarted).MicroSeconds());

        for (const auto& entry : msg->Tables) {
            // N.B. we create all keys as a read operation
            ui32 access = 0;
            switch (entry.KeyDescription->RowOperation) {
                case TKeyDesc::ERowOperation::Update:
                    access |= NACLib::EAccessRights::UpdateRow;
                    break;
                case TKeyDesc::ERowOperation::Read:
                    access |= NACLib::EAccessRights::SelectRow;
                    break;
                case TKeyDesc::ERowOperation::Erase:
                    access |= NACLib::EAccessRights::EraseRow;
                    break;
                default:
                    break;
            }

            if (entry.KeyDescription->TableId.IsSystemView() ||
                TSysTables::IsSystemTable(entry.KeyDescription->TableId))
            {
                const TString explanation = TStringBuilder()
                    << "Cannot commit writes to system tableId# "
                    << entry.KeyDescription->TableId;
                LOG_ERROR_S(ctx, NKikimrServices::TX_PROXY, explanation);
                IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, explanation));
                UnresolvedKeys.push_back(explanation);
                ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError, NKikimrIssues::TStatusIds::SCHEME_ERROR, true, ctx);
                TxProxyMon->ResolveKeySetWrongRequest->Inc();
                return Die(ctx);
            }

            if (access != 0
                && UserToken != nullptr
                && entry.KeyDescription->Status == TKeyDesc::EStatus::Ok
                && entry.KeyDescription->SecurityObject != nullptr
                && !entry.KeyDescription->SecurityObject->CheckAccess(access, *UserToken))
            {
                TStringStream explanation;
                explanation << "Access denied for " << UserToken->GetUserSID()
                    << " with access " << NACLib::AccessRightsToString(access)
                    << " to tableId# " << entry.KeyDescription->TableId;

                LOG_ERROR_S(ctx, NKikimrServices::TX_PROXY, explanation.Str());
                IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::ACCESS_DENIED, explanation.Str()));
                ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::AccessDenied, NKikimrIssues::TStatusIds::ACCESS_DENIED, true, ctx);
                return Die(ctx);
            }

            for (auto& partition : entry.KeyDescription->GetPartitions()) {
                auto& state = PerShardStates[partition.ShardId];
                state.Tables.insert(entry.KeyDescription->TableId);
            }
        }

        if (PerShardStates.empty()) {
            // No real (OLTP or OLAP) tables in the request so we can use current time as a fake PlanStep
            PlanStep = ctx.Now().MilliSeconds();

            // We don't have any shards to commit, report fake success
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete, NKikimrIssues::TStatusIds::SUCCESS, true, ctx);

            return Die(ctx);
        }

        if (!msg->CheckDomainLocality()) {
            IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::DOMAIN_LOCALITY_ERROR));
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::DomainLocalityError, NKikimrIssues::TStatusIds::BAD_REQUEST, true, ctx);
            TxProxyMon->ResolveKeySetDomainLocalityFail->Inc();
            return Die(ctx);
        }

        SelectedCoordinator = SelectCoordinator(msg->FindDomainInfo(), ctx);

        const auto& params = Request->Record.GetTransaction().GetCommitWrites();

        bool immediate = PerShardStates.size() == 1;

        for (auto& kv : PerShardStates) {
            ui64 shardId = kv.first;
            auto& state = kv.second;

            // TODO: support colocated tables
            Y_ABORT_UNLESS(state.Tables.size() == 1, "TODO: support colocated tables");
            Y_ABORT_UNLESS(state.Status == TPerShardState::EStatus::Unknown);

            auto tableId = *state.Tables.begin();
            auto path = tableId.PathId;

            NKikimrTxDataShard::TCommitWritesTransaction tx;
            auto* pTableId = tx.MutableTableId();
            pTableId->SetOwnerId(path.OwnerId);
            pTableId->SetTableId(path.LocalPathId);
            tx.SetWriteTxId(params.GetWriteTxId());

            const TString txBody = tx.SerializeAsString();

            LOG_DEBUG_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
                "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
                << " SEND TEvProposeTransaction to datashard " << shardId
                << " with commit writes request"
                << " affected shards " << PerShardStates.size()
                << " marker# P3");

            Send(Services.LeaderPipeCache, new TEvPipeCache::TEvForward(
                    new TEvDataShard::TEvProposeTransaction(NKikimrTxDataShard::TX_KIND_COMMIT_WRITES,
                        ctx.SelfID, TxId, txBody,
                        TxFlags | (immediate ? NTxDataShard::TTxFlags::Immediate : 0)),
                    shardId, true));

            state.AffectedFlags |= TPerShardState::AffectedRead;
            state.Status = TPerShardState::EStatus::Wait;
            ++TabletsToPrepare;
        }

        Become(&TThis::StateWaitPrepare);
    }

private:
    STFUNC(StateWaitPrepare) {
        TRACE_EVENT(NKikimrServices::TX_PROXY);
        switch (ev->GetTypeRewrite()) {
            HFuncTraced(TEvDataShard::TEvProposeTransactionResult, HandlePrepare);
            HFuncTraced(TEvPipeCache::TEvDeliveryProblem, HandlePrepare);
            HFuncTraced(TEvents::TEvUndelivered, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleExecTimeout);
        }
    }

    void Handle(TEvents::TEvUndelivered::TPtr&, const TActorContext& ctx) {
        IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_TXPROXY_ERROR, "unexpected event delivery problem"));
        ReportStatus(TEvTxUserProxy::TResultStatus::Unknown, NKikimrIssues::TStatusIds::INTERNAL_ERROR, true, ctx);
        return Die(ctx);
    }

    void HandleExecTimeout(const TActorContext& ctx) {
        LOG_ERROR_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
            "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " HANDLE ExecTimeout TCommitWritesReq");
        // TODO: should cancel any active transaction proposals
        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecTimeout, NKikimrIssues::TStatusIds::TIMEOUT, true, ctx);
        return Die(ctx);
    }

    void HandlePrepare(TEvDataShard::TEvProposeTransactionResult::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();
        const auto& record = msg->Record;
        const ui64 shardId = msg->GetOrigin();
        auto& state = PerShardStates[shardId];
        Y_VERIFY_S(state.Status != TPerShardState::EStatus::Unknown,
            "Received TEvProposeTransactionResult from unexpected shard " << shardId);

        LOG_LOG_S_SAMPLED_BY(ctx,
            (msg->GetStatus() != NKikimrTxDataShard::TEvProposeTransactionResult::ERROR
                ? NActors::NLog::PRI_DEBUG
                : NActors::NLog::PRI_ERROR),
            NKikimrServices::TX_PROXY, TxId,
            "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " HANDLE Prepare TEvProposeTransactionResult TCommitWritesReq"
            << " ShardStatus# " << state.Status
            << " ResultStatus# " << msg->GetStatus()
            << " shard id " << shardId
            << " marker# P4");

        // TODO: latencies, counters

        if (state.Status != TPerShardState::EStatus::Wait) {
            // Ignore unexpected messages
            return;
        }

        switch (msg->GetStatus()) {
            case NKikimrTxDataShard::TEvProposeTransactionResult::PREPARED: {
                state.Status = TPerShardState::EStatus::Prepared;
                state.MinStep = record.GetMinStep();
                state.MaxStep = record.GetMaxStep();

                AggrMinStep = Max(AggrMinStep, state.MinStep);
                AggrMaxStep = Min(AggrMaxStep, state.MaxStep);

                if (record.HasExecLatency())
                    ElapsedPrepareExec = Max<TDuration>(ElapsedPrepareExec, TDuration::MilliSeconds(record.GetExecLatency()));
                if (record.HasProposeLatency())
                    ElapsedPrepareComplete = Max<TDuration>(ElapsedPrepareComplete, TDuration::MilliSeconds(record.GetProposeLatency()));

                TxProxyMon->TxResultPrepared->Inc();

                const TVector<ui64> privateCoordinators(
                        record.GetDomainCoordinators().begin(),
                        record.GetDomainCoordinators().end());
                const ui64 privateCoordinator = TCoordinators(privateCoordinators)
                        .Select(TxId);

                if (!SelectedCoordinator) {
                    SelectedCoordinator = privateCoordinator;
                }

                if (!SelectedCoordinator || SelectedCoordinator != privateCoordinator) {
                    CancelProposal();

                    const TString explanation = TStringBuilder()
                        << "Unable to choose coordinator"
                        << " from shard " << shardId
                        << " txId# " << TxId;
                    IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::TX_DECLINED_IMPLICIT_COORDINATOR, explanation));
                    auto errorCode = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::DomainLocalityError;
                    if (SelectedCoordinator == 0) {
                        errorCode = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::CoordinatorUnknown;
                    }
                    ReportStatus(errorCode, NKikimrIssues::TStatusIds::INTERNAL_ERROR, true, ctx);

                    TxProxyMon->TxResultAborted->Inc();

                    LOG_ERROR_S(ctx, NKikimrServices::TX_PROXY,
                        "HANDLE Prepare TEvProposeTransactionResult TCommitWritesReq "
                        << explanation
                        << ", actorId: " << ctx.SelfID.ToString()
                        << ", coordinator selected at resolve keys state: " << SelectedCoordinator
                        << ", coordinator selected at propose result state: " << privateCoordinator);

                    return Die(ctx);
                }

                if (--TabletsToPrepare) {
                    return;
                }

                return RegisterPlan(ctx);
            }
            case NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE: {
                state.Status = TPerShardState::EStatus::Complete;

                TxProxyMon->TxResultComplete->Inc();

                --TabletsToPrepare;

                if (TabletsToPrepare == 0 && PerShardStates.size() == 1) {
                    // Immediate transaction may complete without planning
                    return MergeResult(ev, ctx);
                }

                CancelProposal();

                const TString explanation = TStringBuilder()
                    << "Unexpected COMPLETE result from shard " << shardId << " txId# " << TxId;
                IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_TXPROXY_ERROR, explanation));
                ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError,
                        NKikimrIssues::TStatusIds::INTERNAL_ERROR, true, ctx);
                LOG_ERROR_S(ctx, NKikimrServices::TX_PROXY, explanation);
                TxProxyMon->TxResultComplete->Inc();
                return Die(ctx);
            }
            case NKikimrTxDataShard::TEvProposeTransactionResult::ERROR:
                ExtractDatashardErrors(record);
                CancelProposal(shardId);
                ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable, NKikimrIssues::TStatusIds::REJECTED, true, ctx);
                Become(&TThis::StatePrepareErrors, ctx, TDuration::MilliSeconds(500), new TEvents::TEvWakeup);
                TxProxyMon->TxResultError->Inc();
                return HandlePrepareErrors(ev, ctx);
            case NKikimrTxDataShard::TEvProposeTransactionResult::ABORTED:
                state.Status = TPerShardState::EStatus::Error;
                ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecAborted, NKikimrIssues::TStatusIds::SUCCESS, true, ctx);
                TxProxyMon->TxResultAborted->Inc();
                return Die(ctx);
            case NKikimrTxDataShard::TEvProposeTransactionResult::TRY_LATER:
                state.Status = TPerShardState::EStatus::Error;
                ExtractDatashardErrors(record);
                CancelProposal(shardId);
                ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardTryLater, NKikimrIssues::TStatusIds::REJECTED, true, ctx);
                TxProxyMon->TxResultShardTryLater->Inc();
                return Die(ctx);
            case NKikimrTxDataShard::TEvProposeTransactionResult::OVERLOADED:
                state.Status = TPerShardState::EStatus::Error;
                ExtractDatashardErrors(record);
                CancelProposal(shardId);
                ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardOverloaded, NKikimrIssues::TStatusIds::OVERLOADED, true, ctx);
                TxProxyMon->TxResultShardOverloaded->Inc();
                return Die(ctx);
            case NKikimrTxDataShard::TEvProposeTransactionResult::EXEC_ERROR:
                state.Status = TPerShardState::EStatus::Error;
                ExtractDatashardErrors(record);
                ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError, NKikimrIssues::TStatusIds::ERROR, true, ctx);
                TxProxyMon->TxResultExecError->Inc();
                return Die(ctx);
            case NKikimrTxDataShard::TEvProposeTransactionResult::RESULT_UNAVAILABLE:
                state.Status = TPerShardState::EStatus::Error;
                ExtractDatashardErrors(record);
                ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecResultUnavailable, NKikimrIssues::TStatusIds::ERROR, true, ctx);
                TxProxyMon->TxResultResultUnavailable->Inc();
                return Die(ctx);
            case NKikimrTxDataShard::TEvProposeTransactionResult::CANCELLED:
                state.Status = TPerShardState::EStatus::Error;
                ExtractDatashardErrors(record);
                ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecCancelled, NKikimrIssues::TStatusIds::ERROR, true, ctx);
                TxProxyMon->TxResultCancelled->Inc();
                return Die(ctx);
            case NKikimrTxDataShard::TEvProposeTransactionResult::BAD_REQUEST:
                state.Status = TPerShardState::EStatus::Error;
                ExtractDatashardErrors(record);
                ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest, NKikimrIssues::TStatusIds::BAD_REQUEST, true, ctx);
                TxProxyMon->TxResultCancelled->Inc();
                return Die(ctx);
            default:
                // everything other is hard error
                state.Status = TPerShardState::EStatus::Error;
                ExtractDatashardErrors(record);
                CancelProposal();
                ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardUnknown, NKikimrIssues::TStatusIds::ERROR, true, ctx);
                TxProxyMon->TxResultFatal->Inc();
                return Die(ctx);
        }
    }

    void HandlePrepare(TEvPipeCache::TEvDeliveryProblem::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();
        auto& state = PerShardStates[msg->TabletId];
        Y_VERIFY_S(state.Status != TPerShardState::EStatus::Unknown,
            "Received TEvDeliveryProblem from unexpected shard " << msg->TabletId);

        if (state.Status != TPerShardState::EStatus::Wait) {
            return;
        }

        ComplainingDatashards.push_back(msg->TabletId);
        CancelProposal(msg->TabletId);

        if (msg->NotDelivered) {
            const TString explanation = TStringBuilder()
                << "could not deliver program to shard " << msg->TabletId << " with txid# " << TxId;
            IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, explanation));
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable, NKikimrIssues::TStatusIds::REJECTED, true, ctx);
        } else {
            const TString explanation = TStringBuilder()
                << "tx state unknown for shard " << msg->TabletId << " with txid# " << TxId;
            IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::TX_STATE_UNKNOWN, explanation));
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardUnknown, NKikimrIssues::TStatusIds::TIMEOUT, true, ctx);
        }

        Become(&TThis::StatePrepareErrors, ctx, TDuration::MilliSeconds(500), new TEvents::TEvWakeup);
        TxProxyMon->ClientConnectedError->Inc();
        return HandlePrepareErrors(ev, ctx);
    }

private:
    STFUNC(StatePrepareErrors) {
        TRACE_EVENT(NKikimrServices::TX_PROXY);
        switch (ev->GetTypeRewrite()) {
            HFuncTraced(TEvDataShard::TEvProposeTransactionResult, HandlePrepareErrors);
            HFuncTraced(TEvPipeCache::TEvDeliveryProblem, HandlePrepareErrors);
            HFuncTraced(TEvents::TEvUndelivered, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandlePrepareErrorTimeout);
        }
    }

    void HandlePrepareErrorTimeout(const TActorContext& ctx) {
        TxProxyMon->PrepareErrorTimeout->Inc();
        return Die(ctx);
    }


    void HandlePrepareErrors(TEvDataShard::TEvProposeTransactionResult::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();
        const auto& record = msg->Record;
        const ui64 shardId = msg->GetOrigin();
        auto& state = PerShardStates[shardId];
        Y_VERIFY_S(state.Status != TPerShardState::EStatus::Unknown,
            "Received TEvProposeTransactionResult from unexpected shard " << shardId);

        LOG_LOG_S_SAMPLED_BY(ctx,
            (msg->GetStatus() != NKikimrTxDataShard::TEvProposeTransactionResult::ERROR
                ? NActors::NLog::PRI_DEBUG
                : NActors::NLog::PRI_ERROR),
            NKikimrServices::TX_PROXY, TxId,
            "Actor# " << ctx.SelfID.ToString()
            << " txid# " << TxId
            << " HANDLE PrepareErrors TEvProposeTransactionResult TCommitWritesReq"
            << " ShardStatus# " << state.Status
            << " shard id " << shardId);

        if (state.Status != TPerShardState::EStatus::Wait) {
            return;
        }

        switch (msg->GetStatus()) {
            case NKikimrTxDataShard::TEvProposeTransactionResult::ERROR:
                for (const auto &er : record.GetError()) {
                    const NKikimrTxDataShard::TError::EKind errorKind = er.GetKind();
                    switch (errorKind) {
                        case NKikimrTxDataShard::TError::SCHEME_ERROR:
                        case NKikimrTxDataShard::TError::WRONG_PAYLOAD_TYPE:
                        case NKikimrTxDataShard::TError::WRONG_SHARD_STATE:
                        case NKikimrTxDataShard::TError::SCHEME_CHANGED:
                            return MarkShardPrepareError(shardId, state, true, ctx);
                        default:
                            break;
                    }
                }
                [[fallthrough]];
            default:
                return MarkShardPrepareError(shardId, state, false, ctx);
        }
    }

    void HandlePrepareErrors(TEvPipeCache::TEvDeliveryProblem::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();
        auto& state = PerShardStates[msg->TabletId];
        Y_VERIFY_S(state.Status != TPerShardState::EStatus::Unknown,
            "Received TEvDeliveryProblem from unexpected shard " << msg->TabletId);

        if (state.Status != TPerShardState::EStatus::Wait) {
            return;
        }

        LOG_LOG_S_SAMPLED_BY(ctx, NActors::NLog::PRI_ERROR,
            NKikimrServices::TX_PROXY, TxId,
            "Actor# " << ctx.SelfID.ToString()
            << " txid# " << TxId
            << " shard " << msg->TabletId
            << " delivery problem");

        return MarkShardPrepareError(msg->TabletId, state, true, ctx);
    }

private:
    void RegisterPlan(const TActorContext& ctx) {
        WallClockPrepared = Now();

        if (ProxyFlags & TEvTxUserProxy::TEvProposeTransaction::ProxyReportPrepared) {
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyPrepared, NKikimrIssues::TStatusIds::TRANSIENT, false, ctx);
        }

        Y_ABORT_UNLESS(SelectedCoordinator, "Unexpected null SelectedCoordinator");

        auto req = MakeHolder<TEvTxProxy::TEvProposeTransaction>(
            SelectedCoordinator, TxId, 0, AggrMinStep, AggrMaxStep);

        auto* reqAffectedSet = req->Record.MutableTransaction()->MutableAffectedSet();
        reqAffectedSet->Reserve(PerShardStates.size());

        for (const auto& kv : PerShardStates) {
            ui64 shardId = kv.first;
            auto& state = kv.second;

            auto* x = reqAffectedSet->Add();
            x->SetTabletId(shardId);
            x->SetFlags(state.AffectedFlags);
        }

        LOG_DEBUG_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
            "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " SEND EvProposeTransaction to# " << SelectedCoordinator << " Coordinator marker# P5");

        Send(Services.LeaderPipeCache, new TEvPipeCache::TEvForward(req.Release(), SelectedCoordinator, true));
        Become(&TThis::StateWaitPlan);
    }

    STFUNC(StateWaitPlan) {
        TRACE_EVENT(NKikimrServices::TX_PROXY);
        switch (ev->GetTypeRewrite()) {
            HFuncTraced(TEvTxProxy::TEvProposeTransactionStatus, HandlePlan);
            HFuncTraced(TEvDataShard::TEvProposeTransactionResult, HandlePlan);
            HFuncTraced(TEvPipeCache::TEvDeliveryProblem, HandlePlan);
            HFuncTraced(TEvents::TEvUndelivered, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleExecTimeout);
        }
    }

    void HandlePlan(TEvTxProxy::TEvProposeTransactionStatus::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();
        const auto& record = msg->Record;

        switch (msg->GetStatus()) {
        case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusAccepted:
            TxProxyMon->ClientTxStatusAccepted->Inc();
            // nop
            LOG_DEBUG_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
                "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
                << " HANDLE TEvProposeTransactionStatus TCommitWritesReq marker# P6 Status# " <<  msg->GetStatus());
            break;
        case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusProcessed:
            TxProxyMon->ClientTxStatusProcessed->Inc();
            // nop
            LOG_DEBUG_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
                "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
                << " HANDLE TEvProposeTransactionStatus TCommitWritesReq marker# P6 Status# " <<  msg->GetStatus());
            break;
        case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusConfirmed:
            TxProxyMon->ClientTxStatusConfirmed->Inc();
            // nop
            LOG_DEBUG_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
                "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
                << " HANDLE TEvProposeTransactionStatus TCommitWritesReq marker# P6 Status# " <<  msg->GetStatus());
            break;
        case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusPlanned:
            WallClockPlanned = Now();
            TxProxyMon->ClientTxStatusPlanned->Inc();
            PlanStep = record.GetStepId();
            // ok
            LOG_DEBUG_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
                "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
                << " HANDLE TEvProposeTransactionStatus TCommitWritesReq marker# P6 Status# " << msg->GetStatus());
            if (ProxyFlags & TEvTxUserProxy::TEvProposeTransaction::ProxyReportPlanned) {
                ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::CoordinatorPlanned, NKikimrIssues::TStatusIds::TRANSIENT, false, ctx);
            }
            break;
        case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusOutdated:
        case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusDeclined:
        case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusDeclinedNoSpace:
        case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusRestarting: // TODO: retry
            CancelProposal();
            // cancel proposal only for defined cases and fall through for generic error handling
            [[fallthrough]];
        default:
            TxProxyMon->ClientTxStatusCoordinatorDeclined->Inc();
            // smth goes wrong
            LOG_ERROR_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
                "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
                << " HANDLE TEvProposeTransactionStatus TCommitWritesReq marker# P6 Status# " << msg->GetStatus());
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::CoordinatorDeclined, NKikimrIssues::TStatusIds::REJECTED, true, ctx);
            return Die(ctx);
        }
    }

    void HandlePlan(TEvDataShard::TEvProposeTransactionResult::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();
        const auto& record = msg->Record;

        LOG_LOG_S_SAMPLED_BY(ctx,
            ((msg->GetStatus() == NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE ||
              msg->GetStatus() == NKikimrTxDataShard::TEvProposeTransactionResult::ABORTED ||
              msg->GetStatus() == NKikimrTxDataShard::TEvProposeTransactionResult::RESPONSE_DATA)
                ? NActors::NLog::PRI_DEBUG
                : NActors::NLog::PRI_ERROR),
            NKikimrServices::TX_PROXY, TxId,
            "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " HANDLE Plan TEvProposeTransactionResult TCommitWritesReq"
            << " GetStatus# " << msg->GetStatus()
            << " shard id " << msg->GetOrigin()
            << " marker# P7");

        if (!PlanStep) {
            PlanStep = record.GetStep();
        }

        if (record.HasExecLatency())
            ElapsedExecExec = Max<TDuration>(ElapsedExecExec, TDuration::MilliSeconds(record.GetExecLatency()));
        if (record.HasProposeLatency())
            ElapsedExecComplete = Max<TDuration>(ElapsedExecComplete, TDuration::MilliSeconds(record.GetProposeLatency()));

        switch (msg->GetStatus()) {
        case NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE:
            TxProxyMon->PlanClientTxResultComplete->Inc();
            return MergeResult(ev, ctx);
        case NKikimrTxDataShard::TEvProposeTransactionResult::ABORTED:
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecAborted, NKikimrIssues::TStatusIds::SUCCESS, true, ctx);
            TxProxyMon->PlanClientTxResultAborted->Inc();
            return Die(ctx);
        case NKikimrTxDataShard::TEvProposeTransactionResult::RESULT_UNAVAILABLE:
            ExtractDatashardErrors(record);
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecResultUnavailable, NKikimrIssues::TStatusIds::ERROR, true, ctx);
            TxProxyMon->PlanClientTxResultResultUnavailable->Inc();
            return Die(ctx);
        case NKikimrTxDataShard::TEvProposeTransactionResult::CANCELLED:
            ExtractDatashardErrors(record);
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecCancelled, NKikimrIssues::TStatusIds::ERROR, true, ctx);
            TxProxyMon->PlanClientTxResultCancelled->Inc();
            return Die(ctx);
        default:
            ExtractDatashardErrors(record);
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError, NKikimrIssues::TStatusIds::ERROR, true, ctx);
            TxProxyMon->PlanClientTxResultExecError->Inc();
            return Die(ctx);
        }
    }

    void HandlePlan(TEvPipeCache::TEvDeliveryProblem::TPtr& ev, const TActorContext& ctx) {
        auto* msg = ev->Get();

        if (msg->TabletId == SelectedCoordinator) {
            if (msg->NotDelivered) {
                LOG_LOG_S_SAMPLED_BY(ctx, NActors::NLog::PRI_ERROR,
                    NKikimrServices::TX_PROXY, TxId,
                    "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
                    << " not delivered to coordinator"
                    << " coordinator id " << msg->TabletId << " marker# P8");

                const TString explanation = TStringBuilder()
                    << "tx failed to plan with txid#" << TxId;
                IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::TX_DECLINED_BY_COORDINATOR, explanation));
                ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::CoordinatorDeclined, NKikimrIssues::TStatusIds::REJECTED, true, ctx);
                TxProxyMon->PlanCoordinatorDeclined->Inc();
            } else {
                LOG_LOG_S_SAMPLED_BY(ctx, NActors::NLog::PRI_ERROR,
                    NKikimrServices::TX_PROXY, TxId,
                    "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
                    << " delivery problem to coordinator"
                    << " coordinator id " << msg->TabletId << " marker# P8b");

                const TString explanation = TStringBuilder()
                    << "tx state unknown, lost pipe with selected tx coordinator with txid#" << TxId;
                IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::TX_STATE_UNKNOWN, explanation));
                ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::CoordinatorUnknown, NKikimrIssues::TStatusIds::TIMEOUT, true, ctx);
                TxProxyMon->PlanClientDestroyed->Inc();
            }

            return Die(ctx);
        }

        if (PerShardStates.contains(msg->TabletId)) {
            LOG_LOG_S_SAMPLED_BY(ctx, NActors::NLog::PRI_ERROR,
                NKikimrServices::TX_PROXY, TxId,
                "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
                << " shard " << msg->TabletId
                << " lost pipe while waiting for reply");

            ComplainingDatashards.push_back(msg->TabletId);

            const TString explanation = TStringBuilder()
                << "tx state unknown for shard " << msg->TabletId << " with txid#" << TxId;
            IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::TX_STATE_UNKNOWN, explanation));
            ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardUnknown, NKikimrIssues::TStatusIds::TIMEOUT, true, ctx);
            TxProxyMon->ClientConnectedError->Inc();

            return Die(ctx);
        }

        LOG_LOG_S_SAMPLED_BY(ctx, NActors::NLog::PRI_ERROR,
            NKikimrServices::TX_PROXY, TxId,
            "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
            << " lost pipe with unknown endpoint, ignoring");
    }

    void MergeResult(TEvDataShard::TEvProposeTransactionResult::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;

        ResultsReceivedCount++;

        const ui64 shardId = record.GetOrigin();
        auto& state = PerShardStates.at(shardId);

        state.Status = TPerShardState::EStatus::Complete;

        if (ResultsReceivedCount != PerShardStates.size()) {
            // We must wait for more results
            return;
        }

        ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete, NKikimrIssues::TStatusIds::SUCCESS, true, ctx);

        return Die(ctx);
    }

private:
    ui64 SelectCoordinator(const NSchemeCache::TDomainInfo::TPtr& domainInfo, const TActorContext& ctx) {
        if (domainInfo) {
            return domainInfo->Coordinators.Select(TxId);
        }

        // no tablets keys are found in requests keys
        // it take place when a transaction have only checks locks
        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                    "Actor# " << ctx.SelfID.ToString() <<
                    " txid# " << TxId <<
                    " SelectCoordinator unable to choose coordinator from resolved keys," <<
                    " will try to pick it from TEvProposeTransactionResult from datashard");
        return 0;
    }

    void CancelProposal(ui64 exceptShard = 0) {
        for (const auto& kv : PerShardStates) {
            ui64 shardId = kv.first;
            const auto& state = kv.second;
            if (shardId != exceptShard && (
                    state.Status == TPerShardState::EStatus::Wait ||
                    state.Status == TPerShardState::EStatus::Prepared))
            {
                Send(Services.LeaderPipeCache, new TEvPipeCache::TEvForward(
                    new TEvDataShard::TEvCancelTransactionProposal(TxId),
                    shardId, false));
            }
        }
    }

    void ExtractDatashardErrors(const NKikimrTxDataShard::TEvProposeTransactionResult& record) {
        TStringBuilder builder;
        for (const auto &er : record.GetError()) {
            builder << "[" << er.GetKind() << "] " << er.GetReason() << Endl;
        }

        DatashardErrors = builder;
        ComplainingDatashards.push_back(record.GetOrigin());
    }

    void TryToInvalidateTable(TTableId tableId, const TActorContext& ctx) {
        const bool notYetInvalidated = InvalidatedTables.insert(tableId).second;
        if (notYetInvalidated) {
            ctx.Send(Services.SchemeCache, new TEvTxProxySchemeCache::TEvInvalidateTable(tableId, TActorId()));
        }
    }

    void MarkShardPrepareError(ui64 shardId, TPerShardState& state, bool invalidateDistCache, const TActorContext& ctx) {
        if (state.Status != TPerShardState::EStatus::Wait) {
            return;
        }

        state.Status = TPerShardState::EStatus::Error;

        if (invalidateDistCache) {
            for (const auto& tableId : state.Tables) {
                TryToInvalidateTable(tableId, ctx);
            }
        }

        Y_UNUSED(shardId);

        ++TabletErrors;
        Y_DEBUG_ABORT_UNLESS(TabletsToPrepare > 0);
        if (!--TabletsToPrepare) {
            LOG_ERROR_S_SAMPLED_BY(ctx, NKikimrServices::TX_PROXY, TxId,
                "Actor# " << ctx.SelfID.ToString() << " txid# " << TxId
                << " invalidateDistCache: " << invalidateDistCache
                << " DIE TCommitWritesReq MarkShardPrepareError TabletErrors# " << TabletErrors);
            TxProxyMon->MarkShardError->Inc();
            return Die(ctx);
        }
    }

private:
    void ReportStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus status, NKikimrIssues::TStatusIds::EStatusCode code, bool reportIssues, const TActorContext& ctx) {
        auto x = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>(status);
        x->Record.SetTxId(TxId);

        if (reportIssues && IssueManager.GetIssues()) {
            IssuesToMessage(IssueManager.GetIssues(), x->Record.MutableIssues());
            IssueManager.Reset();
        }

        x->Record.SetStatusCode(code);

        for (auto& unresolvedKey : UnresolvedKeys) {
            x->Record.AddUnresolvedKeys(unresolvedKey);
        }

        if (PlanStep) {
            x->Record.SetStep(PlanStep);
        }

        if (!DatashardErrors.empty()) {
            x->Record.SetDataShardErrors(DatashardErrors);
        }

        if (const ui32 cs = ComplainingDatashards.size()) {
            x->Record.MutableComplainingDataShards()->Reserve(cs);
            for (auto ds : ComplainingDatashards) {
                x->Record.AddComplainingDataShards(ds);
            }
        }

        if (ProxyFlags & TEvTxUserProxy::TEvProposeTransaction::ProxyTrackWallClock) {
            auto* timings = x->Record.MutableTimings();
            if (WallClockAccepted)
                timings->SetWallClockAccepted(WallClockAccepted.MicroSeconds());
            if (WallClockResolved)
                timings->SetWallClockResolved(WallClockResolved.MicroSeconds());
            if (WallClockPrepared)
                timings->SetWallClockPrepared(WallClockPrepared.MicroSeconds());
            if (WallClockPlanned)
                timings->SetWallClockPlanned(WallClockPlanned.MicroSeconds());
            if (ElapsedExecExec)
                timings->SetElapsedExecExec(ElapsedExecExec.MicroSeconds());
            if (ElapsedExecComplete)
                timings->SetElapsedExecComplete(ElapsedExecComplete.MicroSeconds());
            if (ElapsedPrepareExec)
                timings->SetElapsedPrepareExec(ElapsedExecExec.MicroSeconds());
            if (ElapsedPrepareComplete)
                timings->SetElapsedPrepareComplete(ElapsedExecComplete.MicroSeconds());
            timings->SetWallClockNow(Now().MicroSeconds());
        }

        (*TxProxyMon->ResultsReceivedCount) += ResultsReceivedCount;

        // TODO: status counters?

        ctx.Send(Sender, x.Release(), 0, Cookie);
    }

private:
    const TTxProxyServices& Services;
    const ui64 TxId;
    const TActorId Sender;
    const ui64 Cookie;
    THolder<TEvTxUserProxy::TEvProposeTransaction> Request;
    const TIntrusivePtr<TTxProxyMon> TxProxyMon;

    TControlWrapper DefaultTimeoutMs;

    TInstant WallClockAccepted;
    TInstant WallClockResolveStarted;
    TInstant WallClockResolved;
    TInstant WallClockPrepared;
    TInstant WallClockPlanned;

    TDuration ElapsedPrepareExec;
    TDuration ElapsedPrepareComplete;
    TDuration ElapsedExecExec;
    TDuration ElapsedExecComplete;

    ui64 ProxyFlags = 0;
    TDuration ExecTimeoutPeriod;
    TSchedulerCookieHolder ExecTimeoutCookieHolder;

    ui64 TxFlags = 0;
    ui64 SelectedCoordinator = 0;
    THolder<const NACLib::TUserToken> UserToken;

    TActorId ResolveActorID;
    TTablePathHashSet InvalidatedTables;
    TMap<ui64, TPerShardState> PerShardStates;
    size_t TabletsToPrepare = 0;
    size_t TabletErrors = 0;
    size_t ResultsReceivedCount = 0;

    ui64 PlanStep = 0;
    ui64 AggrMinStep = 0;
    ui64 AggrMaxStep = Max<ui64>();

    TString DatashardErrors;
    TVector<ui64> ComplainingDatashards;
    TVector<TString> UnresolvedKeys;
    NYql::TIssueManager IssueManager;
};


////////////////////////////////////////////////////////////////////////////////

IActor* CreateTxProxyCommitWritesReq(const TTxProxyServices& services, const ui64 txid, TEvTxUserProxy::TEvProposeTransaction::TPtr&& ev, const TIntrusivePtr<TTxProxyMon>& mon) {
    const auto& record = ev->Get()->Record;
    Y_ABORT_UNLESS(record.HasTransaction());
    const auto& tx = record.GetTransaction();

    if (tx.HasCommitWrites()) {
        return new TCommitWritesReq(services, txid, std::move(ev), mon);
    }

    Y_ABORT("Unexpected transaction proposal");
}


} // namespace NTxProxy
} // namespace NKikimr
