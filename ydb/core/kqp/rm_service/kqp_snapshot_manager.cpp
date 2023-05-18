#include "kqp_snapshot_manager.h"

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/actorlib_impl/long_timer.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

namespace NKikimr {
namespace NKqp {

#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_RESOURCE_MANAGER, stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_RESOURCE_MANAGER, stream)

namespace {

class TSnapshotManagerActor: public TActorBootstrapped<TSnapshotManagerActor> {
public:
    TSnapshotManagerActor(const TString& database, TDuration queryTimeout)
        : Database(database)
        , RequestTimeout(queryTimeout)
    {}

    void Bootstrap() {
        auto *ev = new IEventHandle(this->SelfId(), this->SelfId(), new TEvents::TEvPoison());
        RequestTimeoutCookieHolder_.Reset(ISchedulerCookie::Make2Way());
        CreateLongTimer(TlsActivationContext->AsActorContext(), RequestTimeout, ev, 0, RequestTimeoutCookieHolder_.Get());

        LOG_D("Start KqpSnapshotManager at " << SelfId());

        Become(&TThis::StateAwaitRequest);
    }

private:
    STATEFN(StateAwaitRequest) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKqpSnapshot::TEvCreateSnapshotRequest, Handle);
            default:
                HandleUnexpectedEvent("AwaitRequest", ev->GetTypeRewrite());
        }
    }

    void Handle(TEvKqpSnapshot::TEvCreateSnapshotRequest::TPtr& ev) {
        ClientActorId = ev->Sender;
        Tables = ev->Get()->Tables;
        MvccSnapshot = ev->Get()->MvccSnapshot;
        Orbit = std::move(ev->Get()->Orbit);

        LOG_D("KqpSnapshotManager: got snapshot request from " << ClientActorId);

        if (MvccSnapshot) {
            auto longTxService = NLongTxService::MakeLongTxServiceID(SelfId().NodeId());
            Send(longTxService, new NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot(Database, std::move(Orbit)));

            Become(&TThis::StateAwaitAcquireResult);
        } else {
            auto req = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
            req->Record.SetExecTimeoutPeriod(RequestTimeout.MilliSeconds());
            req->Record.SetDatabaseName(Database);
            auto* createSnapshot = req->Record.MutableTransaction()->MutableCreateVolatileSnapshot();
            for (const TString& tablePath : Tables) {
                createSnapshot->AddTables()->SetTablePath(tablePath);
            }
            createSnapshot->SetTimeoutMs(SnapshotTimeout.MilliSeconds());
            createSnapshot->SetIgnoreSystemViews(true);

            Send(MakeTxProxyID(), req.Release());
            Become(&TThis::StateAwaitCreation);
        }
    }

    STATEFN(StateAwaitAcquireResult) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult, Handle);
            hFunc(TEvents::TEvPoison, HandlePoison);
            default:
                HandleUnexpectedEvent("AwaitAcquireResult", ev->GetTypeRewrite());
        }
    }

    STATEFN(StateAwaitCreation) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
            hFunc(TEvents::TEvPoison, HandlePoison);
            default:
                HandleUnexpectedEvent("AwaitCreation", ev->GetTypeRewrite());
        }
    }

    void Handle(NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult::TPtr& ev) {
        Y_VERIFY(MvccSnapshot);
        Y_VERIFY(Tables.empty());
        Orbit = std::move(ev->Get()->Orbit);

        const auto& record = ev->Get()->Record;
        if (record.GetStatus() == Ydb::StatusIds::SUCCESS) {
            Snapshot = IKqpGateway::TKqpSnapshot(record.GetSnapshotStep(), record.GetSnapshotTxId());

            LOG_D("KqpSnapshotManager: snapshot " << Snapshot.Step << ":" << Snapshot.TxId << " acquired");

            bool sent = Send(ClientActorId, new TEvKqpSnapshot::TEvCreateSnapshotResponse(
                    Snapshot, NKikimrIssues::TStatusIds::SUCCESS, /* issues */ {}, std::move(Orbit)));
            Y_VERIFY_DEBUG(sent);

            PassAway();
        } else {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(record.GetIssues(), issues);
            LOG_E("KqpSnapshotManager: CreateSnapshot got unexpected status="
                      << record.GetStatus() << ", issues:" << issues.ToString());
            ReplyErrorAndDie(NKikimrIssues::TStatusIds::ERROR, std::move(issues));
        }
    }

    void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        Y_VERIFY(!MvccSnapshot);

        using EStatus = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus;

        const auto* msg = ev->Get();
        const auto status = static_cast<EStatus>(msg->Record.GetStatus());

        if (status == EStatus::ExecComplete && msg->Record.GetStatusCode() == NKikimrIssues::TStatusIds::SUCCESS) {
            Snapshot = IKqpGateway::TKqpSnapshot(msg->Record.GetStep(), msg->Record.GetTxId());

            LOG_D("KqpSnapshotManager: snapshot " << Snapshot.Step << ":" << Snapshot.TxId << " created");

            bool sent = Send(ClientActorId, new TEvKqpSnapshot::TEvCreateSnapshotResponse(
                Snapshot, NKikimrIssues::TStatusIds::SUCCESS, /* issues */ {}, std::move(Orbit)));
            Y_VERIFY_DEBUG(sent);

            Become(&TThis::StateRefreshing);
            ScheduleRefresh();
        } else {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(msg->Record.GetIssues(), issues);

            LOG_E("KqpSnapshotManager: CreateSnapshot got unexpected status " << status << ": " << issues.ToString());
            ReplyErrorAndDie(msg->Record.GetStatusCode(), std::move(issues));
        }
    }

    STATEFN(StateRefreshing) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvWakeup, HandleRefreshTimeout);
            hFunc(TEvTxUserProxy::TEvProposeTransactionStatus, HandleRefreshStatus);
            hFunc(TEvKqpSnapshot::TEvDiscardSnapshot, HandleDiscardRequest);
            hFunc(TEvents::TEvPoison, HandlePoison);
            default:
                HandleUnexpectedEvent("Refreshing", ev->GetTypeRewrite());
        }
    }

    void HandleRefreshTimeout(TEvents::TEvWakeup::TPtr&) {
        auto req = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        req->Record.SetExecTimeoutPeriod(RequestTimeout.MilliSeconds());
        auto* refreshSnapshot = req->Record.MutableTransaction()->MutableRefreshVolatileSnapshot();
        for (const TString& tablePath : Tables) {
            refreshSnapshot->AddTables()->SetTablePath(tablePath);
        }
        refreshSnapshot->SetIgnoreSystemViews(true);
        refreshSnapshot->SetSnapshotStep(Snapshot.Step);
        refreshSnapshot->SetSnapshotTxId(Snapshot.TxId);

        LOG_D("KqpSnapshotManager: refreshing snapshot");

        Send(MakeTxProxyID(), req.Release());
        ScheduleRefresh();
    }

    void HandleRefreshStatus(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        using EStatus = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus;

        const auto* msg = ev->Get();
        const auto status = static_cast<EStatus>(msg->Record.GetStatus());
        if (status != EStatus::ExecComplete || msg->Record.GetStatusCode() != NKikimrIssues::TStatusIds::SUCCESS) {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(msg->Record.GetIssues(), issues);

            LOG_E("KqpSnapshotManager: RefreshSnapshot got unexpected status=" << status
                << ", issues:" << issues.ToString());
            ReplyErrorAndDie(msg->Record.GetStatusCode(), std::move(issues));
        }
    }

    void HandleDiscardRequest(TEvKqpSnapshot::TEvDiscardSnapshot::TPtr& ev) {
        const auto* msg = ev->Get();
        Y_ASSERT(msg->Snapshot == Snapshot);
        LOG_D("KqpSnapshotManager: discarding snapshot; shutting down");
        SendDiscard();
        PassAway();
    }

    void HandlePoison(TEvents::TEvPoison::TPtr&) {
        LOG_D("KqpSnapshotManager: shutting down on timeout");
        ReplyErrorAndDie(NKikimrIssues::TStatusIds::TIMEOUT, {});
    }

private:
    void SendDiscard() {
        auto req = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        req->Record.SetExecTimeoutPeriod(RequestTimeout.MilliSeconds());
        auto* discardSnapshot = req->Record.MutableTransaction()->MutableDiscardVolatileSnapshot();
        for (const TString& tablePath : Tables) {
            discardSnapshot->AddTables()->SetTablePath(tablePath);
        }
        discardSnapshot->SetIgnoreSystemViews(true);
        discardSnapshot->SetSnapshotStep(Snapshot.Step);
        discardSnapshot->SetSnapshotTxId(Snapshot.TxId);

        Send(MakeTxProxyID(), req.Release());
    }

    void ScheduleRefresh() {
        Schedule(RefreshInterval, new TEvents::TEvWakeup());
    }

    void HandleUnexpectedEvent(const TString& state, ui32 eventType) {
        LOG_E("KqpSnapshotManager: unexpected event, state: " << state
            << ", event type: " << eventType);
        ReplyErrorAndDie(NKikimrIssues::TStatusIds::INTERNAL_ERROR, {});
    }

    void ReplyErrorAndDie(NKikimrIssues::TStatusIds::EStatusCode status, NYql::TIssues&& issues) {
        if (CurrentStateFunc() == &TThis::StateAwaitCreation || CurrentStateFunc() == &TThis::StateAwaitAcquireResult) {
            Send(ClientActorId, new TEvKqpSnapshot::TEvCreateSnapshotResponse(
                IKqpGateway::TKqpSnapshot::InvalidSnapshot, status, std::move(issues), std::move(Orbit)));
        } else {
            SendDiscard();
        }
        PassAway();
    }

private:
    const TString Database;
    TVector<TString> Tables;
    TActorId ClientActorId;
    IKqpGateway::TKqpSnapshot Snapshot;
    NLWTrace::TOrbit Orbit;

    bool MvccSnapshot = false;

    TSchedulerCookieHolder RequestTimeoutCookieHolder_;

    const double SnapshotToRequestTimeoutRatio = 1.5;
    const double RefreshToRequestTimeoutRatio = 0.5;
    const TDuration MaxRefreshDuration = TDuration::Seconds(10);

    TDuration RequestTimeout;
    TDuration SnapshotTimeout = RequestTimeout * SnapshotToRequestTimeoutRatio;
    TDuration RefreshInterval = Min(RequestTimeout * RefreshToRequestTimeoutRatio, MaxRefreshDuration);
};

} // anonymous namespace

IActor* CreateKqpSnapshotManager(const TString& database, TDuration queryTimeout) {
    return new TSnapshotManagerActor(database, queryTimeout);
}

} // namespace NKqp
} // namespace NKikimr
