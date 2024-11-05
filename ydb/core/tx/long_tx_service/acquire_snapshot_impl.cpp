#include "long_tx_service_impl.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/random/random.h>

#define TXLOG_LOG(priority, stream) \
    LOG_LOG_S(*TlsActivationContext, priority, NKikimrServices::LONG_TX_SERVICE, LogPrefix << stream)
#define TXLOG_DEBUG(stream) TXLOG_LOG(NActors::NLog::PRI_DEBUG, stream)
#define TXLOG_NOTICE(stream) TXLOG_LOG(NActors::NLog::PRI_NOTICE, stream)
#define TXLOG_ERROR(stream) TXLOG_LOG(NActors::NLog::PRI_ERROR, stream)

namespace NKikimr {
namespace NLongTxService {

    class TLongTxServiceActor::TAcquireSnapshotActor : public TActorBootstrapped<TAcquireSnapshotActor> {
    public:
        TAcquireSnapshotActor(const TActorId& parent, ui64 cookie, const TString& databaseName)
            : Parent(parent)
            , Cookie(cookie)
            , DatabaseName(databaseName)
            , SchemeCache(MakeSchemeCacheID())
            , LeaderPipeCache(MakePipePerNodeCacheID(false))
            , LogPrefix("LongTxService.AcquireSnapshot ")
        { }

        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::LONG_TX_SERVICE_ACQUIRE_SNAPSHOT;
        }

        void Bootstrap() {
            LogPrefix = TStringBuilder() << LogPrefix << SelfId() << " ";
            SendNavigateRequest();
        }

    private:
        void PassAway() override {
            Send(LeaderPipeCache, new TEvPipeCache::TEvUnlink(0));
            TActor::PassAway();
        }

    private:
        void SendNavigateRequest() {
            TXLOG_DEBUG("Sending navigate request for " << DatabaseName);
            auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
            request->DatabaseName = DatabaseName;
            auto& entry = request->ResultSet.emplace_back();
            entry.Path = ::NKikimr::SplitPath(DatabaseName);
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
            Send(SchemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()),
                IEventHandle::FlagTrackDelivery);
            Become(&TThis::StateNavigate);
        }

        STFUNC(StateNavigate) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvents::TEvUndelivered, Handle);
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            }
        }

        void Handle(TEvents::TEvUndelivered::TPtr& ev) {
            TXLOG_DEBUG("Received undelivered from " << ev->Sender);
            ReplyError(Ydb::StatusIds::UNAVAILABLE, "An essential service is unavailable");
        }

        void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
            NSchemeCache::TSchemeCacheNavigate* resp = ev->Get()->Request.Get();

            const auto& entry = resp->ResultSet.at(0);
            TXLOG_DEBUG("Received navigate response status " << entry.Status);

            switch (entry.Status) {
                case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
                    break;

                // Some quite rare errors, but we can`t show details for user
                case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotTable:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotPath:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::TableCreationNotComplete:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::Unknown:
                    // fall through

                // Database doesn't exist
                case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
                // Something /garbageroot
                case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
                    return ReplyError(Ydb::StatusIds::SCHEME_ERROR, TStringBuilder()
                        << "The specified database '" << DatabaseName << "' does not exist");

                case NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError:
                    return ReplyError(Ydb::StatusIds::UNAVAILABLE, "Schema service unavailable");
                
                case NSchemeCache::TSchemeCacheNavigate::EStatus::AccessDenied:
                    return ReplyError(Ydb::StatusIds::UNAUTHORIZED, "Access denied");
            }

            // FIXME: make sure we actually resolved a database, and not something else
            if (!entry.DomainInfo) {
                return ReplyError(Ydb::StatusIds::INTERNAL_ERROR, "Missing domain info for a resolved database");
            }

            const TVector<ui64>& coordinators = entry.DomainInfo->Coordinators.List();
            if (coordinators.empty()) {
                return ReplyError(Ydb::StatusIds::UNAVAILABLE, TStringBuilder()
                    << "The specified database '" << DatabaseName << "' has no coordinators");
            }

            // Prefer a single random coordinator for each request
            ui64 primary = coordinators[RandomNumber<ui64>(coordinators.size())];
            for (ui64 coordinator : coordinators) {
                if (coordinator == primary) {
                    SendAcquireStep(coordinator);
                } else {
                    BackupCoordinators.insert(coordinator);
                }
            }

            Become(&TThis::StateWaitStep);
        }

        void SendAcquireStep(ui64 coordinator) {
            if (WaitingCoordinators.insert(coordinator).second) {
                TXLOG_DEBUG("Sending acquire step to coordinator " << coordinator);
                SendToTablet(coordinator, MakeHolder<TEvTxProxy::TEvAcquireReadStep>(coordinator));
            }
        }

        STFUNC(StateWaitStep) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvents::TEvUndelivered, Handle);
                hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
                hFunc(TEvTxProxy::TEvAcquireReadStepResult, Handle);
            }
        }

        void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
            const auto* msg = ev->Get();
            const ui64 tabletId = msg->TabletId;

            TXLOG_DEBUG("Delivery problem"
                << " TabletId# " << tabletId
                << " NotDelivered# " << msg->NotDelivered);

            WaitingCoordinators.erase(tabletId);
            if (WaitingCoordinators.empty() && !BackupCoordinators.empty()) {
                for (ui64 coordinator : BackupCoordinators) {
                    SendAcquireStep(coordinator);
                }
                BackupCoordinators.clear();
            }
            if (WaitingCoordinators.empty()) {
                return ReplyError(Ydb::StatusIds::UNAVAILABLE, "Database coordinators are unavailable");
            }
        }

        void Handle(TEvTxProxy::TEvAcquireReadStepResult::TPtr& ev) {
            const auto* msg = ev->Get();
            const ui64 step = msg->Record.GetStep();
            TXLOG_DEBUG("Received read step " << step);
            ReplySuccess(TRowVersion(step, Max<ui64>()));
        }

    private:
        void ReplySuccess(const TRowVersion& snapshot) {
            Send(Parent, new TEvPrivate::TEvAcquireSnapshotFinished(snapshot), 0, Cookie);
            PassAway();
        }

        void ReplyError(Ydb::StatusIds::StatusCode status, const TString& message) {
            TXLOG_DEBUG("Replying with error " << status << ": " << message);
            NYql::TIssues issues;
            issues.AddIssue(message);
            Send(Parent, new TEvPrivate::TEvAcquireSnapshotFinished(status, std::move(issues)), 0, Cookie);
            PassAway();
        }

    private:
        void SendToTablet(ui64 tabletId, THolder<IEventBase> event, bool subscribe = true) {
            Send(LeaderPipeCache, new TEvPipeCache::TEvForward(event.Release(), tabletId, subscribe),
                IEventHandle::FlagTrackDelivery);
        }

    private:
        const TActorId Parent;
        const ui64 Cookie;
        const TString DatabaseName;
        const TActorId SchemeCache;
        const TActorId LeaderPipeCache;
        TString LogPrefix;
        THashSet<ui64> WaitingCoordinators;
        THashSet<ui64> BackupCoordinators;
    };

    void TLongTxServiceActor::StartAcquireSnapshotActor(const TString& databaseName, TDatabaseSnapshotState& state) {
        ui64 reqId = ++LastCookie;
        auto& req = AcquireSnapshotInFlight[reqId];
        req.DatabaseName = databaseName;
        req.UserRequests.swap(state.PendingUserRequests);
        req.BeginTxRequests.swap(state.PendingBeginTxRequests);
        Register(new TAcquireSnapshotActor(SelfId(), reqId, databaseName));
        state.ActiveRequests.insert(reqId);

        if (Settings.Counters) {
            Settings.Counters->AcquireReadSnapshotOutRequests->Inc();
            Settings.Counters->AcquireReadSnapshotOutInFlight->Inc();
        }
    }

} // namespace NLongTxService
} // namespace NKikimr
