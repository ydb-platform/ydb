#include "long_tx_service_impl.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/random/random.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::LONG_TX_SERVICE

namespace NKikimr {
namespace NLongTxService {

    class TLongTxServiceActor::TAcquireSnapshotActor : public TActorBootstrapped<TAcquireSnapshotActor> {
    public:
        TAcquireSnapshotActor(const TActorId& parent, ui64 cookie, const TString& databaseName, NWilson::TTraceId traceId)
            : Parent(parent)
            , Cookie(cookie)
            , DatabaseName(databaseName)
            , SchemeCache(MakeSchemeCacheID())
            , LeaderPipeCache(MakePipePerNodeCacheID(false))
            , LogPrefix("LongTxService.AcquireSnapshot ")
            , Span(10, traceId, "Acquire snapshot attempt", NWilson::EFlags::AUTO_END)
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
            for (auto& [coordinator, wait] : CoordinatorSpans) {
                if (wait) {
                    wait.Attribute("ydb.snapshot.wait_outcome", TString("superseded"));
                    wait.End();
                }
            }
            TActor::PassAway();
        }

    private:
        void SendNavigateRequest() {
            NavigateSpan = Span.CreateChild(10, "Resolve snapshot coordinators", NWilson::EFlags::AUTO_END);
            YDB_LOG_DEBUG("Sending navigate request",
                {"logPrefix", LogPrefix},
                {"databaseName", DatabaseName});
            auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
            request->DatabaseName = DatabaseName;
            auto& entry = request->ResultSet.emplace_back();
            entry.Path = ::NKikimr::SplitPath(DatabaseName);
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
            Send(SchemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()),
                IEventHandle::FlagTrackDelivery, 0, NavigateSpan.GetTraceId());
            Become(&TThis::StateNavigate);
        }

        STFUNC(StateNavigate) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvents::TEvUndelivered, Handle);
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            }
        }

        void Handle(TEvents::TEvUndelivered::TPtr& ev) {
            YDB_LOG_DEBUG("Received undelivered",
                {"logPrefix", LogPrefix},
                {"sender", ev->Sender});
            ReplyError(Ydb::StatusIds::UNAVAILABLE, "An essential service is unavailable");
        }

        void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
            NSchemeCache::TSchemeCacheNavigate* resp = ev->Get()->Request.Get();
            NavigateSpan.Attribute("ydb.resolve.status", static_cast<i64>(resp->ResultSet.at(0).Status));
            if (resp->ResultSet.at(0).Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                NavigateSpan.EndOk();
            } else {
                NavigateSpan.EndError("snapshot coordinator resolution failed");
            }

            const auto& entry = resp->ResultSet.at(0);
            YDB_LOG_DEBUG("Received navigate response status",
                {"logPrefix", LogPrefix},
                {"status", entry.Status});

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
                YDB_LOG_DEBUG("Sending acquire step to coordinator",
                    {"logPrefix", LogPrefix},
                    {"coordinator", coordinator});
                NWilson::TTraceId trace = Span.GetTraceId();
                if (Span && trace.GetVerbosity() >= 11 && CoordinatorSpans.size() < 32) {
                    auto& wait = CoordinatorSpans[coordinator];
                    wait = Span.CreateChild(11, "Wait for Coordinator read step", NWilson::EFlags::AUTO_END);
                    wait.Attribute("ydb.coordinator_id", static_cast<i64>(coordinator));
                    wait.Event("acquire step dispatched");
                    trace = wait.GetTraceId();
                } else if (Span && trace.GetVerbosity() >= 11) {
                    Span.Attribute("ydb.snapshot.coordinator_spans_truncated", true);
                }
                SendToTablet(coordinator, MakeHolder<TEvTxProxy::TEvAcquireReadStep>(coordinator), trace);
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

            YDB_LOG_DEBUG("Delivery problem",
                {"logPrefix", LogPrefix},
                {"tabletId", tabletId},
                {"notDelivered", msg->NotDelivered});

            if (auto it = CoordinatorSpans.find(tabletId); it != CoordinatorSpans.end() && it->second) {
                it->second.Attribute("ydb.delivery.not_delivered", msg->NotDelivered);
                it->second.EndError("delivery problem");
            }
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
            YDB_LOG_DEBUG("Received read step",
                {"logPrefix", LogPrefix},
                {"step", step});
            if (auto it = CoordinatorSpans.find(msg->Record.GetCoordinatorID()); it != CoordinatorSpans.end() && it->second) {
                it->second.Event("read step result received");
                it->second.Attribute("ydb.snapshot.step", static_cast<i64>(step));
                it->second.EndOk();
            }
            ReplySuccess(TRowVersion(step, Max<ui64>()));
        }

    private:
        void ReplySuccess(const TRowVersion& snapshot) {
            Span.Attribute("ydb.snapshot.step", static_cast<i64>(snapshot.Step));
            Send(Parent, new TEvPrivate::TEvAcquireSnapshotFinished(snapshot), 0, Cookie, Span.GetTraceId());
            Span.EndOk();
            PassAway();
        }

        void ReplyError(Ydb::StatusIds::StatusCode status, const TString& message) {
            YDB_LOG_DEBUG("Replying with error",
                {"logPrefix", LogPrefix},
                {"status", status},
                {"message", message});
            NYql::TIssues issues;
            issues.AddIssue(message);
            Send(Parent, new TEvPrivate::TEvAcquireSnapshotFinished(status, std::move(issues)), 0, Cookie, Span.GetTraceId());
            if (NavigateSpan) {
                NavigateSpan.EndError("snapshot coordinator resolution failed");
            }
            Span.Attribute("ydb.snapshot.status", static_cast<i64>(status));
            Span.EndError("snapshot acquisition failed");
            PassAway();
        }

    private:
        void SendToTablet(ui64 tabletId, THolder<IEventBase> event, const NWilson::TTraceId& traceId, bool subscribe = true) {
            Send(LeaderPipeCache, new TEvPipeCache::TEvForward(event.Release(), tabletId, subscribe),
                IEventHandle::FlagTrackDelivery, 0, traceId);
        }

    private:
        const TActorId Parent;
        const ui64 Cookie;
        const TString DatabaseName;
        const TActorId SchemeCache;
        const TActorId LeaderPipeCache;
        TString LogPrefix;
        NWilson::TSpan Span;
        NWilson::TSpan NavigateSpan;
        THashMap<ui64, NWilson::TSpan> CoordinatorSpans;
        THashSet<ui64> WaitingCoordinators;
        THashSet<ui64> BackupCoordinators;
    };

    void TLongTxServiceActor::StartAcquireSnapshotActor(const TString& databaseName, TDatabaseSnapshotState& state) {
        ui64 reqId = ++LastCookie;
        auto& req = AcquireSnapshotInFlight[reqId];
        req.DatabaseName = databaseName;
        req.UserRequests.swap(state.PendingUserRequests);
        req.BeginTxRequests.swap(state.PendingBeginTxRequests);
        state.ActiveRequests.insert(reqId);
        NWilson::TTraceId parent;
        for (const auto& user : req.UserRequests) {
            const auto trace = user.Span.GetTraceId();
            if (trace && (!parent || trace.GetVerbosity() > parent.GetVerbosity())) {
                parent = trace;
            }
        }
        req.Span = NWilson::TSpan(10, parent, "Acquire snapshot batch", NWilson::EFlags::AUTO_END);
        req.Span.Attribute("ydb.snapshot.batch_id", static_cast<i64>(reqId));
        req.Span.Attribute("ydb.snapshot.request_count", static_cast<i64>(req.UserRequests.size()));
        size_t links = 0;
        size_t tracedRequests = 0;
        for (auto& user : req.UserRequests) {
            if (!user.Span.GetTraceId()) {
                continue;
            }
            ++tracedRequests;
            user.Span.Link(req.Span.GetTraceId());
            if (links < 32) {
                req.Span.Link(user.Span.GetTraceId());
                ++links;
            }
        }
        req.Span.Attribute("ydb.snapshot.request_links_truncated", tracedRequests > links);
        StartAcquireSnapshotAttempt(reqId, databaseName);

        // Count the batch once, including any freshness retries it needs.
        if (Settings.Counters) {
            Settings.Counters->AcquireReadSnapshotOutRequests->Inc();
            Settings.Counters->AcquireReadSnapshotOutInFlight->Inc();
        }
    }

    void TLongTxServiceActor::StartAcquireSnapshotAttempt(ui64 requestId, const TString& databaseName) {
        auto& request = AcquireSnapshotInFlight.at(requestId);
        Register(new TAcquireSnapshotActor(SelfId(), requestId, databaseName, request.Span.GetTraceId()));
    }

} // namespace NLongTxService
} // namespace NKikimr
