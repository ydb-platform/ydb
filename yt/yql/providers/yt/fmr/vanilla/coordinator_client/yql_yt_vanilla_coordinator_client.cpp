#include "yql_yt_vanilla_coordinator_client.h"

#include <yt/yql/providers/yt/fmr/coordinator/client/yql_yt_coordinator_client.h>
#include <yt/yql/providers/yt/fmr/vanilla/peer_tracker/yql_yt_vanilla_peer_tracker.h>

#include <yt/cpp/mapreduce/client/client.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/operation.h>

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/log/log_component.h>

#include <util/datetime/base.h>
#include <util/generic/guid.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/system/condvar.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>
#include <util/stream/str.h>

namespace NYql::NFmr {

////////////////////////////////////////////////////////////////////////////////

namespace {

class TVanillaFmrCoordinatorClient : public IFmrCoordinator {
public:
    explicit TVanillaFmrCoordinatorClient(TVanillaFmrCoordinatorClientSettings settings)
        : Settings_(std::move(settings))
    {
        auto logCtx = NYql::NLog::CurrentLogContextPath();
        BackgroundThread_ = MakeHolder<TThread>([this, logCtx]() {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);
            BackgroundLoop();
        });
        BackgroundThread_->Start();
    }

    ~TVanillaFmrCoordinatorClient() {
        // Set Shutdown_ and capture the internally-owned tracker atomically.
        // PeerTracker_ is always set under Mutex_ before the refresh loop starts, so if
        // Refresh() is currently blocking in GetPeerAddress(0), the tracker is visible here.
        // The external tracker (ExternalPeerTracker_) is not owned and is never stopped.
        TIntrusivePtr<TVanillaExternalPeerTracker> ownedTracker;
        {
            TGuard guard(Mutex_);
            Shutdown_ = true;
            ownedTracker = PeerTracker_;
            PeerTracker_ = nullptr;
        }
        Cv_.BroadCast();

        if (ownedTracker) {
            // Unblocks any WaitForPeers blocking call inside Refresh().
            ownedTracker->Stop();
        }

        BackgroundThread_->Join();
    }

    // OpenSession returns immediately.
    // — If the session is known to have failed, rethrow.
    // — If Inner_ is ready, delegate.
    // — Otherwise return a synchronous OK and track the session as "early-opened".
    NThreading::TFuture<TOpenSessionResponse> OpenSession(const TOpenSessionRequest& req) override {
        IFmrCoordinator::TPtr inner;
        {
            TGuard guard(Mutex_);
            if (Inner_) {
                inner = Inner_;
            } else {
                EarlyOpenSessions_.insert(req.SessionId);
                return NThreading::MakeFuture(TOpenSessionResponse{});
            }
        }
        return inner->OpenSession(req);
    }

    // ClearSession clears any recorded failure for the session and delegates if the
    // coordinator is reachable. It never cancels background initialization work.
    NThreading::TFuture<void> ClearSession(const TClearSessionRequest& req) override {
        IFmrCoordinator::TPtr inner;
        {
            TGuard guard(Mutex_);
            FailedSessions_.erase(req.SessionId);
            EarlyOpenSessions_.erase(req.SessionId);
            inner = Inner_;
        }
        if (!inner) {
            return NThreading::MakeFuture();
        }
        return inner->ClearSession(req);
    }

    NThreading::TFuture<TStartOperationResponse> StartOperation(const TStartOperationRequest& req) override {
        ThrowIfSessionFailedUnlocked(req.SessionId);
        return GetInner()->StartOperation(req);
    }
    NThreading::TFuture<TGetOperationResponse> GetOperation(const TGetOperationRequest& req) override {
        return GetInner()->GetOperation(req);
    }
    NThreading::TFuture<TDeleteOperationResponse> DeleteOperation(const TDeleteOperationRequest& req) override {
        return GetInner()->DeleteOperation(req);
    }
    NThreading::TFuture<TDropTablesResponse> DropTables(const TDropTablesRequest& req) override {
        ThrowIfSessionFailedUnlocked(req.SessionId);
        return GetInner()->DropTables(req);
    }
    NThreading::TFuture<THeartbeatResponse> SendHeartbeatResponse(const THeartbeatRequest& req) override {
        return GetInner()->SendHeartbeatResponse(req);
    }
    NThreading::TFuture<TGetFmrTableInfoResponse> GetFmrTableInfo(const TGetFmrTableInfoRequest& req) override {
        ThrowIfSessionFailedUnlocked(req.SessionId);
        return GetInner()->GetFmrTableInfo(req);
    }
    NThreading::TFuture<TPingSessionResponse> PingSession(const TPingSessionRequest& req) override {
        ThrowIfSessionFailedUnlocked(req.SessionId);
        return GetInner()->PingSession(req);
    }
    NThreading::TFuture<TListSessionsResponse> ListSessions(const TListSessionsRequest& req) override {
        return GetInner()->ListSessions(req);
    }
    NThreading::TFuture<TPrepareOperationResponse> PrepareOperation(const TPrepareOperationRequest& req) override {
        return GetInner()->PrepareOperation(req);
    }
    NThreading::TFuture<TWaitForOperationsResponse> WaitForOperations(const TWaitForOperationsRequest& req) override {
        return GetInner()->WaitForOperations(req);
    }
    NThreading::TFuture<TWaitForTasksResponse> WaitForTasks(const TWaitForTasksRequest& req) override {
        return GetInner()->WaitForTasks(req);
    }

private:
    // Must be called with Mutex_ held.
    void ThrowIfSessionFailed(const TString& sessionId) const {
        auto it = FailedSessions_.find(sessionId);
        if (it != FailedSessions_.end()) {
            std::rethrow_exception(it->second);
        }
    }

    // Acquires Mutex_ itself — use from methods that don't already hold it.
    void ThrowIfSessionFailedUnlocked(const TString& sessionId) const {
        TGuard guard(Mutex_);
        ThrowIfSessionFailed(sessionId);
    }

    IFmrCoordinator::TPtr GetInner() const {
        TGuard guard(Mutex_);
        Cv_.WaitI(Mutex_, [this] { return LastError_ || Inner_ != nullptr || Shutdown_; });
        if (Inner_) {
            return Inner_;
        }
        Y_ENSURE(!LastError_, "TVanillaFmrCoordinatorClient fatal error: " + *LastError_);
        ythrow yexception() << "TVanillaFmrCoordinatorClient is shutting down";
    }

    TString ResolveAlias(const TString& aliasWithStar) {
        YQL_CLOG(DEBUG, FastMapReduce) << "resolving alias " << aliasWithStar;
        const auto t0 = TInstant::Now();
        NYT::TCreateClientOptions clientOptions;
        if (Settings_.Token) {
            clientOptions.Token(*Settings_.Token);
        }
        auto client = NYT::CreateClient(Settings_.Cluster, clientOptions);
        auto attrs = client->GetOperation(aliasWithStar);
        Y_ENSURE(attrs.Id.Defined(), "GetOperation returned no ID for alias " << aliasWithStar);
        TString operationId = GetGuidAsString(*attrs.Id);
        YQL_CLOG(DEBUG, FastMapReduce) << "resolved alias " << aliasWithStar
            << " to operation id " << operationId
            << " in " << (TInstant::Now() - t0);
        return operationId;
    }

    // Re-opens sessions that got a fake OK before the coordinator was reachable.
    // Fires all requests in parallel, then waits for each result.
    // Called outside the lock, before Inner_ is published.
    void ReOpenEarlySessions(const IFmrCoordinator::TPtr& inner, THashSet<TString> sessions) {
        YQL_CLOG(DEBUG, FastMapReduce) << "re-opening " << sessions.size() << " early session(s)";
        const auto t0 = TInstant::Now();
        TVector<std::pair<TString, NThreading::TFuture<TOpenSessionResponse>>> futures;
        futures.reserve(sessions.size());
        for (const auto& sessionId : sessions) {
            futures.emplace_back(sessionId, inner->OpenSession({.SessionId = sessionId}));
        }
        ui64 failed = 0;
        for (auto& [sessionId, future] : futures) {
            future.Wait();
            if (future.HasException()) {
                ++failed;
                try {
                    future.TryRethrow();
                } catch (...) {
                    TGuard guard(Mutex_);
                    FailedSessions_.emplace(sessionId, std::current_exception());
                }
            }
        }
        YQL_CLOG(DEBUG, FastMapReduce) << "re-opened early sessions in " << (TInstant::Now() - t0)
            << " ok=" << (sessions.size() - failed) << " failed=" << failed;
    }

    // Updates Inner_ when the coordinator IP changes.
    // Before publishing Inner_, re-opens all sessions in EarlyOpenSessions_ on the new
    // coordinator. Loops until the set is empty to catch sessions that arrive while
    // ReOpenEarlySessions is running (Inner_ is not yet visible, so they still queue up).
    void RunRefreshLoop(IVanillaExternalPeerTracker& tracker) {
        while (true) {
            {
                TGuard guard(Mutex_);
                if (Shutdown_ || LastError_) {
                    break;
                }
            }
            Refresh(tracker);
            {
                TGuard guard(Mutex_);
                if (Shutdown_ || LastError_) {
                    break;
                }
                Cv_.WaitT(Mutex_, Settings_.RefreshInterval,
                    [this] { return Shutdown_ || LastError_; });
            }
        }
    }

    void Refresh(IVanillaExternalPeerTracker& tracker) {
        YQL_CLOG(TRACE, FastMapReduce) << "scan jobs";
        try {
            // Blocks until WaitForPeers is satisfied (first ListJobs done).
            const auto tGetPeer = TInstant::Now();
            auto ip = tracker.GetPeerAddress(0);
            if (ip.empty()) {
                YQL_CLOG(TRACE, FastMapReduce) << "coordinator IP is not available yet";
                return;
            }
            YQL_CLOG(DEBUG, FastMapReduce) << "GetPeerAddress(0) returned"
                << " ip=" << ip << " in " << (TInstant::Now() - tGetPeer);

            IFmrCoordinator::TPtr newInner;
            bool isFirst = false;
            {
                TGuard guard(Mutex_);
                if (CoordinatorIp_ == ip) {
                    YQL_CLOG(TRACE, FastMapReduce) << "coordinator IP unchanged ip=" << ip;
                    LastError_.Clear();
                    return;
                }
                isFirst = CoordinatorIp_.empty();
                CoordinatorIp_ = ip;
                newInner = MakeFmrCoordinatorClient(
                    TFmrCoordinatorClientSettings{.Port = Settings_.CoordinatorPort, .Host = ip});
                LastError_.Clear();
            }
            YQL_CLOG(DEBUG, FastMapReduce) << "created coordinator client"
                << " ip=" << ip << " port=" << Settings_.CoordinatorPort;

            // Drain EarlyOpenSessions_ on the new coordinator before publishing Inner_.
            // The empty-check and the Inner_ assignment are done in the same lock
            // acquisition to close the race where a new session arrives after the
            // empty-check but before Inner_ is published.
            const auto tDrain = TInstant::Now();
            for (;;) {
                THashSet<TString> batch;
                {
                    TGuard guard(Mutex_);
                    if (EarlyOpenSessions_.empty()) {
                        Inner_ = newInner;
                        break;
                    }
                    batch.swap(EarlyOpenSessions_);
                }
                ReOpenEarlySessions(newInner, std::move(batch));
            }
            Cv_.BroadCast();
            if (isFirst) {
                YQL_CLOG(INFO, FastMapReduce) << "coordinator IP obtained"
                    << " ip=" << ip << " port=" << Settings_.CoordinatorPort
                    << " drain=" << (TInstant::Now() - tDrain);
            } else {
                YQL_CLOG(INFO, FastMapReduce) << "coordinator IP changed"
                    << " ip=" << ip << " port=" << Settings_.CoordinatorPort
                    << " drain=" << (TInstant::Now() - tDrain);
            }
        } catch (...) {
            TGuard guard(Mutex_);
            if (!Shutdown_) {
                YQL_CLOG(ERROR, FastMapReduce) << "failed to refresh coordinator IP: "
                    << CurrentExceptionMessage();
                LastError_ = CurrentExceptionMessage();
                Cv_.BroadCast();
            }
        }
    }

    void BackgroundLoopImpl() {
        if (Settings_.ExternalPeerTracker) {
            // External tracker: skip alias resolution and tracker creation.
            RunRefreshLoop(*Settings_.ExternalPeerTracker);
            return;
        }

        // Phase 1: resolve alias if OperationId starts with '*'.
        TString operationId = Settings_.OperationId;
        if (!operationId.empty() && operationId[0] == '*') {
            operationId = ResolveAlias(operationId);
            TGuard guard(Mutex_);
            if (Shutdown_) {
                return;
            }
        }

        // Phase 2: create and start peer tracker with WaitForPeers=true so that
        // GetPeerAddress(0) in Refresh() blocks until the first ListJobs is done.
        {
            TGuard guard(Mutex_);
            if (Shutdown_) {
                return;
            }
        }
        TVanillaExternalPeerTrackerSettings trackerSettings{
            .Cluster = Settings_.Cluster,
            .OperationId = operationId,
            .ListJobsInterval = Settings_.ListJobsInterval,
            .MaxFails = Settings_.MaxFails,
            .WaitForPeers = true,
        };
        if (Settings_.Token) {
            trackerSettings.Token = Settings_.Token;
        }
        YQL_CLOG(DEBUG, FastMapReduce) << "starting peer tracker"
            << " cluster=" << Settings_.Cluster << " operation=" << operationId;
        const auto tStart = TInstant::Now();
        auto tracker = MakeIntrusive<TVanillaExternalPeerTracker>(trackerSettings);
        tracker->Start();
        YQL_CLOG(DEBUG, FastMapReduce) << "peer tracker started in " << (TInstant::Now() - tStart);

        {
            TGuard guard(Mutex_);
            PeerTracker_ = tracker;
            if (Shutdown_) {
                // Destructor will call tracker->Stop() after seeing PeerTracker_ != null.
                return;
            }
        }
        Cv_.BroadCast();  // wake destructor waiting for PeerTracker_

        // Phase 3: refresh loop — update Inner_ from the coordinator IP.
        RunRefreshLoop(*tracker);
        // tracker->Stop() is called by the destructor via PeerTracker_.
    }

    void BackgroundLoop() {
        YQL_CLOG(TRACE, FastMapReduce) << "background loop started";
        try {
            BackgroundLoopImpl();
        } catch (...) {
            YQL_CLOG(ERROR, FastMapReduce) << "background loop error: "
                << CurrentExceptionMessage();
            TGuard guard(Mutex_);
            if (!LastError_) {
                LastError_ = CurrentExceptionMessage();
            }
            Cv_.BroadCast();
        }

        YQL_CLOG(TRACE, FastMapReduce) << "background loop stopped";
    }

private:
    const TVanillaFmrCoordinatorClientSettings Settings_;
    mutable TMutex Mutex_;
    mutable TCondVar Cv_;
    TIntrusivePtr<TVanillaExternalPeerTracker> PeerTracker_;         // protected by Mutex_
    TString CoordinatorIp_;                                           // protected by Mutex_
    IFmrCoordinator::TPtr Inner_;                                     // protected by Mutex_
    TMaybe<TString> LastError_;                                       // protected by Mutex_
    bool Shutdown_ = false;                                           // protected by Mutex_
    THashSet<TString> EarlyOpenSessions_;                             // protected by Mutex_
    THashMap<TString, std::exception_ptr> FailedSessions_;            // protected by Mutex_
    THolder<TThread> BackgroundThread_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IFmrCoordinator::TPtr MakeVanillaFmrCoordinatorClient(
    const TVanillaFmrCoordinatorClientSettings& settings)
{
    return MakeIntrusive<TVanillaFmrCoordinatorClient>(settings);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYql::NFmr
