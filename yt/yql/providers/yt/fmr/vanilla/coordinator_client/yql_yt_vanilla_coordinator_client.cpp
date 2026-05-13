#include "yql_yt_vanilla_coordinator_client.h"

#include <yt/yql/providers/yt/fmr/coordinator/client/yql_yt_coordinator_client.h>
#include <yt/yql/providers/yt/fmr/vanilla/common/yql_yt_vanilla_common.h>

#include <yt/cpp/mapreduce/client/client.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/operation.h>

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/log/log_component.h>

#include <util/datetime/base.h>
#include <util/generic/guid.h>
#include <util/system/condvar.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>
#include <util/stream/str.h>

#include <atomic>

namespace NYql::NFmr {

////////////////////////////////////////////////////////////////////////////////

namespace {

class TVanillaFmrCoordinatorClient : public IFmrCoordinator {
public:
    explicit TVanillaFmrCoordinatorClient(const IVanillaExternalPeerTracker& peerTracker,
        TVanillaFmrCoordinatorClientSettings settings)
        : PeerTracker_(peerTracker)
        , Settings_(std::move(settings))
    {
        auto logCtx = NYql::NLog::CurrentLogContextPath();
        RefreshThread_ = MakeHolder<TThread>([this, logCtx]() {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);
            RefreshLoop();
        });
        RefreshThread_->Start();
    }

    ~TVanillaFmrCoordinatorClient() {
        Shutdown_.store(true);
        IpAvailable_.BroadCast();
        RefreshThread_->Join();
    }

    NThreading::TFuture<TStartOperationResponse> StartOperation(const TStartOperationRequest& req) override {
        return GetInner()->StartOperation(req);
    }

    NThreading::TFuture<TGetOperationResponse> GetOperation(const TGetOperationRequest& req) override {
        return GetInner()->GetOperation(req);
    }

    NThreading::TFuture<TDeleteOperationResponse> DeleteOperation(const TDeleteOperationRequest& req) override {
        return GetInner()->DeleteOperation(req);
    }

    NThreading::TFuture<TDropTablesResponse> DropTables(const TDropTablesRequest& req) override {
        return GetInner()->DropTables(req);
    }

    NThreading::TFuture<THeartbeatResponse> SendHeartbeatResponse(const THeartbeatRequest& req) override {
        return GetInner()->SendHeartbeatResponse(req);
    }

    NThreading::TFuture<TGetFmrTableInfoResponse> GetFmrTableInfo(const TGetFmrTableInfoRequest& req) override {
        return GetInner()->GetFmrTableInfo(req);
    }

    NThreading::TFuture<void> ClearSession(const TClearSessionRequest& req) override {
        return GetInner()->ClearSession(req);
    }

    NThreading::TFuture<TOpenSessionResponse> OpenSession(const TOpenSessionRequest& req) override {
        return GetInner()->OpenSession(req);
    }

    NThreading::TFuture<TPingSessionResponse> PingSession(const TPingSessionRequest& req) override {
        return GetInner()->PingSession(req);
    }

    NThreading::TFuture<TListSessionsResponse> ListSessions(const TListSessionsRequest& req) override {
        return GetInner()->ListSessions(req);
    }

    NThreading::TFuture<TPrepareOperationResponse> PrepareOperation(const TPrepareOperationRequest& req) override {
        return GetInner()->PrepareOperation(req);
    }

private:
    IFmrCoordinator::TPtr GetInner() const {
        TGuard guard(Mutex_);
        IpAvailable_.WaitI(Mutex_, [this] { return LastError_ || Inner_ != nullptr || Shutdown_.load(); });
        Y_ENSURE(!LastError_, "TVanillaFmrCoordinatorClient fatal error: " + *LastError_);
        Y_ENSURE(Inner_, "Coordinator client is shutting down");
        return Inner_;
    }

    void RefreshLoop() {
        YQL_CLOG(TRACE, FastMapReduce) << "refresh loop started";
        while (!Shutdown_.load()) {
            Refresh();
            Sleep(Settings_.RefreshInterval);
        }
        YQL_CLOG(TRACE, FastMapReduce) << "refresh loop stopped";
    }

    void Refresh() {
        YQL_CLOG(TRACE, FastMapReduce) << "scan jobs";
        try {
            auto ip = PeerTracker_.GetPeerAddress(0);
            if (ip.empty()) {
                YQL_CLOG(TRACE, FastMapReduce) << "coordinator IP is not available yet";
                return;
            }

            TGuard guard(Mutex_);
            if (CoordinatorIp_ != ip) {
                const bool isFirst = CoordinatorIp_.empty();
                CoordinatorIp_ = ip;
                Inner_ = MakeFmrCoordinatorClient(
                    TFmrCoordinatorClientSettings{.Port = Settings_.CoordinatorPort, .Host = ip});
                IpAvailable_.BroadCast();
                if (isFirst) {
                    YQL_CLOG(INFO, FastMapReduce) << "coordinator IP obtained"
                        << " ip=" << ip << " port=" << Settings_.CoordinatorPort;
                } else {
                    YQL_CLOG(INFO, FastMapReduce) << "coordinator IP changed"
                        << " ip=" << ip << " port=" << Settings_.CoordinatorPort;
                }
            } else {
                YQL_CLOG(TRACE, FastMapReduce) << "coordinator IP unchanged ip=" << ip;
            }
            LastError_.Clear();
        } catch (...) {
            YQL_CLOG(ERROR, FastMapReduce) << "failed to refresh coordinator IP: "
                << CurrentExceptionMessage();

            TGuard guard(Mutex_);
            LastError_ = CurrentExceptionMessage();
            IpAvailable_.BroadCast();
        }
    }

private:
    const IVanillaExternalPeerTracker& PeerTracker_;
    const TVanillaFmrCoordinatorClientSettings Settings_;
    mutable TMutex Mutex_;
    mutable TCondVar IpAvailable_;
    TString CoordinatorIp_;
    IFmrCoordinator::TPtr Inner_;
    TMaybe<TString> LastError_;
    std::atomic<bool> Shutdown_{false};
    THolder<TThread> RefreshThread_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IFmrCoordinator::TPtr MakeVanillaFmrCoordinatorClient(
    const IVanillaExternalPeerTracker& peerTracker,
    const TVanillaFmrCoordinatorClientSettings& settings)
{
    return MakeIntrusive<TVanillaFmrCoordinatorClient>(peerTracker, settings);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYql::NFmr
