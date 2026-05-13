#include "yql_yt_vanilla_peer_tracker.h"

#include <yt/yql/providers/yt/fmr/vanilla/common/yql_yt_vanilla_common.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/operation.h>

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/log/log_component.h>

#include <library/cpp/http/server/http.h>
#include <library/cpp/http/server/response.h>
#include <library/cpp/http/simple/http_client.h>

#include <util/datetime/base.h>
#include <util/generic/guid.h>
#include <util/random/random.h>
#include <util/system/env.h>
#include <util/stream/str.h>

namespace NYql::NFmr {

    ////////////////////////////////////////////////////////////////////////////////

    namespace {

        constexpr int VanillaPingPort = 8000;

    } // namespace

    TVanillaPeerTracker::TVanillaPeerTracker(TVanillaPeerTrackerSettings settings)
        : Settings_(std::move(settings))
        , SelfCookie_(FromString<ui64>(GetEnv("YT_JOB_COOKIE")))
        , SelfJobId_(GetEnv("YT_JOB_ID"))
        , SelfIpAddress_(GetEnv("YT_IP_ADDRESS_DEFAULT"))
        , OperationId_(GetEnv("YT_OPERATION_ID"))
    {
        Y_ENSURE(Settings_.JobCount > 0, "JobCount must be positive");
        Y_ENSURE(SelfCookie_ < Settings_.JobCount,
                 "Self cookie " << SelfCookie_ << " is out of range [0, " << Settings_.JobCount << ")");

        PeerIps_.resize(Settings_.JobCount);

        YQL_CLOG(INFO, FastMapReduce) << "Job started " << SelfJobId_
             << " cookie=" << SelfCookie_
             << " ip=" << SelfIpAddress_;
    }

    TString TVanillaPeerTracker::GetOperationId() const {
        return OperationId_;
    }

    ui64 TVanillaPeerTracker::GetSelfIndex() const {
        return SelfCookie_;
    }

    TString TVanillaPeerTracker::GetSelfJobId() const {
        return SelfJobId_;
    }

    TString TVanillaPeerTracker::GetSelfIpAddress() const {
        return SelfIpAddress_;
    }

    ui64 TVanillaPeerTracker::GetPeerCount() const {
        return Settings_.JobCount;
    }

    TString TVanillaPeerTracker::GetPeerAddress(ui64 index) const {
        TGuard guard(PeersMutex_);
        Y_ENSURE(index < PeerIps_.size(), "Peer index " << index << " is out of range [0, " << PeerIps_.size() << ")");
        return PeerIps_[index];
    }

    TVector<TString> TVanillaPeerTracker::GetPeerAddresses() const {
        TGuard guard(PeersMutex_);
        return PeerIps_;
    }

    void TVanillaPeerTracker::Run() {
        using namespace NYT;

        auto client = CreateClient(Settings_.Cluster);

        Shutdown_.store(false);

        // Thread 1: HTTP ping server.
        ServerThread_ = MakeHolder<TThread>([&]() {
            struct TPingRequest: public TRequestReplier {
                bool DoReply(const TReplyParams& params) override {
                    params.Output << THttpResponse(HTTP_OK).SetContent("ok");
                    return true;
                }
            };
            struct TPingCallback: public THttpServer::ICallBack {
                TClientRequest* CreateClient() override {
                    return new TPingRequest();
                }
            };
            TPingCallback cb;
            THttpServer::TOptions opts;
            opts.AddBindAddress(SelfIpAddress_, VanillaPingPort);
            THttpServer server(&cb, opts);
            server.Start();
            while (!Shutdown_.load()) {
                Sleep(TDuration::Seconds(1));
            }
            server.Stop();
        });

        // Thread 2: periodically ping a random peer.
        ClientThread_ = MakeHolder<TThread>([&]() {
            while (!Shutdown_.load()) {
                TString ip;
                {
                    TGuard guard(PeersMutex_);
                    if (!PeerIps_.empty()) {
                        ip = PeerIps_[RandomNumber<size_t>(PeerIps_.size())];
                    }
                }
                if (!ip.empty()) {
                    try {
                        TKeepAliveHttpClient httpClient(ip, VanillaPingPort,
                                                        Settings_.PingTimeout, Settings_.PingTimeout);
                        TStringStream response;
                        httpClient.DoGet("/ping", &response);
                        YQL_CLOG(TRACE, FastMapReduce) << "pinged " << ip << ": " << response.Str();
                    } catch (...) {
                        YQL_CLOG(ERROR, FastMapReduce) << "ping to " << ip << " failed: " << CurrentExceptionMessage();
                    }
                }
                Sleep(Settings_.PingClientInterval);
            }
        });

        ServerThread_->Start();
        ClientThread_->Start();

        // Main loop: list jobs, update peer snapshot, detect supersede.
        bool running = true;
        while (running) {
            try {
                YQL_CLOG(TRACE, FastMapReduce) << "listing jobs";

                auto result = client->ListJobs(GetGuid(OperationId_),
                                               TListJobsOptions()
                                                   .State(EJobState::Running)
                                                   .DataSource(EListJobsDataSource::Runtime)
                                                   .Limit(static_cast<i64>(Settings_.JobCount)));

                auto& jobs = result.Jobs;
                SortBy(jobs, [](const TJobAttributes& job) { return GetGuidAsString(*job.Id); });

                TVector<TString> newPeerIps(Settings_.JobCount);
                for (const auto& job : jobs) {
                    TString jobId = GetGuidAsString(*job.Id);

                    if (job.Cookie) {
                        Y_ENSURE(*job.Cookie < Settings_.JobCount,
                                 "peer cookie " << *job.Cookie << " is out of range [0, " << Settings_.JobCount << ")");
                    }

                    if (job.Cookie && *job.Cookie == SelfCookie_ && jobId != SelfJobId_) {
                        YQL_CLOG(INFO, FastMapReduce) << "Superseded by job " << jobId << ", exiting";
                        running = false;
                    }

                    TString ip = ExtractBackboneMtnIp(job);

                    YQL_CLOG(TRACE, FastMapReduce) << "job " << jobId
                         << " cookie=" << job.Cookie
                         << " ip=" << (ip.empty() ? "<unknown>" : ip);

                    if (job.Cookie) {
                        newPeerIps[*job.Cookie] = ip;
                    }
                }

                {
                    TGuard guard(PeersMutex_);
                    PeerIps_ = std::move(newPeerIps);
                }
            } catch (...) {
                YQL_CLOG(ERROR, FastMapReduce) << CurrentExceptionMessage();
            }

            if (running) {
                Sleep(Settings_.ListJobsInterval);
            }
        }

        Shutdown_.store(true);
        ServerThread_->Join();
        ServerThread_.Reset();
        ClientThread_->Join();
        ClientThread_.Reset();
    }

    void TVanillaPeerTracker::CheckOperation(
        const TString& cluster,
        const TString& operationId,
        bool withPing,
        TDuration pingTimeout)
    {
        using namespace NYT;

        auto client = CreateClient(cluster);

        auto result = client->ListJobs(
            GetGuid(operationId),
            TListJobsOptions()
                .State(EJobState::Running)
                .DataSource(EListJobsDataSource::Runtime));

        const TJobAttributes* cookie0Job = nullptr;
        for (const auto& job : result.Jobs) {
            if (job.Cookie && *job.Cookie == 0) {
                cookie0Job = &job;
                break;
            }
        }

        if (!cookie0Job) {
            Cerr << "no running job with cookie 0 found in operation " << operationId << Endl;
            return;
        }

        TString ip = ExtractBackboneMtnIp(*cookie0Job);

        Cerr << "job with cookie=0"
             << " id=" << GetGuidAsString(*cookie0Job->Id)
             << " ip=" << (ip.empty() ? "<unknown>" : ip) << Endl;

        if (!withPing) {
            return;
        }

        if (ip.empty()) {
            Cerr << "cannot ping: IP address is unknown" << Endl;
            return;
        }

        try {
            TKeepAliveHttpClient httpClient(ip, VanillaPingPort, pingTimeout, pingTimeout);
            TStringStream response;
            httpClient.DoGet("/ping", &response);
            Cerr << "ping to " << ip << " succeeded: " << response.Str() << Endl;
        } catch (...) {
            Cerr << "ping to " << ip << " failed: " << CurrentExceptionMessage() << Endl;
        }

        TVanillaExternalPeerTracker tracker({.Cluster = cluster, .OperationId = operationId});
        tracker.Start();
        for (;;) {
            auto addresses = tracker.GetPeerAddresses();
            for (ui64 i = 0; i < addresses.size(); ++i) {
                Cerr << "peer " << i << " ip=" << (addresses[i].empty() ? "<unknown>" : addresses[i]) << Endl;
            }

            Sleep(TDuration::Seconds(1));
        }
    }

    ////////////////////////////////////////////////////////////////////////////////

    TVanillaExternalPeerTracker::TVanillaExternalPeerTracker(TVanillaExternalPeerTrackerSettings settings)
        : Settings_(std::move(settings))
    {
    }

    TVanillaExternalPeerTracker::~TVanillaExternalPeerTracker() {
        Stop();
    }

    TString TVanillaExternalPeerTracker::GetOperationId() const {
        return Settings_.OperationId;
    }

    void TVanillaExternalPeerTracker::WaitForJobCount() const {
        Y_ENSURE(Running_, "TVanillaExternalPeerTracker is not running");
        JobCountReady_.WaitI(PeersMutex_, [this] { return LastError_ || JobCount_ > 0 || !Running_; });
        Y_ENSURE(!LastError_, "TVanillaExternalPeerTracker fatal error: " + *LastError_);
        Y_ENSURE(Running_, "TVanillaExternalPeerTracker stopped before job count was obtained");
    }

    ui64 TVanillaExternalPeerTracker::GetPeerCount() const {
        TGuard guard(PeersMutex_);
        WaitForJobCount();
        return JobCount_;
    }

    TString TVanillaExternalPeerTracker::GetPeerAddress(ui64 index) const {
        TGuard guard(PeersMutex_);
        WaitForJobCount();
        Y_ENSURE(index < PeerIps_.size(), "Peer index " << index << " is out of range [0, " << PeerIps_.size() << ")");
        return PeerIps_[index];
    }

    TVector<TString> TVanillaExternalPeerTracker::GetPeerAddresses() const {
        TGuard guard(PeersMutex_);
        WaitForJobCount();
        return PeerIps_;
    }

    ui64 TVanillaExternalPeerTracker::FetchJobCount(const NYT::IClientPtr& client) const {
        using namespace NYT;
        auto attrs = client->GetOperation(
            GetGuid(Settings_.OperationId),
            TGetOperationOptions().AttributeFilter(
                TOperationAttributeFilter().Add(EOperationAttribute::Spec)));

        Y_ENSURE(attrs.Spec, "Operation spec is missing in GetOperation response");
        const auto& tasks = (*attrs.Spec)["tasks"];
        Y_ENSURE(tasks.IsMap() && tasks.Size() == 1,
            "Expected exactly one task in vanilla operation spec, got " << tasks.Size());

        const auto& taskSpec = tasks.AsMap().begin()->second;
        Y_ENSURE(taskSpec.HasKey("job_count"),
            "Vanilla task spec has no job_count field");
        ui64 jobCount = taskSpec["job_count"].AsUint64();
        Y_ENSURE(jobCount > 0, "Vanilla task job_count must be positive");
        return jobCount;
    }

    void TVanillaExternalPeerTracker::Start() {
        {
            TGuard guard(PeersMutex_);
            Running_ = true;
        }
        Shutdown_.store(false);
        auto logCtx = NYql::NLog::CurrentLogContextPath();
        RefreshThread_ = MakeHolder<TThread>([this, logCtx]() {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);
            RefreshLoop();
        });
        RefreshThread_->Start();
    }

    void TVanillaExternalPeerTracker::Stop() {
        Shutdown_.store(true);
        {
            TGuard guard(PeersMutex_);
            Running_ = false;
        }
        JobCountReady_.BroadCast();
        if (RefreshThread_) {
            RefreshThread_->Join();
            RefreshThread_.Reset();
        }
    }

    void TVanillaExternalPeerTracker::RefreshLoop() {
        YQL_CLOG(TRACE, FastMapReduce) << "external peer tracker refresh loop started";

        using namespace NYT;
        auto clientOptions = NYT::TCreateClientOptions();
        if (Settings_.Token.Defined()) {
            clientOptions.Token(*Settings_.Token);
        }

        auto client = CreateClient(Settings_.Cluster, clientOptions);

        // Phase 1: fetch job count, retrying on failure until shutdown.
        while (!Shutdown_.load()) {
            try {
                ui64 jobCount = FetchJobCount(client);
                {
                    TGuard guard(PeersMutex_);
                    JobCount_ = jobCount;
                    PeerIps_.assign(jobCount, TString());
                }
                JobCountReady_.BroadCast();
                YQL_CLOG(INFO, FastMapReduce) << "external peer tracker fetched"
                    << " job_count=" << jobCount;
                break;
            } catch (...) {
                YQL_CLOG(ERROR, FastMapReduce) << "external peer tracker failed to fetch job count: "
                    << CurrentExceptionMessage();
                {
                    TGuard guard(PeersMutex_);
                    ++Fails_;
                    if (Fails_ > Settings_.MaxFails) {
                        LastError_ = CurrentExceptionMessage();
                        JobCountReady_.BroadCast();
                        return;
                    }
                }

                Sleep(Settings_.ListJobsInterval);
            }
        }

        // Phase 2: periodic IP refresh.
        while (!Shutdown_.load()) {
            Refresh(client);
            Sleep(Settings_.ListJobsInterval);
        }

        YQL_CLOG(TRACE, FastMapReduce) << "external peer tracker refresh loop stopped";
    }

    void TVanillaExternalPeerTracker::Refresh(const NYT::IClientPtr& client) {
        YQL_CLOG(TRACE, FastMapReduce) << "scan jobs";

        using namespace NYT;
        try {
            ui64 jobCount;
            {
                TGuard guard(PeersMutex_);
                jobCount = JobCount_;
            }
            auto result = client->ListJobs(
                GetGuid(Settings_.OperationId),
                TListJobsOptions()
                    .State(EJobState::Running)
                    .DataSource(EListJobsDataSource::Runtime)
                    .Limit(static_cast<i64>(jobCount)));

            if (!result.ControllerAgentJobCount.GetOrElse(0)) {
                YQL_CLOG(ERROR, FastMapReduce) << "no controller agent jobs";
                TGuard guard(PeersMutex_);
                LastError_ = "Operation is not running";
                return;
            }

            TVector<TString> newPeerIps(jobCount);
            for (const auto& job : result.Jobs) {
                if (job.Cookie) {
                    Y_ENSURE(*job.Cookie < jobCount,
                        "peer cookie " << *job.Cookie << " is out of range [0, " << jobCount << ")");
                    newPeerIps[*job.Cookie] = ExtractBackboneMtnIp(job);
                }
            }

            TGuard guard(PeersMutex_);
            PeerIps_ = std::move(newPeerIps);
            Fails_ = 0; // reset counter
            LastError_.Clear();
            return;
        } catch (...) {
            YQL_CLOG(ERROR, FastMapReduce) << "external peer tracker failed to refresh: "
                << CurrentExceptionMessage();

            TGuard guard(PeersMutex_);
            ++Fails_;
            if (Fails_ > Settings_.MaxFails) {
                LastError_ = CurrentExceptionMessage();
                return;
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////

} // namespace NYql::NFmr
