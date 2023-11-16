#include "handler.h"

#include <yt/yt/core/concurrency/async_stream.h>

#include <yt/yt/core/http/http.h>
#include <yt/yt/core/http/server.h>

#include <yt/yt/library/ytprof/cpu_profiler.h>
#include <yt/yt/library/ytprof/spinlock_profiler.h>
#include <yt/yt/library/ytprof/heap_profiler.h>
#include <yt/yt/library/ytprof/profile.h>
#include <yt/yt/library/ytprof/symbolize.h>
#include <yt/yt/library/ytprof/external_pprof.h>

#include <yt/yt/library/process/subprocess.h>

#include <yt/yt/core/misc/finally.h>

#include <library/cpp/cgiparam/cgiparam.h>

#include <library/cpp/yt/threading/traceless_guard.h>

#include <util/system/mutex.h>

namespace NYT::NYTProf {

using namespace NHttp;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TBaseHandler
    : public IHttpHandler
{
public:
    explicit TBaseHandler(const TBuildInfo& buildInfo)
        : BuildInfo_(buildInfo)
    { }

    virtual NProto::Profile BuildProfile(const TCgiParameters& params) = 0;

    void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        try {
            auto guard = NThreading::TracelessTryGuard(Lock_);

            if (!guard) {
                rsp->SetStatus(EStatusCode::TooManyRequests);
                WaitFor(rsp->WriteBody(TSharedRef::FromString("Profile fetch already running")))
                    .ThrowOnError();
                return;
            }

            TCgiParameters params(req->GetUrl().RawQuery);
            auto profile = BuildProfile(params);
            Symbolize(&profile, true);
            AddBuildInfo(&profile, BuildInfo_);

            if (auto it = params.Find("symbolize"); it == params.end() || it->second != "0") {
                SymbolizeByExternalPProf(&profile, TSymbolizationOptions{
                    .RunTool = RunSubprocess,
                });
            }

            TStringStream profileBlob;
            WriteProfile(&profileBlob, profile);

            rsp->SetStatus(EStatusCode::OK);
            WaitFor(rsp->WriteBody(TSharedRef::FromString(profileBlob.Str())))
                .ThrowOnError();
        } catch (const std::exception& ex) {
            if (rsp->AreHeadersFlushed()) {
                throw;
            }

            rsp->SetStatus(EStatusCode::InternalServerError);
            WaitFor(rsp->WriteBody(TSharedRef::FromString(ex.what())))
                .ThrowOnError();

            throw;
        }
    }

protected:
    const TBuildInfo BuildInfo_;

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
};

class TCpuProfilerHandler
    : public TBaseHandler
{
public:
    using TBaseHandler::TBaseHandler;

    NProto::Profile BuildProfile(const TCgiParameters& params) override
    {
        auto duration = TDuration::Seconds(15);
        if (auto it = params.Find("d"); it != params.end()) {
            duration = TDuration::Parse(it->second);
        }

        TCpuProfilerOptions options;
        if (auto it = params.Find("freq"); it != params.end()) {
            options.SamplingFrequency = FromString<int>(it->second);
        }

        if (auto it = params.Find("record_action_run_time"); it != params.end()) {
            options.RecordActionRunTime = true;
        }

        if (auto it = params.Find("action_min_exec_time"); it != params.end()) {
            options.SampleFilters.push_back(GetActionMinExecTimeFilter(TDuration::Parse(it->second)));
        }

        TCpuProfiler profiler{options};
        profiler.Start();
        TDelayedExecutor::WaitForDuration(duration);
        profiler.Stop();

        return profiler.ReadProfile();
    }
};

class TSpinlockProfilerHandler
    : public TBaseHandler
{
public:
    TSpinlockProfilerHandler(const TBuildInfo& buildInfo, bool yt)
        : TBaseHandler(buildInfo)
        , YT_(yt)
    { }

    NProto::Profile BuildProfile(const TCgiParameters& params) override
    {
        auto duration = TDuration::Seconds(15);
        if (auto it = params.Find("d"); it != params.end()) {
            duration = TDuration::Parse(it->second);
        }

        TSpinlockProfilerOptions options;
        if (auto it = params.Find("frac"); it != params.end()) {
            options.ProfileFraction = FromString<int>(it->second);
        }

        if (YT_) {
            TBlockingProfiler profiler{options};
            profiler.Start();
            TDelayedExecutor::WaitForDuration(duration);
            profiler.Stop();

            return profiler.ReadProfile();
        } else {
            TSpinlockProfiler profiler{options};
            profiler.Start();
            TDelayedExecutor::WaitForDuration(duration);
            profiler.Stop();

            return profiler.ReadProfile();
        }
    }

private:
    const bool YT_;
};

class TTCMallocSnapshotProfilerHandler
    : public TBaseHandler
{
public:
    TTCMallocSnapshotProfilerHandler(const TBuildInfo& buildInfo, tcmalloc::ProfileType profileType)
        : TBaseHandler(buildInfo)
        , ProfileType_(profileType)
    { }

    NProto::Profile BuildProfile(const TCgiParameters& /*params*/) override
    {
        return ReadHeapProfile(ProfileType_);
    }

private:
    tcmalloc::ProfileType ProfileType_;
};

class TTCMallocAllocationProfilerHandler
    : public TBaseHandler
{
public:
    using TBaseHandler::TBaseHandler;

    NProto::Profile BuildProfile(const TCgiParameters& params) override
    {
        auto duration = TDuration::Seconds(15);
        if (auto it = params.Find("d"); it != params.end()) {
            duration = TDuration::Parse(it->second);
        }

        auto token = tcmalloc::MallocExtension::StartAllocationProfiling();
        TDelayedExecutor::WaitForDuration(duration);
        return ConvertAllocationProfile(std::move(token).Stop());
    }
};

class TTCMallocStatHandler
    : public IHttpHandler
{
public:
    void HandleRequest(const IRequestPtr& /* req */, const IResponseWriterPtr& rsp) override
    {
        auto stat = tcmalloc::MallocExtension::GetStats();
        rsp->SetStatus(EStatusCode::OK);
        WaitFor(rsp->WriteBody(TSharedRef::FromString(TString{stat})))
            .ThrowOnError();
    }
};

class TBinaryHandler
    : public IHttpHandler
{
public:
    void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        try {
            auto buildId = GetBuildId();
            TCgiParameters params(req->GetUrl().RawQuery);

            if (auto it = params.Find("check_build_id"); it != params.end()) {
                if (it->second != buildId) {
                    THROW_ERROR_EXCEPTION("Wrong build id: %v != %v", it->second, buildId);
                }
            }

            rsp->SetStatus(EStatusCode::OK);

            TFileInput file{"/proc/self/exe"};
            auto adapter = CreateBufferedSyncAdapter(rsp);
            file.ReadAll(*adapter);
            adapter->Finish();

            WaitFor(rsp->Close())
                .ThrowOnError();
        } catch (const std::exception& ex) {
            if (rsp->AreHeadersFlushed()) {
                throw;
            }

            rsp->SetStatus(EStatusCode::InternalServerError);
            WaitFor(rsp->WriteBody(TSharedRef::FromString(ex.what())))
                .ThrowOnError();

            throw;
        }
    }
};

class TVersionHandler
    : public IHttpHandler
{
public:
    void HandleRequest(const IRequestPtr& /* req */, const IResponseWriterPtr& rsp) override
    {
        rsp->SetStatus(EStatusCode::OK);
        WaitFor(rsp->WriteBody(TSharedRef::FromString(GetVersion())))
            .ThrowOnError();
    }
};

class TBuildIdHandler
    : public IHttpHandler
{
public:
    void HandleRequest(const IRequestPtr& /* req */, const IResponseWriterPtr& rsp) override
    {
        rsp->SetStatus(EStatusCode::OK);
        WaitFor(rsp->WriteBody(TSharedRef::FromString(GetVersion())))
            .ThrowOnError();
    }
};

void Register(
    const NHttp::IServerPtr& server,
    const TString& prefix,
    const TBuildInfo& buildInfo)
{
    Register(server->GetPathMatcher(), prefix, buildInfo);
}

void Register(
    const IRequestPathMatcherPtr& handlers,
    const TString& prefix,
    const TBuildInfo& buildInfo)
{
    handlers->Add(prefix + "/profile", New<TCpuProfilerHandler>(buildInfo));

    handlers->Add(prefix + "/lock", New<TSpinlockProfilerHandler>(buildInfo, false));
    handlers->Add(prefix + "/block", New<TSpinlockProfilerHandler>(buildInfo, true));

    handlers->Add(prefix + "/heap", New<TTCMallocSnapshotProfilerHandler>(buildInfo, tcmalloc::ProfileType::kHeap));
    handlers->Add(prefix + "/peak", New<TTCMallocSnapshotProfilerHandler>(buildInfo, tcmalloc::ProfileType::kPeakHeap));
    handlers->Add(prefix + "/fragmentation", New<TTCMallocSnapshotProfilerHandler>(buildInfo, tcmalloc::ProfileType::kFragmentation));
    handlers->Add(prefix + "/allocations", New<TTCMallocAllocationProfilerHandler>(buildInfo));

    handlers->Add(prefix + "/tcmalloc", New<TTCMallocStatHandler>());

    handlers->Add(prefix + "/binary", New<TBinaryHandler>());

    handlers->Add(prefix + "/version", New<TVersionHandler>());
    handlers->Add(prefix + "/buildid", New<TBuildIdHandler>());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
