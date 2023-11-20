#pragma once

#include <thread>
#include <array>
#include <variant>

#if defined(_linux_)
#include <sys/types.h>
#endif

#include <yt/yt/library/ytprof/proto/profile.pb.h>
#include <yt/yt/library/ytprof/api/api.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>

#include <util/generic/hash.h>
#include <util/datetime/base.h>

#include "queue.h"
#include "signal_safe_profiler.h"

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

class TCpuProfilerOptions
    : public TSignalSafeProfilerOptions
{
public:
    int SamplingFrequency = 100;

    using TSampleFilter = std::function<bool()>;
    std::vector<TSampleFilter> SampleFilters;
};

////////////////////////////////////////////////////////////////////////////////

class TCpuProfiler
    : public TSignalSafeProfiler
{
public:
    explicit TCpuProfiler(TCpuProfilerOptions options = {});
    ~TCpuProfiler();

private:
#if defined(_linux_)
    const TCpuProfilerOptions Options_;
    const i64 ProfilePeriod_;

    static std::atomic<TCpuProfiler*> ActiveProfiler_;
    static std::atomic<bool> HandlingSigprof_;
    static void SigProfHandler(int sig, siginfo_t* info, void* ucontext);

    std::atomic<i64> SignalOverruns_{0};

    void OnSigProf(siginfo_t* info, ucontext_t* ucontext);
#endif

    void EnableProfiler() override;
    void DisableProfiler() override;
    void AnnotateProfile(NProto::Profile* profile, const std::function<i64(const TString&)>& stringify) override;
    i64 EncodeValue(i64 value) override;
};

////////////////////////////////////////////////////////////////////////////////

TCpuProfilerOptions::TSampleFilter GetActionMinExecTimeFilter(TDuration minExecTime);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
