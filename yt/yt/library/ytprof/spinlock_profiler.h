#pragma once

#include "signal_safe_profiler.h"

#include <library/cpp/yt/threading/spin_wait_hook.h>

#include <mutex>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

struct TSpinlockProfilerOptions
    : public TSignalSafeProfilerOptions
{
    int ProfileFraction = 100;
};

////////////////////////////////////////////////////////////////////////////////

// TSpinlockProfiler profiles wait events from absl spinlock.
class TSpinlockProfiler
    : public TSignalSafeProfiler
{
public:
    explicit TSpinlockProfiler(TSpinlockProfilerOptions options);
    ~TSpinlockProfiler();

private:
    const TSpinlockProfilerOptions Options_;

    static std::atomic<int> SamplingRate_;
    static std::atomic<TSpinlockProfiler*> ActiveProfiler_;
    static std::atomic<bool> HandlingEvent_;
    static std::once_flag HookInitialized_;

    void EnableProfiler() override;
    void DisableProfiler() override;
    void AnnotateProfile(NProto::Profile* profile, const std::function<i64(const TString&)>& stringify) override;
    i64 EncodeValue(i64 value) override;

    static void OnEvent(const void *lock, int64_t waitCycles);
    void RecordEvent(const void *lock, int64_t waitCycles);
};

////////////////////////////////////////////////////////////////////////////////

// TBlockingProfiler profiles wait events from yt spinlocks.
class TBlockingProfiler
    : public TSignalSafeProfiler
{
public:
    explicit TBlockingProfiler(TSpinlockProfilerOptions options);
    ~TBlockingProfiler();

private:
    const TSpinlockProfilerOptions Options_;

    static std::atomic<int> SamplingRate_;
    static std::atomic<TBlockingProfiler*> ActiveProfiler_;
    static std::atomic<bool> HandlingEvent_;
    static std::once_flag HookInitialized_;

    void EnableProfiler() override;
    void DisableProfiler() override;
    void AnnotateProfile(NProto::Profile* profile, const std::function<i64(const TString&)>& stringify) override;
    i64 EncodeValue(i64 value) override;

    static void OnEvent(
        TCpuDuration cpuDelay,
        const ::TSourceLocation& location,
        NThreading::ESpinLockActivityKind activityKind);

    void RecordEvent(
        TCpuDuration cpuDelay,
        const ::TSourceLocation& location,
        NThreading::ESpinLockActivityKind activityKind);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
