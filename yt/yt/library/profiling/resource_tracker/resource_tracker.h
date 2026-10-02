#pragma once

#include "public.h"

#include <yt/yt/library/profiling/tag.h>

#include <optional>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

class TResourceTrackerTagsGuard
    : private TMoveOnly
{
public:
    explicit TResourceTrackerTagsGuard(std::optional<std::string> threadName);
    TResourceTrackerTagsGuard(TResourceTrackerTagsGuard&& other) noexcept;
    TResourceTrackerTagsGuard& operator=(TResourceTrackerTagsGuard&& other) noexcept;
    ~TResourceTrackerTagsGuard();

    explicit operator bool() const;

    const std::optional<std::string>& GetThreadName() const;
    void Release() noexcept;

private:
    std::optional<std::string> ThreadName_;
};

class TResourceTracker
{
public:
    //! Enables collecting background statistics and pushing them to profiler.
    static void Enable();

    static double GetUserCpu();
    static double GetSystemCpu();
    static double GetCpuWait();

    static i64 GetTotalMemoryLimit();
    static i64 GetAnonymousMemoryLimit();

    //! If this factor is set, additional metrics will be reported:
    //! user, system, total cpu multiplied by given factor.
    //! E.g. |system_vcpu = system_cpu * vcpu_factor|.
    static void SetCpuToVCpuFactor(double factor);

    static void Configure(const TResourceTrackerConfigPtr& config);

    [[nodiscard]] static TResourceTrackerTagsGuard RegisterPerThreadExtraTags(
        const std::string& threadName,
        const TTagSet& extraTags);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
