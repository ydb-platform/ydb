#pragma once

#include <array>
#include <variant>
#include <optional>

#include <yt/yt/library/ytprof/api/atomic_signal_ptr.h>

#include <library/cpp/yt/misc/port.h>
#include <library/cpp/yt/cpu_clock/public.h>
#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

struct TProfilerTag final
{
    std::string Name;
    std::optional<std::string> StringValue;
    std::optional<i64> IntValue;

    TProfilerTag(const std::string& name, const std::string& value)
        : Name(name)
        , StringValue(value)
    { }

    TProfilerTag(const std::string& name, i64 value)
        : Name(name)
        , IntValue(value)
    { }
};

using TProfilerTagPtr = TIntrusivePtr<TProfilerTag>;

constexpr int MaxActiveTags = 4;

std::array<TAtomicSignalPtr<TProfilerTag>, MaxActiveTags>* GetCpuProfilerTags();

////////////////////////////////////////////////////////////////////////////////

// Hooks for yt/yt/core fibers.
void* AcquireFiberTagStorage();
std::vector<std::pair<std::string, std::variant<std::string, i64>>> ReadFiberTags(void* storage);
void ReleaseFiberTagStorage(void* storage);
TCpuInstant GetTraceContextTimingCheckpoint();

////////////////////////////////////////////////////////////////////////////////

class TCpuProfilerTagGuard
{
public:
    TCpuProfilerTagGuard() = default;

    explicit TCpuProfilerTagGuard(TProfilerTagPtr tag);
    ~TCpuProfilerTagGuard();

    TCpuProfilerTagGuard(TCpuProfilerTagGuard&& other);
    TCpuProfilerTagGuard(const TCpuProfilerTagGuard& other) = delete;

    TCpuProfilerTagGuard& operator = (TCpuProfilerTagGuard&& other);
    TCpuProfilerTagGuard& operator = (const TCpuProfilerTagGuard& other) = delete;

private:
    int TagIndex_ = -1;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
