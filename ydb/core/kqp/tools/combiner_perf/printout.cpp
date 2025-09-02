#include "printout.h"

#include <sys/resource.h>

#if defined (_darwin_)
#include <mach/mach.h>
#endif

long GetMaxRSS()
{
    rusage usage;
    if (getrusage(RUSAGE_SELF, &usage) != 0) {
        ythrow yexception() << "getrusage failed with errno " << errno << Endl;
    }
    return usage.ru_maxrss;
}

long GetMaxRSSDelta(const long prevMaxRss)
{
    long maxRss = GetMaxRSS();

    if (maxRss <= 0 || prevMaxRss <= 0 || maxRss < prevMaxRss) {
        ythrow yexception() << "Bad maxRSS measurement, before: " << prevMaxRss << ", after: " << maxRss << Endl;
    }

#if defined (_darwin_)
    constexpr const long factor = 1ull;
#else
    constexpr const long factor = 1024ull;
#endif

    return (maxRss - prevMaxRss) * factor;
}

TDuration GetThreadCPUTime()
{
    ui64 us = 0;

#if defined (_darwin_)
    thread_basic_info_data_t info = {};
    mach_msg_type_number_t info_count = THREAD_BASIC_INFO_COUNT;
    kern_return_t kern_err = thread_info(
        mach_thread_self(),
        THREAD_BASIC_INFO,
        (thread_info_t)&info,
        &info_count
    );
    if (kern_err == KERN_SUCCESS) {
        us = 1000000ULL * info.user_time.seconds + info.user_time.microseconds + 1000000ULL*info.system_time.seconds + info.system_time.microseconds;
    }
#else
    us = ThreadCPUTime();
#endif

    if (!us) {
        ythrow yexception() << "Failed to obtain thread CPU time";
    }

    return TDuration::MicroSeconds(us);
}

TDuration GetThreadCPUTimeDelta(const TDuration startTime)
{
    TDuration current = GetThreadCPUTime();
    return current - startTime;
}

void MergeRunResults(const TRunResult& src, TRunResult& dst)
{
    dst.ResultTime = Min(src.ResultTime, dst.ResultTime);
    dst.GeneratorTime = Min(src.GeneratorTime, dst.GeneratorTime);
    dst.ReferenceTime = Min(src.ReferenceTime, dst.ReferenceTime);

    dst.MaxRSSDelta = Max(src.MaxRSSDelta, dst.MaxRSSDelta);
    dst.ReferenceMaxRSSDelta = Max(src.ReferenceMaxRSSDelta, dst.ReferenceMaxRSSDelta);
}