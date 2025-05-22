#include "printout.h"

#include <sys/resource.h>

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

void MergeRunResults(const TRunResult& src, TRunResult& dst)
{
    dst.ResultTime = Min(src.ResultTime, dst.ResultTime);
    dst.GeneratorTime = Min(src.GeneratorTime, dst.GeneratorTime);
    dst.ReferenceTime = Min(src.ReferenceTime, dst.ReferenceTime);

    dst.MaxRSSDelta = Max(src.MaxRSSDelta, dst.MaxRSSDelta);
    dst.ReferenceMaxRSSDelta = Max(src.ReferenceMaxRSSDelta, dst.ReferenceMaxRSSDelta);
}