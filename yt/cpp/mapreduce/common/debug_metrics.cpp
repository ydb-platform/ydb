#include "debug_metrics.h"

#include <util/generic/hash.h>
#include <util/generic/singleton.h>

#include <util/string/cast.h>
#include <util/system/mutex.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class TDebugMetrics {
public:
    static TDebugMetrics& Get()
    {
        return *Singleton<TDebugMetrics>();
    }

    void Inc(TStringBuf name)
    {
        auto g = Guard(Lock_);
        auto it = Metrics_.find(name);
        if (it == Metrics_.end()) {
            it = Metrics_.emplace(ToString(name), 0).first;
        }
        ++it->second;
    }

    ui64 Get(TStringBuf name) const
    {
        auto g = Guard(Lock_);
        auto it = Metrics_.find(name);
        if (it == Metrics_.end()) {
            return 0;
        } else {
            return it->second;
        }
    }

private:
    TMutex Lock_;
    THashMap<TString, ui64> Metrics_;
};

////////////////////////////////////////////////////////////////////////////////

void IncDebugMetricImpl(TStringBuf name)
{
    TDebugMetrics::Get().Inc(name);
}

ui64 GetDebugMetric(TStringBuf name)
{
    return TDebugMetrics::Get().Get(name);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
