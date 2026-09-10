#include "ref_counted_tracker_statistics_producer.h"

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/memory/leaky_singleton.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

void Produce(IYsonConsumer* consumer, const TRefCountedTrackerStatistics& statistics)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("statistics")
                .DoListFor(statistics.NamedStatistics, [] (TFluentList fluent, const auto& namedSlotStatistics) {
                    fluent
                        .Item().BeginMap()
                            .Item("name").Value(namedSlotStatistics.FullName)
                            .Item("objects_alive").Value(namedSlotStatistics.ObjectsAlive)
                            .Item("objects_allocated").Value(namedSlotStatistics.ObjectsAllocated)
                            .Item("bytes_alive").Value(namedSlotStatistics.BytesAlive)
                            .Item("bytes_allocated").Value(namedSlotStatistics.BytesAllocated)
                        .EndMap();
                })
            .Item("total").BeginMap()
                .Item("objects_alive").Value(statistics.TotalStatistics.ObjectsAlive)
                .Item("objects_allocated").Value(statistics.TotalStatistics.ObjectsAllocated)
                .Item("bytes_alive").Value(statistics.TotalStatistics.BytesAlive)
                .Item("bytes_allocated").Value(statistics.TotalStatistics.BytesAllocated)
            .EndMap()
        .EndMap();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TYsonProducer CreateRefCountedTrackerStatisticsProducer()
{
    return BIND([] (IYsonConsumer* consumer) {
        Produce(consumer, TRefCountedTracker::Get()->GetStatistics());
    });
}

////////////////////////////////////////////////////////////////////////////////

class TCachingRefCountedTrackerStatisticsManager
{
public:
    TYsonProducer GetProducer() const
    {
        return Producer_;
    }

private:
    TCachingRefCountedTrackerStatisticsManager()
        : Producer_(BIND([this] (IYsonConsumer* consumer) {
            consumer->OnRaw(GetCachedYson());
        }))
    { }

    DECLARE_LEAKY_SINGLETON_FRIEND()

    const TYsonProducer Producer_;

    static constexpr auto CachedYsonTtl = TDuration::Seconds(5);

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, CachedYsonLock_);
    TYsonString CachedStatisticsYson_;
    TInstant CachedYsonUpdateTime_;

    // Renders the process-global ref-counted tracker statistics into YSON at most
    // once per TTL for the whole process. Multiple monitoring managers (one per daemon,
    // e.g. inside a multidaemon) share this single rendering instead of each walking the
    // tracker and serializing the identical data on every update.
    TYsonString GetCachedYson()
    {
        auto now = TInstant::Now();

        // Fast path.
        {
            auto guard = Guard(CachedYsonLock_);
            if (CachedStatisticsYson_ && now < CachedYsonUpdateTime_ + CachedYsonTtl) {
                return CachedStatisticsYson_;
            }
        }

        // Slow path: walk the tracker and serialize outside the lock.
        auto yson = ConvertToYsonString(CreateRefCountedTrackerStatisticsProducer());
        {
            auto guard = Guard(CachedYsonLock_);
            if (!CachedStatisticsYson_ || now > CachedYsonUpdateTime_ + CachedYsonTtl) {
                CachedStatisticsYson_ = std::move(yson);
                CachedYsonUpdateTime_ = now;
            }
            return CachedStatisticsYson_;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TYsonProducer GetCachingRefCountedTrackerStatisticsProducer()
{
    return LeakySingleton<TCachingRefCountedTrackerStatisticsManager>()->GetProducer();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
