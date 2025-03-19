#include "ref_counted_tracker_statistics_producer.h"

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
    TYsonProducer GetProducer()
    {
        return Producer_;
    }

private:
    TCachingRefCountedTrackerStatisticsManager()
        : Producer_(BIND([this] (IYsonConsumer* consumer) {
            Produce(consumer, *GetCachedStatistics());
        }))
    { }

    DECLARE_LEAKY_SINGLETON_FRIEND()

    const TYsonProducer Producer_;

    static constexpr auto CachedStatisticsTtl = TDuration::Seconds(5);

    NThreading::TSpinLock CachedStatisticsLock_;
    TIntrusivePtr<TRefCountedTrackerStatistics> CachedStatistics_;
    TInstant CachedStatisticsUpdateTime_;

    TIntrusivePtr<TRefCountedTrackerStatistics> GetCachedStatistics()
    {
        auto now = TInstant::Now();

        // Fast path.
        {
            auto guard = Guard(CachedStatisticsLock_);
            if (CachedStatistics_ && now < CachedStatisticsUpdateTime_ + CachedStatisticsTtl) {
                return CachedStatistics_;
            }
        }

        // Slow path.
        auto statistics = New<TRefCountedTrackerStatistics>(TRefCountedTracker::Get()->GetStatistics());
        {
            auto guard = Guard(CachedStatisticsLock_);
            if (!CachedStatistics_ || now > CachedStatisticsUpdateTime_ + CachedStatisticsTtl) {
                // Avoid destruction under spinlock.
                std::swap(CachedStatistics_, statistics);
                CachedStatisticsUpdateTime_ = now;
            }
            return CachedStatistics_;
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
