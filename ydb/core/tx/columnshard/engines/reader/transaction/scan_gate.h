#pragma once
#include <ydb/core/tx/columnshard/common/snapshot.h>

#include <ydb/library/actors/core/event.h>

#include <util/generic/map.h>

#include <memory>
#include <optional>
#include <vector>

namespace NKikimr::NColumnShard {
class TTabletCountersHandle;
}

namespace NKikimr::NOlap {
class IColumnEngine;
class ITableMetadataAccessor;
class TPKRangesFilter;
}   // namespace NKikimr::NOlap

namespace NKikimr::NOlap::NReader {

// what a scan reads, built at the shard entrance once its snapshot is readable
struct TScanTarget {
    TSnapshot Snapshot;
    std::shared_ptr<ITableMetadataAccessor> TableMetadataAccessor;
    std::shared_ptr<TPKRangesFilter> PKRangesFilter;
};

// a held scan is re-sent to the shard as is, so its entrance checks it again
class TScanGate {
private:
    struct TWaitingForLoadedPortions {
        TAutoPtr<NActors::IEventHandle> Ev;
        TScanTarget Target;
    };

    const std::shared_ptr<const NColumnShard::TTabletCountersHandle> TabletCounters;
    TMultiMap<TSnapshot, TAutoPtr<NActors::IEventHandle>> WaitingForSnapshot;
    std::vector<TWaitingForLoadedPortions> WaitingForLoadedPortions;

    static bool ArePortionsLoaded(const IColumnEngine* engine, const TScanTarget& target);
    void UpdateCounters() const;

public:
    explicit TScanGate(const std::shared_ptr<const NColumnShard::TTabletCountersHandle>& tabletCounters)
        : TabletCounters(tabletCounters)
    {
    }

    // false: the gate holds ev
    template <class TEventPtr>
    bool PassSnapshot(TEventPtr& ev, const TSnapshot& readVersion, const TSnapshot& maxReadVersion) {
        if (readVersion <= maxReadVersion) {
            return true;
        }
        WaitingForSnapshot.emplace(readVersion, ev.Release());
        UpdateCounters();
        return false;
    }

    // false: the gate holds ev
    template <class TEventPtr>
    bool PassLoadedPortions(TEventPtr& ev, const IColumnEngine* engine, const TScanTarget& target) {
        if (ArePortionsLoaded(engine, target)) {
            return true;
        }
        WaitingForLoadedPortions.push_back({ ev.Release(), target });
        UpdateCounters();
        return false;
    }

    // returns the plan step the first held scan still waits for
    std::optional<ui64> ReleaseReadable(const TSnapshot& maxReadVersion);
    void ReleaseLoadedPortions(const IColumnEngine& engine);
    ui64 GetMemoryUsage() const;
};

}   // namespace NKikimr::NOlap::NReader
