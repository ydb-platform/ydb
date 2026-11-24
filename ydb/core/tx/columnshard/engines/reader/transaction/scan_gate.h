#pragma once
#include <ydb/core/tx/columnshard/common/snapshot.h>

#include <ydb/library/actors/core/event.h>

#include <util/generic/map.h>

#include <memory>
#include <optional>
#include <vector>

namespace NKikimr::NColumnShard {
class TTabletCountersHandle;
class TTablesManager;
}   // namespace NKikimr::NColumnShard

namespace NKikimr::NOlap {
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
    const NColumnShard::TTablesManager& TablesManager;
    TMultiMap<TSnapshot, TAutoPtr<NActors::IEventHandle>> WaitingForSnapshot;
    std::vector<TWaitingForLoadedPortions> WaitingForLoadedPortions;

    bool ArePortionsLoaded(const TScanTarget& target) const;
    void UpdateCounters() const;

public:
    TScanGate(
        const std::shared_ptr<const NColumnShard::TTabletCountersHandle>& tabletCounters, const NColumnShard::TTablesManager& tablesManager)
        : TabletCounters(tabletCounters)
        , TablesManager(tablesManager)
    {
    }

    // false: the gate holds ev
    template <class TEventPtr>
    bool PassReadableSnapshot(TEventPtr& ev, const TSnapshot& readVersion, const TSnapshot& maxReadVersion) {
        if (readVersion <= maxReadVersion) {
            return true;
        }
        WaitingForSnapshot.emplace(readVersion, ev.Release());
        UpdateCounters();
        return false;
    }

    // false: the gate holds ev
    template <class TEventPtr>
    bool PassLoadedPortions(TEventPtr& ev, const TScanTarget& target) {
        if (ArePortionsLoaded(target)) {
            return true;
        }
        WaitingForLoadedPortions.push_back({ ev.Release(), target });
        UpdateCounters();
        return false;
    }

    // returns the plan step the first held scan still waits for
    std::optional<ui64> ReleaseReadableSnapshot(const TSnapshot& maxReadVersion);
    void ReleaseLoadedPortions();
    ui64 GetMemoryUsage() const;
};

}   // namespace NKikimr::NOlap::NReader
