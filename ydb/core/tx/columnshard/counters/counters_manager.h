#pragma once

#include "columnshard.h"
#include "indexation.h"
#include "scan.h"
#include "column_tables.h"
#include "writes_monitor.h"
#include "tablet_counters.h"
#include "background_controller.h"

#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/core/protos/counters_columnshard.pb.h>
#include <ydb/core/protos/counters_datashard.pb.h>
#include <ydb/core/base/appdata_fwd.h>
#include <library/cpp/time_provider/time_provider.h>

namespace NKikimr::NColumnShard {

class TCountersManager {
private:
    YDB_READONLY(TCSCounters, CSCounters, {});
    YDB_READONLY(TIndexationCounters, EvictionCounters, TIndexationCounters("Eviction"));
    YDB_READONLY(TIndexationCounters, IndexationCounters, TIndexationCounters("Indexation"));
    YDB_READONLY(TIndexationCounters, CompactionCounters, TIndexationCounters("GeneralCompaction"));
    YDB_READONLY(TScanCounters, ScanCounters, TScanCounters("Scan"));
    std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TSubscriberCounters> SubscribeCounters;

    TBackgroundControllerCounters BackgroundControllerCounters;
    TColumnTablesCounters ColumnTablesCounters;

    const TTabletCountersHandle TabletCounters;
    TWritesMonitor WritesMonitor;

public:
    TCountersManager(TTabletCountersBase& tabletCounters)
        : SubscribeCounters(std::make_shared<NOlap::NResourceBroker::NSubscribe::TSubscriberCounters>())
        , TabletCounters(tabletCounters)
        , WritesMonitor(tabletCounters) {
    }

    const TTabletCountersHandle& GetTabletCounters() const {
        return TabletCounters;
    }

    TWritesMonitor& GetWritesMonitor() {
        return WritesMonitor;
    }

    const TWritesMonitor& GetWritesMonitor() const {
        return WritesMonitor;
    }

    std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TSubscriberCounters> GetSubscribeCounters() {
        return SubscribeCounters;
    }

    TColumnTablesCounters& GetColumnTableCounters() {
        return ColumnTablesCounters;
    }

    TBackgroundControllerCounters& GetBackgroundControllerCounters() {
        return BackgroundControllerCounters;
    }
};

} // namespace NKikimr::NColumnShard