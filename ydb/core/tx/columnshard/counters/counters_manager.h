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
    YDB_READONLY(TCSCounters, CSCounters, TCSCounters());
    YDB_READONLY(TIndexationCounters, EvictionCounters, TIndexationCounters("Eviction"));
    YDB_READONLY(TIndexationCounters, IndexationCounters, TIndexationCounters("Indexation"));
    YDB_READONLY(TIndexationCounters, CompactionCounters, TIndexationCounters("GeneralCompaction"));
    YDB_READONLY(TScanCounters, ScanCounters, TScanCounters("Scan"));
    YDB_READONLY_DEF(std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TSubscriberCounters>, SubscribeCounters);

    YDB_READONLY_DEF(std::shared_ptr<TBackgroundControllerCounters>, BackgroundControllerCounters);
    YDB_READONLY_DEF(std::shared_ptr<TColumnTablesCounters>, ColumnTablesCounters);

    YDB_READONLY_DEF(std::shared_ptr<const TTabletCountersHandle>, TabletCounters);
    YDB_READONLY_DEF(std::shared_ptr<TWritesMonitor>, WritesMonitor);

public:
    TCountersManager(TTabletCountersBase& tabletCounters)
        : SubscribeCounters(std::make_shared<NOlap::NResourceBroker::NSubscribe::TSubscriberCounters>())
        , BackgroundControllerCounters(std::make_shared<TBackgroundControllerCounters>())
        , ColumnTablesCounters(std::make_shared<TColumnTablesCounters>())
        , TabletCounters(std::make_shared<const TTabletCountersHandle>(tabletCounters))
        , WritesMonitor(std::make_shared<TWritesMonitor>(tabletCounters)) {
    }
};

} // namespace NKikimr::NColumnShard