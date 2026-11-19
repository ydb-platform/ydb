#pragma once

#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/stats/scan/collector.h>

#include <ydb/library/actors/core/event_local.h>

namespace NKikimr::NOlap::NStats::NScan {

struct TEvScanStats {
    struct TEvStatsUpdate: NActors::TEventLocal<TEvStatsUpdate, TEvColumnShard::EvStatsUpdate> {
        TStatsUpdate Update;

        explicit TEvStatsUpdate(TStatsUpdate update)
            : Update(std::move(update))
        {
        }
    };
};

}   // namespace NKikimr::NOlap::NStats::NScan
