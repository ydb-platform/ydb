#pragma once

#include "util_basics.h"
#include "flat_scan_iface.h"
#include "flat_scan_eggs.h"
#include "flat_part_outset.h"
#include "flat_fwd_sieve.h"
#include "flat_table_subset.h"
#include "flat_executor_misc.h"
#include <ydb/library/actors/core/event_local.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NOps {

    enum class EEv : ui32 {
        Base_ = EventSpaceBegin(TKikimrEvents::ES_FLAT_EXECUTOR) + 1024,
        Result  = Base_ + 0,
        ScanStat = Base_ + 1,
        Continue = Base_ + 2,
    };

    struct TEvResult: public TEventLocal<TEvResult, ui32(EEv::Result)> {
        using EAbort = NTable::EAbort;

        TEvResult(ui64 serial, EAbort abort, THolder<TScanSnapshot> snapshot,
                    TAutoPtr<IDestructable> result)
            : Serial(serial)
            , Status(abort)
            , Result(result)
            , Barrier(std::move(snapshot->Barrier))
            , Subset(std::move(snapshot->Subset))
        {

        }

        ui64 Serial = 0;
        EAbort Status = EAbort::None;
        TAutoPtr<IDestructable> Result;
        TIntrusivePtr<TBarrier> Barrier;
        TAutoPtr<NTable::TSubset> Subset;
        TAutoPtr<NTable::NFwd::TSeen> Trace; /* Seen blobs but not materialized */
    };

    struct TEvScanStat : public TEventLocal<TEvScanStat, ui32(EEv::ScanStat)> {
        ui64 ElapsedUs;
        ui64 ProcessedRows;
        ui64 SkippedRows;

        TEvScanStat(ui64 elapsedUs, ui64 processedRows, ui64 skippedRows)
            : ElapsedUs(elapsedUs)
            , ProcessedRows(processedRows)
            , SkippedRows(skippedRows)
        { }
    };

    struct TEvContinue : public TEventLocal<TEvContinue, ui32(EEv::Continue)> {
        // nothing
    };

}
}
}
