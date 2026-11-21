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
        AdmissionTimeout = Base_ + 3,
        ResumeScan = Base_ + 4,
    };

    struct TEvAdmissionTimeout : public TEventLocal<TEvAdmissionTimeout, ui32(EEv::AdmissionTimeout)> {
        const ui64 Serial;
        const ui64 Generation;
        TEvAdmissionTimeout(ui64 serial, ui64 generation) : Serial(serial), Generation(generation) {}
    };

    struct TEvResumeScan : public TEventLocal<TEvResumeScan, ui32(EEv::ResumeScan)> {
        const ui64 Serial;
        explicit TEvResumeScan(ui64 serial) : Serial(serial) {}
    };

    struct TEvResult: public TEventLocal<TEvResult, ui32(EEv::Result)> {
        using EStatus = NTable::EStatus;

        TEvResult(ui64 serial, EStatus abort, THolder<TScanSnapshot> snapshot,
                    TAutoPtr<IDestructable> result)
            : Serial(serial)
            , Status(abort)
            , Result(result)
            , Barrier(std::move(snapshot->Barrier))
            , Subset(std::move(snapshot->Subset))
        {

        }

        // First member is destroyed after the snapshot/result payload.
        std::shared_ptr<void> ResourceReservation;
        ui64 Serial = 0;
        EStatus Status = EStatus::Done;
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
