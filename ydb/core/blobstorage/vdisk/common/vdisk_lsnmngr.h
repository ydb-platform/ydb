#pragma once
#include "defs.h"
#include "circlebufresize.h"
#include <ydb/core/blobstorage/base/vdisk_lsn.h>
#include <ydb/core/util/serializable_access_check.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TLsnAllocTracker
    ////////////////////////////////////////////////////////////////////////////
    class TLsnAllocTracker {
    public:
        TLsnAllocTracker(ui64 confirmedLsn)
            : InFly(16)
        {
            AtomicSet(ConfirmedLsn, confirmedLsn);
        }

        ~TLsnAllocTracker() = default;
        TLsnAllocTracker(const TLsnAllocTracker &) = delete;
        TLsnAllocTracker &operator = (const TLsnAllocTracker &) = delete;
        TLsnAllocTracker(TLsnAllocTracker &&) = delete;
        TLsnAllocTracker &operator = (TLsnAllocTracker &&) = delete;

        void Allocated(TLsnSeg seg) {
            auto m = Guard(WriteLock);
            Y_ABORT_UNLESS(InFly.Empty() || InFly.Back().Last < seg.First,
                     "this# %s seg# %s", ToString().data(), seg.ToString().data());
            InFly.Push(seg);
        }

        void Confirmed(TLsnSeg seg) {
            auto m = Guard(WriteLock);
            Y_ABORT_UNLESS(seg.Last != ui64(-1) &&
                     seg.Last != 0 &&
                     static_cast<ui64>(AtomicGet(ConfirmedLsn)) < seg.Last &&
                     !InFly.Empty() &&
                     InFly.Top() == seg,
                     "this# %s seg# %s", ToString().data(), seg.ToString().data());
            AtomicSet(ConfirmedLsn, seg.Last);
            InFly.Pop();
        }

        // returned confirmed, i.e. already committed lsn
        ui64 GetConfirmed() const {
            return static_cast<ui64>(AtomicGet(ConfirmedLsn));
        }

        void Output(IOutputStream &str) const {
            str << "{InFly# [";
            TAllocFreeQueue<TLsnSeg>::TIterator it(InFly);
            it.SeekToFirst();
            while (it.Valid()) {
                str << " " << it.Get().ToString();
                it.Next();
            }
            str << "] ConfirmedLsn# " << static_cast<ui64>(AtomicGet(ConfirmedLsn));
            str << "}";
        }

        TString ToString() const {
            TStringStream str;
            Output(str);
            return str.Str();
        }

    private:
        TAllocFreeQueue<TLsnSeg> InFly;
        // ConfirmedLsn is lsn already committed to database (logger returned
        // success for it)
        TAtomic ConfirmedLsn = 0;
        // Write access to this class is not synchronized, so it must be done
        // from the single mailbox, this is doublechecked with WriteLock (which is
        // not 'lock' buy rather a check that there is no concurrent write access);
        // parallel read access is allowed and done via GetAtomic
        TSerializableAccessChecker WriteLock;
    };



    ////////////////////////////////////////////////////////////////////////////
    // DATABASE
    ////////////////////////////////////////////////////////////////////////////
    class TLsnMngr : public TThrRefBase {
        // Basic data
    private:
        // NOTE
        // CurrentLsn is monotonously increasing 'log sequence number'.
        // CurrentLsn is allocated from different threads, we use LogWriter
        // for lsns proper ordering while writing to log. CurrentLsnPtr points
        // to CurrentLsn after recovery process has been finished, otherwise
        // CurrentLsnPtr has value nullptr, which guards us from using incorrect
        // lsn
        const ui64 OriginallyRecoveredLsn = 0;
        const ui64 StartLsn = 0;
        TAtomic CurrentLsn = 0;
        TAtomic *CurrentLsnPtr = nullptr;

        // NOTE about lsn allocation for Hull and SyncLog
        // Lsn goes without holes, but not every lsn goes to Hull or SyncLog, because
        // some Lsns are for internal purposes, say entry point commits.
        // Not every update of Hull goes to SyncLog for some reasons, so we use
        // the following trackers:
        // AllocForHull -- allocations to Hull
        // AllocForSyncLog -- allocations to SyncLog
        // Take into account that currently any allocation for SyncLog implies
        // allocation for Hull. The opposite is wrong.
        std::unique_ptr<TLsnAllocTracker> AllocForHull;
        std::unique_ptr<TLsnAllocTracker> AllocForSyncLog;

        // basic lsn allocation
        TLsnSeg AllocLsn(ui64 lsnAdvance = 1) {
            Y_DEBUG_ABORT_UNLESS(CurrentLsnPtr && lsnAdvance > 0);
            TAtomicBase inc(lsnAdvance);
            TAtomicBase val = AtomicAdd(*CurrentLsnPtr, inc);
            ui64 right = static_cast<ui64>(val);
            ui64 left = right - lsnAdvance + 1;
            Y_DEBUG_ABORT_UNLESS(left != 0); // we never allocate zero lsn!
            return TLsnSeg(left, right);
        }

        static ui64 BuildStartLsn(ui64 recoveredLsn, bool shift) {
            ui64 startLsn = recoveredLsn;
            if (startLsn == 0) {
                // empty db, keep it empty
            } else if (shift) {
                // otherwise shift lsn
                const ui64 shift = 1000000;
                startLsn += shift / 2 - 1;
                startLsn = startLsn / shift * shift + shift;
            }
            return startLsn;
        }

    public:
        TLsnMngr(ui64 recoveredLsn, ui64 lsnToSyncLogRecovered, bool shift)
            : OriginallyRecoveredLsn(recoveredLsn)
            , StartLsn(BuildStartLsn(recoveredLsn, shift))
        {
            AtomicSet(CurrentLsn, StartLsn);
            CurrentLsnPtr = &CurrentLsn;

            // Init trackers
            AllocForHull = std::make_unique<TLsnAllocTracker>(StartLsn);
            AllocForSyncLog = std::make_unique<TLsnAllocTracker>(lsnToSyncLogRecovered);
        }

        TLsnMngr() = delete;
        ~TLsnMngr() = default;
        TLsnMngr(const TLsnMngr &) = delete;
        TLsnMngr(TLsnMngr &&) = delete;
        TLsnMngr &operator=(const TLsnMngr &) = delete;
        TLsnMngr &operator=(TLsnMngr &&) = delete;

        ////////////////////////////// LSN ALLOCATION //////////////////////////////////////
        // update some structures (not hull)
        TLsnSeg AllocLsnForLocalUse(ui64 lsnAdvance = 1) {
            return AllocLsn(lsnAdvance);
        }

        // hull db update
        TLsnSeg AllocLsnForHull(ui64 lsnAdvance = 1) {
            TLsnSeg seg = AllocLsn(lsnAdvance);
            AllocForHull->Allocated(seg);
            return seg;
        }

        // hull db and synclog update
        TLsnSeg AllocLsnForHullAndSyncLog(ui64 lsnAdvance = 1) {
            TLsnSeg seg = AllocLsnForHull(lsnAdvance);
            AllocForSyncLog->Allocated(seg);
            return seg;
        }

        // hull db update
        TLsnSeg AllocDiscreteLsnBatchForHull(ui64 lsnAdvance) {
            TLsnSeg seg = AllocLsn(lsnAdvance);
            for (ui64 lsn = seg.First; lsn <= seg.Last; ++lsn) {
                TLsnSeg point(lsn, lsn);
                AllocForHull->Allocated(point);
            }
            return seg;
        }

        // hull db and synclog update
        TLsnSeg AllocDiscreteLsnBatchForHullAndSyncLog(ui64 lsnAdvance) {
            TLsnSeg seg = AllocDiscreteLsnBatchForHull(lsnAdvance);
            for (ui64 lsn = seg.First; lsn <= seg.Last; ++lsn) {
                TLsnSeg point(lsn, lsn);
                AllocForSyncLog->Allocated(point);
            }
            return seg;
        }
        ////////////////////////////// LSN ALLOCATION //////////////////////////////////////

        ////////////////////////////// LSN CONFIRMATION ////////////////////////////////////
        void ConfirmLsnForHull(TLsnSeg seg, bool syncLogAlso) {
            AllocForHull->Confirmed(seg);
            if (syncLogAlso) {
                AllocForSyncLog->Confirmed(seg);
            }
        }
        ////////////////////////////// LSN CONFIRMATION ////////////////////////////////////

        ////////////////////////////// LSN GETTERS /////////////////////////////////////////
        ui64 GetLsn() const {
            Y_DEBUG_ABORT_UNLESS(CurrentLsnPtr);
            return static_cast<ui64>(AtomicGet(*CurrentLsnPtr));
        }

        // hull confirmed lsn
        ui64 GetConfirmedLsnForHull() const {
            return AllocForHull->GetConfirmed();
        }

        // synclog confirmed lsn
        ui64 GetConfirmedLsnForSyncLog() const {
            return AllocForSyncLog->GetConfirmed();
        }

        // lsn we starting with after local recovery and lsn shift
        ui64 GetStartLsn() const {
            Y_ABORT_UNLESS(CurrentLsnPtr);
            return StartLsn;
        }

        ////////////////////////////// LSN GETTERS /////////////////////////////////////////
        ui64 GetOriginallyRecoveredLsn() const {
            return OriginallyRecoveredLsn;
        }
    };

} // NKikimr
