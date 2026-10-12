#pragma once

#include "flat_scan_iface.h"
#include "flat_saved_row.h"
#include "flat_table_subset.h"

namespace NKikimr {
namespace NTable {

    /*_ TDeltaScan, a version scan that yields the difference between two
        row versions.

        For every key whose visible image differs between the From and To
        versions it calls FeedDelta(key, oldState, newState) with the image
        visible at From (oldState) and at To (newState). A key that did not
        exist at the corresponding version is reported with ERowOp::Absent, so
        a consumer can tell create / update / delete apart.

        Keys that are unchanged, including keys that were not touched in
        (From, To], are not reported at all.

        TDeltaScan is an ordinary IVersionScan and therefore is queued and
        driven by the regular scan machinery. The caller has to queue it on a
        table subset taken at a snapshot at (or after) the To version, so that
        the whole version history down to From stays available.
     */
    class TDeltaScan: public IVersionScan {
    public:
        TDeltaScan(TRowVersion from, TRowVersion to)
            : From(from)
            , To(to)
        {
            Y_ENSURE(From <= To, "Delta scan From version is newer than To");
        }

        TRowVersion GetFrom() const noexcept { return From; }
        TRowVersion GetTo() const noexcept { return To; }

        /*_ The delta is known to be empty when no row version of the subset
            falls into (From, To]: the visible image of every key is then
            determined by the same row at both versions. Parts and memtables
            keep min/max row version bounds, so this can be answered without
            reading any rows.

            The check is conservative. Bounds are computed for a whole part,
            and unknown bounds (cold parts, uncompacted deltas) never prove
            the subset empty, so such a subset is scanned as usual.
         */
        bool ShouldSkipSubset(const TSubset& subset) const override
        {
            if (subset.ColdParts) {
                return false; /* cold parts expose no version bounds */
            }

            for (const auto& memTable : subset.Frozen) {
                if (HasVersionInRange(memTable->GetMinRowVersion(), memTable->GetMaxRowVersion())) {
                    return false;
                }
            }

            for (const auto& part : subset.Flatten) {
                if (part->TxIdStats) {
                    return false; /* part has uncompacted deltas */
                }

                if (HasVersionInRange(part->MinRowVersion, part->MaxRowVersion)) {
                    return false;
                }
            }

            return true;
        }

    protected:
        /* Called for every key whose visible image changed between From and To.

            oldState and newState describe the row at the From and To versions
            respectively. A row that did not exist at the given version has
            ERowOp::Absent.
         */
        virtual EScan FeedDelta(TArrayRef<const TCell> key, const TRowState& oldState, const TRowState& newState) = 0;

    private:
        TInitialState Prepare(IDriver*, TIntrusiveConstPtr<TScheme>) override
        {
            TConf conf;
            conf.NoErased = false; /* a version scan needs technical rows too */

            return { EScan::Feed, conf };
        }

        EScan BeginKey(TArrayRef<const TCell> key) override
        {
            Key = TOwnedCellVec::Make(key);
            OldRow = TSavedRow();
            NewRow = TSavedRow();
            HaveOld = false;
            HaveNew = false;
            NumCols = 0;

            return EScan::Feed;
        }

        EScan BeginDeltas() override
        {
            return EScan::Feed;
        }

        EScan Feed(const TRow&, ui64) override
        {
            /* Uncommitted deltas don't belong to any snapshot, ignore them */
            return EScan::Feed;
        }

        EScan Feed(ELockMode, ui64) override
        {
            /* Locks don't belong to any snapshot, ignore them */
            return EScan::Feed;
        }

        EScan EndDeltas() override
        {
            return EScan::Feed;
        }

        EScan Feed(const TRow& row, TRowVersion& rowVersion) override
        {
            NumCols = row.Size();

            if (!HaveNew) {
                /* This is the first version at or below To, it gives the new image */
                if (To < rowVersion) {
                    /* The image is newer than To, keep walking to older versions */
                    return EScan::Feed;
                }

                NewRow.Save(row);
                HaveNew = true;

                if (rowVersion <= From) {
                    /* No version in (From, To], the image is the same */
                    OldRow = NewRow;
                    HaveOld = true;
                    rowVersion = TRowVersion::Min();
                }
            } else if (From < rowVersion) {
                /* Still newer than From, keep walking to older versions */
                return EScan::Feed;
            } else {
                /* The first version at or below From gives the old image */
                OldRow.Save(row);
                HaveOld = true;
                rowVersion = TRowVersion::Min();
            }

            return EScan::Feed;
        }

        EScan EndKey() override
        {
            if (!HaveNew && !HaveOld) {
                return EScan::Feed;
            }

            if (HaveNew && HaveOld && IsSame(OldRow, NewRow)) {
                return EScan::Feed;
            }

            TRowState oldState(0), newState(0);
            MakeState(OldRow, HaveOld, oldState);
            MakeState(NewRow, HaveNew, newState);

            return FeedDelta(Key, oldState, newState);
        }

        static bool IsPresent(ERowOp op) noexcept
        {
            return op == ERowOp::Upsert || op == ERowOp::Reset;
        }

        static bool IsSame(const TSavedRow& a, const TSavedRow& b)
        {
            if (IsPresent(a.Op) != IsPresent(b.Op)) {
                return false;
            }
            if (!IsPresent(a.Op)) {
                return true;
            }
            if (a.Slots.size() != b.Slots.size() || a.Buf != b.Buf) {
                return false;
            }
            for (size_t i = 0; i < a.Slots.size(); i++) {
                if (a.Slots[i].Size != b.Slots[i].Size ||
                    a.Slots[i].CellOp.Raw() != b.Slots[i].CellOp.Raw())
                {
                    return false;
                }
            }
            return true;
        }

        void MakeState(const TSavedRow& row, bool have, TRowState& out) const
        {
            if (have && IsPresent(row.Op)) {
                row.Restore(out);
            } else {
                out.Init(NumCols);
            }
        }

        /* Whether a row with the given version bounds may have a version in
            (From, To], i.e. whether the row may change the image between the
            two versions of the scan.
         */
        bool HasVersionInRange(const TRowVersion& minVersion, const TRowVersion& maxVersion) const noexcept
        {
            return maxVersion > From && minVersion <= To;
        }

    private:
        const TRowVersion From;
        const TRowVersion To;

        TOwnedCellVec Key;
        TSavedRow OldRow;
        TSavedRow NewRow;
        ui32 NumCols = 0;
        bool HaveOld = false;
        bool HaveNew = false;
    };

}
}
