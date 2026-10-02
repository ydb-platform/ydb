#pragma once

// Divides a snapshot's key space into consecutive sampling units.
// Every row belongs to one unit; callers select or skip whole units.
//
// The owner at each key is the covering part with the most rows, ties by label.
// Its main-group page boundaries and owner changes delimit units. Where no part
// covers the keys, a deterministic hash picks memtable keys as boundaries.
//
// Part slices -> BuildLayout -> owner regions
//                                    |
//                     +--------------+--------------+
//                     |                             |
//                owner present                  no owner
//                     |                             |
//         main-group index separators      hashed memtable keys
//                     |                             |
//                     +--------> key blocks <-------+
//                                    |
//                             caller's decision
//                               +-- skip: no data-page read
//                               `-- select: normal MVCC read of the block
//
// LayoutId and SelectionKey, together with a table namespace and seed, let
// callers repeat sampling choices. Selected units are read from all parts and
// memtables through the normal table iterator, with snapshot visibility.
//
// Row visibility comes from the caller's MVCC read version; this iterator
// never reads rows. The layout is a per-Execute view of the LSM used only to
// cut intervals and may change between Executes. Callers must keep committed
// decisions: retain a selected unit's bounds until fully read, carry cursor
// inclusivity, never redraw.

#include "flat_part_slice.h"
#include "flat_row_eggs.h"
#include "flat_table_subset.h"

#include <ydb/core/scheme/scheme_tablecell.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NKikimr {
namespace NTable {

    struct TKeyBlock {
        TBounds Bounds;
        TString SelectionKey; // tagged binary encoding of the lower bound
        ui64 OwnerRows = 0; // physical owner-page rows, not merged live rows
        bool FromMemtable = false; // no part owns this unit
    };

    struct TOwnerRegion {
        TBounds Bounds;
        const TPart* Owner = nullptr; // borrowed from the subset; null = memtable anchors
    };

    struct TKeyBlocksLayout {
        TVector<TOwnerRegion> Regions; // disjoint, sorted, cover (-inf, +inf)
        TString LayoutId; // 16-byte digest of layout inputs
    };

    struct TKeyBlocksTelemetry {
        // Local to one iterator/Execute; retry totals may count the same pages again.
        // Units visited by Seek/Next since the last Seek.
        ui64 UnitsTotal = 0;
        ui64 UnitsMemtable = 0;
        // The counters below are not cleared by Seek.
        ui64 OwnerRowsPerUnitMax = 0;
        // Unique owner main-group bytes encountered during unit discovery.
        ui64 OwnerMainGroupBytes = 0;
        ui64 IndexPagesTouched = 0; // unique pages over the iterator's lifetime
        // Memtable keys examined while locating units.
        ui64 MemtableKeysVisited = 0;
        // Snapshot counts; Parts and Slices exclude cold parts.
        ui32 Parts = 0;
        ui32 Memtables = 0;
        ui32 Slices = 0;
    };

    class TKeyBlockIterator {
    public:
        struct TConf {
            ui32 MemtableStride = 64; // positive; selects about 1/stride memtable keys
            ui64 AnchorSalt = 0x5A4D504C45; // memtable anchor hash seed
        };

        // Builds owner regions without page reads; O(L log L) in slice edges.
        // Build once per Execute; the layout borrows parts from this subset.
        static TKeyBlocksLayout BuildLayout(
                const TSubset& subset,
                TConf conf,
                TIntrusiveConstPtr<TKeyCellDefaults> keys);

        // Subset, env, and layout must outlive the iterator.
        // Keep subset/layout unchanged; match BuildLayout's subset, keys, and conf.
        // Recreate on the next Execute and Seek at the saved read position.
        TKeyBlockIterator(
                const TSubset& subset,
                IPages* env,
                TIntrusiveConstPtr<TKeyCellDefaults> keys,
                TConf conf,
                const TKeyBlocksLayout& layout);
        ~TKeyBlockIterator();

        TKeyBlockIterator(const TKeyBlockIterator&) = delete;
        TKeyBlockIterator& operator=(const TKeyBlockIterator&) = delete;

        // Seek/Next: Data = valid unit; Gone/Page invalidate the position.
        // On Seek(Page), repeat Seek. On Next(Page), restart past the previous unit:
        // Seek(previous.Bounds.LastKey.GetCells(), !previous.Bounds.LastInclusive).
        // Next after Page is an error; after Gone it stays Gone.
        // Seek uses Before(key) if inclusive, After(key) otherwise.
        // Empty key = -inf; missing suffix cells = +inf.
        EReady Seek(TArrayRef<const TCell> key, bool inclusive);
        EReady Next();
        bool IsValid() const;
        // Full current unit, even after a seek into its middle; requires IsValid().
        const TKeyBlock& Get() const;

        TKeyBlocksTelemetry Telemetry() const;
        // Cold parts are omitted from unit boundaries.
        bool HasColdParts() const;

    private:
        struct TState;
        THolder<TState> State;
    };

}
}
