#pragma once

// Sampling for forward TEvRead ranges. NTable::TKeyBlockIterator divides the
// key space into units using part-page boundaries and memtable key anchors.
// Rows in each unit share a decision; Rate controls the selection probability.
// Rate 1 selects every unit.
//
// ranges -> unit selection
//                +-- skip --> advance without reading data pages
//                `-- select --> normal MVCC read --> advance
//
// Selected units are clipped to the requested range and read from all parts and
// memtables. For a fixed tablet, table and layout, the seed and unit key determine
// each choice. Changing the storage layout may change the sample.
//
// The MVCC snapshot fixes row visibility, but does not freeze the layout between
// read executions. Reads yield through the normal TEvRead stream on time and
// quota limits or page faults. Continuations keep the cursor and any unfinished
// selected interval, so unread rows retain their selection decision.

#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tablet_flat/flat_part_slice.h>

#include <optional>
#include <util/generic/string.h>

namespace NKikimr::NDataShard {

// Before(K) precedes row K; After(K) follows it.
// An empty key is -inf when Before is set and +inf otherwise.
struct TSamplingPos {
    TSerializedCellVec Key;
    bool Before = true;

    bool IsNegInf() const { return !Key && Before; }
    bool IsPosInf() const { return !Key && !Before; }
};

// Finite keys are full-width; supported request prefixes are padded with NULLs.
int CompareSamplingPos(
        const TSamplingPos& left,
        const TSamplingPos& right,
        TConstArrayRef<NScheme::TTypeInfo> keyTypes);

TSamplingPos SamplingStart(const NTable::TBounds& bounds);
TSamplingPos SamplingEnd(const NTable::TBounds& bounds);

TSamplingPos SamplingRangeStart(const TSerializedTableRange& range);
TSamplingPos SamplingRangeEnd(const TSerializedTableRange& range);

// Empty when [from, to) does not intersect bounds.
std::optional<NTable::TBounds> ClipSamplingBounds(
        const NTable::TBounds& bounds,
        const TSamplingPos& from,
        const TSamplingPos& to,
        TConstArrayRef<NScheme::TTypeInfo> keyTypes);

bool ParseSamplingBounds(
        const NKikimrTxDataShard::TReadSamplingBounds& proto,
        NTable::TBounds& bounds,
        TString& error,
        TConstArrayRef<NScheme::TTypeInfo> keyTypes);

void SaveSamplingBounds(
        const NTable::TBounds& bounds,
        NKikimrTxDataShard::TReadSamplingBounds& proto);

// Rate must be finite and in (0, 1]. Rate 1 selects every unit.
// Probability is rounded down in steps of 2^-64, with a 2^-64 minimum.
ui64 SamplingThreshold(double rate);

// Each unit seeds a PCG draw, stable within one (tablet, table, layout, seed) namespace.
class TSamplingSelector {
public:
    TSamplingSelector() = default;
    TSamplingSelector(ui64 tabletId, ui32 localTid, TStringBuf layoutId, ui64 seed, ui64 threshold);

    bool Draw(TStringBuf selectionKey) const;

private:
    ui64 NamespaceHash = 0;
    ui64 Seed = 0;
    ui64 Threshold = 0;
};

}
