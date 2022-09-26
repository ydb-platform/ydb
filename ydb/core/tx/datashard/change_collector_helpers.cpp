#include "change_collector_helpers.h"

namespace NKikimr {
namespace NDataShard {

using namespace NTable;

namespace {

    TString SerializeKey(TArrayRef<const TRawTypeValue> key) {
        TVector<TCell> cells(Reserve(key.size()));
        for (const auto& k : key) {
            cells.emplace_back(k.AsRef());
        }

        return TSerializedCellVec::Serialize(cells);
    }

} // anonymous

EReady TRowsCache::SelectRow(TDatabase& db, ui32 tid,
        TArrayRef<const TRawTypeValue> key, const THashMap<TTag, TPos>& keyTagToPos,
        TArrayRef<const TTag> tags, TRowState& rowState, const TRowVersion& readVersion)
{
    const auto* row = FindCachedRow(tid, key);

    if (!row) {
        TRowState selected;
        const auto ready = db.Select(tid, key, tags, selected, 0, readVersion);

        if (ready == EReady::Page) {
            return ready;
        }

        row = CacheRow(tid, ready, key, tags, selected);
    }

    rowState.Init(tags.size());
    rowState.Touch(row->Rop);

    for (TPos pos = 0; pos < tags.size(); ++pos) {
        auto it = keyTagToPos.find(tags.at(pos));
        if (it == keyTagToPos.end()) {
            continue;
        }

        Y_VERIFY(it->second < key.size());
        rowState.Set(pos, ECellOp::Set, key.at(it->second).AsRef());
    }

    switch (row->Ready) {
    case EReady::Data:
        switch (row->Rop) {
        case ERowOp::Upsert:
        case ERowOp::Reset:
            for (TPos pos = 0; pos < tags.size(); ++pos) {
                const auto tag = tags.at(pos);
                if (keyTagToPos.contains(tag)) {
                    continue;
                }

                if (const auto* cell = row->Cells.FindPtr(tag)) {
                    rowState.Set(pos, ECellOp::Set, TCell(cell->data(), cell->size()));
                } else {
                    rowState.Set(pos, ECellOp::Null, TCell());
                }
            }
            break;
        default:
            Y_FAIL("unreachable");
        }
        break;
    case EReady::Gone:
        break;
    default:
        Y_FAIL("unreachable");
    }

    Y_VERIFY(rowState.IsFinalized());
    return row->Ready;
}

void TRowsCache::UpdateCachedRow(ui32 tid, ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const TUpdateOp> updates)
{
    auto& row = Rows[tid][SerializeKey(key)];

    row.Rop = rop;
    if (rop == ERowOp::Erase) {
        row.Ready = EReady::Gone;
        row.Cells.clear();
        return;
    }

    row.Ready = EReady::Data;
    if (rop == ERowOp::Reset) {
        row.Cells.clear();
    }

    for (const auto& update : updates) {
        if (update.Value.IsEmpty()) {
            continue;
        }

        row.Cells[update.Tag] = update.Value.ToStringBuf();
    }
}

void TRowsCache::Reset() {
    Rows.clear();
}

const TRowsCache::TRow* TRowsCache::CacheRow(ui32 tid, EReady ready,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const TTag> tags, const TRowState& rowState)
{
    Y_VERIFY(ready != EReady::Page);
    Y_VERIFY(tags.size() == rowState.Size());

    auto& row = Rows[tid][SerializeKey(key)];

    row.Ready = ready;
    if (ready == EReady::Gone) {
        row.Rop = ERowOp::Erase;
        return &row;
    }

    row.Rop = rowState.GetRowState();
    for (TPos pos = 0; pos < tags.size(); ++pos) {
        const auto& cell = rowState.Get(pos);
        if (cell.IsNull()) {
            continue;
        }

        row.Cells[tags.at(pos)] = cell.AsBuf();
    }

    return &row;
}

const TRowsCache::TRow* TRowsCache::FindCachedRow(ui32 tid, TArrayRef<const TRawTypeValue> key) const {
    const auto* cached = Rows.FindPtr(tid);
    return cached
        ? cached->FindPtr(SerializeKey(key))
        : nullptr;
}

} // NDataShard
} // NKikimr
