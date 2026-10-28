#include "read_iterator_sampling.h"

#include <util/digest/city.h>
#include <util/random/fast.h>

#include <cmath>

namespace NKikimr::NDataShard {

namespace {

TString ParseSamplingKey(
        const TString& raw,
        TSerializedCellVec& key,
        TConstArrayRef<NScheme::TTypeInfo> keyTypes)
{
    if (raw.empty()) {
        return {};
    }
    if (!TSerializedCellVec::TryParse(raw, key)) {
        return "sampling key is not a serialized cell vector";
    }
    const auto cells = key.GetCells();
    if (cells.size() != keyTypes.size()) {
        return "sampling key width does not match the primary key";
    }
    for (size_t i = 0; i < keyTypes.size(); ++i) {
        if (TString error = NScheme::HasUnexpectedValueSize(cells[i], keyTypes[i])) {
            return error;
        }
        if (!cells[i].IsNull() && keyTypes[i].GetTypeId() == NScheme::NTypeIds::Pg) {
            if (auto error = NPg::PgNativeBinaryValidate(
                    TStringBuf(cells[i].Data(), cells[i].Size()), keyTypes[i].GetPgTypeDesc()))
            {
                return *error;
            }
        }
    }
    return {};
}

void AppendLe(TString& out, ui64 value, size_t bytes) {
    for (size_t i = 0; i < bytes; ++i) {
        out.push_back(char((value >> (8 * i)) & 0xff));
    }
}

} // namespace

int CompareSamplingPos(
        const TSamplingPos& left,
        const TSamplingPos& right,
        TConstArrayRef<NScheme::TTypeInfo> keyTypes)
{
    if (!left.Key) {
        return !right.Key ? int(right.Before) - int(left.Before) : (left.Before ? -1 : 1);
    }
    if (!right.Key) {
        return right.Before ? 1 : -1;
    }
    const auto leftCells = left.Key.GetCells();
    const auto rightCells = right.Key.GetCells();
    const size_t width = Min(keyTypes.size(), Max(leftCells.size(), rightCells.size()));
    for (size_t i = 0; i < width; ++i) {
        const TCell leftCell = i < leftCells.size() ? leftCells[i] : TCell();
        const TCell rightCell = i < rightCells.size() ? rightCells[i] : TCell();
        if (int cmp = CompareTypedCells(leftCell, rightCell, NScheme::TTypeInfoOrder(keyTypes[i]))) {
            return cmp;
        }
    }
    // Before(K) precedes After(K).
    return int(right.Before) - int(left.Before);
}

TSamplingPos SamplingStart(const NTable::TBounds& bounds) {
    return {bounds.FirstKey, !bounds.FirstKey || bounds.FirstInclusive};
}

TSamplingPos SamplingEnd(const NTable::TBounds& bounds) {
    // An inclusive end is After(key).
    return {bounds.LastKey, bounds.LastKey && !bounds.LastInclusive};
}

TSamplingPos SamplingRangeStart(const TSerializedTableRange& range) {
    return {range.From, !range.From || range.FromInclusive};
}

TSamplingPos SamplingRangeEnd(const TSerializedTableRange& range) {
    return {range.To, range.To && !range.ToInclusive};
}

std::optional<NTable::TBounds> ClipSamplingBounds(
        const NTable::TBounds& bounds,
        const TSamplingPos& from,
        const TSamplingPos& to,
        TConstArrayRef<NScheme::TTypeInfo> keyTypes)
{
    TSamplingPos start = SamplingStart(bounds);
    TSamplingPos end = SamplingEnd(bounds);
    if (CompareSamplingPos(start, from, keyTypes) < 0) {
        start = from;
    }
    if (CompareSamplingPos(to, end, keyTypes) < 0) {
        end = to;
    }
    if (CompareSamplingPos(start, end, keyTypes) >= 0) {
        return std::nullopt;
    }
    return NTable::TBounds(start.Key, end.Key, start.Before, !end.IsPosInf() && !end.Before);
}

bool ParseSamplingBounds(
        const NKikimrTxDataShard::TReadSamplingBounds& proto,
        NTable::TBounds& bounds,
        TString& error,
        TConstArrayRef<NScheme::TTypeInfo> keyTypes)
{
    NTable::TBounds parsed;
    if (error = ParseSamplingKey(proto.GetFirstKey(), parsed.FirstKey, keyTypes); error) {
        return false;
    }
    if (error = ParseSamplingKey(proto.GetLastKey(), parsed.LastKey, keyTypes); error) {
        return false;
    }
    parsed.FirstInclusive = !parsed.FirstKey || proto.GetFirstInclusive();
    parsed.LastInclusive = parsed.LastKey && proto.GetLastInclusive();
    if (CompareSamplingPos(SamplingStart(parsed), SamplingEnd(parsed), keyTypes) >= 0) {
        error = "sampling interval is empty";
        return false;
    }
    bounds = std::move(parsed);
    return true;
}

void SaveSamplingBounds(
        const NTable::TBounds& bounds,
        NKikimrTxDataShard::TReadSamplingBounds& proto)
{
    proto.Clear();
    if (bounds.FirstKey) {
        proto.SetFirstKey(bounds.FirstKey.GetBuffer());
        proto.SetFirstInclusive(bounds.FirstInclusive);
    }
    if (bounds.LastKey) {
        proto.SetLastKey(bounds.LastKey.GetBuffer());
        proto.SetLastInclusive(bounds.LastInclusive);
    }
}

ui64 SamplingThreshold(double rate) {
    // Keep rates below 2^-64 nonzero: a zero draw still selects a unit.
    if (rate == 1.0) {
        return Max<ui64>();
    }
    return Max<ui64>(1, static_cast<ui64>(std::ldexp(rate, 64)));
}

TSamplingSelector::TSamplingSelector(
        ui64 tabletId, ui32 localTid, TStringBuf layoutId, ui64 seed, ui64 threshold)
    : Seed(seed)
    , Threshold(threshold)
{
    TString ns;
    ns.push_back(char(1));
    AppendLe(ns, tabletId, 8);
    AppendLe(ns, localTid, 4);
    ns.append(layoutId);
    NamespaceHash = CityHash64(ns.data(), ns.size());
}

bool TSamplingSelector::Draw(TStringBuf selectionKey) const {
    if (Threshold == Max<ui64>()) {
        return true;
    }
    TFastRng64 rng(CityHash64WithSeeds(selectionKey.data(), selectionKey.size(), NamespaceHash, Seed));
    return rng.GenRand() < Threshold;
}

}
