#include "range_ops.h"

NKikimr::TTableRange NKikimr::Intersect(TConstArrayRef<NScheme::TTypeInfo> types, const TTableRange& first, const TTableRange& second)
{
    // all variants
    //=================
    //-----------[aaa]---
    //----[bbb]----------
    //=================
    //-----------[aaa]---
    //----[bbbbbb]-------
    //=================
    //-----------[aaa]---
    //----[bbbbbbbb]-----
    //=================
    //-----------[aaa]---
    //----[bbbbbbbbbb]---
    //=================
    //---------[aaa]----
    //--[bbbbbbbbbbbbb]-

    //=================
    //----[aaaaaa]----------
    //----[]----------
    //=================
    //----[aaaaaa]----------
    //----[bbb]----------
    //=================
    //----[aaaaaa]----------
    //----[bbbbbb]----------
    //=================
    //----[aaaaaa]----------
    //----[bbbbbbbb]----------


    //=================
    //-----[aaaaaaa]----------
    //-------[bbb]----------
    //=================
    //-----[aaaaaaa]----------
    //-------[bbbbb]----------
    //=================
    //-----[aaaaaaa]----------
    //-------[bbbbbbb]----------

    //=================
    //-----[aaa]----------
    //---------[]----------
    //=================
    //-----[aaa]----------
    //---------[bbb]----------


    //=================
    //-----[aaa]----------
    //------------[bbb]---

    if (first.IsEmptyRange(types)) {
        return first;
    }

    if (second.IsEmptyRange(types)) {
        return second;
    }

    int cmpFF = CompareBorders<false, false>(first.From,
                                             second.From,
                                             first.InclusiveFrom,
                                             second.InclusiveFrom,
                                             types);
    int cmpTT = CompareBorders<true, true>(first.To,
                                           second.To,
                                           first.InclusiveTo,
                                           second.InclusiveTo,
                                           types);
    int cmpFT = CompareBorders<false, true>(first.From,
                                            second.To,
                                            first.InclusiveFrom,
                                            second.InclusiveTo,
                                            types);
    int cmpTF = CompareBorders<true, false>(first.To,
                                            second.From,
                                            first.InclusiveTo,
                                            second.InclusiveFrom,
                                            types);
    if (cmpFF < 0)
    {
        if (cmpTF < 0) {
            //=================
            //-----------[aaa]----------
            //----[bbb]----------
            return TTableRange(second.From, second.InclusiveFrom,
                               first.To, first.InclusiveTo);
        } else if (cmpTF == 0) {
            //=================
            //-----------[aaa]----------
            //----[bbbbbb]----------
            return TTableRange(second.From, second.InclusiveFrom,
                               first.To, first.InclusiveTo);
        } else { // if (cmpTF > 0) {
            if (cmpTT < 0) {
                //=================
                //-----------[aaa]----------
                //----[bbbbbbbb]----------
                return TTableRange(second.From, second.InclusiveFrom,
                                   first.To, first.InclusiveTo);
            } else if (cmpTT == 0) {
                //=================
                //-----------[aaa]----------
                //----[bbbbbbbbbb]----------
                return TTableRange(second.From, second.InclusiveFrom,
                                   second.To, second.InclusiveTo);
            } else { // if (cmpTT > 0) {
                //=================
                //---------[aaa]----
                //--[bbbbbbbbbbbbb]-
                return TTableRange(second.From, second.InclusiveFrom,
                                   second.To, second.InclusiveTo);
            }
        }
    } else if (cmpFF == 0) {
        if (cmpTT < 0) {
            if (cmpTF == 0) {
                //=================
                //----[aaaaaa]----------
                //----[]----------
                return TTableRange(second.From, second.InclusiveFrom,
                                   first.To, first.InclusiveTo);
            } else if (cmpTF > 0) {
                //=================
                //----[aaaaaa]----------
                //----[bbb]----------
                return TTableRange(second.From, second.InclusiveFrom,
                                   first.To, first.InclusiveTo);
            } else { // if (cmpTF < 0)
                Y_ENSURE(false, "unreachable");
            }
        } else if (cmpTT == 0) {
            //=================
            //----[aaaaaa]----------
            //----[bbbbbb]----------
            return TTableRange(second.From, second.InclusiveFrom,
                               first.To, first.InclusiveTo);
        } else { // if (cmpTT > 0)
            //=================
            //----[aaaaaa]----------
            //----[bbbbbbbb]----------
            return TTableRange(second.From, second.InclusiveFrom,
                               second.To, second.InclusiveTo);
        }
    } else { //if (cmpFF > 0) {
        if (cmpFT < 0) {
            if (cmpTT < 0) {
                //=================
                //-----[aaaaaaa]----------
                //-------[bbb]----------
                return TTableRange(first.From, first.InclusiveFrom,
                                   first.To, first.InclusiveTo);
            } else if (cmpTT == 0) {
                //=================
                //-----[aaaaaaa]----------
                //-------[bbbbb]----------
                return TTableRange(first.From, first.InclusiveFrom,
                                   first.To, first.InclusiveTo);
            } else { //if (cmpTT > 0) {
                //=================
                //-----[aaaaaaa]----------
                //-------[bbbbbbb]----------
                return TTableRange(first.From, first.InclusiveFrom,
                                   second.To, second.InclusiveTo);
            }
        } else if (cmpFT == 0) {
            if (cmpTT == 0) {
                //=================
                //-----[aaa]----------
                //---------[]----------
                return TTableRange(first.From, first.InclusiveFrom,
                                   first.To, first.InclusiveTo);
            } else if (cmpTT > 0) {
                //=================
                //-----[aaa]----------
                //---------[bbb]----------
                return TTableRange(first.From, first.InclusiveFrom,
                                   second.To, second.InclusiveTo);
            } else {
                // cmpTT < 0
                Y_ENSURE(false, "unreachable");
            }
        } else { // if (cmpFT > 0)
            //=================
            //-----[aaa]----------
            //------------[bbb]---
            return TTableRange(first.From, first.InclusiveFrom,
                               second.To, second.InclusiveTo);
        }
    }
}

TString NKikimr::DebugPrintRange(TConstArrayRef<NScheme::TTypeInfo> types, const NKikimr::TTableRange &range,
    const NScheme::TTypeRegistry& typeRegistry)
{
    if (range.Point) {
        return DebugPrintPoint(types, range.From, typeRegistry);
    }

    return TStringBuilder()
            << (range.InclusiveFrom ? "[" : "(")
            << DebugPrintPoint(types, range.From, typeRegistry)
            << " ; "
            << DebugPrintPoint(types, range.To, typeRegistry)
            << (range.InclusiveTo ? "]" : ")");
}

TString NKikimr::DebugPrintRanges(TConstArrayRef<NScheme::TTypeInfo> types,
  const TSmallVec<TSerializedTableRange>& ranges, const NScheme::TTypeRegistry& typeRegistry)
{
    auto out = TStringBuilder();
    for (auto& range: ranges) {
      out << DebugPrintRange(types, range.ToTableRange(), typeRegistry);
      out << " ";
    }

    return out;
}

TString NKikimr::DebugPrintPoint(TConstArrayRef<NScheme::TTypeInfo> types, const TConstArrayRef<TCell> &point, const NScheme::TTypeRegistry& typeRegistry) {
    Y_ENSURE(types.size() >= point.size());
    TDbTupleRef pointRef(types.data(), point.data(), point.size());

    return DbgPrintTuple(pointRef, typeRegistry);
}

TString NKikimr::DebugPrintPartitionInfo(const TKeyDesc::TPartitionInfo& partition,
    const TVector<NScheme::TTypeInfo>& keyTypes, const NScheme::TTypeRegistry& typeRegistry)
{
    TStringBuilder range;
    if (partition.Range) {
        range << "{ EndKeyPrefix: " << DebugPrintPoint(keyTypes, partition.Range->EndKeyPrefix.GetCells(), typeRegistry)
              << ", IsInclusive: " << partition.Range->IsInclusive
              << ", IsPoint: " << partition.Range->IsPoint
              << " }";
    } else {
        range << "full";
    }

    return TStringBuilder()
        << "TPartitionInfo{"
        << " ShardId: " << partition.ShardId
        << ", Range: " << range
        << " }";
}
