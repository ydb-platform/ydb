#include "range_ops.h"


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
