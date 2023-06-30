#include "kqp_resolve.h"

// #define DBG_TRACE

#ifdef DBG_TRACE
#include <ydb/core/base/appdata.h>
#include <ydb/core/tx/datashard/range_ops.h>
#endif
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>

namespace NKikimr {
namespace NKqp {

using namespace NMiniKQL;
using namespace NYql;
using namespace NYql::NNodes;

TVector<TCell> MakeKeyCells(const NKikimr::NUdf::TUnboxedValue& value, const TVector<NScheme::TTypeInfo>& keyColumnTypes,
    const TVector<ui32>& keyColumnIndices, const NMiniKQL::TTypeEnvironment& typeEnv, bool copyValues)
{
    TVector<TCell> key(keyColumnTypes.size());
    for (ui32 i = 0; i < key.size(); ++i) {
        auto columnValue = value.GetElement(keyColumnIndices[i]);
        key[i] = NMiniKQL::MakeCell(keyColumnTypes[i], columnValue, typeEnv, copyValues);
    }

    return key;
}

TTableId MakeTableId(const TKqpTable& node) {
    auto nodePathId = TKikimrPathId::Parse(node.PathId());

    TTableId tableId;
    tableId.PathId = TPathId(nodePathId.OwnerId(), nodePathId.TableId());
    tableId.SysViewInfo = node.SysView();
    tableId.SchemaVersion = FromString<ui64>(node.Version());
    return tableId;
}

TTableId MakeTableId(const NKqpProto::TKqpPhyTableId& table) {
    TTableId tableId;
    tableId.PathId = TPathId(table.GetOwnerId(), table.GetTableId());
    tableId.SysViewInfo = table.GetSysView();
    tableId.SchemaVersion = table.GetVersion();
    return tableId;
}

TVector<TPartitionWithRange> GetKeyRangePartitions(const TTableRange& range,
    const TVector<TKeyDesc::TPartitionInfo>& partitions, const TVector<NScheme::TTypeInfo>& keyColumnTypes)
{
    auto it = std::lower_bound(partitions.begin(), partitions.end(), true,
        [&range, &keyColumnTypes](const auto& partition, bool) {
            const int cmp = CompareBorders<true, false>(partition.Range->EndKeyPrefix.GetCells(), range.From,
                partition.Range->IsInclusive || partition.Range->IsPoint, range.InclusiveFrom || range.Point,
                keyColumnTypes);

            return (cmp < 0);
        });

    MKQL_ENSURE_S(it != partitions.end());
    auto affectedPartitions = std::distance(it, partitions.end());

#ifdef DBG_TRACE
    auto& typeRegistry = *AppData()->TypeRegistry;
    Cerr << (TStringBuilder() << "-- read range: " << DebugPrintRange(keyColumnTypes, range, typeRegistry)
         << ", affected partitions: " << affectedPartitions << Endl);
#endif

    TVector<TPartitionWithRange> rangePartitions;
    rangePartitions.reserve(affectedPartitions);
    do {
#ifdef DBG_TRACE
        Cerr << (TStringBuilder() << "-- add partition: "
             << DebugPrintPartitionInfo(*it, keyColumnTypes, typeRegistry)
             << "  (total: " << partitions.size() << ")" << Endl);
#endif

        if (range.Point) {
            TPartitionWithRange ret(it);
            if (!it->Range->IsPoint) {
                ret.PointOrRange = TSerializedCellVec(range.From);
            } else {
                ret.FullRange.emplace(TSerializedTableRange(range));
            }
            rangePartitions.emplace_back(std::move(ret));

#ifdef DBG_TRACE
{
            auto& x = rangePartitions.back();
            TStringBuilder sb;
            sb << "-- added point: ";
            if (x.FullRange.has_value()) {
                sb << "FULL " << DebugPrintRange(keyColumnTypes, x.FullRange->ToTableRange(), typeRegistry);
            } else {
                sb << "point " << DebugPrintPoint(keyColumnTypes, std::get<TSerializedCellVec>(x.PointOrRange).GetCells(), typeRegistry);
            }
            sb << Endl;
            Cerr << sb;
}
#endif
            break;
        }

        TConstArrayRef<TCell> fromValues;
        TConstArrayRef<TCell> toValues;
        bool inclusiveFrom;
        bool inclusiveTo;
        int fullRange = 0;

        if (rangePartitions.empty()) {
            fromValues = range.From;
            inclusiveFrom = range.InclusiveFrom;
            if (it == partitions.begin() && !fromValues.empty() && fromValues.begin()->IsNull()) {
                fullRange = 1;
            }
        } else {
            fromValues = rangePartitions.back().PartitionInfo->Range->EndKeyPrefix.GetCells();
            inclusiveFrom = !rangePartitions.back().PartitionInfo->Range->IsInclusive;
            fullRange = 1;
        }

        const int prevCmp = CompareBorders<true, true>(it->Range->EndKeyPrefix.GetCells(), range.To,
            it->Range->IsPoint || it->Range->IsInclusive, range.InclusiveTo, keyColumnTypes);

        if (prevCmp > 0) {
            toValues = range.To;
            inclusiveTo = range.InclusiveTo;
        } else {
            toValues = it->Range->EndKeyPrefix.GetCells();
            inclusiveTo = it->Range->IsInclusive;
            fullRange += 1;
        }

        bool point = false;
        if (inclusiveFrom && inclusiveTo && fromValues.size() == keyColumnTypes.size()) {
            if (toValues.empty()) {
                point = !fromValues.back().IsNull();
            } else if (fromValues.size() == toValues.size()) {
                point = CompareTypedCellVectors(fromValues.data(), toValues.data(), keyColumnTypes.data(), keyColumnTypes.size()) == 0;
            }
        }

        rangePartitions.emplace_back(TPartitionWithRange(it));

        if (point) {
            if (fullRange == 2) {
                rangePartitions.back().FullRange.emplace(TSerializedTableRange(fromValues, true, fromValues, true));
                rangePartitions.back().FullRange->Point = true;
            } else {
                rangePartitions.back().PointOrRange = TSerializedCellVec(fromValues);
            }
        } else {
            auto r = TTableRange(fromValues, inclusiveFrom, toValues, inclusiveTo);
            if (fullRange == 2) {
                rangePartitions.back().FullRange.emplace(TSerializedTableRange(r));
            } else {
                rangePartitions.back().PointOrRange = TSerializedTableRange(r);
            }
        }

#ifdef DBG_TRACE
{
        auto& x = rangePartitions.back();
        TStringBuilder sb;
        sb << "-- added range: ";
        if (x.FullRange.has_value()) {
            sb << "FULL " << DebugPrintRange(keyColumnTypes, x.FullRange->ToTableRange(), typeRegistry);
        } else if (std::holds_alternative<TSerializedCellVec>(x.PointOrRange)) {
            sb << "point " << DebugPrintPoint(keyColumnTypes, std::get<TSerializedCellVec>(x.PointOrRange).GetCells(), typeRegistry);
        } else {
            sb << DebugPrintRange(keyColumnTypes, std::get<TSerializedTableRange>(x.PointOrRange).ToTableRange(), typeRegistry);
        }
        sb << Endl;
        Cerr << sb;
}
#endif

        if (prevCmp >= 0) {
            break;
        }
    } while (++it != partitions.end());

    return rangePartitions;
}

#undef DBG_TRACE
} // namespace NKqp
} // namespace NKikimr
