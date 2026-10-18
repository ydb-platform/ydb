#include "kqp_snapshot_tables.h"

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/kqp/query_data/kqp_query_data.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <util/generic/algorithm.h>

namespace NKikimr::NKqp {
namespace {

using TSnapshotTable = NKikimrTxUserProxy::TCreateVolatileSnapshotTx::TTable;

bool FillBound(const NKqpProto::TKqpPhyKeyBound& bound, TQueryData& queryData, NKikimrMiniKQL::TParams& result) {
    result.MutableType()->SetKind(NKikimrMiniKQL::Tuple);
    result.MutableValue();
    for (const auto& value : bound.GetValues()) {
        NKikimrMiniKQL::TType ownedType;
        NKikimrMiniKQL::TValue ownedValue;
        const NKikimrMiniKQL::TType* type = &ownedType;
        const NKikimrMiniKQL::TValue* item = &ownedValue;
        auto convert = [&](const Ydb::TypedValue& typedValue) {
            ConvertYdbTypeToMiniKQLType(typedValue.type(), ownedType);
            ConvertYdbValueToMiniKQLValue(typedValue.type(), typedValue.value(), ownedValue);
        };
        if (value.HasLiteralValue()) {
            convert(value.GetLiteralValue());
        } else {
            // TPreparedQueryHolder only keeps ranges bound by parameters
            // supplied with the query, so the value is available by now.
            const TString& name = value.HasParamValue()
                ? value.GetParamValue().GetParamName()
                : value.GetParamElementValue().GetParamName();
            const auto* parameter = queryData.GetParameterTypedValue(name);
            if (!parameter) {
                return false;
            }
            convert(*parameter);
            if (value.HasParamElementValue()) {
                const auto index = value.GetParamElementValue().GetElementIndex();
                if (type->GetKind() != NKikimrMiniKQL::Tuple ||
                    index >= type->GetTuple().ElementSize() || index >= item->TupleSize())
                {
                    return false;
                }
                type = &type->GetTuple().GetElement(index);
                item = &item->GetTuple(index);
            }
        }

        // TxProxy's key tuple format expects optional elements, including for
        // NOT NULL keys. KQP's physical bounds may contain non-optional values.
        auto* elementType = result.MutableType()->MutableTuple()->AddElement();
        auto* element = result.MutableValue()->AddTuple();
        if (type->GetKind() == NKikimrMiniKQL::Data) {
            elementType->SetKind(NKikimrMiniKQL::Optional);
            elementType->MutableOptional()->MutableItem()->CopyFrom(*type);
            element->MutableOptional()->CopyFrom(*item);
        } else if (type->GetKind() == NKikimrMiniKQL::Optional &&
            type->GetOptional().GetItem().GetKind() == NKikimrMiniKQL::Data)
        {
            elementType->CopyFrom(*type);
            element->CopyFrom(*item);
        } else {
            return false;
        }
    }
    return true;
}

bool ParseBound(const NKikimrMiniKQL::TParams& bound, const TVector<NScheme::TTypeInfo>& keyTypes,
    bool expandWithNulls, TSerializedCellVec& result)
{
    TVector<TCell> cells;
    TVector<TString> memoryOwner;
    TString error;
    if (!NMiniKQL::CellsFromTuple(&bound.GetType(), bound.GetValue(), keyTypes,
        {}, false, cells, error, memoryOwner))
    {
        return false;
    }
    if (expandWithNulls) {
        cells.resize(keyTypes.size());
    }
    result = TSerializedCellVec(cells);
    return true;
}

enum class EKeyRange {
    Ok,
    Empty,
    Unsupported,
};

EKeyRange FillKeyRange(const NKqpProto::TKqpPhyKeyRange& range, const TTableConstInfo& table,
    TQueryData& queryData, NKikimrTxUserProxy::TKeyRange& result)
{
    if (!FillBound(range.GetFrom(), queryData, *result.MutableFrom())) {
        return EKeyRange::Unsupported;
    }
    // The compiler also sets RangeIsPoint for identical inclusive key prefixes.
    // Like ExtractRanges in kqp_partition_helper.cpp, only treat a complete key
    // as a point; a shorter prefix still describes a range of rows.
    if (range.GetRangeIsPoint() && range.GetFrom().ValuesSize() == table.KeyColumnTypes.size()) {
        result.MutableTo()->CopyFrom(result.GetFrom());
        result.SetFromInclusive(true);
        result.SetToInclusive(true);
        // A complete key is never an empty range, parsing only checks the types.
        TSerializedCellVec key;
        return ParseBound(result.GetFrom(), table.KeyColumnTypes, false, key)
            ? EKeyRange::Ok
            : EKeyRange::Unsupported;
    }
    if (!FillBound(range.GetTo(), queryData, *result.MutableTo())) {
        return EKeyRange::Unsupported;
    }
    result.SetFromInclusive(range.GetFrom().GetIsInclusive());
    result.SetToInclusive(range.GetTo().GetIsInclusive());
    // TxProxy rejects empty ranges, so they have to be detected here. Bounds
    // are expanded the same way TxProxy does it in resolvereq.cpp.
    TSerializedCellVec from;
    TSerializedCellVec to;
    if (!ParseBound(result.GetFrom(), table.KeyColumnTypes, result.GetFromInclusive(), from) ||
        !ParseBound(result.GetTo(), table.KeyColumnTypes, !result.GetToInclusive(), to))
    {
        return EKeyRange::Unsupported;
    }
    if (TTableRange(from.GetCells(), result.GetFromInclusive(), to.GetCells(), result.GetToInclusive())
        .IsEmptyRange(table.KeyColumnTypes))
    {
        return EKeyRange::Empty;
    }
    return EKeyRange::Ok;
}

} // namespace

TVector<TSnapshotTable> BuildSnapshotTables(const TPreparedQueryHolder& preparedQuery, TQueryData& queryData) {
    THashMap<TString, const TTableConstInfo*> infos;
    for (const auto& [_, info] : preparedQuery.GetTableConstInfo()) {
        infos[info->Path] = info.Get();
    }

    TVector<TSnapshotTable> result;
    result.reserve(preparedQuery.GetQueryTables().size());
    for (const auto& table : preparedQuery.GetQueryTables()) {
        const auto* info = infos.Value(table.Path, nullptr);
        bool wholeTable = table.KeyRanges.empty()
            || !info || info->TableKind != ETableKind::Datashard || info->KeyColumnTypes.empty();

        TVector<TSnapshotTable> ranges;
        if (!wholeTable) {
            for (const auto* range : table.KeyRanges) {
                TSnapshotTable entry;
                entry.SetTablePath(table.Path);
                switch (FillKeyRange(*range, *info, queryData, *entry.MutableKeyRange())) {
                    case EKeyRange::Ok:
                        ranges.push_back(std::move(entry));
                        break;
                    case EKeyRange::Empty:
                        break;
                    case EKeyRange::Unsupported:
                        wholeTable = true;
                        break;
                }
                if (wholeTable) {
                    break;
                }
            }
        }

        // A table that is only read by empty ranges is still snapshotted as a
        // whole, the same way it was before ranges were passed to TxProxy.
        if (wholeTable || ranges.empty()) {
            result.emplace_back().SetTablePath(table.Path);
        } else {
            std::move(ranges.begin(), ranges.end(), std::back_inserter(result));
        }
    }
    return result;
}

size_t CountBoundedTables(const TVector<TSnapshotTable>& tables) {
    return CountIf(tables, [](const TSnapshotTable& table) { return table.HasKeyRange(); });
}

} // namespace NKikimr::NKqp
