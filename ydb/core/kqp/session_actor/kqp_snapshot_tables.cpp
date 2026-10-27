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
            if (value.HasParamElementValue()) {
                // Convert only the referenced element: other fields of the tuple
                // may have types that cannot be converted, e.g. Null.
                const auto index = value.GetParamElementValue().GetElementIndex();
                if (parameter->type().type_case() != Ydb::Type::kTupleType ||
                    index >= static_cast<ui32>(parameter->type().tuple_type().elements_size()) ||
                    index >= static_cast<ui32>(parameter->value().items_size()))
                {
                    return false;
                }
                Ydb::TypedValue tupleElement;
                *tupleElement.mutable_type() = parameter->type().tuple_type().elements(index);
                *tupleElement.mutable_value() = parameter->value().items(index);
                convert(tupleElement);
            } else {
                convert(*parameter);
            }
        }

        // TxProxy's key tuple format expects optional elements, including for
        // NOT NULL keys. KQP's physical bounds may contain non-optional values.
        auto* elementType = result.MutableType()->MutableTuple()->AddElement();
        auto* element = result.MutableValue()->AddTuple();
        if (ownedType.GetKind() == NKikimrMiniKQL::Data) {
            elementType->SetKind(NKikimrMiniKQL::Optional);
            elementType->MutableOptional()->MutableItem()->CopyFrom(ownedType);
            element->MutableOptional()->CopyFrom(ownedValue);
        } else if (ownedType.GetKind() == NKikimrMiniKQL::Optional &&
            ownedType.GetOptional().GetItem().GetKind() == NKikimrMiniKQL::Data)
        {
            elementType->CopyFrom(ownedType);
            element->CopyFrom(ownedValue);
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

// Returns false if the range cannot be safely represented as a TxProxy key
// range: the bounds are not convertible, or the range is empty.
bool FillKeyRange(const NKqpProto::TKqpPhyKeyRange& range, const TTableConstInfo& table,
    TQueryData& queryData, NKikimrTxUserProxy::TKeyRange& result)
{
    if (!FillBound(range.GetFrom(), queryData, *result.MutableFrom())) {
        return false;
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
        return ParseBound(result.GetFrom(), table.KeyColumnTypes, false, key);
    }
    if (!FillBound(range.GetTo(), queryData, *result.MutableTo())) {
        return false;
    }
    result.SetFromInclusive(range.GetFrom().GetIsInclusive());
    result.SetToInclusive(range.GetTo().GetIsInclusive());
    // Bounds are expanded the same way TxProxy does it in resolvereq.cpp.
    TSerializedCellVec from;
    TSerializedCellVec to;
    if (!ParseBound(result.GetFrom(), table.KeyColumnTypes, result.GetFromInclusive(), from) ||
        !ParseBound(result.GetTo(), table.KeyColumnTypes, !result.GetToInclusive(), to))
    {
        return false;
    }
    // TxProxy rejects empty ranges. They cannot be skipped either: the legacy
    // scan still sends an empty (inverted) range to the shard that holds its
    // From bound, and that shard needs the snapshot.
    return !TTableRange(from.GetCells(), result.GetFromInclusive(), to.GetCells(), result.GetToInclusive())
        .IsEmptyRange(table.KeyColumnTypes);
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
                if (!FillKeyRange(*range, *info, queryData, *entry.MutableKeyRange())) {
                    wholeTable = true;
                    break;
                }
                ranges.push_back(std::move(entry));
            }
        }

        if (wholeTable) {
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
