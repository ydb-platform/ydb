#include "kqp_partition_helper.h"
#include "kqp_table_resolver.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/core/kqp/common/kqp_types.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/library/mkql_proto/mkql_proto.h>

#include <ydb/library/yql/dq/runtime/dq_columns_resolve.h>
#include <ydb/library/yql/dq/runtime/dq_transport.h>
#include <ydb/library/yql/utils/log/log.h>

namespace NKikimr::NKqp {

namespace {

using namespace NYql;

struct TColumnStats {
    ui32 MaxValueSizeBytes = 0;
};

struct TShardParamValuesAndRanges {
    NDqProto::TData ParamValues;
    NKikimr::NMiniKQL::TType* ParamType;
    // either FullRange or Ranges are set
    TVector<TSerializedPointOrRange> Ranges;
    std::optional<TSerializedTableRange> FullRange;
    THashMap<TString, TShardInfo::TColumnWriteInfo> ColumnWrites;
};

THashMap<ui64, TShardParamValuesAndRanges> PartitionParamByKey(
    const NUdf::TUnboxedValue& value, NKikimr::NMiniKQL::TType* type,
    const TTableId& tableId,
    const TKqpTableKeys& tableKeys, const TKeyDesc& key, const NMiniKQL::THolderFactory& holderFactory,
    const NMiniKQL::TTypeEnvironment& typeEnv)
{
    auto guard = typeEnv.BindAllocator();
    YQL_ENSURE(tableId.HasSamePath(key.TableId));
    auto& table = tableKeys.GetTable(tableId);

    THashMap<ui64, TShardParamValuesAndRanges> ret;
    THashMap<ui64, NMiniKQL::TUnboxedValueVector> shardParamValues;

    YQL_ENSURE(type->GetKind() == NMiniKQL::TType::EKind::List);
    auto* itemType = static_cast<NMiniKQL::TListType*>(type)->GetItemType();
    YQL_ENSURE(itemType->GetKind() == NMiniKQL::TType::EKind::Struct);
    auto* structType = static_cast<NMiniKQL::TStructType*>(itemType);

    const ui64 keyLen = table.KeyColumns.size();

    TVector<ui32> keyColumnIndices;
    keyColumnIndices.reserve(keyLen);
    for (auto& keyColumn : table.KeyColumns) {
        keyColumnIndices.push_back(structType->GetMemberIndex(keyColumn));
    }

    NUdf::TUnboxedValue paramValue;
    std::unique_ptr<NSharding::TShardingBase> sharding = table.BuildSharding();
    std::unique_ptr<NSharding::TUnboxedValueReader> unboxedReader;
    if (sharding) {
        unboxedReader = std::make_unique<NSharding::TUnboxedValueReader>(structType, table.GetColumnsRemap(), sharding->GetShardingColumns());
    }
    auto it = value.GetListIterator();
    while (it.Next(paramValue)) {
        ui64 shardId = 0;
        if (sharding) {
            shardId = key.GetPartitions()[sharding->CalcShardId(paramValue, *unboxedReader)].ShardId;
        } else {
            auto keyValue = MakeKeyCells(paramValue, table.KeyColumnTypes, keyColumnIndices,
                typeEnv, /* copyValues */ true);
            Y_VERIFY_DEBUG(keyValue.size() == keyLen);
            const ui32 partitionIndex = FindKeyPartitionIndex(keyValue, key.GetPartitions(), table.KeyColumnTypes,
                [](const auto& partition) { return *partition.Range; });

            shardId = key.GetPartitions()[partitionIndex].ShardId;

            auto point = TSerializedCellVec(TSerializedCellVec::Serialize(keyValue));
            auto& shardData = ret[shardId];
            if (key.GetPartitions()[partitionIndex].Range->IsPoint) {
                // singular case when partition is just a point
                shardData.FullRange.emplace(TSerializedTableRange(point.GetBuffer(), "", true, true));
                shardData.FullRange->Point = true;
                shardData.Ranges.clear();
            } else {
                shardData.Ranges.emplace_back(std::move(point));
            }
        }
        auto& shardData = ret[shardId];

        for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
            TString columnName(structType->GetMemberName(i));
            auto columnType = structType->GetMemberType(i);
            auto columnValue = paramValue.GetElement(i);

            ui32 sizeBytes = NDq::TDqDataSerializer::EstimateSize(columnValue, columnType);

            // Sanity check, we only expect table columns in param values
            Y_VERIFY_DEBUG(table.Columns.contains(columnName));

            auto& columnWrite = shardData.ColumnWrites[columnName];
            columnWrite.MaxValueSizeBytes = std::max(columnWrite.MaxValueSizeBytes, sizeBytes);
        }

        shardParamValues[shardId].emplace_back(std::move(paramValue));

        shardData.ParamType = itemType;
    }

    NDq::TDqDataSerializer dataSerializer{typeEnv, holderFactory, NDqProto::EDataTransportVersion::DATA_TRANSPORT_UV_PICKLE_1_0};
    for (auto& [shardId, data] : ret) {
        auto& batch = shardParamValues[shardId];
        ret[shardId].ParamValues = dataSerializer.Serialize(batch.begin(), batch.end(), itemType);
    }

    return ret;
}

THashMap<ui64, TShardParamValuesAndRanges> PartitionParamByKeyPrefix(
    const NUdf::TUnboxedValue& value, NKikimr::NMiniKQL::TType* type,
    const TTableId& tableId, const TKqpTableKeys& tableKeys, const TKeyDesc& key,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv)
{
    auto guard = typeEnv.BindAllocator();
    YQL_ENSURE(tableId.HasSamePath(key.TableId));
    auto& table = tableKeys.GetTable(tableId);

    THashMap<ui64, TShardParamValuesAndRanges> ret;
    THashMap<ui64, NMiniKQL::TUnboxedValueVector> shardParamValues;

    YQL_ENSURE(type->GetKind() == NMiniKQL::TType::EKind::List);
    auto itemType = static_cast<NMiniKQL::TListType&>(*type).GetItemType();
    YQL_ENSURE(itemType->GetKind() == NMiniKQL::TType::EKind::Struct);
    auto& structType = static_cast<NMiniKQL::TStructType&>(*itemType);

    const ui64 keyLen = table.KeyColumns.size();

    TVector<NScheme::TTypeInfo> keyFullType{Reserve(keyLen)};
    TVector<NScheme::TTypeInfo> keyPrefixType{Reserve(keyLen)};
    TVector<ui32> keyPrefixIndices{Reserve(keyLen)};

    for (const auto& keyColumn : table.KeyColumns) {
        auto columnInfo = NDq::FindColumnInfo(&structType, keyColumn);
        if (!columnInfo) {
            break;
        }

        auto typeInfo = NScheme::TypeInfoFromMiniKQLType(columnInfo->Type);
        keyFullType.push_back(typeInfo);
        keyPrefixType.push_back(typeInfo);
        keyPrefixIndices.push_back(columnInfo->Index);
    }

    YQL_ENSURE(!keyPrefixType.empty());

    for (ui64 i = keyFullType.size(); i < keyLen; ++i) {
        keyFullType.push_back(table.Columns.at(table.KeyColumns[i]).Type);
    }

    NUdf::TUnboxedValue paramValue;
    auto it = value.GetListIterator();
    while (it.Next(paramValue)) {
        auto fromValues = MakeKeyCells(paramValue, keyPrefixType, keyPrefixIndices, typeEnv, /* copyValues */ false);
        auto toValuesPrefix = fromValues;

        // append `from key` with nulls
        for (ui32 i = keyPrefixIndices.size(); i < keyLen; ++i) {
            fromValues.push_back(TCell()); // null
            // skip or toValuesPrefix.push_back(+inf);
        }
        Y_VERIFY_DEBUG(fromValues.size() == keyLen);

        const bool point = toValuesPrefix.size() == keyLen;

        auto range = TTableRange(fromValues, /* inclusiveFrom */ true,
                                 point ? TConstArrayRef<TCell>() : toValuesPrefix, /* inclusiveTo */ true,
                                 /* point */ point);
        TVector<TPartitionWithRange> rangePartitions = GetKeyRangePartitions(range, key.GetPartitions(), keyFullType);

        for (TPartitionWithRange& partitionWithRange : rangePartitions) {
            ui64 shardId = partitionWithRange.PartitionInfo->ShardId;

            shardParamValues[shardId].emplace_back(paramValue);

            auto& shardData = ret[shardId];
            if (partitionWithRange.FullRange) {
                shardData.FullRange = std::move(partitionWithRange.FullRange);
                shardData.Ranges.clear();
            } else if (!shardData.FullRange) {
                shardData.Ranges.emplace_back(std::move(partitionWithRange.PointOrRange));
            }
            shardData.ParamType = itemType;
        }
    }

    NDq::TDqDataSerializer dataSerializer(typeEnv, holderFactory, NDqProto::EDataTransportVersion::DATA_TRANSPORT_UV_PICKLE_1_0);
    for (auto& [shardId, data] : ret) {
        auto& batch = shardParamValues[shardId];
        data.ParamValues = dataSerializer.Serialize(batch.begin(), batch.end(), itemType);
    }

    return ret;
}

TVector<TCell> FillKeyValues(const TVector<NScheme::TTypeInfo>& keyColumnTypes, const NKqpProto::TKqpPhyKeyBound& bound,
    const TStageInfo& stageInfo, const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv)
{
    YQL_ENSURE(bound.ValuesSize() <= keyColumnTypes.size());

    TVector<TCell> keyValues;
    keyValues.reserve(bound.ValuesSize());

    for (ui32 i = 0; i < bound.ValuesSize(); ++i) {
        auto& tupleValue = bound.GetValues(i);

        TString paramName;
        TMaybe<ui32> paramIndex;
        switch (tupleValue.GetKindCase()) {
            case NKqpProto::TKqpPhyValue::kParamValue: {
                paramName = tupleValue.GetParamValue().GetParamName();
                break;
            }
            case NKqpProto::TKqpPhyValue::kParamElementValue: {
                paramName = tupleValue.GetParamElementValue().GetParamName();
                paramIndex = tupleValue.GetParamElementValue().GetElementIndex();
                break;
            }
            case NKqpProto::TKqpPhyValue::kLiteralValue: {
                const auto& literal = tupleValue.GetLiteralValue();
                auto [type, value] = ImportValueFromProto(literal.GetType(), literal.GetValue(), typeEnv, holderFactory);
                keyValues.emplace_back(NMiniKQL::MakeCell(keyColumnTypes[i], value, typeEnv, /* copy */ true));
                continue;
            }
            default: {
                YQL_ENSURE(false, "Unexpected type case " << (int) tupleValue.GetKindCase());
            }
        }

        auto [type, value] = stageInfo.Meta.Tx.Params->GetParameterUnboxedValue(paramName);
        if (paramIndex) {
            YQL_ENSURE(type->GetKind() == NKikimr::NMiniKQL::TType::EKind::Tuple);
            auto actual = static_cast<NKikimr::NMiniKQL::TTupleType*>(type);
            YQL_ENSURE(*paramIndex < actual->GetElementsCount());
            type = actual->GetElementType(*paramIndex);
            value = value.GetElement(*paramIndex);
        }

        keyValues.emplace_back(NMiniKQL::MakeCell(keyColumnTypes[i], value, typeEnv, /* copy */ true));
    }

    return keyValues;
}

TSerializedPointOrRange FillOneRange(NUdf::TUnboxedValue& begin, NUdf::TUnboxedValue& end,
    const TVector<NScheme::TTypeInfo>& keyColumnTypes, const NMiniKQL::TTypeEnvironment& typeEnv)
{
    const ui32 keyColumnsSize = keyColumnTypes.size();

    // Range tuple contains ranges over all key colums + inclusive sign
    YQL_ENSURE((keyColumnsSize + 1) == begin.GetListLength());

    auto fillKeyValues = [keyColumnsSize, &keyColumnTypes, &typeEnv](NUdf::TUnboxedValue& value) {
        TVector<TCell> keyValues;
        keyValues.reserve(keyColumnsSize);

        for (ui32 i = 0; i < keyColumnsSize; i++) {
            auto element = value.GetElement(i);

            if (!element) {
                return keyValues;
            }

            element = element.GetOptionalValue();

            if (!element) {
                keyValues.emplace_back(TCell());
                continue;
            }

            auto cell = NMiniKQL::MakeCell(keyColumnTypes[i], element, typeEnv, /* copy */ true);
            keyValues.emplace_back(std::move(cell));
        }

        return keyValues;
    };

    TVector<TCell> fromKeyValues = fillKeyValues(begin);
    TVector<TCell> toKeyValues = fillKeyValues(end);

    bool fromInclusive = !!begin.GetElement(keyColumnsSize).Get<int>();

    /*
     * Range rules:
     * - no value - +inf
     * - any other value means itself, please note that NULL is value and it is the minimum value in column
     *
     * `From` should be padded with NULL values to the count of key columns if left border is inclusive.
     *     For example table with Key1, Key2, X, Y, Z with predicate WHERE Key1 >= 10 will lead to
     *     left border [ (10, NULL), i.e. first element will be located at 10, NULL. If it is not padded, then
     *     first element will be located at 10, +inf which definitely is first element after this border in case
     *     we do not support +inf values in keys.
     *
     * `From` should not be padded if border is exclusive.
     *     For example table with Key1, Key2, X, Y, Z with predicate WHERE Key1 > 10 will lead to
     *     next left border ( (10,). I.e. the item will be located at 10, +inf, which definitely is first
     *     element after this border in case we do not support +inf values in keys.
     *
     * `To` should not be padded with NULLs when right border is not inclusive.
     *     For example table with Key1, Key2, X, Y, Z with predicate WHERE Key1 < 10 will lead to
     *     right border (10, NULL) ). I.e. the range ends at element before 10, NULL
     *
     * Note: -inf is an array full of NULLs with inclusive flag set, i.e. minimum value in table.
     * Note: For `To` border +infinity is an empty array
     */
    if (fromKeyValues.empty()) {
        fromInclusive = true;
    }

    if (fromInclusive) {
        while (fromKeyValues.size() != keyColumnsSize) {
            fromKeyValues.emplace_back(TCell());
        }
    }

    bool toInclusive = !!end.GetElement(keyColumnsSize).Get<int>();

    if (!toInclusive && !toKeyValues.empty()) {
        while (toKeyValues.size() != keyColumnsSize) {
            toKeyValues.emplace_back(TCell());
        }
    }

    bool point = false;
    if (fromInclusive && toInclusive && fromKeyValues.size() == keyColumnsSize) {
        if (toKeyValues.empty()) {
            point = true;
        } else if (toKeyValues.size() == keyColumnsSize) {
            point = CompareTypedCellVectors(fromKeyValues.data(), toKeyValues.data(), keyColumnTypes.data(), keyColumnTypes.size()) == 0;
        }
    }

    if (point) {
        YQL_CLOG(DEBUG, ProviderKqp) << "Formed point [extract predicate]: "
            << DebugPrintPoint(keyColumnTypes, fromKeyValues, *AppData()->TypeRegistry);

        return TSerializedCellVec(TSerializedCellVec::Serialize(fromKeyValues));
    }

    auto range = TSerializedTableRange(fromKeyValues, fromInclusive, toKeyValues, toInclusive);

    YQL_CLOG(DEBUG, ProviderKqp) << "Formed range [extract predicate]: "
        << DebugPrintRange(keyColumnTypes, range.ToTableRange(), *AppData()->TypeRegistry);

    return range;
}

TVector<TSerializedPointOrRange> BuildFullRange(const TVector<NScheme::TTypeInfo>& keyColumnTypes) {
    // Build range from NULL, NULL ... NULL to +inf, +inf ... +inf
    TVector<TCell> fromKeyValues(keyColumnTypes.size());

    auto range = TSerializedTableRange(fromKeyValues, true, TVector<TCell>(), false);

    YQL_CLOG(DEBUG, ProviderKqp) << "Formed full range [extract predicate]: "
        << DebugPrintRange(keyColumnTypes, range.ToTableRange(), *AppData()->TypeRegistry);

    return {std::move(range)};
}

TVector<TSerializedPointOrRange> FillRangesFromParameter(const TVector<NScheme::TTypeInfo>& keyColumnTypes,
    const NKqpProto::TKqpPhyParamValue& rangesParam, const TStageInfo& stageInfo, const NMiniKQL::TTypeEnvironment& typeEnv)
{
    auto guard = typeEnv.BindAllocator();
    TString paramName = rangesParam.GetParamName();
    auto [_, value] = stageInfo.Meta.Tx.Params->GetParameterUnboxedValue(paramName);
    YQL_ENSURE(value, "Param not found: " << paramName);

    // First element is Flow wrapping Ranges List
    YQL_ENSURE(value.IsBoxed());
    YQL_ENSURE(value.GetListLength() == 1);

    auto rangesList = value.GetElement(0);
    YQL_ENSURE(rangesList.IsBoxed());

    TVector<TSerializedPointOrRange> out;
    out.reserve(rangesList.GetListLength());

    const auto it = rangesList.GetListIterator();
    for (NUdf::TUnboxedValue range; it.Next(range);) {
        YQL_ENSURE(range.IsBoxed());
        // Range consists of two tuples: begin and end
        YQL_ENSURE(range.GetListLength() == 2);

        auto begin = range.GetElement(0);
        auto end = range.GetElement(1);

        out.emplace_back(FillOneRange(begin, end, keyColumnTypes, typeEnv));
    }

    return out;
}

template <typename PhyOpReadRanges>
TVector<TSerializedPointOrRange> FillReadRangesInternal(const TVector<NScheme::TTypeInfo>& keyColumnTypes,
    const PhyOpReadRanges& readRanges, const TStageInfo& stageInfo, const NMiniKQL::TTypeEnvironment& typeEnv)
{
    if (readRanges.HasKeyRanges()) {
        return FillRangesFromParameter(keyColumnTypes, readRanges.GetKeyRanges(), stageInfo, typeEnv);
    }

    return BuildFullRange(keyColumnTypes);
}

} // anonymous namespace

TVector<TSerializedPointOrRange> FillReadRanges(const TVector<NScheme::TTypeInfo>& keyColumnTypes,
    const NKqpProto::TKqpPhyOpReadOlapRanges& readRange, const TStageInfo& stageInfo, const NMiniKQL::TTypeEnvironment& typeEnv)
{
    return FillReadRangesInternal(keyColumnTypes, readRange, stageInfo, typeEnv);
}

TVector<TSerializedPointOrRange> FillReadRanges(const TVector<NScheme::TTypeInfo>& keyColumnTypes,
    const NKqpProto::TKqpPhyOpReadRanges& readRange, const TStageInfo& stageInfo, const NMiniKQL::TTypeEnvironment& typeEnv)
{
    return FillReadRangesInternal(keyColumnTypes, readRange, stageInfo, typeEnv);
}

TSerializedTableRange MakeKeyRange(const TVector<NScheme::TTypeInfo>& keyColumnTypes,
    const NKqpProto::TKqpPhyKeyRange& range, const TStageInfo& stageInfo, const NMiniKQL::THolderFactory& holderFactory,
    const NMiniKQL::TTypeEnvironment& typeEnv)
{
    YQL_ENSURE(range.HasFrom());
    YQL_ENSURE(range.HasTo());

    auto fromValues = FillKeyValues(keyColumnTypes, range.GetFrom(), stageInfo, holderFactory, typeEnv);
    if (range.GetFrom().GetIsInclusive()) {
        for (ui32 i = fromValues.size(); i < keyColumnTypes.size(); ++i) {
            fromValues.emplace_back(TCell());
        }
    }

    auto toValues = FillKeyValues(keyColumnTypes, range.GetTo(), stageInfo, holderFactory, typeEnv);
    if (!range.GetTo().GetIsInclusive()) {
        for (ui32 i = toValues.size(); i < keyColumnTypes.size(); ++i) {
            toValues.emplace_back(TCell());
        }
    }

    auto serialized = TSerializedTableRange(fromValues, range.GetFrom().GetIsInclusive(), toValues, range.GetTo().GetIsInclusive());
    YQL_CLOG(DEBUG, ProviderKqp) << "Formed range: "
        << DebugPrintRange(keyColumnTypes, serialized.ToTableRange(), *AppData()->TypeRegistry);

    return serialized;
}

namespace {

void FillFullRange(const TStageInfo& stageInfo, THashMap<ui64, TShardInfo>& shardInfoMap, bool read) {
    for (ui64 i = 0; i < stageInfo.Meta.ShardKey->GetPartitions().size(); ++i) {
        auto& partition = stageInfo.Meta.ShardKey->GetPartitions()[i];
        auto& partitionRange = *partition.Range;
        auto& shardInfo = shardInfoMap[partition.ShardId];

        auto& ranges = read ? shardInfo.KeyReadRanges : shardInfo.KeyWriteRanges;

        ranges.ConstructInPlace();

        if (partitionRange.IsPoint) {
            YQL_ENSURE(partitionRange.IsInclusive);
            auto point = TSerializedTableRange(partitionRange.EndKeyPrefix.GetCells(), true, {}, true);
            point.Point = true;

            ranges->MakeFullRange(std::move(point));
            continue;
        }

        if (i != 0) {
            auto& prevPartition = stageInfo.Meta.ShardKey->GetPartitions()[i - 1];

            ranges->MakeFull(TSerializedTableRange(prevPartition.Range->EndKeyPrefix.GetCells(), !prevPartition.Range->IsInclusive,
                partitionRange.EndKeyPrefix.GetCells(), partitionRange.IsInclusive));
        } else {
            TVector<TCell> fromValues;
            for (auto x : partitionRange.EndKeyPrefix.GetCells()) {
                Y_UNUSED(x);
                fromValues.emplace_back(TCell());
            }

            ranges->MakeFullRange(TSerializedTableRange(fromValues, true,
                partitionRange.EndKeyPrefix.GetCells(), partitionRange.IsInclusive));
        }
    }
}
} // anonymous namespace

TString TShardInfo::ToString(const TVector<NScheme::TTypeInfo>& keyTypes, const NScheme::TTypeRegistry& typeRegistry) const
{
    TStringBuilder sb;
    sb << "TShardInfo{ ";
    sb << "ReadRanges: " << (KeyReadRanges ? KeyReadRanges->ToString(keyTypes, typeRegistry) : "<none>");
    sb << ", WriteRanges: " << (KeyWriteRanges ? KeyWriteRanges->ToString(keyTypes, typeRegistry) : "<none>");
    sb << ", Parameters: {";
    for (auto& param: Params) {
        sb << param.first << ", ";
    }
    sb << "} }";
    return sb;
}

THashMap<ui64, TShardInfo> PrunePartitions(const TKqpTableKeys& tableKeys,
    const NKqpProto::TKqpPhyOpReadRange& readRange, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv)
{
    auto guard = typeEnv.BindAllocator();
    const auto* table = tableKeys.FindTablePtr(stageInfo.Meta.TableId);
    YQL_ENSURE(table);

    const auto& keyColumnTypes = table->KeyColumnTypes;
    YQL_ENSURE(readRange.HasKeyRange());

    auto range = MakeKeyRange(keyColumnTypes, readRange.GetKeyRange(), stageInfo, holderFactory, typeEnv);
    auto readPartitions = GetKeyRangePartitions(range.ToTableRange(), stageInfo.Meta.ShardKey->GetPartitions(),
        keyColumnTypes);

    THashMap<ui64, TShardInfo> shardInfoMap;
    for (TPartitionWithRange& partitionWithRange : readPartitions) {
        auto& shardInfo = shardInfoMap[partitionWithRange.PartitionInfo->ShardId];

        YQL_ENSURE(!shardInfo.KeyReadRanges);
        shardInfo.KeyReadRanges.ConstructInPlace();

        if (partitionWithRange.FullRange) {
            shardInfo.KeyReadRanges->MakeFullRange(std::move(*partitionWithRange.FullRange));
        } else if (!shardInfo.KeyReadRanges->IsFullRange()) {
            shardInfo.KeyReadRanges->Add(std::move(partitionWithRange.PointOrRange));
        }
    }

    return shardInfoMap;
}

THashMap<ui64, TShardInfo> PrunePartitions(const TKqpTableKeys& tableKeys,
    const NKqpProto::TKqpPhyOpReadRanges& readRanges, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv)
{
    Y_UNUSED(holderFactory);
    const auto* table = tableKeys.FindTablePtr(stageInfo.Meta.TableId);
    YQL_ENSURE(table);

    const auto& keyColumnTypes = table->KeyColumnTypes;
    auto ranges = FillReadRangesInternal(keyColumnTypes, readRanges, stageInfo, typeEnv);

    THashMap<ui64, TShardInfo> shardInfoMap;

    // KeyReadRanges must be sorted & non-intersecting, they came in such condition from predicate extraction.
    for (auto& range: ranges) {
        TTableRange tableRange = std::holds_alternative<TSerializedCellVec>(range)
            ? TTableRange(std::get<TSerializedCellVec>(range).GetCells(), true, std::get<TSerializedCellVec>(range).GetCells(), true, true)
            : TTableRange(std::get<TSerializedTableRange>(range).ToTableRange());

        auto readPartitions = GetKeyRangePartitions(tableRange, stageInfo.Meta.ShardKey->GetPartitions(),
            keyColumnTypes);

        for (TPartitionWithRange& partitionWithRange : readPartitions) {
            auto& shardInfo = shardInfoMap[partitionWithRange.PartitionInfo->ShardId];

            if (!shardInfo.KeyReadRanges) {
                shardInfo.KeyReadRanges.ConstructInPlace();
            }

            if (partitionWithRange.FullRange) {
                shardInfo.KeyReadRanges->MakeFullRange(std::move(*partitionWithRange.FullRange));
                continue;
            }

            shardInfo.KeyReadRanges->Add(std::move(partitionWithRange.PointOrRange));
        }
    }

    return shardInfoMap;
}

THashMap<ui64, TShardInfo> PrunePartitions(const TKqpTableKeys& tableKeys,
    const NKqpProto::TKqpReadRangesSource& source, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv)
{
    auto guard = typeEnv.BindAllocator();
    const auto* table = tableKeys.FindTablePtr(stageInfo.Meta.TableId);
    YQL_ENSURE(table);

    const auto& keyColumnTypes = table->KeyColumnTypes;
    TVector<TSerializedPointOrRange> ranges;

    if (source.HasRanges()) {
        ranges = FillRangesFromParameter(
            keyColumnTypes, source.GetRanges(), stageInfo, typeEnv
        );
    } else if (source.HasKeyRange()) {
        const auto& range = source.GetKeyRange();
        if (range.GetFrom().SerializeAsString() == range.GetTo().SerializeAsString() &&
            range.GetFrom().ValuesSize() == keyColumnTypes.size()) {
            auto cells = FillKeyValues(keyColumnTypes, range.GetFrom(), stageInfo, holderFactory, typeEnv);
            ranges.push_back(TSerializedCellVec(TSerializedCellVec::Serialize(cells)));
        } else {
            ranges.push_back(MakeKeyRange(keyColumnTypes, range, stageInfo, holderFactory, typeEnv));
        }
    } else {
        ranges = BuildFullRange(keyColumnTypes);
    }

    THashMap<ui64, TShardInfo> shardInfoMap;

    // KeyReadRanges must be sorted & non-intersecting, they came in such condition from predicate extraction.
    for (auto& range: ranges) {
        TTableRange tableRange = std::holds_alternative<TSerializedCellVec>(range)
            ? TTableRange(std::get<TSerializedCellVec>(range).GetCells(), true, std::get<TSerializedCellVec>(range).GetCells(), true, true)
            : TTableRange(std::get<TSerializedTableRange>(range).ToTableRange());

        auto readPartitions = GetKeyRangePartitions(tableRange, stageInfo.Meta.ShardKey->GetPartitions(),
            keyColumnTypes);

        for (TPartitionWithRange& partitionWithRange : readPartitions) {
            auto& shardInfo = shardInfoMap[partitionWithRange.PartitionInfo->ShardId];

            if (!shardInfo.KeyReadRanges) {
                shardInfo.KeyReadRanges.ConstructInPlace();
            }

            if (partitionWithRange.FullRange) {
                shardInfo.KeyReadRanges->MakeFullRange(std::move(*partitionWithRange.FullRange));
                continue;
            }

            shardInfo.KeyReadRanges->Add(std::move(partitionWithRange.PointOrRange));
        }
    }

    return shardInfoMap;
}


THashMap<ui64, TShardInfo> PrunePartitions(const TKqpTableKeys& tableKeys,
    const NKqpProto::TKqpPhyOpReadOlapRanges& readRanges, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv)
{
    Y_UNUSED(holderFactory);
    auto guard = typeEnv.BindAllocator();
    const auto* table = tableKeys.FindTablePtr(stageInfo.Meta.TableId);
    YQL_ENSURE(table);
    YQL_ENSURE(table->TableKind == ETableKind::Olap);
    YQL_ENSURE(stageInfo.Meta.TableKind == ETableKind::Olap);

    const auto& keyColumnTypes = table->KeyColumnTypes;
    auto ranges = FillReadRanges(keyColumnTypes, readRanges, stageInfo, typeEnv);

    THashMap<ui64, TShardInfo> shardInfoMap;

    if (ranges.empty())
        return shardInfoMap;

    for (const auto& partition :  stageInfo.Meta.ShardKey->GetPartitions()) {
        auto& shardInfo = shardInfoMap[partition.ShardId];

        YQL_ENSURE(!shardInfo.KeyReadRanges);
        shardInfo.KeyReadRanges.ConstructInPlace();
        shardInfo.KeyReadRanges->CopyFrom(ranges);
    }

    return shardInfoMap;
}

THashMap<ui64, TShardInfo> PrunePartitions(const TKqpTableKeys& tableKeys,
    const NKqpProto::TKqpPhyTableOperation& operation, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv)
{
    switch (operation.GetTypeCase()) {
        case NKqpProto::TKqpPhyTableOperation::kReadRanges:
            return PrunePartitions(tableKeys, operation.GetReadRanges(), stageInfo, holderFactory, typeEnv);
        case NKqpProto::TKqpPhyTableOperation::kReadRange:
            return PrunePartitions(tableKeys, operation.GetReadRange(), stageInfo, holderFactory, typeEnv);
        case NKqpProto::TKqpPhyTableOperation::kLookup:
            return PrunePartitions(tableKeys, operation.GetLookup(), stageInfo, holderFactory, typeEnv);
        case NKqpProto::TKqpPhyTableOperation::kReadOlapRange:
            return PrunePartitions(tableKeys, operation.GetReadOlapRange(), stageInfo, holderFactory, typeEnv);
        default:
            YQL_ENSURE(false, "Unexpected table scan operation: " << static_cast<ui32>(operation.GetTypeCase()));
            break;
    }
}


namespace {

using namespace NMiniKQL;

THashMap<ui64, TShardInfo> PartitionLookupByParameterValue(const NKqpProto::TKqpPhyParamValue& proto,
    const TKqpTableKeys& tableKeys, const TStageInfo& stageInfo, const THolderFactory& holderFactory,
    const TTypeEnvironment& typeEnv)
{
    const auto& name = proto.GetParamName();
    auto [type, value] = stageInfo.Meta.Tx.Params->GetParameterUnboxedValue(name);
    auto shardsMap = PartitionParamByKeyPrefix(value, type, stageInfo.Meta.TableId, tableKeys, *stageInfo.Meta.ShardKey,
        holderFactory, typeEnv);

    THashMap<ui64, TShardInfo> shardInfoMap;

    for (auto& [shardId, shardData] : shardsMap) {
        auto& shardInfo = shardInfoMap[shardId];

        if (!shardInfo.KeyReadRanges) {
            shardInfo.KeyReadRanges.ConstructInPlace();
        }

        auto ret = shardInfo.Params.emplace(name, std::move(shardData.ParamValues));
        Y_VERIFY_DEBUG(ret.second);

        if (shardData.FullRange) {
            shardInfo.KeyReadRanges->MakeFullRange(std::move(*shardData.FullRange));
        } else {
            for (auto& range : shardData.Ranges) {
                shardInfo.KeyReadRanges->Add(std::move(range));
            }
        }
    }

    return shardInfoMap;
}

THashMap<ui64, TShardInfo> PartitionLookupByRowsList(const NKqpProto::TKqpPhyRowsList& proto,
    const TKqpTableKeys& tableKeys, const TStageInfo& stageInfo, const THolderFactory& holderFactory,
    const TTypeEnvironment& typeEnv)
{
    const auto& table = tableKeys.GetTable(stageInfo.Meta.ShardKey->TableId);

    std::unordered_map<ui64, THashSet<TString>> shardParams; // shardId -> paramNames
    std::unordered_map<ui64, TShardParamValuesAndRanges> ret;

    THashMap<ui64, TShardInfo> shardInfoMap;

    for (const auto& row : proto.GetRows()) {
        TVector<TCell> keyFrom, keyTo;
        keyFrom.resize(table.KeyColumns.size());
        keyTo.resize(row.GetColumns().size());
        NUdf::TUnboxedValue mkqlValue;

        NMiniKQL::TType* mkqlType = nullptr;
        for (const auto& [columnName, columnValue]: row.GetColumns()) {
            switch (columnValue.GetKindCase()) {
                case NKqpProto::TKqpPhyRowsList_TValue::kParamValue: {
                    const auto& paramName = columnValue.GetParamValue().GetParamName();
                    std::tie(mkqlType, mkqlValue) = stageInfo.Meta.Tx.Params->GetParameterUnboxedValue(
                        paramName);
                    break;
                }

                case NKqpProto::TKqpPhyRowsList_TValue::kLiteralValue: {
                    const auto& literal = columnValue.GetLiteralValue();
                    std::tie(mkqlType, mkqlValue) = ImportValueFromProto(
                        literal.GetType(), literal.GetValue(), typeEnv, holderFactory);
                    break;
                }

                case NKqpProto::TKqpPhyRowsList_TValue::KIND_NOT_SET: {
                    YQL_ENSURE(false);
                }
            }

            for (ui64 i = 0; i < table.KeyColumns.size(); ++i) {
                if (table.KeyColumns[i] == columnName) {
                    keyFrom[i] = keyTo[i] = NMiniKQL::MakeCell(
                        table.KeyColumnTypes[i], mkqlValue, typeEnv, /* copyValue */ false);
                    break;
                }
            }
        }

        auto range = TTableRange(keyFrom, true, keyTo, true, /* point */  false);
        auto partitions = GetKeyRangePartitions(range, stageInfo.Meta.ShardKey->GetPartitions(), table.KeyColumnTypes);

        for (auto& partitionWithRange: partitions) {
            ui64 shardId = partitionWithRange.PartitionInfo->ShardId;

            for (const auto& [columnName, columnValue] : row.GetColumns()) {
                if (columnValue.GetKindCase() == NKqpProto::TKqpPhyRowsList_TValue::kParamValue) {
                    shardParams[shardId].emplace(columnValue.GetParamValue().GetParamName());
                }
            }

            auto& shardData = ret[shardId];
            if (partitionWithRange.FullRange) {
                shardData.FullRange = std::move(partitionWithRange.FullRange);
                shardData.Ranges.clear();
            } else {
                shardData.Ranges.emplace_back(std::move(partitionWithRange.PointOrRange));
            }
        }
    }

    for (auto& [shardId, shardData] : ret) {
        auto& shardInfo = shardInfoMap[shardId];

        if (!shardInfo.KeyReadRanges) {
            shardInfo.KeyReadRanges.ConstructInPlace();
        }

        for (const auto& paramName : shardParams[shardId]) {
            shardInfo.Params.emplace(
                paramName,
                stageInfo.Meta.Tx.Params->SerializeParamValue(paramName)
            );
        }

        if (shardData.FullRange) {
            shardInfo.KeyReadRanges->MakeFullRange(std::move(*shardData.FullRange));
        } else {
            for (auto& range : shardData.Ranges) {
                shardInfo.KeyReadRanges->Add(std::move(range));
            }
        }
    }

    return shardInfoMap;
}

} // namespace

THashMap<ui64, TShardInfo> PrunePartitions(const TKqpTableKeys& tableKeys, const NKqpProto::TKqpPhyOpLookup& lookup,
    const TStageInfo& stageInfo, const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv)
{
    auto guard = typeEnv.BindAllocator();
    YQL_CLOG(TRACE, ProviderKqp) << "PrunePartitions: " << lookup.DebugString();

    if (!lookup.HasKeysValue()) {
        THashMap<ui64, TShardInfo> shardInfoMap;
        FillFullRange(stageInfo, shardInfoMap, /* read */ true);
        return shardInfoMap;
    }

    switch (auto kind = lookup.GetKeysValue().GetKindCase()) {
        case NKqpProto::TKqpPhyValue::kParamValue: {
            return PartitionLookupByParameterValue(lookup.GetKeysValue().GetParamValue(), tableKeys, stageInfo,
                holderFactory, typeEnv);
        }

        case NKqpProto::TKqpPhyValue::kRowsList: {
            return PartitionLookupByRowsList(lookup.GetKeysValue().GetRowsList(), tableKeys, stageInfo,
                holderFactory, typeEnv);
        }

        case NKqpProto::TKqpPhyValue::kParamElementValue:
        case NKqpProto::TKqpPhyValue::kLiteralValue:
        case NKqpProto::TKqpPhyValue::KIND_NOT_SET:
            YQL_ENSURE(false, "Unexpected lookup kind " << (int) kind);
            return {};
    }
}

template <typename TEffect>
THashMap<ui64, TShardInfo> PruneEffectPartitionsImpl(const TKqpTableKeys& tableKeys, const TEffect& effect,
    const TStageInfo& stageInfo, const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv)
{
    auto guard = typeEnv.BindAllocator();
    THashMap<ui64, TShardInfo> shardInfoMap;
    if (effect.HasRowsValue() &&
        effect.GetRowsValue().GetKindCase() == NKqpProto::TKqpPhyValue::kParamValue)
    {
        const auto& name = effect.GetRowsValue().GetParamValue().GetParamName();
        auto [type, value] = stageInfo.Meta.Tx.Params->GetParameterUnboxedValue(name);
        auto shardsMap = PartitionParamByKey(value, type, stageInfo.Meta.TableId, tableKeys, *stageInfo.Meta.ShardKey,
             holderFactory, typeEnv);

        for (auto& [shardId, shardData] : shardsMap) {
            auto& shardInfo = shardInfoMap[shardId];

            auto ret = shardInfo.Params.emplace(name, std::move(shardData.ParamValues));
            YQL_ENSURE(ret.second);

            if (!shardInfo.KeyWriteRanges) {
                shardInfo.KeyWriteRanges.ConstructInPlace();
            }

            if (shardData.FullRange) {
                shardInfo.KeyWriteRanges->MakeFullRange(std::move(*shardData.FullRange));
            } else {
                for (auto& range : shardData.Ranges) {
                    shardInfo.KeyWriteRanges->Add(std::move(range));
                }
            }

            shardInfo.ColumnWrites = shardData.ColumnWrites;
        }
    } else {
        FillFullRange(stageInfo, shardInfoMap, /* read */ false);
    }

    return shardInfoMap;
}

THashMap<ui64, TShardInfo> PruneEffectPartitions(const TKqpTableKeys& tableKeys,
    const NKqpProto::TKqpPhyOpUpsertRows& effect, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv)
{
    return PruneEffectPartitionsImpl(tableKeys, effect, stageInfo, holderFactory, typeEnv);
}

THashMap<ui64, TShardInfo> PruneEffectPartitions(const TKqpTableKeys& tableKeys,
    const NKqpProto::TKqpPhyOpDeleteRows& effect, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv)
{
    return PruneEffectPartitionsImpl(tableKeys, effect, stageInfo, holderFactory, typeEnv);
}

THashMap<ui64, TShardInfo> PruneEffectPartitions(const TKqpTableKeys& tableKeys,
    const NKqpProto::TKqpPhyTableOperation& operation, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv)
{
    switch(operation.GetTypeCase()) {
        case NKqpProto::TKqpPhyTableOperation::kUpsertRows:
            return PruneEffectPartitions(tableKeys, operation.GetUpsertRows(), stageInfo, holderFactory, typeEnv);
        case NKqpProto::TKqpPhyTableOperation::kDeleteRows:
            return PruneEffectPartitions(tableKeys, operation.GetDeleteRows(), stageInfo, holderFactory, typeEnv);
        default:
            YQL_ENSURE(false, "Unexpected table operation: " << static_cast<ui32>(operation.GetTypeCase()));
    }
}

void ExtractItemsLimit(const TStageInfo& stageInfo, const NKqpProto::TKqpPhyValue& protoItemsLimit,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv,
    ui64& itemsLimit, TString& itemsLimitParamName, NYql::NDqProto::TData& itemsLimitBytes,
    NKikimr::NMiniKQL::TType*& itemsLimitType)
{
    switch (protoItemsLimit.GetKindCase()) {
        case NKqpProto::TKqpPhyValue::kLiteralValue: {
            const auto& literalValue = protoItemsLimit.GetLiteralValue();

            auto [type, value] = NMiniKQL::ImportValueFromProto(
                literalValue.GetType(), literalValue.GetValue(), typeEnv, holderFactory);

            YQL_ENSURE(type->GetKind() == NMiniKQL::TType::EKind::Data);
            itemsLimit = value.Get<ui64>();
            itemsLimitType = type;

            return;
        }

        case NKqpProto::TKqpPhyValue::kParamValue: {
            itemsLimitParamName = protoItemsLimit.GetParamValue().GetParamName();
            if (!itemsLimitParamName) {
                return;
            }

            auto [type, value] = stageInfo.Meta.Tx.Params->GetParameterUnboxedValue(itemsLimitParamName);
            YQL_ENSURE(type->GetKind() == NMiniKQL::TType::EKind::Data);
            itemsLimit = value.Get<ui64>();

            NYql::NDq::TDqDataSerializer dataSerializer(typeEnv, holderFactory, NYql::NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0);
            itemsLimitBytes = dataSerializer.Serialize(value, type);
            itemsLimitType = type;

            return;
        }

        case NKqpProto::TKqpPhyValue::kParamElementValue:
        case NKqpProto::TKqpPhyValue::kRowsList:
            YQL_ENSURE(false, "Unexpected ItemsLimit kind " << protoItemsLimit.DebugString());

        case NKqpProto::TKqpPhyValue::KIND_NOT_SET:
            return;
    }
}

TPhysicalShardReadSettings ExtractReadSettings(const NKqpProto::TKqpPhyTableOperation& operation, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv)
{
    TPhysicalShardReadSettings readSettings;

    switch(operation.GetTypeCase()){
        case NKqpProto::TKqpPhyTableOperation::kReadRanges: {
            ExtractItemsLimit(stageInfo, operation.GetReadRanges().GetItemsLimit(), holderFactory, typeEnv,
                readSettings.ItemsLimit, readSettings.ItemsLimitParamName, readSettings.ItemsLimitBytes, readSettings.ItemsLimitType);
            readSettings.Reverse = operation.GetReadRanges().GetReverse();

            break;
        }

        case NKqpProto::TKqpPhyTableOperation::kReadRange: {
            ExtractItemsLimit(stageInfo, operation.GetReadRange().GetItemsLimit(), holderFactory, typeEnv,
                readSettings.ItemsLimit, readSettings.ItemsLimitParamName, readSettings.ItemsLimitBytes, readSettings.ItemsLimitType);
            readSettings.Reverse = operation.GetReadRange().GetReverse();
            break;
        }

        case NKqpProto::TKqpPhyTableOperation::kReadOlapRange: {
            readSettings.Sorted = operation.GetReadOlapRange().GetSorted();
            readSettings.Reverse = operation.GetReadOlapRange().GetReverse();
            ExtractItemsLimit(stageInfo, operation.GetReadOlapRange().GetItemsLimit(), holderFactory, typeEnv,
                readSettings.ItemsLimit, readSettings.ItemsLimitParamName, readSettings.ItemsLimitBytes, readSettings.ItemsLimitType);
            NKikimrMiniKQL::TType minikqlProtoResultType;
            ConvertYdbTypeToMiniKQLType(operation.GetReadOlapRange().GetResultType(), minikqlProtoResultType);
            readSettings.ResultType = ImportTypeFromProto(minikqlProtoResultType, typeEnv);
            break;
        }

        default:
            break;
    }

    return readSettings;
}

} // namespace NKikimr::NKqp
