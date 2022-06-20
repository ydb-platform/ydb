#include "kqp_partition_helper.h"
#include "kqp_table_resolver.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/library/mkql_proto/mkql_proto.h>

#include <ydb/library/yql/dq/runtime/dq_columns_resolve.h>
#include <ydb/library/yql/dq/runtime/dq_transport.h>
#include <ydb/library/yql/utils/log/log.h>

namespace NKikimr::NKqp {

namespace {

using namespace NYql;

struct TShardParamValuesAndRanges {
    NDqProto::TData ParamValues;
    NKikimr::NMiniKQL::TType* ParamType;
    // either FullRange or Ranges are set
    TVector<TSerializedPointOrRange> Ranges;
    std::optional<TSerializedTableRange> FullRange;
};

THashMap<ui64, TShardParamValuesAndRanges> PartitionParamByKey(const NDq::TMkqlValueRef& param, const TTableId& tableId,
    const TKqpTableKeys& tableKeys, const TKeyDesc& key, const NMiniKQL::THolderFactory& holderFactory,
    const NMiniKQL::TTypeEnvironment& typeEnv)
{
    YQL_ENSURE(tableId.HasSamePath(key.TableId));
    auto& table = tableKeys.GetTable(tableId);

    THashMap<ui64, TShardParamValuesAndRanges> ret;
    THashMap<ui64, NMiniKQL::TUnboxedValueVector> shardParamValues;

    auto [type, value] = ImportValueFromProto(param.GetType(), param.GetValue(), typeEnv, holderFactory);

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
    auto it = value.GetListIterator();
    while (it.Next(paramValue)) {
        auto keyValue = MakeKeyCells(paramValue, table.KeyColumnTypes, keyColumnIndices, typeEnv, /* copyValues */ true);
        Y_VERIFY_DEBUG(keyValue.size() == keyLen);

        ui32 partitionIndex = FindKeyPartitionIndex(keyValue, key.GetPartitions(), table.KeyColumnTypes,
            [] (const auto& partition) { return *partition.Range; });

        ui64 shardId = key.GetPartitions()[partitionIndex].ShardId;

        shardParamValues[shardId].emplace_back(std::move(paramValue));

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
        shardData.ParamType = itemType;
    }

    NDq::TDqDataSerializer dataSerializer{typeEnv, holderFactory, NDqProto::EDataTransportVersion::DATA_TRANSPORT_UV_PICKLE_1_0};
    for (auto& [shardId, data] : ret) {
        ret[shardId].ParamValues = dataSerializer.Serialize(shardParamValues[shardId], itemType);
    }

    return ret;
}

THashMap<ui64, TShardParamValuesAndRanges> PartitionParamByKeyPrefix(const NDq::TMkqlValueRef& param,
    const TTableId& tableId, const TKqpTableKeys& tableKeys, const TKeyDesc& key,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv)
{
    YQL_ENSURE(tableId.HasSamePath(key.TableId));
    auto& table = tableKeys.GetTable(tableId);

    THashMap<ui64, TShardParamValuesAndRanges> ret;
    THashMap<ui64, NMiniKQL::TUnboxedValueVector> shardParamValues;

    auto [type, value] = ImportValueFromProto(param.GetType(), param.GetValue(), typeEnv, holderFactory);

    YQL_ENSURE(type->GetKind() == NMiniKQL::TType::EKind::List);
    auto itemType = static_cast<NMiniKQL::TListType&>(*type).GetItemType();
    YQL_ENSURE(itemType->GetKind() == NMiniKQL::TType::EKind::Struct);
    auto& structType = static_cast<NMiniKQL::TStructType&>(*itemType);

    const ui64 keyLen = table.KeyColumns.size();

    TVector<NUdf::TDataTypeId> keyFullType{Reserve(keyLen)};
    TVector<NUdf::TDataTypeId> keyPrefixType{Reserve(keyLen)};
    TVector<ui32> keyPrefixIndices{Reserve(keyLen)};

    for (const auto& keyColumn : table.KeyColumns) {
        auto columnInfo = NDq::FindColumnInfo(&structType, keyColumn);
        if (!columnInfo) {
            break;
        }

        keyFullType.push_back(columnInfo->TypeId);
        keyPrefixType.push_back(columnInfo->TypeId);
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
        data.ParamValues = dataSerializer.Serialize(shardParamValues[shardId], itemType);
    }

    return ret;
}

TVector<TCell> FillKeyValues(const TVector<NUdf::TDataTypeId>& keyColumnTypes, const NKqpProto::TKqpPhyKeyBound& bound,
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

        auto param = stageInfo.Meta.Tx.Params.Values.FindPtr(paramName);
        YQL_ENSURE(param, "Param not found: " << paramName);

        const auto* protoType = &param->GetType();
        const auto* protoValue = &param->GetValue();
        if (paramIndex) {
            YQL_ENSURE(protoType->GetKind() == NKikimrMiniKQL::Tuple);
            YQL_ENSURE(*paramIndex < protoType->GetTuple().ElementSize());
            YQL_ENSURE(*paramIndex < protoValue->TupleSize());
            protoType = &protoType->GetTuple().GetElement(*paramIndex);
            protoValue = &protoValue->GetTuple(*paramIndex);
        }

        auto [type, value] = ImportValueFromProto(*protoType, *protoValue, typeEnv, holderFactory);

        keyValues.emplace_back(NMiniKQL::MakeCell(keyColumnTypes[i], value, typeEnv, /* copy */ true));
    }

    return keyValues;
}

TSerializedPointOrRange FillOneRange(NUdf::TUnboxedValue& begin, NUdf::TUnboxedValue& end,
    const TVector<NUdf::TDataTypeId>& keyColumnTypes, const NMiniKQL::TTypeEnvironment& typeEnv)
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

TVector<TSerializedPointOrRange> BuildFullRange(const TVector<NUdf::TDataTypeId>& keyColumnTypes) {
    // Build range from NULL, NULL ... NULL to +inf, +inf ... +inf
    TVector<TCell> fromKeyValues(keyColumnTypes.size());

    auto range = TSerializedTableRange(fromKeyValues, true, TVector<TCell>(), false);

    YQL_CLOG(DEBUG, ProviderKqp) << "Formed full range [extract predicate]: "
        << DebugPrintRange(keyColumnTypes, range.ToTableRange(), *AppData()->TypeRegistry);

    return {std::move(range)};
}

TVector<TSerializedPointOrRange> FillRangesFromParameter(const TVector<NUdf::TDataTypeId>& keyColumnTypes,
    const NKqpProto::TKqpPhyParamValue& rangesParam, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv)
{
    TString paramName = rangesParam.GetParamName();

    auto param = stageInfo.Meta.Tx.Params.Values.FindPtr(paramName);
    YQL_ENSURE(param, "Param not found: " << paramName);

    const auto* protoType = &param->GetType();
    const auto* protoValue = &param->GetValue();

    auto [type, value] = ImportValueFromProto(*protoType, *protoValue, typeEnv, holderFactory);

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
TVector<TSerializedPointOrRange> FillReadRangesInternal(const TVector<NUdf::TDataTypeId>& keyColumnTypes,
    const PhyOpReadRanges& readRanges, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv)
{
    if (readRanges.HasKeyRanges()) {
        return FillRangesFromParameter(
            keyColumnTypes, readRanges.GetKeyRanges(), stageInfo, holderFactory, typeEnv
        );
    }

    return BuildFullRange(keyColumnTypes);
}

} // anonymous namespace

TVector<TSerializedPointOrRange> FillReadRanges(const TVector<NUdf::TDataTypeId>& keyColumnTypes,
    const NKqpProto::TKqpPhyOpReadOlapRanges& readRange, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv)
{
    return FillReadRangesInternal(keyColumnTypes, readRange, stageInfo, holderFactory, typeEnv);
}

TVector<TSerializedPointOrRange> FillReadRanges(const TVector<NUdf::TDataTypeId>& keyColumnTypes,
    const NKqpProto::TKqpPhyOpReadRanges& readRange, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv)
{
    return FillReadRangesInternal(keyColumnTypes, readRange, stageInfo, holderFactory, typeEnv);
}

TSerializedTableRange MakeKeyRange(const TVector<NUdf::TDataTypeId>& keyColumnTypes,
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

TString TShardInfo::ToString(const TVector<NScheme::TTypeId>& keyTypes, const NScheme::TTypeRegistry& typeRegistry) const
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
    const auto* table = tableKeys.FindTablePtr(stageInfo.Meta.TableId);
    YQL_ENSURE(table);

    const auto& keyColumnTypes = table->KeyColumnTypes;
    auto ranges = FillReadRangesInternal(keyColumnTypes, readRanges, stageInfo, holderFactory, typeEnv);

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
    const auto* table = tableKeys.FindTablePtr(stageInfo.Meta.TableId);
    YQL_ENSURE(table);
    YQL_ENSURE(table->TableKind == ETableKind::Olap);
    YQL_ENSURE(stageInfo.Meta.TableKind == ETableKind::Olap);

    const auto& keyColumnTypes = table->KeyColumnTypes;
    auto ranges = FillReadRanges(keyColumnTypes, readRanges, stageInfo, holderFactory, typeEnv);

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

THashMap<ui64, TShardInfo> PrunePartitions(TKqpTableKeys& tableKeys,
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
    auto param = stageInfo.Meta.Tx.Params.Values.FindPtr(name);
    YQL_ENSURE(param);

    auto shardsMap = PartitionParamByKeyPrefix(*param, stageInfo.Meta.TableId, tableKeys, *stageInfo.Meta.ShardKey,
        holderFactory, typeEnv);

    THashMap<ui64, TShardInfo> shardInfoMap;

    for (auto& [shardId, shardData] : shardsMap) {
        auto& shardInfo = shardInfoMap[shardId];

        if (!shardInfo.KeyReadRanges) {
            shardInfo.KeyReadRanges.ConstructInPlace();
        }

        auto ret = shardInfo.Params.emplace(name, std::move(shardData.ParamValues));
        Y_VERIFY_DEBUG(ret.second);
        auto retType = shardInfo.ParamTypes.emplace(name, std::move(shardData.ParamType));
        Y_VERIFY_DEBUG(retType.second);

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

    struct TParamDesc {
        const NDq::TMkqlValueRef* MkqlValueRef = nullptr;
        NMiniKQL::TType* MkqlType = nullptr;
        NUdf::TUnboxedValue MkqlValue;
    };

    std::unordered_map<std::string_view, TParamDesc> params; // already known parameters
    std::unordered_map<ui64, THashSet<TString>> shardParams; // shardId -> paramNames
    std::unordered_map<ui64, TShardParamValuesAndRanges> ret;

    THashMap<ui64, TShardInfo> shardInfoMap;

    for (const auto& row : proto.GetRows()) {
        TVector<TCell> keyFrom, keyTo;
        keyFrom.resize(table.KeyColumns.size());
        keyTo.resize(row.GetColumns().size());

        NMiniKQL::TType* mkqlType = nullptr;
        NUdf::TUnboxedValue mkqlValue;

        for (const auto& [columnName, columnValue]: row.GetColumns()) {
            switch (columnValue.GetKindCase()) {
                case NKqpProto::TKqpPhyRowsList_TValue::kParamValue: {
                    const auto& paramName = columnValue.GetParamValue().GetParamName();

                    TParamDesc* paramDesc;
                    if (auto it = params.find(paramName); it != params.end()) {
                        paramDesc = &it->second;
                    } else {
                        auto param = stageInfo.Meta.Tx.Params.Values.FindPtr(paramName);
                        YQL_ENSURE(param);

                        auto iter = params.emplace(paramName, TParamDesc());
                        paramDesc = &iter.first->second;

                        paramDesc->MkqlValueRef = param;
                        std::tie(paramDesc->MkqlType, paramDesc->MkqlValue) = ImportValueFromProto(param->GetType(), param->GetValue(), typeEnv, holderFactory);
                    }

                    mkqlType = paramDesc->MkqlType;
                    mkqlValue = paramDesc->MkqlValue;

                    break;
                }

                case NKqpProto::TKqpPhyRowsList_TValue::kLiteralValue: {
                    const auto& literal = columnValue.GetLiteralValue();

                    std::tie(mkqlType, mkqlValue) = ImportValueFromProto(literal.GetType(), literal.GetValue(), typeEnv, holderFactory);

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
            auto it = params.find(paramName);
            YQL_ENSURE(it != params.end());
            shardInfo.Params.emplace(
                paramName,
                NDq::TDqDataSerializer::SerializeParamValue(it->second.MkqlType, it->second.MkqlValue)
            );
            shardInfo.ParamTypes.emplace(paramName, it->second.MkqlType);
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
    THashMap<ui64, TShardInfo> shardInfoMap;
    if (effect.HasRowsValue() &&
        effect.GetRowsValue().GetKindCase() == NKqpProto::TKqpPhyValue::kParamValue)
    {
        const auto& name = effect.GetRowsValue().GetParamValue().GetParamName();
        auto param = stageInfo.Meta.Tx.Params.Values.FindPtr(name);
        YQL_ENSURE(param);

        auto shardsMap = PartitionParamByKey(*param, stageInfo.Meta.TableId, tableKeys, *stageInfo.Meta.ShardKey,
             holderFactory, typeEnv);

        for (auto& [shardId, shardData] : shardsMap) {
            auto& shardInfo = shardInfoMap[shardId];

            auto ret = shardInfo.Params.emplace(name, std::move(shardData.ParamValues));
            YQL_ENSURE(ret.second);
            auto retType = shardInfo.ParamTypes.emplace(name, std::move(shardData.ParamType));
            YQL_ENSURE(retType.second);

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

} // namespace NKikimr::NKqp
