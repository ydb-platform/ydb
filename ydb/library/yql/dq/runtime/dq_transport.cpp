#include "dq_transport.h"

#include <ydb/library/mkql_proto/mkql_proto.h>
#include <ydb/library/yql/minikql/computation/mkql_block_reader.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/comp_factory.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/pack.h>
#include <ydb/library/yql/providers/common/mkql/yql_type_mkql.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <util/system/yassert.h>

namespace NYql::NDq {

using namespace NKikimr;
using namespace NMiniKQL;
using namespace NYql;

namespace {

TDqSerializedBatch SerializeValue(NDqProto::EDataTransportVersion version, const TType* type, const NUdf::TUnboxedValuePod& value) {
    TRope packResult;
    switch (version) {
        case NDqProto::DATA_TRANSPORT_VERSION_UNSPECIFIED:
            version = NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0;
            [[fallthrough]];
        case NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0:
        case NDqProto::DATA_TRANSPORT_OOB_PICKLE_1_0: {
            TValuePackerTransport<false> packer(/* stable */ false, type);
            packResult = packer.Pack(value);
            break;
        }
        case NDqProto::DATA_TRANSPORT_UV_FAST_PICKLE_1_0:
        case NDqProto::DATA_TRANSPORT_OOB_FAST_PICKLE_1_0: {
            TValuePackerTransport<true> packer(/* stable */ false, type);
            packResult = packer.Pack(value);
            break;
        }
        default:
            YQL_ENSURE(false, "Unsupported TransportVersion");
    }

    TDqSerializedBatch result;
    result.Proto.SetTransportVersion(version);
    result.Proto.SetRows(1);
    result.SetPayload(std::move(packResult));
    return result;
}

template<bool Fast>
TRope DoSerializeBuffer(const TType* type, const TUnboxedValueBatch& buffer) {
    using TPacker = TValuePackerTransport<Fast>;

    TPacker packer(/* stable */ false, type);
    if (type->IsMulti()) {
        buffer.ForEachRowWide([&packer](const auto* values, ui32 width) {
            packer.AddWideItem(values, width);
        });
    } else {
        buffer.ForEachRow([&packer](const auto value) {
            packer.AddItem(value);
        });
    }
    return packer.Finish();
}

TDqSerializedBatch SerializeBuffer(NDqProto::EDataTransportVersion version, const TType* type, const TUnboxedValueBatch& buffer) {
    TRope packResult;
    switch (version) {
        case NDqProto::DATA_TRANSPORT_VERSION_UNSPECIFIED:
            version = NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0;
            [[fallthrough]];
        case NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0:
        case NDqProto::DATA_TRANSPORT_OOB_PICKLE_1_0: {
            packResult = DoSerializeBuffer<false>(type, buffer);
            break;
        }
        case NDqProto::DATA_TRANSPORT_UV_FAST_PICKLE_1_0:
        case NDqProto::DATA_TRANSPORT_OOB_FAST_PICKLE_1_0: {
            packResult = DoSerializeBuffer<true>(type, buffer);
            break;
        }
        default:
            YQL_ENSURE(false, "Unsupported TransportVersion");
    }

    TDqSerializedBatch result;
    result.Proto.SetTransportVersion(version);
    result.Proto.SetRows(buffer.RowCount());
    result.SetPayload(std::move(packResult));
    return result;
}

template<bool Fast>
void DeserializeValue(const TType* type, TRope&& data, const THolderFactory& holderFactory, NUdf::TUnboxedValue& value)
{
    using TPacker = TValuePackerTransport<Fast>;
    TPacker packer(/* stable */ false, type);
    value = packer.Unpack(std::move(data), holderFactory);
}

template<bool Fast>
void DeserializeBuffer(const TType* itemType, TRope&& data, const THolderFactory& holderFactory, TUnboxedValueBatch& buffer)
{
    using TPacker = TValuePackerTransport<Fast>;
    TPacker packer(/* stable */ false, itemType);
    packer.UnpackBatch(std::move(data), holderFactory, buffer);
}

} // namespace

NDqProto::EDataTransportVersion TDqDataSerializer::GetTransportVersion() const {
    return TransportVersion;
}

TDqSerializedBatch TDqDataSerializer::Serialize(const NUdf::TUnboxedValue& value, const TType* itemType) const {
    auto guard = TypeEnv.BindAllocator();
    return SerializeValue(TransportVersion, itemType, value);
}

TDqSerializedBatch TDqDataSerializer::Serialize(const NKikimr::NMiniKQL::TUnboxedValueBatch& buffer,
    const NKikimr::NMiniKQL::TType* itemType) const
{
    auto guard = TypeEnv.BindAllocator();
    return SerializeBuffer(TransportVersion, itemType, buffer);
}

void TDqDataSerializer::Deserialize(TDqSerializedBatch&& data, const TType* itemType, TUnboxedValueBatch& buffer) const
{
    auto guard = TypeEnv.BindAllocator();
    switch (data.Proto.GetTransportVersion()) {
        case NDqProto::DATA_TRANSPORT_VERSION_UNSPECIFIED:
        case NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0:
        case NDqProto::DATA_TRANSPORT_OOB_PICKLE_1_0:
            return DeserializeBuffer<false>(itemType, data.PullPayload(), HolderFactory, buffer);
        case NDqProto::DATA_TRANSPORT_UV_FAST_PICKLE_1_0:
        case NDqProto::DATA_TRANSPORT_OOB_FAST_PICKLE_1_0:
            return DeserializeBuffer<true>(itemType, data.PullPayload(), HolderFactory, buffer);
        default:
            YQL_ENSURE(false, "Unsupported TransportVersion");
    }
}

void TDqDataSerializer::Deserialize(TDqSerializedBatch&& data, const TType* itemType, NUdf::TUnboxedValue& value) const
{
    auto guard = TypeEnv.BindAllocator();
    switch (data.Proto.GetTransportVersion()) {
        case NDqProto::DATA_TRANSPORT_VERSION_UNSPECIFIED:
        case NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0:
        case NDqProto::DATA_TRANSPORT_OOB_PICKLE_1_0:
            return DeserializeValue<false>(itemType, data.PullPayload(), HolderFactory, value);
        case NDqProto::DATA_TRANSPORT_UV_FAST_PICKLE_1_0:
        case NDqProto::DATA_TRANSPORT_OOB_FAST_PICKLE_1_0:
            return DeserializeValue<true>(itemType, data.PullPayload(), HolderFactory, value);
        default:
            YQL_ENSURE(false, "Unsupported TransportVersion");
    }
}

void TDqDataSerializer::DeserializeParam(const NDqProto::TData& data, const TType* type,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory, NUdf::TUnboxedValue& value)
{
    YQL_ENSURE(data.GetTransportVersion() == (ui32) NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0);
    using TPacker = TValuePackerGeneric<false>;
    TPacker packer(/* stable */ false, type);
    value = packer.Unpack(data.GetRaw(), holderFactory);
}

NDqProto::TData TDqDataSerializer::SerializeParamValue(const TType* type, const NUdf::TUnboxedValuePod& value) {
    using TPacker = TValuePackerGeneric<false>;

    TPacker packer(/* stable */ false, type);
    TStringBuf packResult = packer.Pack(value);

    NDqProto::TData data;
    data.SetTransportVersion(NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0);
    data.SetRaw(packResult.data(), packResult.size());
    data.SetRows(1);

    return data;
}

namespace {

std::optional<ui64> EstimateIntegralDataSize(const TDataType* dataType) {
    switch (*dataType->GetDataSlot()) {
        case NUdf::EDataSlot::Bool:
        case NUdf::EDataSlot::Int8:
        case NUdf::EDataSlot::Uint8:
            return 1;
        case NUdf::EDataSlot::Int16:
        case NUdf::EDataSlot::Uint16:
            return 2;
        case NUdf::EDataSlot::Int32:
        case NUdf::EDataSlot::Uint32:
        case NUdf::EDataSlot::Float:
        case NUdf::EDataSlot::Date:
        case NUdf::EDataSlot::TzDate:
        case NUdf::EDataSlot::Datetime:
        case NUdf::EDataSlot::TzDatetime:
        case NUdf::EDataSlot::Date32:
            return 4;
        case NUdf::EDataSlot::Int64:
        case NUdf::EDataSlot::Uint64:
        case NUdf::EDataSlot::Double:
        case NUdf::EDataSlot::Timestamp:
        case NUdf::EDataSlot::TzTimestamp:
        case NUdf::EDataSlot::Interval:
        case NUdf::EDataSlot::Datetime64:
        case NUdf::EDataSlot::Timestamp64:
        case NUdf::EDataSlot::Interval64:
        case NUdf::EDataSlot::TzDate32:
            return 8;
        case NUdf::EDataSlot::TzDatetime64:
        case NUdf::EDataSlot::TzTimestamp64:
        case NUdf::EDataSlot::Uuid:
        case NUdf::EDataSlot::Decimal:
            return 16;
        case NUdf::EDataSlot::String:
        case NUdf::EDataSlot::Utf8:
        case NUdf::EDataSlot::DyNumber:
        case NUdf::EDataSlot::Json:
        case NUdf::EDataSlot::JsonDocument:
        case NUdf::EDataSlot::Yson:
            return std::nullopt;
    }
}

ui64 EstimateSizeImpl(const NUdf::TUnboxedValuePod& value, const NKikimr::NMiniKQL::TType* type, bool* fixed, TDqDataSerializer::TEstimateSizeSettings settings) {
    switch (type->GetKind()) {
        case TType::EKind::Void:
        case TType::EKind::Null:
        case TType::EKind::EmptyList:
        case TType::EKind::EmptyDict:
            return 0;

        case TType::EKind::Data: {
            auto dataType = static_cast<const TDataType*>(type);
            if (auto size = EstimateIntegralDataSize(dataType); size.has_value()) {
                return *size;
            }
            if (fixed) {
                *fixed = false;
            }
            switch (*dataType->GetDataSlot()) {
                case NUdf::EDataSlot::String:
                case NUdf::EDataSlot::Utf8:
                case NUdf::EDataSlot::DyNumber:
                case NUdf::EDataSlot::Json:
                case NUdf::EDataSlot::JsonDocument:
                case NUdf::EDataSlot::Yson:
                    return (settings.WithHeaders?2:0) + value.AsStringRef().Size();
                default:
                    YQL_ENSURE(false, "" << dataType->GetKindAsStr());
            }
        }

        case TType::EKind::Optional: {
            auto optionalType = static_cast<const TOptionalType*>(type);
            if (value) {
                if (optionalType->GetItemType()->GetKind() == TType::EKind::Data) {
                    auto dataType = static_cast<const TDataType*>(optionalType->GetItemType());
                    if (auto size = EstimateIntegralDataSize(dataType); size.has_value()) {
                        return *size;
                    }
                }
                return EstimateSizeImpl(value.GetOptionalValue(), optionalType->GetItemType(), fixed, settings);
            }
            return 0;
        }

        case TType::EKind::List: {
            auto listType = static_cast<const TListType*>(type);
            auto itemType = listType->GetItemType();
            ui64 size = (settings.WithHeaders?2:0);
            if (value.HasFastListLength() && value.GetListLength() > 0 && value.GetElements()) {
                auto len = value.GetListLength();
                auto p = value.GetElements();
                do {
                    size += EstimateSizeImpl(*p++, itemType, fixed, settings);
                }
                while (--len);
            } else {
                const auto iter = value.GetListIterator();
                for (NUdf::TUnboxedValue item; iter.Next(item);) {
                    size += EstimateSizeImpl(item, itemType, fixed, settings);
                }
            }
            return size;
        }

        case TType::EKind::Struct: {
            auto structType = static_cast<const TStructType*>(type);
            ui64 size = (settings.WithHeaders?2:0);
            for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
                auto memberType = structType->GetMemberType(index);

                if (memberType->GetKind() == TType::EKind::Data) {
                    auto dataType = static_cast<const TDataType*>(memberType);
                    if (auto s = EstimateIntegralDataSize(dataType); s.has_value()) {
                        size += *s;
                        continue;
                    }
                }

                size += EstimateSizeImpl(value.GetElement(index), memberType, fixed, settings);
            }

            return size;
        }

        case TType::EKind::Tuple: {
            auto tupleType = static_cast<const TTupleType*>(type);
            ui64 size = (settings.WithHeaders?2:0);
            for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
                auto elementType = tupleType->GetElementType(index);

                if (elementType->GetKind() == TType::EKind::Data) {
                    auto dataType = static_cast<const TDataType*>(elementType);
                    if (auto s = EstimateIntegralDataSize(dataType); s.has_value()) {
                        size += *s;
                        continue;
                    }
                }

                size += EstimateSizeImpl(value.GetElement(index), elementType, fixed, settings);
            }
            return size;
        }

        case TType::EKind::Dict: {
            auto dictType = static_cast<const TDictType*>(type);
            auto keyType = dictType->GetKeyType();
            auto payloadType = dictType->GetPayloadType();

            ui64 size = (settings.WithHeaders?2:0);
            const auto iter = value.GetDictIterator();
            for (NUdf::TUnboxedValue key, payload; iter.NextPair(key, payload);) {
                size += EstimateSizeImpl(key, keyType, fixed, settings);
                size += EstimateSizeImpl(payload, payloadType, fixed, settings);
            }
            return size;
        }

        case TType::EKind::Variant: {
            auto variantType = static_cast<const TVariantType*>(type);
            ui32 variantIndex = value.GetVariantIndex();
            TType* innerType = variantType->GetUnderlyingType();
            if (innerType->IsStruct()) {
                innerType = static_cast<TStructType*>(innerType)->GetMemberType(variantIndex);
            } else {
                MKQL_ENSURE(innerType->IsTuple(), "Unexpected underlying variant type: " << innerType->GetKindAsStr());
                innerType = static_cast<TTupleType*>(innerType)->GetElementType(variantIndex);
            }
            return (settings.WithHeaders?2:0) + EstimateSizeImpl(value.GetVariantItem(), innerType, fixed, settings);
        }

        case TType::EKind::Pg: {
            if (value) {
                auto pgType = static_cast<const TPgType*>(type);
                return NKikimr::NMiniKQL::PgValueSize(pgType, value);
            }
            return 0;
        }

        case TType::EKind::Tagged: {
            auto taggedType = static_cast<const TTaggedType*>(type);
            return EstimateSizeImpl(value, taggedType->GetBaseType(), fixed, settings);
        }

        case TType::EKind::Block: {
            auto blockType = static_cast<const TBlockType*>(type);
            if (fixed) {
                *fixed = false;
            }

            auto reader = MakeBlockReader(TTypeInfoHelper(), blockType->GetItemType());
            ui64 size;
            if (blockType->GetShape() == TBlockType::EShape::Many) {
                size = reader->GetDataWeight(*TArrowBlock::From(value).GetDatum().array());
            } else {
                auto blockItem = reader->GetScalarItem(*TArrowBlock::From(value).GetDatum().scalar());
                size = reader->GetDataWeight(blockItem);
            }
            return size;
        }

        case TType::EKind::Type:
        case TType::EKind::Stream:
        case TType::EKind::Callable:
        case TType::EKind::Any:
        case TType::EKind::Resource:
        case TType::EKind::Flow:
        case TType::EKind::ReservedKind:
        case TType::EKind::Multi: {
            if (settings.DiscardUnsupportedTypes) {
                return 0;
            }
            THROW yexception() << "Unsupported type: " << type->GetKindAsStr();
        }
    }
}

} // namespace

ui64 TDqDataSerializer::EstimateSize(const NUdf::TUnboxedValue& value, const NKikimr::NMiniKQL::TType* type, bool* fixed, TDqDataSerializer::TEstimateSizeSettings settings)
{
    if (fixed) {
        *fixed = true;
    }
    return EstimateSizeImpl(value, type, fixed, settings);
}

} // namespace NYql::NDq
