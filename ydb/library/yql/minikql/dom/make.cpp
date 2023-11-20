#include "make.h"
#include "node.h"
#include "yson.h"
#include "json.h"

#include <ydb/library/yql/public/udf/udf_type_inspection.h>

#include <util/string/builder.h>

namespace NYql::NDom {
using namespace NUdf;

namespace {

TUnboxedValuePod MakeData(const TDataTypeId nodeType, const TUnboxedValuePod value, const IValueBuilder* valueBuilder) {
    switch (nodeType) {
        case TDataType<char*>::Id:  return value;
        case TDataType<TUtf8>::Id:  return value;
        case TDataType<bool>::Id:   return SetNodeType<ENodeType::Bool>(value);
        case TDataType<i8>::Id:     return SetNodeType<ENodeType::Int64>(TUnboxedValuePod(i64(value.Get<i8>())));
        case TDataType<i16>::Id:    return SetNodeType<ENodeType::Int64>(TUnboxedValuePod(i64(value.Get<i16>())));
        case TDataType<i32>::Id:    return SetNodeType<ENodeType::Int64>(TUnboxedValuePod(i64(value.Get<i32>())));
        case TDataType<i64>::Id:    return SetNodeType<ENodeType::Int64>(value);
        case TDataType<ui8>::Id:    return SetNodeType<ENodeType::Uint64>(TUnboxedValuePod(ui64(value.Get<ui8>())));
        case TDataType<ui16>::Id:   return SetNodeType<ENodeType::Uint64>(TUnboxedValuePod(ui64(value.Get<ui16>())));
        case TDataType<ui32>::Id:   return SetNodeType<ENodeType::Uint64>(TUnboxedValuePod(ui64(value.Get<ui32>())));
        case TDataType<ui64>::Id:   return SetNodeType<ENodeType::Uint64>(value);
        case TDataType<float>::Id:  return SetNodeType<ENodeType::Double>(TUnboxedValuePod(double(value.Get<float>())));
        case TDataType<double>::Id: return SetNodeType<ENodeType::Double>(value);
        case TDataType<TYson>::Id:  return TryParseYsonDom(value.AsStringRef(), valueBuilder).Release();
        case TDataType<TJson>::Id:  return TryParseJsonDom(value.AsStringRef(), valueBuilder).Release();
        default: break;
    }

    Y_ABORT("Unsupported data type.");
}

TUnboxedValuePod MakeList(const ITypeInfoHelper* typeHelper, const TType* itemType, const TUnboxedValuePod value, const IValueBuilder* valueBuilder) {
    if (const auto elements = value.GetElements()) {
        if (const auto size = value.GetListLength()) {
            TUnboxedValue* items = nullptr;
            auto res = valueBuilder->NewArray(size, items);
            for (ui64 i = 0ULL; i < size; ++i) {
                *items++ = MakeDom(typeHelper, itemType, elements[i], valueBuilder);
            }
            return SetNodeType<ENodeType::List>(res.Release());
        }
    } else {
        TSmallVec<TUnboxedValue> items;
        if (value.HasFastListLength()) {
            items.reserve(value.GetListLength());
        }
        const auto iterator = value.GetListIterator();
        for (TUnboxedValue current; iterator.Next(current);) {
            items.emplace_back(MakeDom(typeHelper, itemType, current, valueBuilder));
        }
        if (!items.empty()) {
            auto res = valueBuilder->NewList(items.data(), items.size());
            return SetNodeType<ENodeType::List>(res.Release());
        }
    }

    return SetNodeType<ENodeType::List>(TUnboxedValuePod::Void());
}

TUnboxedValuePod MakeDict(const ITypeInfoHelper* typeHelper, const TType* itemType, const TUnboxedValuePod value, const IValueBuilder* valueBuilder) {
    TSmallVec<TPair, TStdAllocatorForUdf<TPair>> items;
    items.reserve(value.GetDictLength());
    const auto it = value.GetDictIterator();
    for (TUnboxedValue x, y; it.NextPair(x, y);) {
        items.emplace_back(x, MakeDom(typeHelper, itemType, y, valueBuilder));
    }

    if (items.empty()) {
        return SetNodeType<ENodeType::Dict>(TUnboxedValuePod::Void());
    }

    return SetNodeType<ENodeType::Dict>(TUnboxedValuePod(new TMapNode(items.data(), items.size())));
}

TUnboxedValuePod MakeTuple(const ITypeInfoHelper* typeHelper, const TType* shape, const TUnboxedValuePod value, const IValueBuilder* valueBuilder) {
    if (const auto tupleTypeInspector = TTupleTypeInspector(*typeHelper, shape); const auto size = tupleTypeInspector.GetElementsCount()) {
        TUnboxedValue* items = nullptr;
        auto res = valueBuilder->NewArray(size, items);
        for (ui64 i = 0ULL; i < size; ++i) {
            *items++ = MakeDom(typeHelper, tupleTypeInspector.GetElementType(i), static_cast<const TUnboxedValuePod&>(value.GetElement(i)), valueBuilder);
        }
        return SetNodeType<ENodeType::List>(res.Release());
    }

    return SetNodeType<ENodeType::List>(TUnboxedValuePod::Void());
}

TUnboxedValuePod MakeStruct(const ITypeInfoHelper* typeHelper, const TType* shape, const TUnboxedValuePod value, const IValueBuilder* valueBuilder) {
    if (const auto structTypeInspector = TStructTypeInspector(*typeHelper, shape); const auto size = structTypeInspector.GetMembersCount()) {
        TSmallVec<TPair, TStdAllocatorForUdf<TPair>> items;
        items.reserve(size);

        for (ui64 i = 0ULL; i < size; ++i) {
            items.emplace_back(
                valueBuilder->NewString(structTypeInspector.GetMemberName(i)),
                MakeDom(typeHelper, structTypeInspector.GetMemberType(i), static_cast<const TUnboxedValuePod&>(value.GetElement(i)), valueBuilder)
            );
        }

        return SetNodeType<ENodeType::Dict>(TUnboxedValuePod(new TMapNode(items.data(), items.size())));
    }

    return SetNodeType<ENodeType::Dict>(TUnboxedValuePod::Void());
}

TUnboxedValuePod MakeVariant(const ITypeInfoHelper* typeHelper, const TType* shape, const TUnboxedValuePod value, const IValueBuilder* valueBuilder) {
    const auto index = value.GetVariantIndex();
    const auto& item = value.GetVariantItem();
    const auto underlyingType = TVariantTypeInspector(*typeHelper, shape).GetUnderlyingType();
    switch (const auto kind = typeHelper->GetTypeKind(underlyingType)) {
        case ETypeKind::Tuple:
            if (const auto tupleTypeInspector = TTupleTypeInspector(*typeHelper, underlyingType); index < tupleTypeInspector.GetElementsCount())
                return MakeDom(typeHelper, tupleTypeInspector.GetElementType(index), item, valueBuilder);
            break;
        case ETypeKind::Struct:
            if (const auto structTypeInspector = TStructTypeInspector(*typeHelper, underlyingType); index < structTypeInspector.GetMembersCount())
                return MakeDom(typeHelper, structTypeInspector.GetMemberType(index), item, valueBuilder);
            break;
        default:
            break;
    }
    Y_ABORT("Unsupported underlying type.");
}

}

TUnboxedValuePod MakeDom(const ITypeInfoHelper* typeHelper, const TType* shape, const TUnboxedValuePod value, const IValueBuilder* valueBuilder) {
    switch (const auto kind = typeHelper->GetTypeKind(shape)) {
        case ETypeKind::Null:
            return MakeEntity();
        case ETypeKind::EmptyList:
            return SetNodeType<ENodeType::List>(TUnboxedValuePod::Void());
        case ETypeKind::EmptyDict:
            return SetNodeType<ENodeType::Dict>(TUnboxedValuePod::Void());
        case ETypeKind::Data:
            return MakeData(TDataTypeInspector(*typeHelper, shape).GetTypeId(), value, valueBuilder);
        case ETypeKind::Optional:
            return value ? MakeDom(typeHelper, TOptionalTypeInspector(*typeHelper, shape).GetItemType(), value.GetOptionalValue(), valueBuilder) : MakeEntity();
        case ETypeKind::List:
            return MakeList(typeHelper, TListTypeInspector(*typeHelper, shape).GetItemType(), value, valueBuilder);
        case ETypeKind::Dict: {
            const auto dictTypeInspector = TDictTypeInspector(*typeHelper, shape);
            const auto keyType = dictTypeInspector.GetKeyType();
            Y_ABORT_UNLESS(ETypeKind::Data == typeHelper->GetTypeKind(keyType), "Unsupported dict key type kind.");
            const auto keyId = TDataTypeInspector(*typeHelper, keyType).GetTypeId();
            Y_ABORT_UNLESS(keyId == TDataType<char*>::Id || keyId == TDataType<TUtf8>::Id, "Unsupported dict key data type.");
            return MakeDict(typeHelper, dictTypeInspector.GetValueType(), value, valueBuilder);
        }
        case ETypeKind::Tuple:
            return MakeTuple(typeHelper, shape, value, valueBuilder);
        case ETypeKind::Struct:
            return MakeStruct(typeHelper, shape, value, valueBuilder);
        case ETypeKind::Variant:
            return MakeVariant(typeHelper, shape, value, valueBuilder);
        case ETypeKind::Resource:
            if (const auto inspector = TResourceTypeInspector(*typeHelper, shape); TStringBuf(inspector.GetTag()) == NodeResourceName)
                return value;
            [[fallthrough]];
        default:
            Y_ABORT("Unsupported data kind: %s", ToCString(kind));
    }
}

}
