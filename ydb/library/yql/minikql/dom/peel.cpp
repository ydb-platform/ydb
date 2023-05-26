#include "peel.h"
#include "node.h"
#include "yson.h"
#include "json.h"
#include "convert.h"

#include <ydb/library/yql/public/udf/udf_type_inspection.h>
#include <ydb/library/yql/public/udf/udf_type_printer.h>

namespace NYql::NDom {
using namespace NUdf;

namespace {

template<bool Strict, bool AutoConvert>
TUnboxedValuePod PeelData(const TDataTypeId nodeType, const TUnboxedValuePod value, const IValueBuilder* valueBuilder, const TSourcePosition& pos) {
    switch (nodeType) {
        case TDataType<char*>::Id:  return ConvertToString<Strict, AutoConvert, false>(value, valueBuilder, pos);
        case TDataType<TUtf8>::Id:  return ConvertToString<Strict, AutoConvert, true>(value, valueBuilder, pos);
        case TDataType<bool>::Id:   return ConvertToBool<Strict, AutoConvert>(value, valueBuilder, pos);
        case TDataType<i8>::Id:     return ConvertToIntegral<Strict, AutoConvert, i8>(value, valueBuilder, pos);
        case TDataType<i16>::Id:    return ConvertToIntegral<Strict, AutoConvert, i16>(value, valueBuilder, pos);
        case TDataType<i32>::Id:    return ConvertToIntegral<Strict, AutoConvert, i32>(value, valueBuilder, pos);
        case TDataType<i64>::Id:    return ConvertToIntegral<Strict, AutoConvert, i64>(value, valueBuilder, pos);
        case TDataType<ui8>::Id:    return ConvertToIntegral<Strict, AutoConvert, ui8>(value, valueBuilder, pos);
        case TDataType<ui16>::Id:   return ConvertToIntegral<Strict, AutoConvert, ui16>(value, valueBuilder, pos);
        case TDataType<ui32>::Id:   return ConvertToIntegral<Strict, AutoConvert, ui32>(value, valueBuilder, pos);
        case TDataType<ui64>::Id:   return ConvertToIntegral<Strict, AutoConvert, ui64>(value, valueBuilder, pos);
        case TDataType<float>::Id:  return ConvertToFloat<Strict, AutoConvert, float>(value, valueBuilder, pos);
        case TDataType<double>::Id: return ConvertToFloat<Strict, AutoConvert, double>(value, valueBuilder, pos);
        case TDataType<TYson>::Id:  return valueBuilder->NewString(SerializeYsonDomToBinary(value)).Release();
        case TDataType<TJson>::Id:  return valueBuilder->NewString(SerializeJsonDom(value)).Release();
        default: break;
    }

    UdfTerminate((::TStringBuilder() << "Unsupported data type: " << static_cast<int>(nodeType)).c_str());
}

template<bool Strict, bool AutoConvert>
TUnboxedValuePod TryPeelDom(const ITypeInfoHelper* typeHelper, const TType* shape, const TUnboxedValuePod value, const IValueBuilder* valueBuilder, const TSourcePosition& pos);

template<bool Strict, bool AutoConvert>
TUnboxedValuePod PeelList(const ITypeInfoHelper* typeHelper, const TType* itemType, const TUnboxedValuePod x, const IValueBuilder* valueBuilder, const TSourcePosition& pos) {
    switch (GetNodeType(x)) {
        case ENodeType::List: {
            if (!x.IsBoxed())
                break;
            if constexpr (Strict || AutoConvert) {
                return TUnboxedValuePod(new TLazyConveter(x, std::bind(&PeelDom<Strict, AutoConvert>, typeHelper, itemType, std::placeholders::_1, valueBuilder, pos)));
            }
            TSmallVec<TUnboxedValue, TUnboxedValue::TAllocator> values;
            if (const auto elements = x.GetElements()) {
                const auto size = x.GetListLength();
                values.reserve(size);
                for (ui32 i = 0U; i < size; ++i) {
                    if (const auto item = TryPeelDom<Strict, AutoConvert>(typeHelper, itemType, elements[i], valueBuilder, pos))
                        values.emplace_back(item.GetOptionalValue());
                    else if constexpr (Strict)
                        UdfTerminate("Error on convert list item.");
                }
            } else {
                const auto it = x.GetListIterator();
                for (TUnboxedValue v; it.Next(v);) {
                    if (const auto item = TryPeelDom<Strict, AutoConvert>(typeHelper, itemType, v, valueBuilder, pos))
                        values.emplace_back(item.GetOptionalValue());
                    else if constexpr (Strict)
                        UdfTerminate("Error on convert list item.");
                }
            }
            if (values.empty()) {
                break;
            }
            return valueBuilder->NewList(values.data(), values.size()).Release();
        }
        case ENodeType::Attr:
            return PeelList<Strict, AutoConvert>(typeHelper, itemType, x.GetVariantItem().Release(), valueBuilder, pos);
        default:
            if constexpr (AutoConvert)
                break;
            else if constexpr (Strict)
                UdfTerminate("Cannot parse list from entity, scalar value or dict.");
            else
                return {};
    }

    return valueBuilder->NewEmptyList().Release();
}

template<bool Strict, bool AutoConvert, bool Utf8Keys>
TUnboxedValuePod PeelDict(const ITypeInfoHelper* typeHelper, const TType* itemType, const TUnboxedValuePod x, const IValueBuilder* valueBuilder, const TSourcePosition& pos) {
    switch (GetNodeType(x)) {
        case ENodeType::Dict:
            if (!x.IsBoxed())
                break;
            if constexpr (!Utf8Keys && (Strict || AutoConvert)) {
                return TUnboxedValuePod(new TLazyConveter(x, std::bind(&PeelDom<Strict, AutoConvert>, typeHelper, itemType, std::placeholders::_1, valueBuilder, pos)));
            }
            if (const auto size = x.GetDictLength()) {
                TSmallVec<TPair, TStdAllocatorForUdf<TPair>> pairs;
                pairs.reserve(size);
                const auto it = x.GetDictIterator();
                for (TUnboxedValue key, payload; it.NextPair(key, payload);) {
                    if (const auto k = ConvertToString<Strict, AutoConvert, Utf8Keys>(key.Release(), valueBuilder, pos)) {
                        if (const auto item = TryPeelDom<Strict, AutoConvert>(typeHelper, itemType, payload, valueBuilder, pos)) {
                            pairs.emplace_back(std::move(k), item.GetOptionalValue());
                            continue;
                        }
                    }

                    if constexpr (Strict)
                        UdfTerminate("Error on convert dict payload.");
                }
                if (pairs.empty()) {
                    break;
                }
                return TUnboxedValuePod(new TMapNode(pairs.data(), pairs.size()));
            }
            break;
        case ENodeType::Attr:
            return PeelDict<Strict, AutoConvert, Utf8Keys>(typeHelper, itemType, x.GetVariantItem().Release(), valueBuilder, pos);
        default:
            if constexpr (AutoConvert)
                break;
            else if constexpr (Strict)
                UdfTerminate("Cannot parse dict from entity, scalar value or list.");
            else
                return {};
    }

    return valueBuilder->NewEmptyList().Release();
}

TUnboxedValuePod MakeStub(const ITypeInfoHelper* typeHelper, const TType* shape, const IValueBuilder* valueBuilder, const TSourcePosition& pos) {
    switch (const auto kind = typeHelper->GetTypeKind(shape)) {
        case ETypeKind::Optional:
            return TUnboxedValuePod();
        case ETypeKind::Data:
            switch (const auto nodeType = TDataTypeInspector(*typeHelper, shape).GetTypeId()) {
                case TDataType<char*>::Id:
                case TDataType<TUtf8>::Id:
                case TDataType<bool>::Id:
                case TDataType<i8>::Id:
                case TDataType<i16>::Id:
                case TDataType<i32>::Id:
                case TDataType<i64>::Id:
                case TDataType<ui8>::Id:
                case TDataType<ui16>::Id:
                case TDataType<ui32>::Id:
                case TDataType<ui64>::Id:
                case TDataType<float>::Id:
                case TDataType<double>::Id:
                case TDataType<TDecimal>::Id:
                    return TUnboxedValuePod::Zero();
                case TDataType<TYson>::Id:
                    return TUnboxedValuePod::Embedded("#");
                case TDataType<TJson>::Id:
                    return TUnboxedValuePod::Embedded("null");
                default:
                    UdfTerminate((::TStringBuilder() << "Unsupported data type: " << static_cast<int>(nodeType)).c_str());
            }
        case ETypeKind::Tuple:
            if (const auto tupleTypeInspector = TTupleTypeInspector(*typeHelper, shape); auto count = tupleTypeInspector.GetElementsCount()) {
                TUnboxedValue* items = nullptr;
                auto result = valueBuilder->NewArray(count, items);
                items += count;
                do *--items = MakeStub(typeHelper, tupleTypeInspector.GetElementType(--count), valueBuilder, pos);
                while (count);
                return result.Release();
            }
            return valueBuilder->NewEmptyList().Release();
        case ETypeKind::Struct:
            if (const auto structTypeInspector = TStructTypeInspector(*typeHelper, shape); auto count = structTypeInspector.GetMembersCount()) {
                TUnboxedValue* items = nullptr;
                auto result = valueBuilder->NewArray(count, items);
                items += count;
                do *--items = MakeStub(typeHelper, structTypeInspector.GetMemberType(--count), valueBuilder, pos);
                while (count);
                return result.Release();
            }
            return valueBuilder->NewEmptyList().Release();
        case ETypeKind::List:
        case ETypeKind::Dict:
            return valueBuilder->NewEmptyList().Release();
        case ETypeKind::Resource:
            if (const auto inspector = TResourceTypeInspector(*typeHelper, shape); TStringBuf(inspector.GetTag()) == NodeResourceName)
                return MakeEntity();
            [[fallthrough]];
        default:
            UdfTerminate((::TStringBuilder() << "Unsupported data kind: " << kind).c_str());
    }
}

template<bool Strict, bool AutoConvert>
TUnboxedValuePod PeelTuple(const ITypeInfoHelper* typeHelper, const TType* shape, const TUnboxedValuePod x, const IValueBuilder* valueBuilder, const TSourcePosition& pos) {
    if (const auto tupleTypeInspector = TTupleTypeInspector(*typeHelper, shape); auto count = tupleTypeInspector.GetElementsCount()) {
        switch (GetNodeType(x)) {
            case ENodeType::List: {
                TUnboxedValue* items = nullptr;
                auto result = valueBuilder->NewArray(count, items);
                ui32 i = 0U;
                if (x.IsBoxed()) {
                    if (auto elements = x.GetElements()) {
                        for (auto size = x.GetListLength(); count && size--; --count) {
                            if (const auto item = TryPeelDom<Strict, AutoConvert>(typeHelper, tupleTypeInspector.GetElementType(i++), *elements++, valueBuilder, pos))
                                *items++ = item.GetOptionalValue();
                            else if constexpr (Strict)
                                UdfTerminate("Error on convert tuple item.");
                            else
                                return {};
                        }
                    } else if (const auto it = x.GetListIterator()) {
                        for (TUnboxedValue v; count && it.Next(v); --count) {
                            if (const auto item = TryPeelDom<Strict, AutoConvert>(typeHelper, tupleTypeInspector.GetElementType(i++), v, valueBuilder, pos))
                                *items++ = item.GetOptionalValue();
                            else if constexpr (Strict)
                                UdfTerminate("Error on convert tuple item.");
                            else
                                return {};
                        }
                    }
                }
                if (count) do
                    if constexpr (AutoConvert)
                        *items++ = MakeStub(typeHelper, tupleTypeInspector.GetElementType(i++), valueBuilder, pos);
                    else if (ETypeKind::Optional == typeHelper->GetTypeKind(tupleTypeInspector.GetElementType(i++)))
                        ++items;
                    else if constexpr (Strict)
                        UdfTerminate((::TStringBuilder() << "DOM list has less items then " << tupleTypeInspector.GetElementsCount() << " tuple elements.").c_str());
                    else
                        return {};
                while (--count);
                return result.Release();
            }
            case ENodeType::Attr:
                return PeelTuple<Strict, AutoConvert>(typeHelper, shape, x.GetVariantItem().Release(), valueBuilder, pos);
            default:
                if constexpr (AutoConvert) {
                    TUnboxedValue* items = nullptr;
                    auto result = valueBuilder->NewArray(count, items);
                    for (ui32 i = 0ULL; i < count; ++i)
                        if (ETypeKind::Optional != typeHelper->GetTypeKind(tupleTypeInspector.GetElementType(i)))
                            *items++ = MakeStub(typeHelper, tupleTypeInspector.GetElementType(i), valueBuilder, pos);
                        else
                            ++items;
                    return result.Release();
                 } else if constexpr (Strict)
                    UdfTerminate("Cannot parse tuple from entity, scalar value or dict.");
                else
                    break;
        }
    }

    return {};
}

template<bool Strict, bool AutoConvert>
TUnboxedValuePod PeelStruct(const ITypeInfoHelper* typeHelper, const TType* shape, const TUnboxedValuePod x, const IValueBuilder* valueBuilder, const TSourcePosition& pos) {
    if (const auto structTypeInspector = TStructTypeInspector(*typeHelper, shape)) {
        const auto size = structTypeInspector.GetMembersCount();
        switch (GetNodeType(x)) {
            case ENodeType::Dict: {
                TUnboxedValue* items = nullptr;
                auto result = valueBuilder->NewArray(size, items);
                for (ui32 i = 0ULL; i < size; ++i) {
                    if (x.IsBoxed()) {
                        if (const auto v = x.Lookup(valueBuilder->NewString(structTypeInspector.GetMemberName(i)))) {
                            if (const auto item = TryPeelDom<Strict, AutoConvert>(typeHelper, structTypeInspector.GetMemberType(i), v.GetOptionalValue(), valueBuilder, pos))
                                *items++ = item.GetOptionalValue();
                            else if constexpr (Strict)
                                UdfTerminate((::TStringBuilder() << "Error on convert struct member '" << structTypeInspector.GetMemberName(i) << "'.").c_str());
                            else
                                return {};
                            continue;
                        }
                    }
                    if constexpr (AutoConvert)
                        *items++ = MakeStub(typeHelper, structTypeInspector.GetMemberType(i), valueBuilder, pos);
                    else if (ETypeKind::Optional == typeHelper->GetTypeKind(structTypeInspector.GetMemberType(i)))
                        ++items;
                    else if constexpr (Strict)
                        UdfTerminate((::TStringBuilder() << "Missed struct member '" << structTypeInspector.GetMemberName(i) << "'.").c_str());
                    else
                        return {};
                }
                return result.Release();
            }
            case ENodeType::Attr:
                return PeelStruct<Strict, AutoConvert>(typeHelper, shape, x.GetVariantItem().Release(), valueBuilder, pos);
            default:
                if constexpr (AutoConvert) {
                    TUnboxedValue* items = nullptr;
                    auto result = valueBuilder->NewArray(size, items);
                    for (ui32 i = 0ULL; i < size; ++i)
                        if (ETypeKind::Optional != typeHelper->GetTypeKind(structTypeInspector.GetMemberType(i)))
                            *items++ = MakeStub(typeHelper, structTypeInspector.GetMemberType(i), valueBuilder, pos);
                        else
                            ++items;
                    return result.Release();
                } else if constexpr (Strict)
                    UdfTerminate("Cannot parse struct from entity, scalar value or list.");
                else
                    break;
        }
    }

    return {};
}

template<bool Strict, bool AutoConvert>
TUnboxedValuePod PeelOptional(const ITypeInfoHelper* typeHelper, const TType* itemType, const TUnboxedValuePod value, const IValueBuilder* valueBuilder, const TSourcePosition& pos) {
    if (IsNodeType<ENodeType::Entity>(value))
        return TUnboxedValuePod().MakeOptional();

    if (const auto result = TryPeelDom<Strict, AutoConvert>(typeHelper, itemType, value, valueBuilder, pos); AutoConvert || result)
        return result;
    else if constexpr (Strict)
        UdfTerminate("Failed to convert Yson DOM.");
    else
        return TUnboxedValuePod().MakeOptional();
}

template<bool Strict, bool AutoConvert>
TUnboxedValuePod TryPeelDom(const ITypeInfoHelper* typeHelper, const TType* shape, const TUnboxedValuePod value, const IValueBuilder* valueBuilder, const TSourcePosition& pos) {
    switch (const auto kind = typeHelper->GetTypeKind(shape)) {
        case ETypeKind::Data:
            return PeelData<Strict, AutoConvert>(TDataTypeInspector(*typeHelper, shape).GetTypeId(), value, valueBuilder, pos);
        case ETypeKind::Optional:
            return PeelOptional<Strict, AutoConvert>(typeHelper, TOptionalTypeInspector(*typeHelper, shape).GetItemType(), value, valueBuilder, pos);
        case ETypeKind::List:
            return PeelList<Strict, AutoConvert>(typeHelper, TListTypeInspector(*typeHelper, shape).GetItemType(), value, valueBuilder, pos);
        case ETypeKind::Dict: {
            const auto dictTypeInspector = TDictTypeInspector(*typeHelper, shape);
            const auto keyType = dictTypeInspector.GetKeyType();
            if (const auto keyKind = typeHelper->GetTypeKind(keyType); ETypeKind::Data == keyKind)
                switch (const auto keyId = TDataTypeInspector(*typeHelper, keyType).GetTypeId()) {
                    case TDataType<char*>::Id: return PeelDict<Strict, AutoConvert, false>(typeHelper, dictTypeInspector.GetValueType(), value, valueBuilder, pos);
                    case TDataType<TUtf8>::Id: return PeelDict<Strict, AutoConvert, true>(typeHelper, dictTypeInspector.GetValueType(), value, valueBuilder, pos);
                    default: UdfTerminate((::TStringBuilder() << "Unsupported dict key type: " << keyId).c_str());
                }
            else
                UdfTerminate((::TStringBuilder() << "Unsupported dict key kind: " << keyKind).c_str());
        }
        case ETypeKind::Tuple:
            return PeelTuple<Strict, AutoConvert>(typeHelper, shape, value, valueBuilder, pos);
        case ETypeKind::Struct:
            return PeelStruct<Strict, AutoConvert>(typeHelper, shape, value, valueBuilder, pos);
        case ETypeKind::Resource:
            if (const auto inspector = TResourceTypeInspector(*typeHelper, shape); TStringBuf(inspector.GetTag()) == NodeResourceName)
                return value;
            [[fallthrough]]; // AUTOGENERATED_FALLTHROUGH_FIXME
        default:
            UdfTerminate((::TStringBuilder() << "Unsupported data kind: " << kind).c_str());
    }
}

}

template<bool Strict, bool AutoConvert>
TUnboxedValuePod PeelDom(const ITypeInfoHelper* typeHelper, const TType* shape, const TUnboxedValuePod value, const IValueBuilder* valueBuilder, const TSourcePosition& pos) {
    if (const auto result = TryPeelDom<Strict, AutoConvert>(typeHelper, shape, value, valueBuilder, pos))
        return result.GetOptionalValue();
    ::TStringBuilder sb;
    sb << "Failed to convert Yson DOM into strict type: ";
    TTypePrinter(*typeHelper, shape).Out(sb.Out);
    UdfTerminate(sb.c_str());
}

template TUnboxedValuePod PeelDom<true, true>(const ITypeInfoHelper* typeHelper, const TType* shape, const TUnboxedValuePod value, const IValueBuilder* valueBuilder, const TSourcePosition& pos);
template TUnboxedValuePod PeelDom<false, true>(const ITypeInfoHelper* typeHelper, const TType* shape, const TUnboxedValuePod value, const IValueBuilder* valueBuilder, const TSourcePosition& pos);
template TUnboxedValuePod PeelDom<true, false>(const ITypeInfoHelper* typeHelper, const TType* shape, const TUnboxedValuePod value, const IValueBuilder* valueBuilder, const TSourcePosition& pos);
template TUnboxedValuePod PeelDom<false, false>(const ITypeInfoHelper* typeHelper, const TType* shape, const TUnboxedValuePod value, const IValueBuilder* valueBuilder, const TSourcePosition& pos);

}
