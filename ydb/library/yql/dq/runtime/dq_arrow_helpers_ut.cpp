#include "dq_arrow_helpers.h"

#include <ydb/library/yverify_stream/yverify_stream.h>

#include <memory>
#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/public/udf/udf_string_ref.h>
#include <ydb/library/yql/public/udf/udf_type_ops.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_value_builder.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_nested.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>

#include <util/string/builder.h>
#include <util/system/yassert.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NYql;

namespace {
NUdf::TUnboxedValue GetValueOfBasicType(TType* type, ui64 value) {
    Y_ABORT_UNLESS(type->GetKind() == TType::EKind::Data);
    auto dataType = static_cast<const TDataType*>(type);
    auto slot = *dataType->GetDataSlot().Get();
    switch(slot) {
        case NUdf::EDataSlot::Bool:
            return NUdf::TUnboxedValuePod(static_cast<bool>(value % 2 == 0));
        case NUdf::EDataSlot::Int8:
            return NUdf::TUnboxedValuePod(static_cast<i8>(-(value % 126)));
        case NUdf::EDataSlot::Uint8:
            return NUdf::TUnboxedValuePod(static_cast<ui8>(value % 255));
        case NUdf::EDataSlot::Int16:
            return NUdf::TUnboxedValuePod(static_cast<i16>(-(value % ((1 << 15) - 1))));
        case NUdf::EDataSlot::Uint16:
            return NUdf::TUnboxedValuePod(static_cast<ui16>(value % (1 << 16)));
        case NUdf::EDataSlot::Int32:
            return NUdf::TUnboxedValuePod(static_cast<i32>(-(value % ((1 << 31) - 1))));
        case NUdf::EDataSlot::Uint32:
            return NUdf::TUnboxedValuePod(static_cast<ui32>(value % (1 << 31)));
        case NUdf::EDataSlot::Int64:
            return NUdf::TUnboxedValuePod(static_cast<i64>(- (value / 2)));
        case NUdf::EDataSlot::Uint64:
            return NUdf::TUnboxedValuePod(static_cast<ui64>(value));
        case NUdf::EDataSlot::Float:
            return NUdf::TUnboxedValuePod(static_cast<float>(value) / 1234);
        case NUdf::EDataSlot::Double:
            return NUdf::TUnboxedValuePod(static_cast<double>(value) / 12345);
        default:
            Y_ABORT("Not implemented creation value for such type");
    }
}

struct TTestContext {
    TScopedAlloc Alloc;
    TTypeEnvironment TypeEnv;
    TMemoryUsageInfo MemInfo;
    THolderFactory HolderFactory;
    TDefaultValueBuilder Vb;
    ui16 VariantSize = 0;

    // Used to create LargeVariantType
    TVector<TType*> BasicTypes = {
        TDataType::Create(NUdf::TDataType<bool>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<i8>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<ui8>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<i16>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<ui16>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<i32>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<ui32>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<i64>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<ui64>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<float>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<double>::Id, TypeEnv)
    };

    TTestContext()
        : Alloc(__LOCATION__)
        , TypeEnv(Alloc)
        , MemInfo("TestMem")
        , HolderFactory(Alloc.Ref(), MemInfo)
        , Vb(HolderFactory)
    {
    }

    TType* GetStructType() {
        TStructMember members[3] = {
            {"s", TDataType::Create(NUdf::TDataType<char*>::Id, TypeEnv)},
            {"x", TDataType::Create(NUdf::TDataType<i32>::Id, TypeEnv)},
            {"y", TDataType::Create(NUdf::TDataType<ui64>::Id, TypeEnv)}
        };
        return TStructType::Create(3, members, TypeEnv);
    }

    TUnboxedValueVector CreateStructs(ui32 quantity) {
        TUnboxedValueVector values;
        for (ui32 value = 0; value < quantity; ++value) {
            NUdf::TUnboxedValue* items;
            auto structValue = Vb.NewArray(3, items);
            std::string string = TStringBuilder() << value;
            items[0] = MakeString(NUdf::TStringRef(string.data(), string.size()));
            items[1] = NUdf::TUnboxedValuePod(static_cast<i32>(-value));
            items[2] = NUdf::TUnboxedValuePod((ui64) (value * value));
            values.emplace_back(std::move(structValue));
        }
        return values;
    }

    TType* GetTupleType() {
        TType* members[3] = {
            TDataType::Create(NUdf::TDataType<bool>::Id, TypeEnv),
            TDataType::Create(NUdf::TDataType<i8>::Id, TypeEnv),
            TDataType::Create(NUdf::TDataType<ui8>::Id, TypeEnv)
        };
        return TTupleType::Create(3, members, TypeEnv);
    }

    TUnboxedValueVector CreateTuples(ui32 quantity) {
        NKikimr::NMiniKQL::TUnboxedValueVector values;
        for (ui32 value = 0; value < quantity; ++value) {
            auto array = NArrow::MakeArray(values, GetTupleType());
            auto str = NArrow::SerializeArray(array);
            NUdf::TUnboxedValue* items;
            auto tupleValue = Vb.NewArray(3, items);
            items[0] = NUdf::TUnboxedValuePod(value % 3 == 0);
            items[1] = NUdf::TUnboxedValuePod(static_cast<i8>(-value));
            items[2] = NUdf::TUnboxedValuePod(static_cast<ui8>(value));
            values.push_back(std::move(tupleValue));
        }
        return values;
    }

    TType* GetDictUtf8ToIntervalType() {
        TType* keyType = TDataType::Create(NUdf::TDataType<NUdf::TUtf8>::Id, TypeEnv);
        TType* payloadType = TDataType::Create(NUdf::TDataType<NUdf::TInterval>::Id, TypeEnv);
        return TDictType::Create(keyType, payloadType, TypeEnv);
    }

    TUnboxedValueVector CreateDictUtf8ToInterval(ui32 quantity) {
        NKikimr::NMiniKQL::TUnboxedValueVector values;
        auto dictType = GetDictUtf8ToIntervalType();
        for (ui32 value = 0; value < quantity; ++value) {
            auto dictBuilder = Vb.NewDict(dictType, 0);
            for (ui32 i = 0; i < value * value; ++i) {
                std::string string = TStringBuilder() << "This is a long string #" << i;
                NUdf::TUnboxedValue key = MakeString(NUdf::TStringRef(string.data(), string.size()));
                NUdf::TUnboxedValue payload = NUdf::TUnboxedValuePod(static_cast<i64>(value * i));
                dictBuilder->Add(std::move(key), std::move(payload));
            }
            auto dictValue = dictBuilder->Build();
            values.emplace_back(std::move(dictValue));
        }
        return values;
    }

    TType* GetListOfJsonsType() {
        TType* itemType = TDataType::Create(NUdf::TDataType<NUdf::TJson>::Id, TypeEnv);
        return TListType::Create(itemType, TypeEnv);
    }

    TUnboxedValueVector CreateListOfJsons(ui32 quantity) {
        TUnboxedValueVector values;
        for (ui64 value = 0; value < quantity; ++value) {
            TUnboxedValueVector items;
            items.reserve(value);
            for (ui64 i = 0; i < value; ++i) {
                std::string json = TStringBuilder() << "{'item':" << i << "}";
                items.push_back(MakeString(NUdf::TStringRef(json.data(), json.size())));
            }
            auto listValue = Vb.NewList(items.data(), value);
            values.emplace_back(std::move(listValue));
        }
        return values;
    }

    TType* GetVariantOverStructType() {
        TStructMember members[4] = {
            {"0_yson", TDataType::Create(NUdf::TDataType<NUdf::TYson>::Id, TypeEnv)},
            {"1_json-document", TDataType::Create(NUdf::TDataType<NUdf::TJsonDocument>::Id, TypeEnv)},
            {"2_uuid", TDataType::Create(NUdf::TDataType<NUdf::TUuid>::Id, TypeEnv)},
            {"3_float", TDataType::Create(NUdf::TDataType<float>::Id, TypeEnv)}
        };
        auto structType = TStructType::Create(4, members, TypeEnv);
        return TVariantType::Create(structType, TypeEnv);
    }

    TUnboxedValueVector CreateVariantOverStruct(ui32 quantity) {
        TUnboxedValueVector values;
        for (ui64 value = 0; value < quantity; ++value) {
            auto typeIndex = value % 4;
            NUdf::TUnboxedValue item;
            if (typeIndex == 0) {
                std::string data = TStringBuilder() << "{value=" << value << "}";
                item = MakeString(NUdf::TStringRef(data.data(), data.size()));
            } else if (typeIndex == 1) {
                std::string data = TStringBuilder() << "{value:" << value << "}";
                item = MakeString(NUdf::TStringRef(data.data(), data.size()));
            } else if (typeIndex == 2) {
                std::string data = TStringBuilder() << "id-QwErY-" << value;
                item = MakeString(NUdf::TStringRef(data.data(), data.size()));
            } else if (typeIndex == 3) {
                item = NUdf::TUnboxedValuePod(static_cast<float>(value) / 4);
            }
            auto wrapped = Vb.NewVariant(typeIndex, std::move(item));
            values.push_back(std::move(wrapped));
        }
        return values;
    }

    TType* GetVariantOverTupleWithOptionalsType() {
        TType* members[5] = {
            TDataType::Create(NUdf::TDataType<bool>::Id, TypeEnv),
            TDataType::Create(NUdf::TDataType<i16>::Id, TypeEnv),
            TDataType::Create(NUdf::TDataType<ui16>::Id, TypeEnv),
            TDataType::Create(NUdf::TDataType<i32>::Id, TypeEnv),
            TOptionalType::Create(TDataType::Create(NUdf::TDataType<ui32>::Id, TypeEnv), TypeEnv)
        };
        auto tupleType = TTupleType::Create(5, members, TypeEnv);
        return TVariantType::Create(tupleType, TypeEnv);
    }

    TUnboxedValueVector CreateVariantOverTupleWithOptionals(ui32 quantity) {
        NKikimr::NMiniKQL::TUnboxedValueVector values;
        for (ui64 value = 0; value < quantity; ++value) {
            auto typeIndex = value % 5;
            NUdf::TUnboxedValue item;
            if (typeIndex == 0) {
                item = NUdf::TUnboxedValuePod(value % 3 == 0);
            } else if (typeIndex == 1) {
                item = NUdf::TUnboxedValuePod(static_cast<i16>(-value));
            } else if (typeIndex == 2) {
                item = NUdf::TUnboxedValuePod(static_cast<ui16>(value));
            } else if (typeIndex == 3) {
                item = NUdf::TUnboxedValuePod(static_cast<i32>(-value));
            } else if (typeIndex == 4) {
                NUdf::TUnboxedValue innerItem;
                innerItem = value % 2 == 0
                            ? NUdf::TUnboxedValuePod(static_cast<i32>(value))
                            : NUdf::TUnboxedValuePod();
                item = innerItem.MakeOptional();
            }
            auto wrapped = Vb.NewVariant(typeIndex, std::move(item));
            values.emplace_back(std::move(wrapped));
        }
        return values;
    }

    TType* GetDictOptionalToTupleType() {
        TType* keyType = TOptionalType::Create(TDataType::Create(NUdf::TDataType<double>::Id, TypeEnv), TypeEnv);
        TType* members[2] = {
            TDataType::Create(NUdf::TDataType<i32>::Id, TypeEnv),
            TDataType::Create(NUdf::TDataType<ui32>::Id, TypeEnv),
        };
        TType* payloadType = TTupleType::Create(2, members, TypeEnv);
        return TDictType::Create(keyType, payloadType, TypeEnv);
    }

    TUnboxedValueVector CreateDictOptionalToTuple(ui32 quantity) {
        NKikimr::NMiniKQL::TUnboxedValueVector values;
        for (ui64 value = 0; value < quantity; ++value) {
            auto dictBuilder = Vb.NewDict(GetDictOptionalToTupleType(), 0);
            for (ui64 i = 0; i < value * value; ++i) {
                NUdf::TUnboxedValue key;
                if (i == 0) {
                    key = NUdf::TUnboxedValuePod();
                } else {
                    key = NUdf::TUnboxedValuePod(value / 4).MakeOptional();
                }
                NUdf::TUnboxedValue* items;
                auto payload = Vb.NewArray(2, items);
                items[0] = NUdf::TUnboxedValuePod(static_cast<i32>(-value));
                items[1] = NUdf::TUnboxedValuePod(static_cast<ui32>(value));
                dictBuilder->Add(std::move(key), std::move(payload));
            }
            auto dictValue = dictBuilder->Build();
            values.emplace_back(std::move(dictValue));
        }
        return values;
    }

    TType* GetOptionalOfOptionalType() {
        return TOptionalType::Create(
                   TOptionalType::Create(
                       TDataType::Create(NUdf::TDataType<i32>::Id, TypeEnv),
                       TypeEnv),
                   TypeEnv);
    }

    TUnboxedValueVector CreateOptionalOfOptional(ui32 quantity) {
        TUnboxedValueVector values;
        for (ui64 value = 0; value < quantity; ++value) {
            NUdf::TUnboxedValue element = value % 3 == 0
                                        ? NUdf::TUnboxedValuePod(value).MakeOptional()
                                        : NUdf::TUnboxedValuePod();
            if (value % 3 != 2) {
                element = element.MakeOptional();
            }
            values.emplace_back(std::move(element));
        }
        return values;
    }

    TType* GetLargeVariantType(const ui16 variantSize) {
        VariantSize = variantSize;
        TVector<TType*> tupleTypes;
        tupleTypes.reserve(variantSize);
        for (ui64 index = 0; index < variantSize; ++index) {
            TVector<TType*> selectedTypes;
            for (ui32 i = 0; i < BasicTypes.size(); ++i) {
                if ((index >> i) % 2 == 1) {
                    selectedTypes.push_back(BasicTypes[i]);
                }
            }
            tupleTypes.push_back(TTupleType::Create(selectedTypes.size(), selectedTypes.data(), TypeEnv));
        }
        auto tupleOfTuplesType = TTupleType::Create(variantSize, tupleTypes.data(), TypeEnv);
        return TVariantType::Create(tupleOfTuplesType, TypeEnv);
    }

    TUnboxedValueVector CreateLargeVariant(ui32 quantity) {
        TUnboxedValueVector values;
        for (ui64 index = 0; index < quantity; ++index) {
            NUdf::TUnboxedValue item;
            auto typeIndex = index % VariantSize;
            TUnboxedValueVector tupleItems;
            for (ui64 i = 0; i < BasicTypes.size(); ++i) {
                if ((typeIndex >> i) % 2 == 1) {
                    tupleItems.push_back(GetValueOfBasicType(BasicTypes[i], i));
                }
            }
            auto wrapped = Vb.NewVariant(typeIndex, HolderFactory.VectorAsArray(tupleItems));
            values.emplace_back(std::move(wrapped));
        }
        return values;
    }
};

// Note this equality check is not fully valid. But it is sufficient for UnboxedValues used in tests.
void AssertUnboxedValuesAreEqual(NUdf::TUnboxedValue& left, NUdf::TUnboxedValue& right, TType* type) {
    switch (type->GetKind()) {
        case TType::EKind::Void:
        case TType::EKind::Null:
        case TType::EKind::EmptyList:
        case TType::EKind::EmptyDict: {
            UNIT_ASSERT(!left.HasValue());
            UNIT_ASSERT(!right.HasValue());
            break;
        }

        case TType::EKind::Data: {
            auto dataType = static_cast<const TDataType*>(type);
            auto dataSlot = *dataType->GetDataSlot().Get();
            // Json-like type are not comparable so just skip them
            if (dataSlot != NUdf::EDataSlot::Json && dataSlot != NUdf::EDataSlot::Yson && dataSlot != NUdf::EDataSlot::JsonDocument) {
                UNIT_ASSERT(NUdf::EquateValues(dataSlot, left, right));
            }
            break;
        }

        case TType::EKind::Optional: {
            UNIT_ASSERT_EQUAL(left.HasValue(), right.HasValue());
            if (left.HasValue()) {
                auto innerType = static_cast<const TOptionalType*>(type)->GetItemType();
                NUdf::TUnboxedValue leftInner = left.GetOptionalValue();
                NUdf::TUnboxedValue rightInner = right.GetOptionalValue();
                AssertUnboxedValuesAreEqual(leftInner, rightInner, innerType);
            }
            break;
        }

        case TType::EKind::List: {
            auto listType = static_cast<const TListType*>(type);
            auto itemType = listType->GetItemType();
            auto leftPtr = left.GetElements();
            auto rightPtr = right.GetElements();
            UNIT_ASSERT_EQUAL(leftPtr != nullptr, rightPtr != nullptr);
            if (leftPtr != nullptr) {
                auto leftLen = left.GetListLength();
                auto rightLen = right.GetListLength();
                UNIT_ASSERT_EQUAL(leftLen, rightLen);
                while (leftLen > 0) {
                    NUdf::TUnboxedValue leftItem = *leftPtr++;
                    NUdf::TUnboxedValue rightItem = *rightPtr++;
                    AssertUnboxedValuesAreEqual(leftItem, rightItem, itemType);
                    --leftLen;
                }
            } else {
                const auto leftIter = left.GetListIterator();
                const auto rightIter = right.GetListIterator();
                NUdf::TUnboxedValue leftItem;
                NUdf::TUnboxedValue rightItem;
                bool leftHasValue = leftIter.Next(leftItem);
                bool rightHasValue = rightIter.Next(leftItem);
                while (leftHasValue && rightHasValue) {
                    AssertUnboxedValuesAreEqual(leftItem, rightItem, itemType);
                    leftHasValue = leftIter.Next(leftItem);
                    rightHasValue = rightIter.Next(leftItem);
                }
                UNIT_ASSERT_EQUAL(leftHasValue, rightHasValue);
            }
            break;
        }

        case TType::EKind::Struct: {
            auto structType = static_cast<const TStructType*>(type);
            UNIT_ASSERT_EQUAL(left.GetListLength(), structType->GetMembersCount());
            UNIT_ASSERT_EQUAL(right.GetListLength(), structType->GetMembersCount());
            for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
                auto memberType = structType->GetMemberType(index);
                NUdf::TUnboxedValue leftMember = left.GetElement(index);
                NUdf::TUnboxedValue rightMember = right.GetElement(index);
                AssertUnboxedValuesAreEqual(leftMember, rightMember, memberType);
            }
            break;
        }

        case TType::EKind::Tuple: {
            auto tupleType = static_cast<const TTupleType*>(type);
            UNIT_ASSERT_EQUAL(left.GetListLength(), tupleType->GetElementsCount());
            UNIT_ASSERT_EQUAL(right.GetListLength(), tupleType->GetElementsCount());
            for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
                auto elementType = tupleType->GetElementType(index);
                NUdf::TUnboxedValue leftMember = left.GetElement(index);
                NUdf::TUnboxedValue rightMember = right.GetElement(index);
                AssertUnboxedValuesAreEqual(leftMember, rightMember, elementType);
            }
            break;
        }

        case TType::EKind::Dict: {
            auto dictType = static_cast<const TDictType*>(type);
            auto payloadType = dictType->GetPayloadType();

            UNIT_ASSERT_EQUAL(left.GetDictLength(), right.GetDictLength());
            const auto leftIter = left.GetDictIterator();
            for (NUdf::TUnboxedValue key, leftPayload; leftIter.NextPair(key, leftPayload);) {
                UNIT_ASSERT(right.Contains(key));
                NUdf::TUnboxedValue rightPayload = right.Lookup(key);
                AssertUnboxedValuesAreEqual(leftPayload, rightPayload, payloadType);
            }
            break;
        }

        case TType::EKind::Variant: {
            auto variantType = static_cast<const TVariantType*>(type);
            UNIT_ASSERT_EQUAL(left.GetVariantIndex(), right.GetVariantIndex());
            ui32 variantIndex = left.GetVariantIndex();
            TType* innerType = variantType->GetUnderlyingType();
            if (innerType->IsStruct()) {
                innerType = static_cast<TStructType*>(innerType)->GetMemberType(variantIndex);
            } else {
                Y_VERIFY_S(innerType->IsTuple(), "Unexpected underlying variant type: " << innerType->GetKindAsStr());
                innerType = static_cast<TTupleType*>(innerType)->GetElementType(variantIndex);
            }
            NUdf::TUnboxedValue leftValue = left.GetVariantItem();
            NUdf::TUnboxedValue rightValue = right.GetVariantItem();
            AssertUnboxedValuesAreEqual(leftValue, rightValue, innerType);
            break;
        }

    default:
        THROW yexception() << "Unsupported type: " << type->GetKindAsStr();
    }
}
}


Y_UNIT_TEST_SUITE(DqUnboxedValueToNativeArrowConversion) {
    Y_UNIT_TEST(Struct) {
        TTestContext context;

        auto structType = context.GetStructType();
        UNIT_ASSERT(NArrow::IsArrowCompatible(structType));

        auto values = context.CreateStructs(100);
        auto array = NArrow::MakeArray(values, structType);

        UNIT_ASSERT(array->ValidateFull().ok());
        UNIT_ASSERT(array->length() == static_cast<i64>(values.size()));
        UNIT_ASSERT(array->type_id() == arrow::Type::STRUCT);
        auto structArray = static_pointer_cast<arrow::StructArray>(array);
        UNIT_ASSERT(structArray->num_fields() == 3);
        UNIT_ASSERT(structArray->field(0)->type_id() == arrow::Type::BINARY);
        UNIT_ASSERT(structArray->field(1)->type_id() == arrow::Type::INT32);
        UNIT_ASSERT(structArray->field(2)->type_id() == arrow::Type::UINT64);
        UNIT_ASSERT(static_cast<ui64>(structArray->field(0)->length()) == values.size());
        UNIT_ASSERT(static_cast<ui64>(structArray->field(1)->length()) == values.size());
        UNIT_ASSERT(static_cast<ui64>(structArray->field(2)->length()) == values.size());
        auto binaryArray = static_pointer_cast<arrow::BinaryArray>(structArray->field(0));
        auto int32Array = static_pointer_cast<arrow::Int32Array>(structArray->field(1));
        auto uint64Array = static_pointer_cast<arrow::UInt64Array>(structArray->field(2));
        auto index = 0;
        for (const auto& value: values) {
            auto stringValue = value.GetElement(0);
            auto stringRef = stringValue.AsStringRef();
            auto stringView = binaryArray->GetView(index);
            UNIT_ASSERT_EQUAL(std::string(stringRef.Data(), stringRef.Size()), std::string(stringView));

            auto intValue = value.GetElement(1).Get<i32>();
            auto intArrow = int32Array->Value(index);
            UNIT_ASSERT_EQUAL(intValue, intArrow);

            auto uIntValue = value.GetElement(2).Get<ui64>();
            auto uIntArrow = uint64Array->Value(index);
            UNIT_ASSERT_EQUAL(uIntValue, uIntArrow);
            ++index;
        }
    }

    Y_UNIT_TEST(Tuple) {
        TTestContext context;

        auto tupleType = context.GetTupleType();
        UNIT_ASSERT(NArrow::IsArrowCompatible(tupleType));

        auto values = context.CreateTuples(100);
        auto array = NArrow::MakeArray(values, tupleType);
        UNIT_ASSERT(array->ValidateFull().ok());

        UNIT_ASSERT(array->length() == static_cast<i64>(values.size()));
        UNIT_ASSERT(array->type_id() == arrow::Type::STRUCT);
        auto structArray = static_pointer_cast<arrow::StructArray>(array);
        UNIT_ASSERT(structArray->num_fields() == 3);
        UNIT_ASSERT(structArray->field(0)->type_id() == arrow::Type::BOOL);
        UNIT_ASSERT(structArray->field(1)->type_id() == arrow::Type::INT8);
        UNIT_ASSERT(structArray->field(2)->type_id() == arrow::Type::UINT8);
        UNIT_ASSERT(static_cast<ui64>(structArray->field(0)->length()) == values.size());
        UNIT_ASSERT(static_cast<ui64>(structArray->field(1)->length()) == values.size());
        UNIT_ASSERT(static_cast<ui64>(structArray->field(2)->length()) == values.size());
        auto boolArray = static_pointer_cast<arrow::BooleanArray>(structArray->field(0));
        auto int8Array = static_pointer_cast<arrow::Int8Array>(structArray->field(1));
        auto uint8Array = static_pointer_cast<arrow::UInt8Array>(structArray->field(2));
        auto index = 0;
        for (const auto& value: values) {
            auto boolValue = value.GetElement(0).Get<bool>();
            auto boolArrow = boolArray->Value(index);
            UNIT_ASSERT(boolValue == boolArrow);

            auto intValue = value.GetElement(1).Get<i8>();
            auto intArrow = int8Array->Value(index);
            UNIT_ASSERT(intValue == intArrow);

            auto uIntValue = value.GetElement(2).Get<ui8>();
            auto uIntArrow = uint8Array->Value(index);
            UNIT_ASSERT(uIntValue == uIntArrow);
            ++index;
        }
    }

    Y_UNIT_TEST(DictUtf8ToInterval) {
        TTestContext context;

        auto dictType = context.GetDictUtf8ToIntervalType();
        UNIT_ASSERT(NArrow::IsArrowCompatible(dictType));

        auto values = context.CreateDictUtf8ToInterval(100);
        auto array = NArrow::MakeArray(values, dictType);
        UNIT_ASSERT(array->ValidateFull().ok());
        UNIT_ASSERT(static_cast<ui64>(array->length()) == values.size());
        UNIT_ASSERT(array->type_id() == arrow::Type::MAP);
        auto mapArray = static_pointer_cast<arrow::MapArray>(array);

        UNIT_ASSERT(mapArray->num_fields() == 1);
        UNIT_ASSERT(mapArray->keys()->type_id() == arrow::Type::STRING);
        UNIT_ASSERT(mapArray->items()->type_id() == arrow::Type::DURATION);
        auto utf8Array = static_pointer_cast<arrow::StringArray>(mapArray->keys());
        auto intervalArray = static_pointer_cast<arrow::NumericArray<arrow::DurationType>>(mapArray->items());
        ui64 index = 0;
        for (const auto& value: values) {
            UNIT_ASSERT(value.GetDictLength() == static_cast<ui64>(mapArray->value_length(index)));
            for (auto subindex = mapArray->value_offset(index); subindex < mapArray->value_offset(index + 1); ++subindex) {
                auto keyArrow = utf8Array->GetView(subindex);
                NUdf::TUnboxedValue key = MakeString(NUdf::TStringRef(keyArrow.data(), keyArrow.size()));
                UNIT_ASSERT(value.Contains(key));
                NUdf::TUnboxedValue payloadValue = value.Lookup(key);
                UNIT_ASSERT(intervalArray->Value(subindex) == payloadValue.Get<i64>());
            }
            ++index;
        }
    }

    Y_UNIT_TEST(ListOfJsons) {
        TTestContext context;

        auto listType = context.GetListOfJsonsType();
        Y_ABORT_UNLESS(NArrow::IsArrowCompatible(listType));

        auto values = context.CreateListOfJsons(100);
        auto array = NArrow::MakeArray(values, listType);
        UNIT_ASSERT(array->ValidateFull().ok());
        UNIT_ASSERT(static_cast<ui64>(array->length()) == values.size());
        UNIT_ASSERT(array->type_id() == arrow::Type::LIST);
        auto listArray = static_pointer_cast<arrow::ListArray>(array);

        UNIT_ASSERT(listArray->num_fields() == 1);
        UNIT_ASSERT(listArray->value_type()->id() == arrow::Type::STRING);
        auto jsonArray = static_pointer_cast<arrow::StringArray>(listArray->values());
        auto index = 0;
        auto innerIndex = 0;
        for (const auto& value: values) {
            UNIT_ASSERT(value.GetListLength() == static_cast<ui64>(listArray->value_length(index)));
            const auto iter = value.GetListIterator();
            for (NUdf::TUnboxedValue item; iter.Next(item);) {
                auto view = jsonArray->GetView(innerIndex);
                std::string itemArrow(view.data(), view.size());
                auto stringRef = item.AsStringRef();
                std::string itemList(stringRef.Data(), stringRef.Size());
                UNIT_ASSERT(itemList == itemArrow);
                ++innerIndex;
            }
            ++index;
        }
    }

    Y_UNIT_TEST(VariantOverStruct) {
        TTestContext context;

        auto variantType = context.GetVariantOverStructType();
        UNIT_ASSERT(NArrow::IsArrowCompatible(variantType));

        auto values = context.CreateVariantOverStruct(100);
        auto array = NArrow::MakeArray(values, variantType);
        UNIT_ASSERT(array->ValidateFull().ok());
        UNIT_ASSERT(static_cast<ui64>(array->length()) == values.size());
        UNIT_ASSERT(array->type_id() == arrow::Type::DENSE_UNION);
        auto unionArray = static_pointer_cast<arrow::DenseUnionArray>(array);

        UNIT_ASSERT(unionArray->num_fields() == 4);
        UNIT_ASSERT(unionArray->field(0)->type_id() == arrow::Type::STRING);
        UNIT_ASSERT(unionArray->field(1)->type_id() == arrow::Type::STRING);
        UNIT_ASSERT(unionArray->field(2)->type_id() == arrow::Type::BINARY);
        UNIT_ASSERT(unionArray->field(3)->type_id() == arrow::Type::FLOAT);
        auto ysonArray = static_pointer_cast<arrow::StringArray>(unionArray->field(0));
        auto jsonDocArray = static_pointer_cast<arrow::StringArray>(unionArray->field(1));
        auto uuidArray = static_pointer_cast<arrow::BinaryArray>(unionArray->field(2));
        auto floatArray = static_pointer_cast<arrow::FloatArray>(unionArray->field(3));
        for (ui64 index = 0; index < values.size(); ++index) {
            auto value = values[index];
            UNIT_ASSERT(value.GetVariantIndex() == static_cast<ui32>(unionArray->child_id(index)));
            auto fieldIndex = unionArray->value_offset(index);
            if (value.GetVariantIndex() == 3) {
                auto valueArrow = floatArray->Value(fieldIndex);
                auto valueInner = value.GetVariantItem().Get<float>();
                UNIT_ASSERT(valueArrow == valueInner);
            } else {
                arrow::util::string_view viewArrow;
                if (value.GetVariantIndex() == 0) {
                    viewArrow = ysonArray->GetView(fieldIndex);
                } else if (value.GetVariantIndex() == 1) {
                    viewArrow = jsonDocArray->GetView(fieldIndex);
                } else if (value.GetVariantIndex() == 2) {
                    viewArrow = uuidArray->GetView(fieldIndex);
                }
                std::string valueArrow(viewArrow.data(), viewArrow.size());
                auto innerItem = value.GetVariantItem();
                auto refInner = innerItem.AsStringRef();
                std::string valueInner(refInner.Data(), refInner.Size());
                UNIT_ASSERT(valueArrow == valueInner);
            }
        }
    }

    Y_UNIT_TEST(VariantOverTupleWithOptionals) {
        TTestContext context;

        auto variantType = context.GetVariantOverTupleWithOptionalsType();
        UNIT_ASSERT(NArrow::IsArrowCompatible(variantType));

        auto values = context.CreateVariantOverStruct(100);
        auto array = NArrow::MakeArray(values, variantType);
        UNIT_ASSERT(array->ValidateFull().ok());
        UNIT_ASSERT(static_cast<ui64>(array->length()) == values.size());
        UNIT_ASSERT(array->type_id() == arrow::Type::DENSE_UNION);
        auto unionArray = static_pointer_cast<arrow::DenseUnionArray>(array);

        UNIT_ASSERT(unionArray->num_fields() == 5);
        UNIT_ASSERT(unionArray->field(0)->type_id() == arrow::Type::BOOL);
        UNIT_ASSERT(unionArray->field(1)->type_id() == arrow::Type::INT16);
        UNIT_ASSERT(unionArray->field(2)->type_id() == arrow::Type::UINT16);
        UNIT_ASSERT(unionArray->field(3)->type_id() == arrow::Type::INT32);
        UNIT_ASSERT(unionArray->field(4)->type_id() == arrow::Type::UINT32);
        auto boolArray = static_pointer_cast<arrow::BooleanArray>(unionArray->field(0));
        auto i16Array = static_pointer_cast<arrow::Int16Array>(unionArray->field(1));
        auto ui16Array = static_pointer_cast<arrow::UInt16Array>(unionArray->field(2));
        auto i32Array = static_pointer_cast<arrow::Int32Array>(unionArray->field(3));
        auto ui32Array = static_pointer_cast<arrow::UInt32Array>(unionArray->field(4));
        for (ui64 index = 0; index < values.size(); ++index) {
            auto value = values[index];
            UNIT_ASSERT(value.GetVariantIndex() == static_cast<ui32>(unionArray->child_id(index)));
            auto fieldIndex = unionArray->value_offset(index);
            if (value.GetVariantIndex() == 0) {
                bool valueArrow = boolArray->Value(fieldIndex);
                auto valueInner = value.GetVariantItem().Get<bool>();
                UNIT_ASSERT(valueArrow == valueInner);
            } else if (value.GetVariantIndex() == 1) {
                auto valueArrow = i16Array->Value(fieldIndex);
                auto valueInner = value.GetVariantItem().Get<i16>();
                UNIT_ASSERT(valueArrow == valueInner);
            } else if (value.GetVariantIndex() == 2) {
                auto valueArrow = ui16Array->Value(fieldIndex);
                auto valueInner = value.GetVariantItem().Get<ui16>();
                UNIT_ASSERT(valueArrow == valueInner);
            } else if (value.GetVariantIndex() == 3) {
                auto valueArrow = i32Array->Value(fieldIndex);
                auto valueInner = value.GetVariantItem().Get<i32>();
                UNIT_ASSERT(valueArrow == valueInner);
            } else if (value.GetVariantIndex() == 4) {
                if (!value.GetVariantItem().HasValue()) {
                    UNIT_ASSERT(ui32Array->IsNull(fieldIndex));
                } else {
                    auto valueArrow = ui32Array->Value(fieldIndex);
                    auto valueInner = value.GetVariantItem().Get<ui32>();
                    UNIT_ASSERT(valueArrow == valueInner);
                }
            }
        }
    }
}

Y_UNIT_TEST_SUITE(DqUnboxedValueDoNotFitToArrow) {
    Y_UNIT_TEST(DictOptionalToTuple) {
        TTestContext context;

        auto dictType = context.GetDictOptionalToTupleType();
        UNIT_ASSERT(!NArrow::IsArrowCompatible(dictType));

        auto values = context.CreateDictOptionalToTuple(100);
        auto array = NArrow::MakeArray(values, dictType);
        UNIT_ASSERT(array->ValidateFull().ok());
        UNIT_ASSERT_EQUAL(static_cast<ui64>(array->length()), values.size());
        UNIT_ASSERT_EQUAL(array->type_id(), arrow::Type::LIST);
        auto listArray = static_pointer_cast<arrow::ListArray>(array);
        UNIT_ASSERT_EQUAL(listArray->value_type()->id(), arrow::Type::STRUCT);
        auto structArray = static_pointer_cast<arrow::StructArray>(listArray->values());

        UNIT_ASSERT_EQUAL(listArray->num_fields(), 1);
        UNIT_ASSERT_EQUAL(structArray->num_fields(), 2);
        UNIT_ASSERT_EQUAL(structArray->field(0)->type_id(), arrow::Type::DOUBLE);
        UNIT_ASSERT_EQUAL(structArray->field(1)->type_id(), arrow::Type::STRUCT);
        auto keysArray = static_pointer_cast<arrow::DoubleArray>(structArray->field(0));
        auto itemsArray = static_pointer_cast<arrow::StructArray>(structArray->field(1));
        UNIT_ASSERT_EQUAL(itemsArray->num_fields(), 2);
        UNIT_ASSERT_EQUAL(itemsArray->field(0)->type_id(), arrow::Type::INT32);
        UNIT_ASSERT_EQUAL(itemsArray->field(1)->type_id(), arrow::Type::UINT32);
        auto i32Array = static_pointer_cast<arrow::Int32Array>(itemsArray->field(0));
        auto ui32Array = static_pointer_cast<arrow::UInt32Array>(itemsArray->field(1));

        ui64 index = 0;
        for (const auto& value: values) {
            UNIT_ASSERT(value.GetDictLength() == static_cast<ui64>(listArray->value_length(index)));
            for (auto subindex = listArray->value_offset(index); subindex < listArray->value_offset(index + 1); ++subindex) {
                NUdf::TUnboxedValue key = keysArray->IsNull(subindex)
                                        ? NUdf::TUnboxedValuePod()
                                        : NUdf::TUnboxedValuePod(keysArray->Value(subindex));
                UNIT_ASSERT(value.Contains(key));
                NUdf::TUnboxedValue payloadValue = value.Lookup(key);
                UNIT_ASSERT_EQUAL(payloadValue.GetElement(0).Get<i32>(), i32Array->Value(subindex));
                UNIT_ASSERT_EQUAL(payloadValue.GetElement(1).Get<ui32>(), ui32Array->Value(subindex));
            }
            ++index;
        }
    }

    Y_UNIT_TEST(OptionalOfOptional) {
        TTestContext context;

        auto doubleOptionalType = context.GetOptionalOfOptionalType();
        UNIT_ASSERT(!NArrow::IsArrowCompatible(doubleOptionalType));

        auto values = context.CreateOptionalOfOptional(100);
        auto array = NArrow::MakeArray(values, doubleOptionalType);
        UNIT_ASSERT(array->ValidateFull().ok());
        UNIT_ASSERT_EQUAL(static_cast<ui64>(array->length()), values.size());
        UNIT_ASSERT_EQUAL(array->type_id(), arrow::Type::STRUCT);
        auto structArray = static_pointer_cast<arrow::StructArray>(array);
        UNIT_ASSERT_EQUAL(structArray->num_fields(), 2);
        UNIT_ASSERT_EQUAL(structArray->field(0)->type_id(), arrow::Type::UINT64);
        UNIT_ASSERT_EQUAL(structArray->field(1)->type_id(), arrow::Type::INT32);
        auto depthArray = static_pointer_cast<arrow::UInt64Array>(structArray->field(0));
        auto i32Array = static_pointer_cast<arrow::Int32Array>(structArray->field(1));

        auto index = 0;
        for (auto value: values) {
            auto depth = depthArray->Value(index);
            while (depth > 0) {
                UNIT_ASSERT(value.HasValue());
                value = value.GetOptionalValue();
                --depth;
            }
            if (value.HasValue()) {
                UNIT_ASSERT_EQUAL(value.Get<i32>(), i32Array->Value(index));
            } else {
                UNIT_ASSERT(i32Array->IsNull(index));
            }
            ++index;
        }
    }

    Y_UNIT_TEST(LargeVariant) {
        TTestContext context;

        ui32 numberOfTypes = 500;
        auto variantType = context.GetLargeVariantType(numberOfTypes);
        bool isCompatible = NArrow::IsArrowCompatible(variantType);
        UNIT_ASSERT(!isCompatible);

        auto values = context.CreateLargeVariant(1000);
        auto array = NArrow::MakeArray(values, variantType);
        UNIT_ASSERT(array->ValidateFull().ok());
        UNIT_ASSERT_EQUAL(static_cast<ui64>(array->length()), values.size());
        UNIT_ASSERT_EQUAL(array->type_id(), arrow::Type::DENSE_UNION);
        auto unionArray = static_pointer_cast<arrow::DenseUnionArray>(array);
        ui32 numberOfGroups = (numberOfTypes - 1) / arrow::UnionType::kMaxTypeCode + 1;
        UNIT_ASSERT_EQUAL(numberOfGroups, static_cast<ui32>(unionArray->num_fields()));
        ui32 typesInArrow = 0;
        for (auto i = 0 ; i < unionArray->num_fields(); ++i) {
            UNIT_ASSERT_EQUAL(unionArray->field(i)->type_id(), arrow::Type::DENSE_UNION);
            typesInArrow += unionArray->field(i)->num_fields();
        }
        UNIT_ASSERT_EQUAL(numberOfTypes, typesInArrow);
        // TODO Check array content.
    }
}

Y_UNIT_TEST_SUITE(ConvertUnboxedValueToArrowAndBack){
    Y_UNIT_TEST(Struct) {
        TTestContext context;

        auto structType = context.GetStructType();
        auto values = context.CreateStructs(100);
        auto array = NArrow::MakeArray(values, structType);
        auto restoredValues = NArrow::ExtractUnboxedValues(array, structType, context.HolderFactory);
        UNIT_ASSERT_EQUAL(values.size(), restoredValues.size());
        for (ui64 index = 0; index < values.size(); ++index) {
            AssertUnboxedValuesAreEqual(values[index], restoredValues[index], structType);
        }
    }

    Y_UNIT_TEST(Tuple) {
        TTestContext context;

        auto tupleType = context.GetTupleType();
        UNIT_ASSERT(NArrow::IsArrowCompatible(tupleType));

        auto values = context.CreateTuples(100);
        auto array = NArrow::MakeArray(values, tupleType);
        auto restoredValues = NArrow::ExtractUnboxedValues(array, tupleType, context.HolderFactory);
        UNIT_ASSERT_EQUAL(values.size(), restoredValues.size());
        for (ui64 index = 0; index < values.size(); ++index) {
            AssertUnboxedValuesAreEqual(values[index], restoredValues[index], tupleType);
        }
    }

    Y_UNIT_TEST(DictUtf8ToInterval) {
        TTestContext context;

        auto dictType = context.GetDictUtf8ToIntervalType();
        UNIT_ASSERT(NArrow::IsArrowCompatible(dictType));

        auto values = context.CreateDictUtf8ToInterval(100);
        auto array = NArrow::MakeArray(values, dictType);
        auto restoredValues = NArrow::ExtractUnboxedValues(array, dictType, context.HolderFactory);
        UNIT_ASSERT_EQUAL(values.size(), restoredValues.size());
        for (ui64 index = 0; index < values.size(); ++index) {
            AssertUnboxedValuesAreEqual(values[index], restoredValues[index], dictType);
        }
    }

    Y_UNIT_TEST(ListOfJsons) {
        TTestContext context;

        auto listType = context.GetListOfJsonsType();
        Y_ABORT_UNLESS(NArrow::IsArrowCompatible(listType));

        auto values = context.CreateListOfJsons(100);
        auto array = NArrow::MakeArray(values, listType);
        auto restoredValues = NArrow::ExtractUnboxedValues(array, listType, context.HolderFactory);
        UNIT_ASSERT_EQUAL(values.size(), restoredValues.size());
        for (ui64 index = 0; index < values.size(); ++index) {
            AssertUnboxedValuesAreEqual(values[index], restoredValues[index], listType);
        }
    }

    Y_UNIT_TEST(VariantOverStruct) {
        TTestContext context;

        auto variantType = context.GetVariantOverStructType();
        UNIT_ASSERT(NArrow::IsArrowCompatible(variantType));

        auto values = context.CreateVariantOverStruct(100);
        auto array = NArrow::MakeArray(values, variantType);
        auto restoredValues = NArrow::ExtractUnboxedValues(array, variantType, context.HolderFactory);
        UNIT_ASSERT_EQUAL(values.size(), restoredValues.size());
        for (ui64 index = 0; index < values.size(); ++index) {
            AssertUnboxedValuesAreEqual(values[index], restoredValues[index], variantType);
        }
    }

    Y_UNIT_TEST(VariantOverTupleWithOptionals) {
        TTestContext context;

        auto variantType = context.GetVariantOverTupleWithOptionalsType();
        UNIT_ASSERT(NArrow::IsArrowCompatible(variantType));

        auto values = context.CreateVariantOverStruct(100);
        auto array = NArrow::MakeArray(values, variantType);
        auto restoredValues = NArrow::ExtractUnboxedValues(array, variantType, context.HolderFactory);
        UNIT_ASSERT_EQUAL(values.size(), restoredValues.size());
        for (ui64 index = 0; index < values.size(); ++index) {
            AssertUnboxedValuesAreEqual(values[index], restoredValues[index], variantType);
        }
    }

    Y_UNIT_TEST(DictOptionalToTuple) {
        TTestContext context;

        auto dictType = context.GetDictOptionalToTupleType();
        UNIT_ASSERT(!NArrow::IsArrowCompatible(dictType));

        auto values = context.CreateDictOptionalToTuple(100);
        auto array = NArrow::MakeArray(values, dictType);
        auto restoredValues = NArrow::ExtractUnboxedValues(array, dictType, context.HolderFactory);
        UNIT_ASSERT_EQUAL(values.size(), restoredValues.size());
        for (ui64 index = 0; index < values.size(); ++index) {
            AssertUnboxedValuesAreEqual(values[index], restoredValues[index], dictType);
        }
    }

    Y_UNIT_TEST(OptionalOfOptional) {
        TTestContext context;

        auto doubleOptionalType = context.GetOptionalOfOptionalType();
        UNIT_ASSERT(!NArrow::IsArrowCompatible(doubleOptionalType));

        auto values = context.CreateOptionalOfOptional(100);
        auto array = NArrow::MakeArray(values, doubleOptionalType);
        auto restoredValues = NArrow::ExtractUnboxedValues(array, doubleOptionalType, context.HolderFactory);
        UNIT_ASSERT_EQUAL(values.size(), restoredValues.size());
        for (ui64 index = 0; index < values.size(); ++index) {
            AssertUnboxedValuesAreEqual(values[index], restoredValues[index], doubleOptionalType);
        }
    }

    Y_UNIT_TEST(LargeVariant) {
        TTestContext context;

        auto variantType = context.GetLargeVariantType(500);
        bool isCompatible = NArrow::IsArrowCompatible(variantType);
        UNIT_ASSERT(!isCompatible);

        auto values = context.CreateLargeVariant(1000);
        auto array = NArrow::MakeArray(values, variantType);
        auto restoredValues = NArrow::ExtractUnboxedValues(array, variantType, context.HolderFactory);
        UNIT_ASSERT_EQUAL(values.size(), restoredValues.size());
        for (ui64 index = 0; index < values.size(); ++index) {
            AssertUnboxedValuesAreEqual(values[index], restoredValues[index], variantType);
        }
    }
}

