#include "yql_json_codec.h"

#include <ydb/library/yql/minikql/computation/mkql_value_builder.h>
#include <ydb/library/yql/minikql/mkql_type_ops.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/json/json_reader.h>

#include <util/string/cast.h>

namespace NYql {
namespace NCommon {
namespace NJsonCodec {

using namespace NYql::NCommon;
using namespace NKikimr;
using namespace NKikimr::NMiniKQL;


namespace {
struct TTestContext {
    TScopedAlloc Alloc;
    TTypeEnvironment TypeEnv;
    TMemoryUsageInfo MemInfo;
    THolderFactory HolderFactory;
    TDefaultValueBuilder Vb;

    TTestContext()
    : Alloc(__LOCATION__)
    , TypeEnv(Alloc)
    , MemInfo("Mem")
    , HolderFactory(Alloc.Ref(), MemInfo)
    , Vb(HolderFactory)
    {

    }
};

TString WriteValueToExportJsonStr(const NUdf::TUnboxedValuePod& value, NMiniKQL::TType* type, TValueConvertPolicy policy = {}) {
    TStringStream out;
    NJson::TJsonWriter jsonWriter(&out, MakeJsonConfig());
    WriteValueToJson(jsonWriter,value, type, policy);
    jsonWriter.Flush();
    return out.Str();
}

TString WriteValueToFuncJsonStr(const NUdf::TUnboxedValuePod& value, NMiniKQL::TType* type) {
    TStringStream out;
    NJson::TJsonWriter jsonWriter(&out, MakeJsonConfig());
    TValueConvertPolicy policy;
    policy.Set(EValueConvertPolicy::NUMBER_AS_STRING);
    WriteValueToJson(jsonWriter,value, type, policy);
    jsonWriter.Flush();
    return out.Str();
}

NUdf::TUnboxedValue ReadJsonStrValue(IInputStream* in, NMiniKQL::TType* type, const NMiniKQL::THolderFactory& holderFactory)
{
    NJson::TJsonValue json;
    if (!NJson::ReadJsonTree(in, &json, false)) {
        YQL_ENSURE(false, "Error parse json");
    }
    return ReadJsonValue(json, type, holderFactory);
}
}

Y_UNIT_TEST_SUITE(SerializeVoid) {
    Y_UNIT_TEST(Null) {
        TTestContext ctx;
        auto value = NUdf::TUnboxedValuePod();
        auto nullJson = WriteValueToFuncJsonStr(value, ctx.TypeEnv.GetTypeOfNullLazy());
        UNIT_ASSERT_VALUES_EQUAL(nullJson, "null");

        auto voidJson = WriteValueToFuncJsonStr(value, ctx.TypeEnv.GetTypeOfVoidLazy());
        UNIT_ASSERT_VALUES_EQUAL(voidJson, "null");
    }
}

Y_UNIT_TEST_SUITE(SerializeBool) {
    Y_UNIT_TEST(Bool) {
        TTestContext ctx;
        auto type = TDataType::Create(NUdf::TDataType<bool>::Id, ctx.TypeEnv);
        auto json1 = WriteValueToFuncJsonStr(NUdf::TUnboxedValuePod(true), type);
        UNIT_ASSERT_VALUES_EQUAL(json1, "true");

        auto json2 = WriteValueToFuncJsonStr(NUdf::TUnboxedValuePod(false), type);
        UNIT_ASSERT_VALUES_EQUAL(json2, "false");
    }

    Y_UNIT_TEST(StringBool) {
        TTestContext ctx;
        auto type = TDataType::Create(NUdf::TDataType<bool>::Id, ctx.TypeEnv);
        TValueConvertPolicy policy{EValueConvertPolicy::BOOL_AS_STRING};
        auto json = WriteValueToExportJsonStr(NUdf::TUnboxedValuePod(true), type, policy);
        UNIT_ASSERT_VALUES_EQUAL(json, "\"true\"");
    }
}

Y_UNIT_TEST_SUITE(SerializeUuid) {
    Y_UNIT_TEST(Uuid) {
        TTestContext ctx;
        auto type = TDataType::Create(NUdf::TDataType<NUdf::TUuid>::Id, ctx.TypeEnv);
        auto value = ctx.Vb.NewString("80070214-6ad1-4077-add8-74b68f105e3c");
        auto json = WriteValueToFuncJsonStr(value, type);
        UNIT_ASSERT_VALUES_EQUAL(json, "\"80070214-6ad1-4077-add8-74b68f105e3c\"");
    }
}

Y_UNIT_TEST_SUITE(SerializeJson) {
    Y_UNIT_TEST(ScalarJson) {
        TTestContext ctx;
        auto type = TDataType::Create(NUdf::TDataType<NUdf::TJson>::Id, ctx.TypeEnv);
        auto value = ctx.Vb.NewString("\"some string с русскими йЁ");
        auto json = WriteValueToFuncJsonStr(value, type);
        UNIT_ASSERT_VALUES_EQUAL(json, "\"\\\"some string \xD1\x81 \xD1\x80\xD1\x83\xD1\x81\xD1\x81\xD0\xBA\xD0\xB8\xD0\xBC\xD0\xB8 \xD0\xB9\xD0\x81\"");
    }

    Y_UNIT_TEST(ComplexJson) {
        TTestContext ctx;
        TStructMember members[] = {
            TStructMember("X", TDataType::Create(NUdf::TDataType<NUdf::TJson>::Id, ctx.TypeEnv)),
            TStructMember("Y", TDataType::Create(NUdf::TDataType<ui32>::Id, ctx.TypeEnv))
        };
        auto type = TStructType::Create(2, members, ctx.TypeEnv);

        NUdf::TUnboxedValue* items;
        auto value = ctx.Vb.NewArray(2, items);
        items[0] = ctx.Vb.NewString(R"({"a":500,"b":[1,2,3]})");
        items[1] = NUdf::TUnboxedValuePod(ui32(73));

        auto json = WriteValueToExportJsonStr(value, type);
        UNIT_ASSERT_VALUES_EQUAL(json, R"({"X":"{\"a\":500,\"b\":[1,2,3]}","Y":73})");
    }
}

Y_UNIT_TEST_SUITE(SerializeContainers) {
    Y_UNIT_TEST(EmptyContainer) {
        TTestContext ctx;
        auto value = ctx.HolderFactory.GetEmptyContainerLazy();
        auto dictJson = WriteValueToFuncJsonStr(value, ctx.TypeEnv.GetTypeOfEmptyDictLazy());
        UNIT_ASSERT_VALUES_EQUAL(dictJson, "[]");

        auto listJson = WriteValueToFuncJsonStr(value, ctx.TypeEnv.GetTypeOfEmptyListLazy());
        UNIT_ASSERT_VALUES_EQUAL(listJson, "[]");
    }

    Y_UNIT_TEST(NormalList) {
        TTestContext ctx;
        NUdf::TUnboxedValue* items;
        auto value = ctx.Vb.NewArray(4, items);
        items[0] = NUdf::TUnboxedValuePod(1);
        items[1] = NUdf::TUnboxedValuePod(5);
        items[2] = NUdf::TUnboxedValuePod(-18);
        items[3] = NUdf::TUnboxedValuePod(4);
        auto type = TListType::Create(TDataType::Create(NUdf::TDataType<i32>::Id, ctx.TypeEnv), ctx.TypeEnv);
        auto json = WriteValueToFuncJsonStr(value, type);
        UNIT_ASSERT_VALUES_EQUAL(json, "[\"1\",\"5\",\"-18\",\"4\"]");
    }

    Y_UNIT_TEST(EmptyList) {
        TTestContext ctx;
        NUdf::TUnboxedValue* items;
        auto value = ctx.Vb.NewArray(0, items);
        auto type = TListType::Create(TDataType::Create(NUdf::TDataType<i32>::Id, ctx.TypeEnv), ctx.TypeEnv);
        auto json = WriteValueToFuncJsonStr(value, type);
        UNIT_ASSERT_VALUES_EQUAL(json, "[]");
    }

    Y_UNIT_TEST(FullTuple) {
        TTestContext ctx;

        TType* members[] = {
            TDataType::Create(NUdf::TDataType<ui8>::Id, ctx.TypeEnv),
            TDataType::Create(NUdf::TDataType<NUdf::TUtf8>::Id, ctx.TypeEnv),
            TDataType::Create(NUdf::TDataType<NUdf::TDate>::Id, ctx.TypeEnv),
        };
        auto type = TTupleType::Create(3, members, ctx.TypeEnv);

        NUdf::TUnboxedValue* items;
        NUdf::TUnboxedValue value = ctx.HolderFactory.CreateDirectArrayHolder(type->GetElementsCount(), items);
        items[0] = NUdf::TUnboxedValuePod(ui8(65));
        items[1] = ctx.Vb.NewString("абсдева");
        ui16 date;
        ctx.Vb.MakeDate(2021, 5, 5, date);
        items[2] = NUdf::TUnboxedValuePod(date);

        auto json = WriteValueToExportJsonStr(value, type);
        UNIT_ASSERT_VALUES_EQUAL(json, "[65,\"абсдева\",\"2021-05-05\"]");
    }

    Y_UNIT_TEST(EmptyTuple) {
        TTestContext ctx;
        TType* members[] = {};
        auto type = TTupleType::Create(0, members, ctx.TypeEnv);
        NUdf::TUnboxedValue* items;
        NUdf::TUnboxedValue value = ctx.HolderFactory.CreateDirectArrayHolder(type->GetElementsCount(), items);
        auto json = WriteValueToFuncJsonStr(value, type);
        UNIT_ASSERT_VALUES_EQUAL(json, "[]");
    }

    Y_UNIT_TEST(DictType) {
        TTestContext ctx;
        auto type = TDictType::Create(
            TDataType::Create(NUdf::TDataType<NUdf::TUtf8>::Id, ctx.TypeEnv),
            TDataType::Create(NUdf::TDataType<i32>::Id, ctx.TypeEnv),
            ctx.TypeEnv
        );
        auto dictBuilder = ctx.Vb.NewDict(type, NUdf::TDictFlags::EDictKind::Hashed);
        dictBuilder->Add(
            ctx.Vb.NewString("key_a"),
            NUdf::TUnboxedValuePod(781)
        );
        dictBuilder->Add(
            ctx.Vb.NewString("key_b"),
            NUdf::TUnboxedValuePod(-500)
        );
        auto json = WriteValueToExportJsonStr(dictBuilder->Build(), type);
        UNIT_ASSERT_VALUES_EQUAL(json, "[[\"key_b\",-500],[\"key_a\",781]]");
    }

    Y_UNIT_TEST(SetType) {
        TTestContext ctx;
        auto type = TDictType::Create(
            TDataType::Create(NUdf::TDataType<NUdf::TUtf8>::Id, ctx.TypeEnv),
            ctx.TypeEnv.GetTypeOfVoidLazy(),
            ctx.TypeEnv
        );
        auto dictBuilder = ctx.Vb.NewDict(type, NUdf::TDictFlags::EDictKind::Hashed);
        dictBuilder->Add(ctx.Vb.NewString("key_a"), NUdf::TUnboxedValuePod());
        dictBuilder->Add(ctx.Vb.NewString("key_b"), NUdf::TUnboxedValuePod());
        auto json = WriteValueToExportJsonStr(dictBuilder->Build(), type);
        UNIT_ASSERT_VALUES_EQUAL(json, "[\"key_b\",\"key_a\"]");
    }

    Y_UNIT_TEST(Tagged) {
        TTestContext ctx;
        auto type = TTaggedType::Create(TDataType::Create(NUdf::TDataType<NUdf::TDatetime>::Id, ctx.TypeEnv),
                        "test-tag", ctx.TypeEnv);
        ui32 datetime;
        ctx.Vb.MakeDatetime(2021, 1, 1, 14, 5, 43, datetime);
        auto json = WriteValueToFuncJsonStr(NUdf::TUnboxedValuePod(datetime), type);
        UNIT_ASSERT_VALUES_EQUAL(json, "\"2021-01-01T14:05:43Z\"");
    }

    Y_UNIT_TEST(TupleVariant) {
        TTestContext ctx;
        TType* tupleTypes[] = {
            TDataType::Create(NUdf::TDataType<bool>::Id, ctx.TypeEnv),
            TDataType::Create(NUdf::TDataType<ui32>::Id, ctx.TypeEnv)
        };
        auto underlying = TTupleType::Create(2, tupleTypes, ctx.TypeEnv);
        auto type = TVariantType::Create(underlying, ctx.TypeEnv);

        auto value0 = ctx.HolderFactory.CreateVariantHolder(NUdf::TUnboxedValuePod(false), 0);
        auto json0 = WriteValueToFuncJsonStr(value0, type);
        UNIT_ASSERT_VALUES_EQUAL(json0, "[0,false]");

        auto value1 = ctx.HolderFactory.CreateVariantHolder(NUdf::TUnboxedValuePod(200), 1);
        auto json1 = WriteValueToExportJsonStr(value1, type);
        UNIT_ASSERT_VALUES_EQUAL(json1, "[1,200]");
    }

    Y_UNIT_TEST(StructVariant) {
        TTestContext ctx;

        TStructMember members[] = {
            TStructMember("A", TDataType::Create(NUdf::TDataType<bool>::Id, ctx.TypeEnv)),
            TStructMember("B", TDataType::Create(NUdf::TDataType<ui32>::Id, ctx.TypeEnv))
        };
        auto underlying = TStructType::Create(2, members, ctx.TypeEnv);
        auto type = TVariantType::Create(underlying, ctx.TypeEnv);

        auto value0 = ctx.HolderFactory.CreateVariantHolder(NUdf::TUnboxedValuePod(true), 0);
        auto json0 = WriteValueToExportJsonStr(value0, type);
        UNIT_ASSERT_VALUES_EQUAL(json0, "[\"A\",true]");

        auto value1 = ctx.HolderFactory.CreateVariantHolder(NUdf::TUnboxedValuePod(200), 1);
        auto json1 = WriteValueToExportJsonStr(value1, type);
        UNIT_ASSERT_VALUES_EQUAL(json1, "[\"B\",200]");
    }

    Y_UNIT_TEST(StructType) {
        TTestContext ctx;

        TStructMember members[] = {
            {"A", TDataType::Create(NUdf::TDataType<bool>::Id, ctx.TypeEnv)},
            {"B", TDataType::Create(NUdf::TDataType<ui32>::Id, ctx.TypeEnv)}
        };
        auto type = TStructType::Create(2, members, ctx.TypeEnv);

        NUdf::TUnboxedValue* items;
        auto value = ctx.Vb.NewArray(2, items);
        items[0] = NUdf::TUnboxedValuePod(false);
        items[1] = NUdf::TUnboxedValuePod(ui32(555));

        auto json = WriteValueToExportJsonStr(value, type);
        UNIT_ASSERT_VALUES_EQUAL(json, "{\"A\":false,\"B\":555}");
    }
}

Y_UNIT_TEST_SUITE(DeserializeTagged) {
    Y_UNIT_TEST(Tagged) {
        TTestContext ctx;
        TStringStream json;
        json << "55.751244";
        auto type = TTaggedType::Create(TDataType::Create(NUdf::TDataType<float>::Id, ctx.TypeEnv), "TLatitude", ctx.TypeEnv);
        auto value = ReadJsonStrValue(&json, type, ctx.HolderFactory);
        UNIT_ASSERT_DOUBLES_EQUAL(value.Get<float>(), 55.751244, 1e-6);
    }
}

Y_UNIT_TEST_SUITE(DeserializeVariant) {
    Y_UNIT_TEST(VariantTuple) {
        TTestContext ctx;
        TStringStream json;
        json << "[1, {\"A\":true,\"B\":800}]";

        TStructMember members[] = {
            TStructMember("A", TDataType::Create(NUdf::TDataType<bool>::Id, ctx.TypeEnv)),
            TStructMember("B", TDataType::Create(NUdf::TDataType<ui32>::Id, ctx.TypeEnv))
        };
        TType* tupleTypes[] = {
            TDataType::Create(NUdf::TDataType<bool>::Id, ctx.TypeEnv),
            TStructType::Create(2, members, ctx.TypeEnv)
        };
        auto underlying = TTupleType::Create(2, tupleTypes, ctx.TypeEnv);
        auto type = TVariantType::Create(underlying, ctx.TypeEnv);
        auto value = ReadJsonStrValue(&json, type, ctx.HolderFactory);

        UNIT_ASSERT_VALUES_EQUAL(value.GetVariantIndex(), 1);
        auto structValue = value.GetVariantItem();
        UNIT_ASSERT_VALUES_EQUAL(structValue.GetElement(0).Get<bool>(), true);
        UNIT_ASSERT_VALUES_EQUAL(structValue.GetElement(1).Get<ui32>(), 800);
    }

    Y_UNIT_TEST(VariantStruct) {
        TTestContext ctx;
        TStringStream json;
        json << "[\"B\", 800]";

        TStructMember members[] = {
            TStructMember("A", TDataType::Create(NUdf::TDataType<bool>::Id, ctx.TypeEnv)),
            TStructMember("B", TDataType::Create(NUdf::TDataType<ui32>::Id, ctx.TypeEnv))
        };
        auto underlying = TStructType::Create(2, members, ctx.TypeEnv);
        auto type = TVariantType::Create(underlying, ctx.TypeEnv);
        auto value = ReadJsonStrValue(&json, type, ctx.HolderFactory);

        UNIT_ASSERT_VALUES_EQUAL(value.GetVariantIndex(), 1);
        UNIT_ASSERT_VALUES_EQUAL(value.GetVariantItem().Get<ui32>(), 800);
    }
}

Y_UNIT_TEST_SUITE(DeserializeDict) {
    Y_UNIT_TEST(Set) {
        TTestContext ctx;
        TStringStream json;
        json << "[1,3,5,7,9]";

        auto type = TDictType::Create(
            TDataType::Create(NUdf::TDataType<i32>::Id, ctx.TypeEnv),
            ctx.TypeEnv.GetTypeOfVoidLazy(),
            ctx.TypeEnv
        );
        auto value = ReadJsonStrValue(&json, type, ctx.HolderFactory);

        UNIT_ASSERT_VALUES_EQUAL(value.GetDictLength(), 5);

        std::vector<ui32> keys;
        for (NUdf::TUnboxedValue it = value.GetKeysIterator(), key; it.Next(key);) {
            keys.push_back(key.Get<ui32>());
        }
        std::sort(keys.begin(), keys.end());
        UNIT_ASSERT_VALUES_EQUAL(keys.at(0), 1);
        UNIT_ASSERT_VALUES_EQUAL(keys.at(4), 9);
    }

    Y_UNIT_TEST(DictOfUtf8) {
        TTestContext ctx;
        TStringStream json;
        json << "[[\"key_1\",101], [\"key_2\",201]]";

        auto type = TDictType::Create(
            TDataType::Create(NUdf::TDataType<NUdf::TUtf8>::Id, ctx.TypeEnv),
            TDataType::Create(NUdf::TDataType<i32>::Id, ctx.TypeEnv),
            ctx.TypeEnv
        );
        auto value = ReadJsonStrValue(&json, type, ctx.HolderFactory);

        UNIT_ASSERT_VALUES_EQUAL(value.GetDictLength(), 2);

        for (NUdf::TUnboxedValue it = value.GetDictIterator(), k, v; it.NextPair(k, v);) {
            auto key = TStringBuf(k.AsStringRef());
            if (key == TStringBuf("key_1")) {
                UNIT_ASSERT_VALUES_EQUAL(v.Get<ui32>(), 101);
            } else if (key == TStringBuf("key_2")) {
                UNIT_ASSERT_VALUES_EQUAL(v.Get<ui32>(), 201);
            } else {
                UNIT_FAIL("Unknown key");
            }
        }
    }
}


Y_UNIT_TEST_SUITE(SerializeOptional) {
    Y_UNIT_TEST(OptionalOfList) {
        TTestContext ctx;
        NUdf::TUnboxedValue* items;
        auto value = ctx.HolderFactory.CreateDirectArrayHolder(3, items).MakeOptional();
        items[0] = NUdf::TUnboxedValuePod(ui8(0));
        items[1] = NUdf::TUnboxedValuePod(ui8(67));
        items[2] = NUdf::TUnboxedValuePod(ui8(4));
        auto elementType = TListType::Create(TDataType::Create(NUdf::TDataType<ui8>::Id, ctx.TypeEnv), ctx.TypeEnv);
        auto type = TOptionalType::Create(elementType, ctx.TypeEnv);
        auto json = WriteValueToExportJsonStr(value, type);
        UNIT_ASSERT_VALUES_EQUAL(json, "[[0,67,4]]");
    }

    Y_UNIT_TEST(SimpleJust) {
        TTestContext ctx;
        auto elementType = TDataType::Create(NUdf::TDataType<ui32>::Id, ctx.TypeEnv);
        TType* type = TOptionalType::Create(elementType, ctx.TypeEnv);
        auto value = NUdf::TUnboxedValuePod(ui32(6641)).MakeOptional();
        auto json = WriteValueToExportJsonStr(value, type);
        UNIT_ASSERT_VALUES_EQUAL(json, "[6641]");
    }

    Y_UNIT_TEST(NothingOptional) {
        TTestContext ctx;
        auto value = NUdf::TUnboxedValuePod();
        auto elementType = TDataType::Create(NUdf::TDataType<ui8>::Id, ctx.TypeEnv);
        auto type = TOptionalType::Create(elementType, ctx.TypeEnv);
        auto json = WriteValueToFuncJsonStr(value, type);
        UNIT_ASSERT_VALUES_EQUAL(json, "[]");
    }

    Y_UNIT_TEST(SeveralOptionals) {
        TTestContext ctx;
        auto elementType = TDataType::Create(NUdf::TDataType<ui8>::Id, ctx.TypeEnv);
        TType* type = TOptionalType::Create(elementType, ctx.TypeEnv);
        type = TOptionalType::Create(type, ctx.TypeEnv);
        type = TOptionalType::Create(type, ctx.TypeEnv);

        auto value1 = NUdf::TUnboxedValuePod().MakeOptional().MakeOptional();
        auto json1 = WriteValueToFuncJsonStr(value1, type);
        UNIT_ASSERT_VALUES_EQUAL(json1, "[[[]]]");

        auto value2 = NUdf::TUnboxedValuePod(ui8(120)).MakeOptional().MakeOptional().MakeOptional();
        auto json2 = WriteValueToExportJsonStr(value2, type);
        UNIT_ASSERT_VALUES_EQUAL(json2, "[[[120]]]");
    }
}

Y_UNIT_TEST_SUITE(SerializeNumbers) {

#define TEST_SERIALIZE_NUMBER_TYPE(type) \
    Y_UNIT_TEST(type) { \
        TTestContext ctx; \
        auto maxValue = NUdf::TUnboxedValuePod(std::numeric_limits<type>::max()); \
        auto maxJson = WriteValueToFuncJsonStr(maxValue, TDataType::Create(NUdf::TDataType<type>::Id, ctx.TypeEnv)); \
        UNIT_ASSERT_VALUES_EQUAL(maxJson, "\"" + ToString(std::numeric_limits<type>::max()) + "\""); \
                                \
        auto minValue = NUdf::TUnboxedValuePod(std::numeric_limits<type>::min()); \
        auto minJson = WriteValueToFuncJsonStr(minValue, TDataType::Create(NUdf::TDataType<type>::Id, ctx.TypeEnv)); \
        UNIT_ASSERT_VALUES_EQUAL(minJson, "\"" + ToString(std::numeric_limits<type>::min()) + "\""); \
    }

    TEST_SERIALIZE_NUMBER_TYPE(i8)
    TEST_SERIALIZE_NUMBER_TYPE(i16)
    TEST_SERIALIZE_NUMBER_TYPE(i32)

    TEST_SERIALIZE_NUMBER_TYPE(ui8)
    TEST_SERIALIZE_NUMBER_TYPE(ui16)
    TEST_SERIALIZE_NUMBER_TYPE(ui32)

#undef TEST_SERIALIZE_NUMBER_TYPE

    Y_UNIT_TEST(float) {
        TTestContext ctx;
        auto value = NUdf::TUnboxedValuePod(std::numeric_limits<float>::max());
        auto json = WriteValueToFuncJsonStr(value, TDataType::Create(NUdf::TDataType<float>::Id, ctx.TypeEnv));
        auto expected = FloatToString(std::numeric_limits<float>::max(), EFloatToStringMode::PREC_NDIGITS, std::numeric_limits<float>::max_digits10);
        UNIT_ASSERT_VALUES_EQUAL(json, "\"" + expected + "\"");
    }

    Y_UNIT_TEST(double) {
        TTestContext ctx;
        auto value = NUdf::TUnboxedValuePod(std::numeric_limits<double>::min());
        auto json = WriteValueToFuncJsonStr(value, TDataType::Create(NUdf::TDataType<double>::Id, ctx.TypeEnv));
        auto expected = FloatToString(std::numeric_limits<double>::min(), EFloatToStringMode::PREC_NDIGITS, std::numeric_limits<double>::max_digits10);
        UNIT_ASSERT_VALUES_EQUAL(json, "\"" + expected + "\"");
    }

    Y_UNIT_TEST(DoubleSignificantNumbers) {
        TTestContext ctx;
        auto value = NUdf::TUnboxedValuePod(double(3.1415926535897932384626433832795));
        auto type = TDataType::Create(NUdf::TDataType<double>::Id, ctx.TypeEnv);
        auto json = WriteValueToFuncJsonStr(value, type);
        UNIT_ASSERT_VALUES_EQUAL(json, "\"3.1415926535897931\"");

        auto json2 = WriteValueToExportJsonStr(value, type);
        UNIT_ASSERT_VALUES_EQUAL(json2, "3.1415926535897931");
    }

    Y_UNIT_TEST(i64) {
        TTestContext ctx;
        auto maxValue = NUdf::TUnboxedValuePod(std::numeric_limits<i64>::max());
        auto maxJson = WriteValueToFuncJsonStr(maxValue, TDataType::Create(NUdf::TDataType<i64>::Id, ctx.TypeEnv));
        UNIT_ASSERT_VALUES_EQUAL(maxJson, "\"" + ToString(std::numeric_limits<i64>::max()) + "\"");

        auto minValue = NUdf::TUnboxedValuePod(std::numeric_limits<i64>::min());
        auto minJson = WriteValueToFuncJsonStr(minValue, TDataType::Create(NUdf::TDataType<i64>::Id, ctx.TypeEnv));
        UNIT_ASSERT_VALUES_EQUAL(minJson, "\"" + ToString(std::numeric_limits<i64>::min()) + "\"");

        auto fitInJS = NUdf::TUnboxedValuePod(i64(-9007199254740991)); // == Number.MIN_SAFE_INTEGER
        auto fitInJSJson = WriteValueToFuncJsonStr(fitInJS, TDataType::Create(NUdf::TDataType<i64>::Id, ctx.TypeEnv));
        UNIT_ASSERT_VALUES_EQUAL(fitInJSJson, "\"-9007199254740991\"");
    }

    Y_UNIT_TEST(ui64) {
        TTestContext ctx;
        auto maxValue = NUdf::TUnboxedValuePod(std::numeric_limits<ui64>::max());
        auto maxJson = WriteValueToFuncJsonStr(maxValue, TDataType::Create(NUdf::TDataType<ui64>::Id, ctx.TypeEnv));
        UNIT_ASSERT_VALUES_EQUAL(maxJson, "\"" + ToString(std::numeric_limits<ui64>::max()) + "\"");

        auto minValue = NUdf::TUnboxedValuePod(std::numeric_limits<ui64>::min());
        auto minJson = WriteValueToFuncJsonStr(minValue, TDataType::Create(NUdf::TDataType<ui64>::Id, ctx.TypeEnv));
        UNIT_ASSERT_VALUES_EQUAL(minJson, "\"" + ToString(std::numeric_limits<ui64>::min()) + "\"");

        auto fitInJS = NUdf::TUnboxedValuePod(ui64(2419787883133419));
        auto fitInJSJson = WriteValueToFuncJsonStr(fitInJS, TDataType::Create(NUdf::TDataType<ui64>::Id, ctx.TypeEnv));
        UNIT_ASSERT_VALUES_EQUAL(fitInJSJson, "\"2419787883133419\"");
    }

   Y_UNIT_TEST(Decimal) {
        TTestContext ctx;
        auto value = NUdf::TUnboxedValuePod(NYql::NDecimal::TInt128(78876543LL));
        auto type = TDataDecimalType::Create(10, 2, ctx.TypeEnv);
        auto json1 = WriteValueToFuncJsonStr(value, type);
        UNIT_ASSERT_VALUES_EQUAL(json1, "\"788765.43\"");
    }

    Y_UNIT_TEST(InfValue) {
        TTestContext ctx;
        auto value = NUdf::TUnboxedValuePod(std::numeric_limits<float>::infinity());
        auto json1 = WriteValueToExportJsonStr(value, TDataType::Create(NUdf::TDataType<float>::Id, ctx.TypeEnv));
        UNIT_ASSERT_VALUES_EQUAL(json1, "\"inf\"");

        auto json2 = WriteValueToFuncJsonStr(value, TDataType::Create(NUdf::TDataType<float>::Id, ctx.TypeEnv));
        UNIT_ASSERT_VALUES_EQUAL(json2, "\"inf\"");
    }

    Y_UNIT_TEST(NaNValue) {
        TTestContext ctx;
        auto value = NUdf::TUnboxedValuePod(std::numeric_limits<double>::quiet_NaN());
        auto json1 = WriteValueToExportJsonStr(value, TDataType::Create(NUdf::TDataType<double>::Id, ctx.TypeEnv));
        UNIT_ASSERT_VALUES_EQUAL(json1, "\"nan\"");

        auto json2 = WriteValueToFuncJsonStr(value, TDataType::Create(NUdf::TDataType<double>::Id, ctx.TypeEnv));
        UNIT_ASSERT_VALUES_EQUAL(json2, "\"nan\"");
    }

    Y_UNIT_TEST(DisallowNaN) {
        TTestContext ctx;
        auto valueNan = NUdf::TUnboxedValuePod(std::numeric_limits<float>::quiet_NaN());
        TValueConvertPolicy policy{EValueConvertPolicy::DISALLOW_NaN};

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            WriteValueToExportJsonStr(valueNan, TDataType::Create(NUdf::TDataType<float>::Id, ctx.TypeEnv), policy),
            yexception,
            "NaN and Inf aren't allowed"
        );

        auto valueInf = NUdf::TUnboxedValuePod(std::numeric_limits<double>::infinity());
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            WriteValueToExportJsonStr(valueInf, TDataType::Create(NUdf::TDataType<double>::Id, ctx.TypeEnv), policy),
            yexception,
            "NaN and Inf aren't allowed"
        );
    }
}

Y_UNIT_TEST_SUITE(DefaultPolicy) {
    Y_UNIT_TEST(CloudFunction) {
        TTestContext ctx;
        auto value = NUdf::TUnboxedValuePod(i64(540138));
        auto policy = DefaultPolicy::getInstance().CloudFunction();
        auto json = WriteValueToExportJsonStr(value, TDataType::Create(NUdf::TDataType<i64>::Id, ctx.TypeEnv), policy);
        UNIT_ASSERT_VALUES_EQUAL(json, "\"540138\"");
    }
}


Y_UNIT_TEST_SUITE(DeserializeNumbers) {

#define TEST_DESERIALIZE_NUMBER_TYPE(type, wideType) \
    Y_UNIT_TEST(type) { \
        TTestContext ctx; \
            \
        TStringStream maxJson; \
        maxJson << ToString(std::numeric_limits<type>::max()); \
        auto maxValue = ReadJsonStrValue(&maxJson, TDataType::Create(NUdf::TDataType<type>::Id, ctx.TypeEnv), ctx.HolderFactory); \
        UNIT_ASSERT_VALUES_EQUAL(maxValue.Get<type>(), std::numeric_limits<type>::max()); \
            \
        TStringStream minJson; \
        minJson << ToString(std::numeric_limits<type>::min()); \
        auto minValue = ReadJsonStrValue(&minJson, TDataType::Create(NUdf::TDataType<type>::Id, ctx.TypeEnv), ctx.HolderFactory); \
        UNIT_ASSERT_VALUES_EQUAL(minValue.Get<type>(), std::numeric_limits<type>::min()); \
            \
        TStringStream exceededJson; \
        exceededJson << ToString(wideType(std::numeric_limits<type>::max() + wideType(1))); \
        UNIT_ASSERT_EXCEPTION_CONTAINS( \
            ReadJsonStrValue(&exceededJson, TDataType::Create(NUdf::TDataType<type>::Id, ctx.TypeEnv), ctx.HolderFactory), \
            yexception, \
            "Exceeded the range" \
        ); \
    }

    TEST_DESERIALIZE_NUMBER_TYPE(i8, i64)
    TEST_DESERIALIZE_NUMBER_TYPE(i16, i64)
    TEST_DESERIALIZE_NUMBER_TYPE(i32, i64)

    TEST_DESERIALIZE_NUMBER_TYPE(ui8, ui64)
    TEST_DESERIALIZE_NUMBER_TYPE(ui16, ui64)
    TEST_DESERIALIZE_NUMBER_TYPE(ui32, ui64)

#undef TEST_DESERIALIZE_NUMBER_TYPE

    Y_UNIT_TEST(float) {
        TTestContext ctx;
        TStringStream json;
        json << "0.0431";
        auto value = ReadJsonStrValue(&json, TDataType::Create(NUdf::TDataType<float>::Id, ctx.TypeEnv), ctx.HolderFactory);
        UNIT_ASSERT_VALUES_EQUAL(value.Get<float>(), 0.0431f);

        TStringStream exceededJson;
        exceededJson << ToString(std::numeric_limits<double>::max());
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ReadJsonStrValue(&exceededJson, TDataType::Create(NUdf::TDataType<float>::Id, ctx.TypeEnv), ctx.HolderFactory),
            yexception,
            "Exceeded the range"
        );
    }

    Y_UNIT_TEST(FloatFromInt) {
        TTestContext ctx;
        TStringStream json;
        json << "1766718243";
        auto value = ReadJsonStrValue(&json, TDataType::Create(NUdf::TDataType<float>::Id, ctx.TypeEnv), ctx.HolderFactory);
        UNIT_ASSERT_VALUES_EQUAL(value.Get<float>(), 1766718243.0f);
    }

    Y_UNIT_TEST(double) {
        TTestContext ctx;
        TStringStream json;
        json << "7773.13";
        auto value = ReadJsonStrValue(&json, TDataType::Create(NUdf::TDataType<double>::Id, ctx.TypeEnv), ctx.HolderFactory);
        UNIT_ASSERT_VALUES_EQUAL(value.Get<double>(), 7773.13);
    }

    Y_UNIT_TEST(DoubleFromInt) {
        TTestContext ctx;
        TStringStream json;
        json << "-667319001";
        auto value = ReadJsonStrValue(&json, TDataType::Create(NUdf::TDataType<double>::Id, ctx.TypeEnv), ctx.HolderFactory);
        UNIT_ASSERT_VALUES_EQUAL(value.Get<double>(), -667319001.0l);
    }

}

Y_UNIT_TEST_SUITE(SerializeStringTypes) {
    Y_UNIT_TEST(Utf8) {
        TTestContext ctx;
        auto value = ctx.Vb.NewString("aaaaabbb \" ' ` bbcccccc");
        auto json = WriteValueToFuncJsonStr(value, TDataType::Create(NUdf::TDataType<NUdf::TUtf8>::Id, ctx.TypeEnv));
        UNIT_ASSERT_VALUES_EQUAL(json, "\"aaaaabbb \\\" ' ` bbcccccc\"");
    }

    Y_UNIT_TEST(String) {
        TTestContext ctx;

        auto type = TDataType::Create(NUdf::TDataType<char*>::Id, ctx.TypeEnv);

        auto value1 = ctx.Vb.NewString("aaaaabbbbbcccccc");
        auto json1 = WriteValueToFuncJsonStr(value1, type);
        UNIT_ASSERT_VALUES_EQUAL(json1, "\"YWFhYWFiYmJiYmNjY2NjYw==\"");

        auto value2 = ctx.Vb.NewString("абсёЙabc");
        auto json2 = WriteValueToFuncJsonStr(value2, type);
        UNIT_ASSERT_VALUES_EQUAL(json2, "\"0LDQsdGB0ZHQmWFiYw==\"");
    }
}

Y_UNIT_TEST_SUITE(DeserializeStringTypes) {
    Y_UNIT_TEST(String) {
        TTestContext ctx;
        TStringStream json;
        json << "\"fffaaae423\"";
        auto value = ReadJsonStrValue(&json, TDataType::Create(NUdf::TDataType<NUdf::TUtf8>::Id, ctx.TypeEnv), ctx.HolderFactory);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf("fffaaae423"), TStringBuf(value.AsStringRef()));
    }
}

Y_UNIT_TEST_SUITE(SerializeDateTypes) {
    Y_UNIT_TEST(Date) {
        TTestContext ctx;
        ui16 date;
        ctx.Vb.MakeDate(2022, 2, 9, date);
        auto json = WriteValueToFuncJsonStr(NUdf::TUnboxedValuePod(date), TDataType::Create(NUdf::TDataType<NUdf::TDate>::Id, ctx.TypeEnv));
        UNIT_ASSERT_VALUES_EQUAL(json, "\"2022-02-09\"");
    }

    Y_UNIT_TEST(Datetime) {
        TTestContext ctx;
        ui32 datetime;
        ctx.Vb.MakeDatetime(2021, 1, 1, 14, 5, 43, datetime);
        auto json = WriteValueToFuncJsonStr(NUdf::TUnboxedValuePod(datetime), TDataType::Create(NUdf::TDataType<NUdf::TDatetime>::Id, ctx.TypeEnv));
        UNIT_ASSERT_VALUES_EQUAL(json, "\"2021-01-01T14:05:43Z\"");
    }

    Y_UNIT_TEST(Timestamp) {
        TTestContext ctx;
        auto value = ui64(1644755212879622);
        auto json = WriteValueToFuncJsonStr(NUdf::TUnboxedValuePod(value), TDataType::Create(NUdf::TDataType<NUdf::TTimestamp>::Id, ctx.TypeEnv));
        UNIT_ASSERT_VALUES_EQUAL(json, "\"2022-02-13T12:26:52.879622Z\"");
    }

    Y_UNIT_TEST(TzDate) {
        TTestContext ctx;
        ui16 rawDate;
        ctx.Vb.MakeDate(2023, 4, 14, rawDate);
        auto date = NUdf::TUnboxedValuePod(rawDate);
        date.SetTimezoneId(530);
        auto json = WriteValueToFuncJsonStr(date, TDataType::Create(NUdf::TDataType<NUdf::TTzDate>::Id, ctx.TypeEnv));
        UNIT_ASSERT_VALUES_EQUAL(json, "\"2023-04-14,Pacific/Easter\"");
    }

    Y_UNIT_TEST(TzDateTime) {
        TTestContext ctx;
        ui16 timeZone = 530;
        ui32 rawDate;
        ctx.Vb.MakeDatetime(2023, 4, 14, 0, 15, 0, rawDate, timeZone);
        auto date = NUdf::TUnboxedValuePod(rawDate);
        date.SetTimezoneId(timeZone);
        auto json = WriteValueToFuncJsonStr(date, TDataType::Create(NUdf::TDataType<NUdf::TTzDatetime>::Id, ctx.TypeEnv));
        UNIT_ASSERT_VALUES_EQUAL(json, "\"2023-04-14T00:15:00,Pacific/Easter\"");
    }

    Y_UNIT_TEST(TzTimestamp) {
        TTestContext ctx;
        auto value = NUdf::TUnboxedValuePod(ui64(1644755564087924));
        value.SetTimezoneId(327);
        auto json = WriteValueToFuncJsonStr(value, TDataType::Create(NUdf::TDataType<NUdf::TTzTimestamp>::Id, ctx.TypeEnv));
        UNIT_ASSERT_VALUES_EQUAL(json, "\"2022-02-13T19:32:44.087924,Asia/Vientiane\"");
    }

    Y_UNIT_TEST(Interval) {
        TTestContext ctx;
        // 2 days 2 hours in ms
        auto value = NUdf::TUnboxedValuePod(i64(180000000000));
        auto json = WriteValueToFuncJsonStr(value, TDataType::Create(NUdf::TDataType<NUdf::TInterval>::Id, ctx.TypeEnv));
        UNIT_ASSERT_VALUES_EQUAL(json, "\"P2DT2H\"");
    }
}

Y_UNIT_TEST_SUITE(DeserializeDateTypes) {

    Y_UNIT_TEST(Date) {
        TTestContext ctx;
        TStringStream json;
        json << "\"2020-09-11\"";
        auto value = ReadJsonStrValue(&json, TDataType::Create(NUdf::TDataType<NUdf::TDate>::Id, ctx.TypeEnv), ctx.HolderFactory);
        ui16 date;
        ctx.Vb.MakeDate(2020, 9, 11, date);
        UNIT_ASSERT_VALUES_EQUAL(value.Get<ui16>(), date);
    }

    Y_UNIT_TEST(Datetime) {
        TTestContext ctx;
        TStringStream json;
        json << "\"2021-07-14T00:00:43Z\"";
        auto value = ReadJsonStrValue(&json, TDataType::Create(NUdf::TDataType<NUdf::TDatetime>::Id, ctx.TypeEnv), ctx.HolderFactory);
        ui32 datetime;
        ctx.Vb.MakeDatetime(2021, 7, 14, 0, 0, 43, datetime);
        UNIT_ASSERT_VALUES_EQUAL(value.Get<ui32>(), datetime);
    }

    Y_UNIT_TEST(TzDate) {
        TTestContext ctx;
        TStringStream json;
        json << "\"2020-09-11,Europe/Moscow\""; // timeZoneId == 1
        auto value = ReadJsonStrValue(&json, TDataType::Create(NUdf::TDataType<NUdf::TTzDate>::Id, ctx.TypeEnv), ctx.HolderFactory);
        ui16 date;
        ctx.Vb.MakeDate(2020, 9, 10, date);
        UNIT_ASSERT_VALUES_EQUAL(value.GetTimezoneId(), 1);
        UNIT_ASSERT_VALUES_EQUAL(value.Get<ui16>(), date);
    }

    Y_UNIT_TEST(TzDatetime) {
        TTestContext ctx;
        TStringStream json;
        json << "\"2020-09-11T01:11:05,Europe/Moscow\""; // timeZoneId == 1
        auto value = ReadJsonStrValue(&json, TDataType::Create(NUdf::TDataType<NUdf::TTzDatetime>::Id, ctx.TypeEnv), ctx.HolderFactory);
        ui32 datetime;
        ctx.Vb.MakeDatetime(2020, 9, 11, 1, 11, 5, datetime, 1);
        UNIT_ASSERT_VALUES_EQUAL(value.GetTimezoneId(), 1);
        UNIT_ASSERT_VALUES_EQUAL(value.Get<ui32>(), datetime);
    }

    Y_UNIT_TEST(WrongTypeEx) {
        TTestContext ctx;
        TStringStream json;
        json << "[\"2020-22-12\"]";
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ReadJsonStrValue(&json, TDataType::Create(NUdf::TDataType<NUdf::TDate>::Id, ctx.TypeEnv), ctx.HolderFactory),
            yexception,
            "Unexpected json type (expected string"
        );
    }

    Y_UNIT_TEST(WrongFormatEx) {
        TTestContext ctx;
        TStringStream json;
        json << "\"2020-18-43\"";
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ReadJsonStrValue(&json, TDataType::Create(NUdf::TDataType<NUdf::TDate>::Id, ctx.TypeEnv), ctx.HolderFactory),
            yexception,
            "Invalid date format"
        );
    }
}

} // namespace
} // namespace
} // namespace

