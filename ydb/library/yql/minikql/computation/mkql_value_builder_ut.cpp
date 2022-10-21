#include "mkql_value_builder.h"
#include "mkql_computation_node_holders.h"

#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <library/cpp/testing/unittest/registar.h>

#include <arrow/array/builder_primitive.h>
#include <arrow/c/abi.h>
#include <arrow/scalar.h>

namespace NYql {
namespace NCommon {

TString PgValueToNativeText(const NUdf::TUnboxedValuePod& value, ui32 pgTypeId);
TString PgValueToNativeBinary(const NUdf::TUnboxedValuePod& value, ui32 pgTypeId);

}
}

namespace NKikimr {

using namespace NUdf;
using namespace NYql::NCommon;

namespace NMiniKQL {

namespace {
    TString AsString(const TStringValue& v) {
        return { v.Data(), v.Size() };
    }
}

class TMiniKQLValueBuilderTest: public TTestBase {
public:
    TMiniKQLValueBuilderTest()
        : FunctionRegistry(CreateFunctionRegistry(CreateBuiltinRegistry()))
        , Alloc(__LOCATION__)
        , Env(Alloc)
        , MemInfo("Memory")
        , HolderFactory(Alloc.Ref(), MemInfo, FunctionRegistry.Get())
        , Builder(HolderFactory, NUdf::EValidatePolicy::Exception)
        , TypeInfoHelper(new TTypeInfoHelper())
        , FunctionTypeInfoBuilder(Env, TypeInfoHelper, "", nullptr, {})
    {
        BoolOid = NYql::NPg::LookupType("bool").TypeId;
    }

    const IPgBuilder& GetPgBuilder() const {
        return Builder.GetPgBuilder();
    }

private:
    TIntrusivePtr<NMiniKQL::IFunctionRegistry> FunctionRegistry;
    TScopedAlloc Alloc;
    TTypeEnvironment Env;
    TMemoryUsageInfo MemInfo;
    THolderFactory HolderFactory;
    TDefaultValueBuilder Builder;
    NUdf::ITypeInfoHelper::TPtr TypeInfoHelper;
    TFunctionTypeInfoBuilder FunctionTypeInfoBuilder;
    ui32 BoolOid = 0;

    UNIT_TEST_SUITE(TMiniKQLValueBuilderTest);
        UNIT_TEST(TestEmbeddedVariant);
        UNIT_TEST(TestBoxedVariant);
        UNIT_TEST(TestSubstring);
        UNIT_TEST(TestPgValueFromErrors);
        UNIT_TEST(TestPgValueFromText);
        UNIT_TEST(TestPgValueFromBinary);
        UNIT_TEST(TestConvertToFromPg);
        UNIT_TEST(TestConvertToFromPgNulls);
        UNIT_TEST(TestPgNewString);
        UNIT_TEST(TestArrowBlock);
    UNIT_TEST_SUITE_END();


    void TestEmbeddedVariant() {
        const auto v = Builder.NewVariant(62, TUnboxedValuePod((ui64) 42));
        UNIT_ASSERT(v);
        UNIT_ASSERT(!v.IsBoxed());
        UNIT_ASSERT_VALUES_EQUAL(62, v.GetVariantIndex());
        UNIT_ASSERT_VALUES_EQUAL(42, v.GetVariantItem().Get<ui64>());
    }

    void TestBoxedVariant() {
        const auto v = Builder.NewVariant(63, TUnboxedValuePod((ui64) 42));
        UNIT_ASSERT(v);
        UNIT_ASSERT(v.IsBoxed());
        UNIT_ASSERT_VALUES_EQUAL(63, v.GetVariantIndex());
        UNIT_ASSERT_VALUES_EQUAL(42, v.GetVariantItem().Get<ui64>());
    }

    void TestSubstring() {
        const auto string = Builder.NewString("0123456789qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM");
        UNIT_ASSERT(string);

        const auto zero = Builder.SubString(string, 7, 0);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(""), TStringBuf(zero.AsStringRef()));

        const auto tail = Builder.SubString(string, 60, 8);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf("NM"), TStringBuf(tail.AsStringRef()));

        const auto small = Builder.SubString(string, 2, 14);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf("23456789qwerty"), TStringBuf(small.AsStringRef()));

        const auto one = Builder.SubString(string, 3, 15);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf("3456789qwertyui"), TStringBuf(one.AsStringRef()));
        UNIT_ASSERT_VALUES_EQUAL(string.AsStringValue().Data(), one.AsStringValue().Data());

        const auto two = Builder.SubString(string, 10, 30);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf("qwertyuiopasdfghjklzxcvbnmQWER"), TStringBuf(two.AsStringRef()));
        UNIT_ASSERT_VALUES_EQUAL(string.AsStringValue().Data(), two.AsStringValue().Data());
    }

    void TestPgValueFromErrors() {
        const TBindTerminator bind(&Builder); // to raise exception instead of abort
        {
            TStringValue error("");
            auto r = GetPgBuilder().ValueFromText(BoolOid, "", error);
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(AsString(error), "invalid input syntax for type boolean: \"\"");
        }

        {
            TStringValue error("");
            auto r = GetPgBuilder().ValueFromText(BoolOid, "zzzz", error);
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(AsString(error), "Terminate was called, reason(85): Error in 'in' function: boolin, reason: invalid input syntax for type boolean: \"zzzz\"");
        }

        {
            TStringValue error("");
            auto r = GetPgBuilder().ValueFromBinary(BoolOid, "", error);
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(AsString(error), "Error in 'recv' function: boolrecv, reason: no data left in message");
        }

        {
            TStringValue error("");
            auto r = GetPgBuilder().ValueFromBinary(BoolOid, "zzzz", error);
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(AsString(error), "Not all data has been consumed by 'recv' function: boolrecv, data size: 4, consumed size: 1");
        }
    }

    void TestPgValueFromText() {
        const TBindTerminator bind(&Builder);
        for (auto validTrue : { "t"sv, "true"sv }) {
            TStringValue error("");
            auto r = GetPgBuilder().ValueFromText(BoolOid, validTrue, error);
            UNIT_ASSERT(r);
            UNIT_ASSERT_VALUES_EQUAL(AsString(error), "");
            auto s = PgValueToNativeText(r, BoolOid);
            UNIT_ASSERT_VALUES_EQUAL(s, "t");
        }

        for (auto validFalse : { "f"sv, "false"sv }) {
            TStringValue error("");
            auto r = GetPgBuilder().ValueFromText(BoolOid, validFalse, error);
            UNIT_ASSERT(r);
            UNIT_ASSERT_VALUES_EQUAL(AsString(error), "");
            auto s = PgValueToNativeText(r, BoolOid);
            UNIT_ASSERT_VALUES_EQUAL(s, "f");
        }
    }

    void TestPgValueFromBinary() {
        const TBindTerminator bind(&Builder);
        TStringValue error("");
        auto t = GetPgBuilder().ValueFromText(BoolOid, "true", error);
        UNIT_ASSERT(t);
        auto f = GetPgBuilder().ValueFromText(BoolOid, "false", error);
        UNIT_ASSERT(f);

        auto ts = PgValueToNativeBinary(t, BoolOid);
        auto fs = PgValueToNativeBinary(f, BoolOid);
        {
            auto r = GetPgBuilder().ValueFromBinary(BoolOid, ts, error);
            UNIT_ASSERT(r);
            auto s = PgValueToNativeText(r, BoolOid);
            UNIT_ASSERT_VALUES_EQUAL(s, "t");
        }

        {
            auto r = GetPgBuilder().ValueFromBinary(BoolOid, fs, error);
            UNIT_ASSERT(r);
            auto s = PgValueToNativeText(r, BoolOid);
            UNIT_ASSERT_VALUES_EQUAL(s, "f");
        }
    }

    void TestConvertToFromPg() {
        const TBindTerminator bind(&Builder);
        auto boolType = FunctionTypeInfoBuilder.SimpleType<bool>();
        {
            auto v = GetPgBuilder().ConvertToPg(TUnboxedValuePod(true), boolType, BoolOid);
            auto s = PgValueToNativeText(v, BoolOid);
            UNIT_ASSERT_VALUES_EQUAL(s, "t");

            auto from = GetPgBuilder().ConvertFromPg(v, BoolOid, boolType);
            UNIT_ASSERT_VALUES_EQUAL(from.Get<bool>(), true);
        }

        {
            auto v = GetPgBuilder().ConvertToPg(TUnboxedValuePod(false), boolType, BoolOid);
            auto s = PgValueToNativeText(v, BoolOid);
            UNIT_ASSERT_VALUES_EQUAL(s, "f");

            auto from = GetPgBuilder().ConvertFromPg(v, BoolOid, boolType);
            UNIT_ASSERT_VALUES_EQUAL(from.Get<bool>(), false);
        }
    }

    void TestConvertToFromPgNulls() {
        const TBindTerminator bind(&Builder);
        auto boolOptionalType = FunctionTypeInfoBuilder.Optional()->Item<bool>().Build();

        {
            auto v = GetPgBuilder().ConvertToPg(TUnboxedValuePod(), boolOptionalType, BoolOid);
            UNIT_ASSERT(!v);
        }

        {
            auto v = GetPgBuilder().ConvertFromPg(TUnboxedValuePod(), BoolOid, boolOptionalType);
            UNIT_ASSERT(!v);
        }
    }

    void TestPgNewString() {
        {
            auto& pgText = NYql::NPg::LookupType("text");
            UNIT_ASSERT_VALUES_EQUAL(pgText.TypeLen, -1);

            auto s = GetPgBuilder().NewString(pgText.TypeLen, pgText.TypeId, "ABC");
            auto utf8Type = FunctionTypeInfoBuilder.SimpleType<TUtf8>();
            auto from = GetPgBuilder().ConvertFromPg(s, pgText.TypeId, utf8Type);
            UNIT_ASSERT_VALUES_EQUAL((TStringBuf)from.AsStringRef(), "ABC"sv);
        }

        {
            auto& pgCString = NYql::NPg::LookupType("cstring");
            UNIT_ASSERT_VALUES_EQUAL(pgCString.TypeLen, -2);

            auto s = GetPgBuilder().NewString(pgCString.TypeLen, pgCString.TypeId, "ABC");
            auto utf8Type = FunctionTypeInfoBuilder.SimpleType<TUtf8>();
            auto from = GetPgBuilder().ConvertFromPg(s, pgCString.TypeId, utf8Type);
            UNIT_ASSERT_VALUES_EQUAL((TStringBuf)from.AsStringRef(), "ABC"sv);
        }

        {
            auto& byteaString = NYql::NPg::LookupType("bytea");
            UNIT_ASSERT_VALUES_EQUAL(byteaString.TypeLen, -1);

            auto s = GetPgBuilder().NewString(byteaString.TypeLen, byteaString.TypeId, "ABC");
            auto stringType = FunctionTypeInfoBuilder.SimpleType<char*>();
            auto from = GetPgBuilder().ConvertFromPg(s, byteaString.TypeId, stringType);
            UNIT_ASSERT_VALUES_EQUAL((TStringBuf)from.AsStringRef(), "ABC"sv);
        }
    }

    void TestArrowBlock() {
        auto type = FunctionTypeInfoBuilder.SimpleType<ui64>();
        auto atype = TypeInfoHelper->MakeArrowType(type);

        {
            arrow::Datum d1(std::make_shared<arrow::UInt64Scalar>(123));
            auto val1 = HolderFactory.CreateArrowBlock(std::move(d1));
            ArrowArray arr1;
            bool isScalar;
            Builder.ExportArrowBlock(val1, isScalar, &arr1);
            UNIT_ASSERT(isScalar);
            auto val2 = Builder.ImportArrowBlock(&arr1, *atype, isScalar);
            const auto d2 = TArrowBlock::From(val2).GetDatum();
            UNIT_ASSERT(d2.is_scalar());
            UNIT_ASSERT_VALUES_EQUAL(d2.scalar_as<arrow::UInt64Scalar>().value, 123);
        }

        {
            arrow::UInt64Builder builder;
            UNIT_ASSERT(builder.Reserve(3).ok());
            builder.UnsafeAppend(ui64(10));
            builder.UnsafeAppend(ui64(20));
            builder.UnsafeAppend(ui64(30));
            std::shared_ptr<arrow::ArrayData> builderResult;
            UNIT_ASSERT(builder.FinishInternal(&builderResult).ok());
            arrow::Datum d1(builderResult);
            auto val1 = HolderFactory.CreateArrowBlock(std::move(d1));
            ArrowArray arr1;
            bool isScalar;
            Builder.ExportArrowBlock(val1, isScalar, &arr1);
            UNIT_ASSERT(!isScalar);
            auto val2 = Builder.ImportArrowBlock(&arr1, *atype, isScalar);
            const auto d2 = TArrowBlock::From(val2).GetDatum();
            UNIT_ASSERT(d2.is_array());
            UNIT_ASSERT_VALUES_EQUAL(d2.array()->length, 3);
            UNIT_ASSERT_VALUES_EQUAL(d2.array()->GetNullCount(), 0);
            auto flat = d2.array()->GetValues<ui64>(1);
            UNIT_ASSERT_VALUES_EQUAL(flat[0], 10);
            UNIT_ASSERT_VALUES_EQUAL(flat[1], 20);
            UNIT_ASSERT_VALUES_EQUAL(flat[2], 30);
        }
    }
};

UNIT_TEST_SUITE_REGISTRATION(TMiniKQLValueBuilderTest);

}
}
