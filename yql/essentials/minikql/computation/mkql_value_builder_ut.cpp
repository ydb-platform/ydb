#include "mkql_value_builder.h"
#include "mkql_computation_node_holders.h"

#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <library/cpp/testing/unittest/registar.h>

#include <arrow/array/builder_primitive.h>
#include <arrow/c/abi.h>
#include <arrow/scalar.h>
#include <arrow/chunked_array.h>

namespace NYql::NCommon {

TString PgValueToNativeText(const NUdf::TUnboxedValuePod& value, ui32 pgTypeId);
TString PgValueToNativeBinary(const NUdf::TUnboxedValuePod& value, ui32 pgTypeId);

} // namespace NYql::NCommon

namespace NKikimr {

using namespace NUdf;
using namespace NYql::NCommon;

namespace NMiniKQL {

namespace {
TString AsString(const TStringValue& v) {
    return {v.Data(), v.Size()};
}
} // namespace

class TMiniKQLValueBuilderTest: public TTestBase {
public:
    TMiniKQLValueBuilderTest()
        : FunctionRegistry_(CreateFunctionRegistry(CreateBuiltinRegistry()))
        , Alloc_(__LOCATION__)
        , Env_(Alloc_)
        , MemInfo_("Memory")
        , HolderFactory_(Alloc_.Ref(), MemInfo_, FunctionRegistry_.Get())
        , Builder_(HolderFactory_, NUdf::EValidatePolicy::Exception)
        , TypeInfoHelper_(new TTypeInfoHelper())
        , FunctionTypeInfoBuilder_(NYql::UnknownLangVersion, Env_, TypeInfoHelper_, "", nullptr, TSourcePosition())
    {
        BoolOid_ = NYql::NPg::LookupType("bool").TypeId;
    }

    const IPgBuilder& GetPgBuilder() const {
        return Builder_.GetPgBuilder();
    }

private:
    TIntrusivePtr<NMiniKQL::IFunctionRegistry> FunctionRegistry_;
    TScopedAlloc Alloc_;
    TTypeEnvironment Env_;
    TMemoryUsageInfo MemInfo_;
    THolderFactory HolderFactory_;
    TDefaultValueBuilder Builder_;
    NUdf::ITypeInfoHelper::TPtr TypeInfoHelper_;
    TFunctionTypeInfoBuilder FunctionTypeInfoBuilder_;
    ui32 BoolOid_ = 0;

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
        const auto v = Builder_.NewVariant(62, TUnboxedValuePod((ui64)42));
        UNIT_ASSERT(v);
        UNIT_ASSERT(!v.IsBoxed());
        UNIT_ASSERT_VALUES_EQUAL(62, v.GetVariantIndex());
        UNIT_ASSERT_VALUES_EQUAL(42, v.GetVariantItem().Get<ui64>());
    }

    void TestBoxedVariant() {
        const auto v = Builder_.NewVariant(63, TUnboxedValuePod((ui64)42));
        UNIT_ASSERT(v);
        UNIT_ASSERT(v.IsBoxed());
        UNIT_ASSERT_VALUES_EQUAL(63, v.GetVariantIndex());
        UNIT_ASSERT_VALUES_EQUAL(42, v.GetVariantItem().Get<ui64>());
    }

    void TestSubstring() {
        const auto string = Builder_.NewString("0123456789qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM");
        UNIT_ASSERT(string);

        const auto zero = Builder_.SubString(string, 7, 0);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(""), TStringBuf(zero.AsStringRef()));

        const auto tail = Builder_.SubString(string, 60, 8);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf("NM"), TStringBuf(tail.AsStringRef()));

        const auto small = Builder_.SubString(string, 2, 14);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf("23456789qwerty"), TStringBuf(small.AsStringRef()));

        const auto one = Builder_.SubString(string, 3, 15);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf("3456789qwertyui"), TStringBuf(one.AsStringRef()));
        UNIT_ASSERT_VALUES_EQUAL(string.AsStringValue().Data(), one.AsStringValue().Data());

        const auto two = Builder_.SubString(string, 10, 30);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf("qwertyuiopasdfghjklzxcvbnmQWER"), TStringBuf(two.AsStringRef()));
        UNIT_ASSERT_VALUES_EQUAL(string.AsStringValue().Data(), two.AsStringValue().Data());
    }

    void TestPgValueFromErrors() {
        const TBindTerminator bind(&Builder_); // to raise exception instead of abort
        {
            TStringValue error("");
            auto r = GetPgBuilder().ValueFromText(BoolOid_, "", error);
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(AsString(error), "ERROR:  invalid input syntax for type boolean: \"\"");
        }

        {
            TStringValue error("");
            auto r = GetPgBuilder().ValueFromText(BoolOid_, "zzzz", error);
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(AsString(error), "ERROR:  invalid input syntax for type boolean: \"zzzz\"");
        }

        {
            TStringValue error("");
            auto r = GetPgBuilder().ValueFromBinary(BoolOid_, "", error);
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(AsString(error), "ERROR:  no data left in message");
        }

        {
            TStringValue error("");
            auto r = GetPgBuilder().ValueFromBinary(BoolOid_, "zzzz", error);
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(AsString(error), "Not all data has been consumed by 'recv' function: boolrecv, data size: 4, consumed size: 1");
        }
    }

    void TestPgValueFromText() {
        const TBindTerminator bind(&Builder_);
        for (auto validTrue : {"t"sv, "true"sv}) {
            TStringValue error("");
            auto r = GetPgBuilder().ValueFromText(BoolOid_, validTrue, error);
            UNIT_ASSERT(r);
            UNIT_ASSERT_VALUES_EQUAL(AsString(error), "");
            auto s = PgValueToNativeText(r, BoolOid_);
            UNIT_ASSERT_VALUES_EQUAL(s, "t");
        }

        for (auto validFalse : {"f"sv, "false"sv}) {
            TStringValue error("");
            auto r = GetPgBuilder().ValueFromText(BoolOid_, validFalse, error);
            UNIT_ASSERT(r);
            UNIT_ASSERT_VALUES_EQUAL(AsString(error), "");
            auto s = PgValueToNativeText(r, BoolOid_);
            UNIT_ASSERT_VALUES_EQUAL(s, "f");
        }
    }

    void TestPgValueFromBinary() {
        const TBindTerminator bind(&Builder_);
        TStringValue error("");
        auto t = GetPgBuilder().ValueFromText(BoolOid_, "true", error);
        UNIT_ASSERT(t);
        auto f = GetPgBuilder().ValueFromText(BoolOid_, "false", error);
        UNIT_ASSERT(f);

        auto ts = PgValueToNativeBinary(t, BoolOid_);
        auto fs = PgValueToNativeBinary(f, BoolOid_);
        {
            auto r = GetPgBuilder().ValueFromBinary(BoolOid_, ts, error);
            UNIT_ASSERT(r);
            auto s = PgValueToNativeText(r, BoolOid_);
            UNIT_ASSERT_VALUES_EQUAL(s, "t");
        }

        {
            auto r = GetPgBuilder().ValueFromBinary(BoolOid_, fs, error);
            UNIT_ASSERT(r);
            auto s = PgValueToNativeText(r, BoolOid_);
            UNIT_ASSERT_VALUES_EQUAL(s, "f");
        }
    }

    void TestConvertToFromPg() {
        const TBindTerminator bind(&Builder_);
        auto boolType = FunctionTypeInfoBuilder_.SimpleType<bool>();
        {
            auto v = GetPgBuilder().ConvertToPg(TUnboxedValuePod(true), boolType, BoolOid_);
            auto s = PgValueToNativeText(v, BoolOid_);
            UNIT_ASSERT_VALUES_EQUAL(s, "t");

            auto from = GetPgBuilder().ConvertFromPg(v, BoolOid_, boolType);
            UNIT_ASSERT_VALUES_EQUAL(from.Get<bool>(), true);
        }

        {
            auto v = GetPgBuilder().ConvertToPg(TUnboxedValuePod(false), boolType, BoolOid_);
            auto s = PgValueToNativeText(v, BoolOid_);
            UNIT_ASSERT_VALUES_EQUAL(s, "f");

            auto from = GetPgBuilder().ConvertFromPg(v, BoolOid_, boolType);
            UNIT_ASSERT_VALUES_EQUAL(from.Get<bool>(), false);
        }
    }

    void TestConvertToFromPgNulls() {
        const TBindTerminator bind(&Builder_);
        auto boolOptionalType = FunctionTypeInfoBuilder_.Optional()->Item<bool>().Build();

        {
            auto v = GetPgBuilder().ConvertToPg(TUnboxedValuePod(), boolOptionalType, BoolOid_);
            UNIT_ASSERT(!v);
        }

        {
            auto v = GetPgBuilder().ConvertFromPg(TUnboxedValuePod(), BoolOid_, boolOptionalType);
            UNIT_ASSERT(!v);
        }
    }

    void TestPgNewString() {
        {
            auto& pgText = NYql::NPg::LookupType("text");
            UNIT_ASSERT_VALUES_EQUAL(pgText.TypeLen, -1);

            auto s = GetPgBuilder().NewString(pgText.TypeLen, pgText.TypeId, "ABC");
            auto utf8Type = FunctionTypeInfoBuilder_.SimpleType<TUtf8>();
            auto from = GetPgBuilder().ConvertFromPg(s, pgText.TypeId, utf8Type);
            UNIT_ASSERT_VALUES_EQUAL((TStringBuf)from.AsStringRef(), "ABC"sv);
        }

        {
            auto& pgCString = NYql::NPg::LookupType("cstring");
            UNIT_ASSERT_VALUES_EQUAL(pgCString.TypeLen, -2);

            auto s = GetPgBuilder().NewString(pgCString.TypeLen, pgCString.TypeId, "ABC");
            auto utf8Type = FunctionTypeInfoBuilder_.SimpleType<TUtf8>();
            auto from = GetPgBuilder().ConvertFromPg(s, pgCString.TypeId, utf8Type);
            UNIT_ASSERT_VALUES_EQUAL((TStringBuf)from.AsStringRef(), "ABC"sv);
        }

        {
            auto& byteaString = NYql::NPg::LookupType("bytea");
            UNIT_ASSERT_VALUES_EQUAL(byteaString.TypeLen, -1);

            auto s = GetPgBuilder().NewString(byteaString.TypeLen, byteaString.TypeId, "ABC");
            auto stringType = FunctionTypeInfoBuilder_.SimpleType<char*>();
            auto from = GetPgBuilder().ConvertFromPg(s, byteaString.TypeId, stringType);
            UNIT_ASSERT_VALUES_EQUAL((TStringBuf)from.AsStringRef(), "ABC"sv);
        }
    }

    void TestArrowBlock() {
        auto type = FunctionTypeInfoBuilder_.SimpleType<ui64>();
        auto atype = TypeInfoHelper_->MakeArrowType(type);

        {
            arrow::Datum d1(std::make_shared<arrow::UInt64Scalar>(123));
            NUdf::TUnboxedValue val1 = HolderFactory_.CreateArrowBlock(std::move(d1));
            bool isScalar;
            ui64 length;
            auto chunks = Builder_.GetArrowBlockChunks(val1, isScalar, length);
            UNIT_ASSERT_VALUES_EQUAL(chunks, 1);
            UNIT_ASSERT(isScalar);
            UNIT_ASSERT_VALUES_EQUAL(length, 1);

            ArrowArray arr1;
            Builder_.ExportArrowBlock(val1, 0, &arr1);
            NUdf::TUnboxedValue val2 = Builder_.ImportArrowBlock(&arr1, 1, isScalar, *atype);
            const auto& d2 = TArrowBlock::From(val2).GetDatum();
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
            NUdf::TUnboxedValue val1 = HolderFactory_.CreateArrowBlock(std::move(d1));

            bool isScalar;
            ui64 length;
            auto chunks = Builder_.GetArrowBlockChunks(val1, isScalar, length);
            UNIT_ASSERT_VALUES_EQUAL(chunks, 1);
            UNIT_ASSERT(!isScalar);
            UNIT_ASSERT_VALUES_EQUAL(length, 3);

            ArrowArray arr1;
            Builder_.ExportArrowBlock(val1, 0, &arr1);
            NUdf::TUnboxedValue val2 = Builder_.ImportArrowBlock(&arr1, 1, isScalar, *atype);
            const auto& d2 = TArrowBlock::From(val2).GetDatum();
            UNIT_ASSERT(d2.is_array());
            UNIT_ASSERT_VALUES_EQUAL(d2.array()->length, 3);
            UNIT_ASSERT_VALUES_EQUAL(d2.array()->GetNullCount(), 0);
            auto flat = d2.array()->GetValues<ui64>(1);
            UNIT_ASSERT_VALUES_EQUAL(flat[0], 10);
            UNIT_ASSERT_VALUES_EQUAL(flat[1], 20);
            UNIT_ASSERT_VALUES_EQUAL(flat[2], 30);
        }

        {
            arrow::UInt64Builder builder1;
            UNIT_ASSERT(builder1.Reserve(3).ok());
            builder1.UnsafeAppend(ui64(10));
            builder1.UnsafeAppend(ui64(20));
            builder1.UnsafeAppend(ui64(30));
            std::shared_ptr<arrow::Array> builder1Result;
            UNIT_ASSERT(builder1.Finish(&builder1Result).ok());

            arrow::UInt64Builder builder2;
            UNIT_ASSERT(builder2.Reserve(2).ok());
            builder2.UnsafeAppend(ui64(40));
            builder2.UnsafeAppend(ui64(50));
            std::shared_ptr<arrow::Array> builder2Result;
            UNIT_ASSERT(builder2.Finish(&builder2Result).ok());

            auto chunked = arrow::ChunkedArray::Make({builder1Result, builder2Result}).ValueOrDie();
            arrow::Datum d1(chunked);
            NUdf::TUnboxedValue val1 = HolderFactory_.CreateArrowBlock(std::move(d1));

            bool isScalar;
            ui64 length;
            auto chunks = Builder_.GetArrowBlockChunks(val1, isScalar, length);
            UNIT_ASSERT_VALUES_EQUAL(chunks, 2);
            UNIT_ASSERT(!isScalar);
            UNIT_ASSERT_VALUES_EQUAL(length, 5);

            std::array<ArrowArray, 2> arrs;
            Builder_.ExportArrowBlock(val1, 0, &arrs[0]);
            Builder_.ExportArrowBlock(val1, 1, &arrs[1]);
            NUdf::TUnboxedValue val2 = Builder_.ImportArrowBlock(arrs.data(), 2, isScalar, *atype);
            const auto& d2 = TArrowBlock::From(val2).GetDatum();
            UNIT_ASSERT(d2.is_arraylike() && !d2.is_array());
            UNIT_ASSERT_VALUES_EQUAL(d2.length(), 5);
            UNIT_ASSERT_VALUES_EQUAL(d2.chunks().size(), 2);

            UNIT_ASSERT_VALUES_EQUAL(d2.chunks()[0]->data()->length, 3);
            UNIT_ASSERT_VALUES_EQUAL(d2.chunks()[0]->data()->GetNullCount(), 0);
            auto flat = d2.chunks()[0]->data()->GetValues<ui64>(1);
            UNIT_ASSERT_VALUES_EQUAL(flat[0], 10);
            UNIT_ASSERT_VALUES_EQUAL(flat[1], 20);
            UNIT_ASSERT_VALUES_EQUAL(flat[2], 30);

            UNIT_ASSERT_VALUES_EQUAL(d2.chunks()[1]->data()->length, 2);
            UNIT_ASSERT_VALUES_EQUAL(d2.chunks()[1]->data()->GetNullCount(), 0);
            flat = d2.chunks()[1]->data()->GetValues<ui64>(1);
            UNIT_ASSERT_VALUES_EQUAL(flat[0], 40);
            UNIT_ASSERT_VALUES_EQUAL(flat[1], 50);
        }
    }
};

UNIT_TEST_SUITE_REGISTRATION(TMiniKQLValueBuilderTest);

} // namespace NMiniKQL
} // namespace NKikimr
