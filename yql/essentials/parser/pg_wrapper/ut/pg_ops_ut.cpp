#include <yql/essentials/parser/pg_wrapper/pg_ops.h>

#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

using namespace NKikimr::NMiniKQL;

namespace {

ui32 PgTypeId(TStringBuf typeName) {
    return NPg::LookupType(TString(typeName)).TypeId;
}

class TTestContext {
public:
    TTestContext()
        : Alloc(__LOCATION__)
    {
    }

private:
    TScopedAlloc Alloc;
    TThrowingBindTerminator BindTerminator;
};

void AssertSign(const TPgConst& value, i32 expectedSign) {
    auto signFunc = TPgSign::Create(value.GetTypeId());
    auto result = signFunc->MakeCallState()->GetSign(value.ExtractConst());
    UNIT_ASSERT_C(result.Defined(), "Sign returned NULL");
    UNIT_ASSERT_VALUES_EQUAL(*result, expectedSign);
}

} // namespace

Y_UNIT_TEST_SUITE(TPgOpsTests) {

Y_UNIT_TEST(TestSignInt4) {
    TTestContext ctx;

    TPgConst positive(PgTypeId("int4"), "42");
    TPgConst negative(PgTypeId("int4"), "-42");
    TPgConst zero(PgTypeId("int4"), "0");

    AssertSign(positive, 1);
    AssertSign(negative, -1);
    AssertSign(zero, 0);
}

Y_UNIT_TEST(TestSignInt8) {
    TTestContext ctx;

    TPgConst positive(PgTypeId("int8"), "9223372036854775807");
    TPgConst negative(PgTypeId("int8"), "-9223372036854775808");
    TPgConst zero(PgTypeId("int8"), "0");

    AssertSign(positive, 1);
    AssertSign(negative, -1);
    AssertSign(zero, 0);
}

Y_UNIT_TEST(TestSignFloat4) {
    TTestContext ctx;

    TPgConst positive(PgTypeId("float4"), "3.14");
    TPgConst negative(PgTypeId("float4"), "-2.71");
    TPgConst zero(PgTypeId("float4"), "0.0");

    AssertSign(positive, 1);
    AssertSign(negative, -1);
    AssertSign(zero, 0);
}

Y_UNIT_TEST(TestSignFloat8) {
    TTestContext ctx;

    TPgConst positive(PgTypeId("float8"), "1.23456789012345");
    TPgConst negative(PgTypeId("float8"), "-1.23456789012345");
    TPgConst zero(PgTypeId("float8"), "0.0");

    AssertSign(positive, 1);
    AssertSign(negative, -1);
    AssertSign(zero, 0);
}

Y_UNIT_TEST(TestSignNumeric) {
    TTestContext ctx;

    TPgConst positive(PgTypeId("numeric"), "123456789012345678901234567890.123456");
    TPgConst negative(PgTypeId("numeric"), "-123456789012345678901234567890.123456");
    TPgConst zero(PgTypeId("numeric"), "0");

    AssertSign(positive, 1);
    AssertSign(negative, -1);
    AssertSign(zero, 0);
}

Y_UNIT_TEST(TestSignInt2) {
    TTestContext ctx;

    TPgConst positive(PgTypeId("int2"), "100");
    TPgConst negative(PgTypeId("int2"), "-100");
    TPgConst zero(PgTypeId("int2"), "0");

    AssertSign(positive, 1);
    AssertSign(negative, -1);
    AssertSign(zero, 0);
}

Y_UNIT_TEST(TestSignInterval) {
    TTestContext ctx;

    TPgConst positive(PgTypeId("interval"), "1 day");
    TPgConst negative(PgTypeId("interval"), "-1 day");
    TPgConst zero(PgTypeId("interval"), "0 seconds");

    AssertSign(positive, 1);
    AssertSign(negative, -1);
    AssertSign(zero, 0);
}

Y_UNIT_TEST(TestSignTextThrowsException) {
    TTestContext ctx;

    TPgConst textVal(PgTypeId("text"), "hello");
    UNIT_ASSERT_EXCEPTION(
        TPgSign::Create(textVal.GetTypeId()),
        yexception);
}

Y_UNIT_TEST(TestConstInvalidInt) {
    TTestContext ctx;
    UNIT_ASSERT_EXCEPTION(
        TPgConst(PgTypeId("int4"), "not_a_number").ExtractConst(),
        yexception);
}

Y_UNIT_TEST(TestConstEmptyInt) {
    TTestContext ctx;
    UNIT_ASSERT_EXCEPTION(
        TPgConst(PgTypeId("int4"), "").ExtractConst(),
        yexception);
}

} // Y_UNIT_TEST_SUITE(TPgOpsTests)

} // namespace NYql
