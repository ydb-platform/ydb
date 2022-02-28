#include <ydb/library/yql/parser/pg_catalog/catalog.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYql::NPg;

const TProcDesc& LookupProcByStrArgTypes(const TString& name, const TVector<TString>& argTypes) {
    TVector<ui32> argTypeIds;
    argTypeIds.reserve(argTypes.size());
    for (const auto& a : argTypes) {
        argTypeIds.push_back(LookupType(a).TypeId);
    }

    return LookupProc(name, argTypeIds);
}

Y_UNIT_TEST_SUITE(TTypesTests) {
    Y_UNIT_TEST(TestMissing) {
        UNIT_ASSERT_EXCEPTION(LookupType("_foo_bar_"), yexception);
        UNIT_ASSERT_EXCEPTION(LookupType(0), yexception);
    }

    Y_UNIT_TEST(TestOk) {
        auto ret = LookupType("text");
        UNIT_ASSERT_VALUES_EQUAL(ret.TypeId, 25);
        UNIT_ASSERT_VALUES_EQUAL(ret.ArrayTypeId, 1009);
        UNIT_ASSERT_VALUES_EQUAL(ret.Name, "text");
        UNIT_ASSERT_VALUES_EQUAL(ret.ElementTypeId, 0);

        ret = LookupType("point");
        UNIT_ASSERT_VALUES_EQUAL(ret.TypeId, 600);
        UNIT_ASSERT_VALUES_EQUAL(ret.ArrayTypeId, 1017);
        UNIT_ASSERT_VALUES_EQUAL(ret.Name, "point");
        UNIT_ASSERT_VALUES_EQUAL(ret.ElementTypeId, LookupType("float8").TypeId);

        ret = LookupType(1009);
        UNIT_ASSERT_VALUES_EQUAL(ret.TypeId, 25);
        UNIT_ASSERT_VALUES_EQUAL(ret.ArrayTypeId, 1009);
        UNIT_ASSERT_VALUES_EQUAL(ret.Name, "text");
        UNIT_ASSERT_VALUES_EQUAL(ret.ElementTypeId, 0);
    }
}

Y_UNIT_TEST_SUITE(TFunctionsTests) {
    Y_UNIT_TEST(TestMissing) {
        UNIT_ASSERT_EXCEPTION(LookupProcByStrArgTypes("_foo_bar_", {}), yexception);
    }

    Y_UNIT_TEST(TestMismatchArgTypes) {
        UNIT_ASSERT_EXCEPTION(LookupProcByStrArgTypes("int4pl", {}), yexception);
    }

    Y_UNIT_TEST(TestOk) {
        auto ret = LookupProcByStrArgTypes("int4pl", {"int4", "int4"});
        UNIT_ASSERT_VALUES_EQUAL(ret.ResultType, LookupType("int4").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes[0], LookupType("int4").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes[1], LookupType("int4").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(ret.Src, "int4pl");

        ret = LookupProcByStrArgTypes("substring", {"text", "int4", "int4"});
        UNIT_ASSERT_VALUES_EQUAL(ret.ResultType, LookupType("text").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes[0], LookupType("text").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes[1], LookupType("int4").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes[2], LookupType("int4").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(ret.Src, "text_substr");

        ret = LookupProcByStrArgTypes("substring", {"text", "int4"});
        UNIT_ASSERT_VALUES_EQUAL(ret.ResultType, LookupType("text").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes[0], LookupType("text").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes[1], LookupType("int4").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(ret.Src, "text_substr_no_len");
    }
}

Y_UNIT_TEST_SUITE(TCastsTests) {
    Y_UNIT_TEST(TestMissing) {
        UNIT_ASSERT_EXCEPTION(LookupCast(LookupType("circle").TypeId, LookupType("int8").TypeId), yexception);
    }

    Y_UNIT_TEST(TestOk) {
        auto ret = LookupCast(LookupType("int8").TypeId, LookupType("int4").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(ret.SourceId, LookupType("int8").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(ret.TargetId, LookupType("int4").TypeId);
        UNIT_ASSERT(ret.Method == ECastMethod::Function);
        UNIT_ASSERT_VALUES_UNEQUAL(ret.FunctionId, 0);

        ret = LookupCast(LookupType("int4").TypeId, LookupType("oid").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(ret.SourceId, LookupType("int4").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(ret.TargetId, LookupType("oid").TypeId);
        UNIT_ASSERT(ret.Method == ECastMethod::Binary);
        UNIT_ASSERT_VALUES_EQUAL(ret.FunctionId, 0);

        ret = LookupCast(LookupType("json").TypeId, LookupType("jsonb").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(ret.SourceId, LookupType("json").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(ret.TargetId, LookupType("jsonb").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(ret.FunctionId, 0);
    }
}
