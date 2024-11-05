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
        UNIT_ASSERT_VALUES_EQUAL(ret.TypeCollation, DefaultCollationOid);
        UNIT_ASSERT_VALUES_EQUAL(ret.TypType, ETypType::Base);
        UNIT_ASSERT(ret.LessProcId);
        UNIT_ASSERT(ret.EqualProcId);
        UNIT_ASSERT(ret.CompareProcId);
        UNIT_ASSERT(ret.HashProcId);

        ret = LookupType("point");
        UNIT_ASSERT_VALUES_EQUAL(ret.TypeId, 600);
        UNIT_ASSERT_VALUES_EQUAL(ret.ArrayTypeId, 1017);
        UNIT_ASSERT_VALUES_EQUAL(ret.Name, "point");
        UNIT_ASSERT_VALUES_EQUAL(ret.ElementTypeId, LookupType("float8").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(ret.TypType, ETypType::Base);
        UNIT_ASSERT(ret.TypeSubscriptFuncId);
        UNIT_ASSERT(!ret.LessProcId);
        UNIT_ASSERT(!ret.EqualProcId);
        UNIT_ASSERT(!ret.CompareProcId);
        UNIT_ASSERT(!ret.HashProcId);

        ret = LookupType(1009);
        UNIT_ASSERT_VALUES_EQUAL(ret.TypeId, 1009);
        UNIT_ASSERT_VALUES_EQUAL(ret.ArrayTypeId, 1009);
        UNIT_ASSERT_VALUES_EQUAL(ret.Name, "_text");
        UNIT_ASSERT_VALUES_EQUAL(ret.ElementTypeId, 25);
        UNIT_ASSERT_VALUES_EQUAL(ret.TypeCollation, DefaultCollationOid);
        UNIT_ASSERT_VALUES_EQUAL(ret.TypType, ETypType::Base);
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
        UNIT_ASSERT(ret.Kind == EProcKind::Function);

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

        ret = LookupProcByStrArgTypes("sum", {"int4"});
        UNIT_ASSERT(ret.Kind == EProcKind::Aggregate);

        ret = LookupProcByStrArgTypes("row_number", {});
        UNIT_ASSERT(ret.Kind == EProcKind::Window);
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
        UNIT_ASSERT_VALUES_EQUAL(ret.CoercionCode, ECoercionCode::Assignment);
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

Y_UNIT_TEST_SUITE(TAggregationsTests) {
    Y_UNIT_TEST(TestMissing) {
        UNIT_ASSERT_EXCEPTION(LookupAggregation("foo", {}), yexception);
    }

    Y_UNIT_TEST(TestOk) {
        auto ret = LookupAggregation("sum", {LookupType("int4").TypeId});
        UNIT_ASSERT_VALUES_EQUAL(ret.TransTypeId, LookupType("int8").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(ret.Name, "sum");
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes[0], LookupType("int4").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(LookupProc(ret.TransFuncId).Name, "int4_sum");
        UNIT_ASSERT_VALUES_EQUAL(LookupProc(ret.CombineFuncId).Name, "int8pl");
        UNIT_ASSERT_VALUES_EQUAL(ret.FinalFuncId, 0);
        UNIT_ASSERT_VALUES_EQUAL(ret.SerializeFuncId, 0);
        UNIT_ASSERT_VALUES_EQUAL(ret.DeserializeFuncId, 0);

        ret = LookupAggregation("sum", {LookupType("int8").TypeId});
        UNIT_ASSERT_VALUES_EQUAL(ret.TransTypeId, LookupType("internal").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(ret.Name, "sum");
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes[0], LookupType("int8").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(LookupProc(ret.TransFuncId).Name, "int8_avg_accum");
        UNIT_ASSERT_VALUES_EQUAL(LookupProc(ret.CombineFuncId).Name, "int8_avg_combine");
        UNIT_ASSERT_VALUES_EQUAL(LookupProc(ret.FinalFuncId).Name, "numeric_poly_sum");
        UNIT_ASSERT_VALUES_EQUAL(LookupProc(ret.SerializeFuncId).Name, "int8_avg_serialize");
        UNIT_ASSERT_VALUES_EQUAL(LookupProc(ret.DeserializeFuncId).Name, "int8_avg_deserialize");

        ret = LookupAggregation("xmlagg", {LookupType("xml").TypeId});
        UNIT_ASSERT_VALUES_EQUAL(ret.TransTypeId, LookupType("xml").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(ret.Name, "xmlagg");
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes[0], LookupType("xml").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(LookupProc(ret.TransFuncId).Name, "xmlconcat2");
        UNIT_ASSERT_VALUES_EQUAL(ret.CombineFuncId, 0);
        UNIT_ASSERT_VALUES_EQUAL(ret.FinalFuncId, 0);
        UNIT_ASSERT_VALUES_EQUAL(ret.SerializeFuncId, 0);
        UNIT_ASSERT_VALUES_EQUAL(ret.DeserializeFuncId, 0);

        ret = LookupAggregation("regr_count", {LookupType("float8").TypeId, LookupType("float8").TypeId});
        UNIT_ASSERT_VALUES_EQUAL(ret.TransTypeId, LookupType("int8").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(ret.Name, "regr_count");
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes[0], LookupType("float8").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes[1], LookupType("float8").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(LookupProc(ret.TransFuncId).Name, "int8inc_float8_float8");
        UNIT_ASSERT_VALUES_EQUAL(LookupProc(ret.CombineFuncId).Name, "int8pl");
        UNIT_ASSERT_VALUES_EQUAL(ret.FinalFuncId, 0);
        UNIT_ASSERT_VALUES_EQUAL(ret.SerializeFuncId, 0);
        UNIT_ASSERT_VALUES_EQUAL(ret.DeserializeFuncId, 0);
    }
}

Y_UNIT_TEST_SUITE(TOpClassesTests) {
    Y_UNIT_TEST(TestMissing) {
        UNIT_ASSERT_EXCEPTION(LookupDefaultOpClass(EOpClassMethod::Btree, LookupType("json").TypeId), yexception);
    }

   Y_UNIT_TEST(TestOk) {
        auto ret = *LookupDefaultOpClass(EOpClassMethod::Btree, LookupType("int4").TypeId);
        UNIT_ASSERT(ret.Method == EOpClassMethod::Btree);
        UNIT_ASSERT_VALUES_EQUAL(ret.TypeId, LookupType("int4").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(ret.Name, "int4_ops");
        UNIT_ASSERT_VALUES_EQUAL(ret.Family, "btree/integer_ops");
        UNIT_ASSERT_VALUES_EQUAL(ret.FamilyId, 1976);

        ret = *LookupDefaultOpClass(EOpClassMethod::Hash, LookupType("int4").TypeId);
        UNIT_ASSERT(ret.Method == EOpClassMethod::Hash);
        UNIT_ASSERT_VALUES_EQUAL(ret.TypeId, LookupType("int4").TypeId);
        UNIT_ASSERT_VALUES_EQUAL(ret.Name, "int4_ops");
        UNIT_ASSERT_VALUES_EQUAL(ret.Family, "hash/integer_ops");
        UNIT_ASSERT_VALUES_EQUAL(ret.FamilyId, 1977);
   }
}

Y_UNIT_TEST_SUITE(TConversionTests) {
    Y_UNIT_TEST(TestMissing) {
        UNIT_ASSERT_EXCEPTION(LookupConversion("foo", "bar"), yexception);
    }

   Y_UNIT_TEST(TestOk) {
        auto procId = LookupConversion("LATIN1", "UTF8").ProcId;
        UNIT_ASSERT_VALUES_EQUAL(LookupProc(procId).Name, "iso8859_1_to_utf8");
   }
}
