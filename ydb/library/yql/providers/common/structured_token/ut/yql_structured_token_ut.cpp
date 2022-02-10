#include <ydb/library/yql/providers/common/structured_token/yql_structured_token.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

Y_UNIT_TEST_SUITE(StructuredTokenTest) {
    Y_UNIT_TEST(EmptyToken) {
        const TStructuredToken t1;
        UNIT_ASSERT_VALUES_EQUAL("{}", t1.ToJson());
        UNIT_ASSERT_VALUES_EQUAL("xyz", t1.GetFieldOrDefault("key", "xyz"));

        auto t2 = ParseStructuredToken("{}");
        UNIT_ASSERT_VALUES_EQUAL("{}", t2.ToJson());
    }

    Y_UNIT_TEST(IsStructuredTokenJson) {
        UNIT_ASSERT(!IsStructuredTokenJson(""));
        UNIT_ASSERT(!IsStructuredTokenJson("my_token"));
        UNIT_ASSERT(IsStructuredTokenJson("{}"));
        UNIT_ASSERT(IsStructuredTokenJson(R"({"f1":"my_token"})"));
    }

    Y_UNIT_TEST(SetField) {
        TStructuredToken t1;
        t1.SetField("f1", "xxx");
        t1.SetField("f2", "yyy");
        t1.SetField("oops", "zzzzzzzzzzzzz");

        UNIT_ASSERT_VALUES_EQUAL("xxx", t1.GetFieldOrDefault("f1", "xyz"));
        UNIT_ASSERT_VALUES_EQUAL("xyz", t1.GetFieldOrDefault("f3", "xyz"));
        UNIT_ASSERT_VALUES_EQUAL("yyy", t1.GetField("f2"));
        UNIT_ASSERT_VALUES_EQUAL(R"({"f1":"xxx","f2":"yyy","oops":"zzzzzzzzzzzzz"})", t1.ToJson());

        const TStructuredToken t2 = ParseStructuredToken(t1.ToJson());
        UNIT_ASSERT_VALUES_EQUAL("xxx", t2.GetFieldOrDefault("f1", "xyz"));
        UNIT_ASSERT_VALUES_EQUAL("xyz", t2.GetFieldOrDefault("f3", "xyz"));
        UNIT_ASSERT_VALUES_EQUAL("yyy", t2.GetField("f2"));
        UNIT_ASSERT_VALUES_EQUAL(t1.ToJson(), t2.ToJson());
    }

    Y_UNIT_TEST(SetNonUtf8Field) {
        TStructuredToken t1;
        TString nonUtf8(TStringBuf("\xF0\x9F\x94"));
        t1.SetField("f1", nonUtf8);
        UNIT_ASSERT_VALUES_EQUAL(R"foo({"f1(base64)":"8J+U"})foo", t1.ToJson());
        UNIT_ASSERT_VALUES_EQUAL(nonUtf8, t1.GetField("f1"));
        UNIT_ASSERT_VALUES_EQUAL("empty", t1.GetFieldOrDefault("f1(base64)", "empty"));

        const TStructuredToken t2 = ParseStructuredToken(t1.ToJson());
        UNIT_ASSERT_VALUES_EQUAL(nonUtf8, t2.GetField("f1"));
        UNIT_ASSERT_VALUES_EQUAL("xyz", t2.GetFieldOrDefault("f2", "xyz"));
        UNIT_ASSERT_VALUES_EQUAL(t1.ToJson(), t2.ToJson());
    }

    Y_UNIT_TEST(Move) {
        TStructuredToken t1;
        t1
            .SetField("f1", "xxx")
            .SetField("f2", "yyy");

        UNIT_ASSERT_VALUES_EQUAL("xxx", t1.GetFieldOrDefault("f1", "xyz"));
        UNIT_ASSERT_VALUES_EQUAL("yyy", t1.GetFieldOrDefault("f2", "xyz"));
        UNIT_ASSERT_VALUES_EQUAL("xyz", t1.GetFieldOrDefault("f3", "xyz"));

        const TStructuredToken t2(std::move(t1));
        UNIT_ASSERT_VALUES_EQUAL("xxx", t2.GetFieldOrDefault("f1", "xyz"));
        UNIT_ASSERT_VALUES_EQUAL("yyy", t2.GetFieldOrDefault("f2", "xyz"));
        UNIT_ASSERT_VALUES_EQUAL("xyz", t2.GetFieldOrDefault("f3", "xyz"));

        UNIT_ASSERT_VALUES_EQUAL("xyz", t1.GetFieldOrDefault("f1", "xyz"));
        UNIT_ASSERT_VALUES_EQUAL("xyz", t1.GetFieldOrDefault("f2", "xyz"));
        UNIT_ASSERT_VALUES_EQUAL("xyz", t1.GetFieldOrDefault("f3", "xyz"));
    }
}

}
