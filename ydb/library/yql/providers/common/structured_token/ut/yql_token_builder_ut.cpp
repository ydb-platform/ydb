#include <ydb/library/yql/providers/common/structured_token/yql_token_builder.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

Y_UNIT_TEST_SUITE(TokenBuilderTest) {
    Y_UNIT_TEST(Empty) {
        const TStructuredTokenBuilder b;
        UNIT_ASSERT_VALUES_EQUAL("{}", b.ToJson());
        const TStructuredTokenParser p = CreateStructuredTokenParser(b.ToJson());
        UNIT_ASSERT(!p.HasServiceAccountIdAuth());
        UNIT_ASSERT(!p.HasBasicAuth());
        UNIT_ASSERT(!p.HasIAMToken());
        UNIT_ASSERT(!p.IsNoAuth());
    }

    Y_UNIT_TEST(ServiceAccountId) {
        TStructuredTokenBuilder b;
        b.SetServiceAccountIdAuth("my_sa_id", "my_sa_sign");
        UNIT_ASSERT_VALUES_EQUAL(R"({"sa_id":"my_sa_id","sa_id_signature":"my_sa_sign"})", b.ToJson());
        const TStructuredTokenParser p = CreateStructuredTokenParser(b.ToJson());
        UNIT_ASSERT(p.HasServiceAccountIdAuth());
        UNIT_ASSERT(!p.HasBasicAuth());
        UNIT_ASSERT(!p.HasIAMToken());
        UNIT_ASSERT(!p.IsNoAuth());
        TString id, sign;
        UNIT_ASSERT(p.GetServiceAccountIdAuth(id, sign));
        UNIT_ASSERT_VALUES_EQUAL(id, "my_sa_id");
        UNIT_ASSERT_VALUES_EQUAL(sign, "my_sa_sign");
    }

    Y_UNIT_TEST(BasicAuth) {
        TStructuredTokenBuilder b;
        b.SetBasicAuth("my_login", "my_passw");
        UNIT_ASSERT_VALUES_EQUAL(R"({"basic_login":"my_login","basic_password":"my_passw"})", b.ToJson());
        const TStructuredTokenParser p = CreateStructuredTokenParser(b.ToJson());
        UNIT_ASSERT(!p.HasServiceAccountIdAuth());
        UNIT_ASSERT(p.HasBasicAuth());
        UNIT_ASSERT(!p.HasIAMToken());
        UNIT_ASSERT(!p.IsNoAuth());
        TString login, password;
        UNIT_ASSERT(p.GetBasicAuth(login, password));
        UNIT_ASSERT_VALUES_EQUAL(login, "my_login");
        UNIT_ASSERT_VALUES_EQUAL(password, "my_passw");
    }

    Y_UNIT_TEST(IAMToken) {
        TStructuredTokenBuilder b;
        b.SetIAMToken("my_token");
        UNIT_ASSERT_VALUES_EQUAL(R"({"token":"my_token"})", b.ToJson());
        const TStructuredTokenParser p = CreateStructuredTokenParser(b.ToJson());
        UNIT_ASSERT(!p.HasServiceAccountIdAuth());
        UNIT_ASSERT(!p.HasBasicAuth());
        UNIT_ASSERT(p.HasIAMToken());
        UNIT_ASSERT(!p.IsNoAuth());
        TString token = p.GetIAMToken();
        UNIT_ASSERT_VALUES_EQUAL(token, "my_token");
    }

    Y_UNIT_TEST(NoAuth) {
        TStructuredTokenBuilder b;
        b.SetNoAuth();
        UNIT_ASSERT_VALUES_EQUAL(R"({"no_auth":""})", b.ToJson());
        const TStructuredTokenParser p = CreateStructuredTokenParser(b.ToJson());
        UNIT_ASSERT(!p.HasServiceAccountIdAuth());
        UNIT_ASSERT(!p.HasBasicAuth());
        UNIT_ASSERT(!p.HasIAMToken());
        UNIT_ASSERT(p.IsNoAuth());
    }

    Y_UNIT_TEST(BasicAuthAndToken) {
        TStructuredTokenBuilder b;
        b.SetBasicAuth("my_login", "my_passw");
        b.SetIAMToken("my_token");
        UNIT_ASSERT_VALUES_EQUAL(R"({"basic_login":"my_login","basic_password":"my_passw","token":"my_token"})", b.ToJson());
        const TStructuredTokenParser p = CreateStructuredTokenParser(b.ToJson());
        UNIT_ASSERT(!p.HasServiceAccountIdAuth());
        UNIT_ASSERT(p.HasBasicAuth());
        UNIT_ASSERT(p.HasIAMToken());
        UNIT_ASSERT(!p.IsNoAuth());

        TString login, password;
        UNIT_ASSERT(p.GetBasicAuth(login, password));
        UNIT_ASSERT_VALUES_EQUAL(login, "my_login");
        UNIT_ASSERT_VALUES_EQUAL(password, "my_passw");

        TString token = p.GetIAMToken();
        UNIT_ASSERT_VALUES_EQUAL(token, "my_token");

    }
}

}
