#include <library/cpp/tvmauth/client/misc/roles/parser.h>

#include <library/cpp/tvmauth/unittest.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NTvmAuth;
using namespace NTvmAuth::NRoles;

Y_UNIT_TEST_SUITE(Parser) {
    static NJson::TJsonValue ToJsonValue(TStringBuf body) {
        NJson::TJsonValue doc;
        UNIT_ASSERT(NJson::ReadJsonTree(body, &doc));
        return doc;
    }

    Y_UNIT_TEST(GetEntity) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TParser::GetEntity(ToJsonValue(R"({"scope": false})"),
                               "cons",
                               "read"),
            yexception,
            "entity is map (str->str), got value Boolean. consumer 'cons' with role 'read'");

        TEntityPtr en;
        UNIT_ASSERT_NO_EXCEPTION(
            en = TParser::GetEntity(ToJsonValue(R"({})"),
                                    "cons",
                                    "read"));
        UNIT_ASSERT_VALUES_EQUAL(en->size(), 0);

        UNIT_ASSERT_NO_EXCEPTION(
            en = TParser::GetEntity(ToJsonValue(R"({"key1": "val1", "key2": "val2"})"),
                                    "cons",
                                    "read"));
        UNIT_ASSERT_VALUES_EQUAL(en->size(), 2);
    }

    Y_UNIT_TEST(GetEntities) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TParser::GetEntities(ToJsonValue(R"([{},[]])"),
                                 "cons",
                                 "read"),
            yexception,
            "role entity for role must be map: consumer 'cons' with role 'read' has Array");

        TEntitiesPtr en;
        UNIT_ASSERT_NO_EXCEPTION(
            en = TParser::GetEntities(ToJsonValue(R"([])"),
                                      "cons",
                                      "read"));
        UNIT_ASSERT(!en->Contains({}));

        UNIT_ASSERT_NO_EXCEPTION(
            en = TParser::GetEntities(ToJsonValue(R"([{}])"),
                                      "cons",
                                      "read"));
        UNIT_ASSERT(en->Contains({}));
    }

    Y_UNIT_TEST(GetConsumer) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TParser::GetConsumer(ToJsonValue(R"({"role1": [],"role2": {}})"),
                                 "cons"),
            yexception,
            "entities for roles must be array: 'role2' is Map");

        TConsumerRolesPtr c;
        UNIT_ASSERT_NO_EXCEPTION(
            c = TParser::GetConsumer(ToJsonValue(R"({"role1": [],"role2": []})"),
                                     "cons"));
        UNIT_ASSERT(c->HasRole("role1"));
        UNIT_ASSERT(c->HasRole("role2"));
        UNIT_ASSERT(!c->HasRole("role3"));
    }

    Y_UNIT_TEST(GetConsumers) {
        TRoles::TTvmConsumers cons;
        UNIT_ASSERT_NO_EXCEPTION(
            cons = TParser::GetConsumers<TTvmId>(ToJsonValue(R"({})"),
                                                 "tvm"));
        UNIT_ASSERT_VALUES_EQUAL(0, cons.size());

        UNIT_ASSERT_NO_EXCEPTION(
            cons = TParser::GetConsumers<TTvmId>(ToJsonValue(R"({"tvm": {}})"),
                                                 "tvm"));
        UNIT_ASSERT_VALUES_EQUAL(0, cons.size());

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TParser::GetConsumers<TTvmId>(ToJsonValue(R"({"tvm": []})"),
                                          "tvm"),
            yexception,
            "'tvm' must be object");

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TParser::GetConsumers<TTvmId>(ToJsonValue(R"({"tvm": {"asd": []}})"),
                                          "tvm"),
            yexception,
            "roles for consumer must be map: 'asd' is Array");

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TParser::GetConsumers<TTvmId>(ToJsonValue(R"({"tvm": {"asd": {}}})"),
                                          "tvm"),
            yexception,
            "id must be valid positive number of proper size for tvm. got 'asd'");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TParser::GetConsumers<TTvmId>(ToJsonValue(R"({"tvm": {"1120000000001062": {}}})"),
                                          "tvm"),
            yexception,
            "id must be valid positive number of proper size for tvm. got '1120000000001062'");
        UNIT_ASSERT_NO_EXCEPTION(
            TParser::GetConsumers<TUid>(ToJsonValue(R"({"user": {"1120000000001062": {}}})"),
                                        "user"));

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TParser::GetConsumers<TTvmId>(ToJsonValue(R"({"tvm": {"42": {}, "042": {}}})"),
                                          "tvm"),
            yexception,
            "consumer duplicate detected: '42' for tvm");

        UNIT_ASSERT_NO_EXCEPTION(
            cons = TParser::GetConsumers<TTvmId>(ToJsonValue(R"({"tvm": {"42": {}}})"),
                                                 "tvm"));
        UNIT_ASSERT_VALUES_EQUAL(1, cons.size());
    }

    Y_UNIT_TEST(GetMeta) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TParser::GetMeta(ToJsonValue(R"({})")),
            yexception,
            "Missing 'revision'");

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TParser::GetMeta(ToJsonValue(R"({"revision": null})")),
            yexception,
            "'revision' has unexpected type: Null");

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TParser::GetMeta(ToJsonValue(R"({"revision": 100500})")),
            yexception,
            "Missing 'born_date'");

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TParser::GetMeta(ToJsonValue(R"({"revision": 100500, "born_date": false})")),
            yexception,
            "key 'born_date' must be uint");

        TRoles::TMeta meta;
        UNIT_ASSERT_NO_EXCEPTION(
            meta = TParser::GetMeta(ToJsonValue(R"({"revision": 100500, "born_date": 42})")));
        UNIT_ASSERT_VALUES_EQUAL("100500", meta.Revision);
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(42), meta.BornTime);

        UNIT_ASSERT_NO_EXCEPTION(
            meta = TParser::GetMeta(ToJsonValue(R"({"revision": "100501", "born_date": 42})")));
        UNIT_ASSERT_VALUES_EQUAL("100501", meta.Revision);
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(42), meta.BornTime);
    }
}
