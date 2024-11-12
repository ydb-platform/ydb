#include "json_envelope.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

#define UNIT_ASSERT_JSONS_EQUAL(j1, j2) {                    \
    const TString js1 = (j1), js2 = (j2);                    \
    UNIT_ASSERT(!js1.empty());                               \
    UNIT_ASSERT_C(js1.back() == '\n', js1);                  \
    NJson::TJsonValue jv1, jv2;                              \
    UNIT_ASSERT(ReadJsonTree(j1, &jv1));                     \
    UNIT_ASSERT(ReadJsonTree(j2, &jv2));                     \
    const TString jsn1 = NJson::WriteJson(&jv1, true, true); \
    const TString jsn2 = NJson::WriteJson(&jv2, true, true); \
    UNIT_ASSERT_VALUES_EQUAL(jsn1, jsn2);                    \
}

Y_UNIT_TEST_SUITE(JsonEnvelopeTest) {
    Y_UNIT_TEST(Simple) {
        TJsonEnvelope env1(R"json({
            "a": "b",
            "m": "abc%message%def"
        })json");

        UNIT_ASSERT_JSONS_EQUAL(env1.ApplyJsonEnvelope("msg"), R"json({"a":"b","m":"abcmsgdef"})json");
        UNIT_ASSERT_JSONS_EQUAL(env1.ApplyJsonEnvelope("xyz"), R"json({"a":"b","m":"abcxyzdef"})json");


        TJsonEnvelope env2(R"json({
            "a": "b",
            "m": "%message%def"
        })json");

        UNIT_ASSERT_JSONS_EQUAL(env2.ApplyJsonEnvelope("msg"), R"json({"a":"b","m":"msgdef"})json");
        UNIT_ASSERT_JSONS_EQUAL(env2.ApplyJsonEnvelope("xyz"), R"json({"a":"b","m":"xyzdef"})json");


        TJsonEnvelope env3(R"json({
            "a": "b",
            "m": "abc%message%"
        })json");

        UNIT_ASSERT_JSONS_EQUAL(env3.ApplyJsonEnvelope("msg"), R"json({"a":"b","m":"abcmsg"})json");
        UNIT_ASSERT_JSONS_EQUAL(env3.ApplyJsonEnvelope("xyz"), R"json({"a":"b","m":"abcxyz"})json");
    }

    Y_UNIT_TEST(NoReplace) {
        TJsonEnvelope env(R"json({
            "a": "b",
            "x": "%y%",
            "subfield": {
                "s": "% message %",
                "t": "%Message%",
                "x": 42,
                "a": [
                    42
                ]
            }
        })json");

        UNIT_ASSERT_JSONS_EQUAL(env.ApplyJsonEnvelope("msg"), R"json({"a":"b","x":"%y%","subfield":{"s":"% message %","t":"%Message%","x":42,"a":[42]}})json");
        UNIT_ASSERT_JSONS_EQUAL(env.ApplyJsonEnvelope("xyz"), R"json({"a":"b","x":"%y%","subfield":{"s":"% message %","t":"%Message%","x":42,"a":[42]}})json");
    }

    Y_UNIT_TEST(ArrayItem) {
        TJsonEnvelope env(R"json({
            "a": "b",
            "subfield": {
                "a": [
                    42,
                    "%message%",
                    53
                ]
            }
        })json");

        UNIT_ASSERT_JSONS_EQUAL(env.ApplyJsonEnvelope("msg"), R"json({"a":"b","subfield":{"a":[42,"msg",53]}})json");
        UNIT_ASSERT_JSONS_EQUAL(env.ApplyJsonEnvelope("xyz"), R"json({"a":"b","subfield":{"a":[42,"xyz",53]}})json");
    }

    Y_UNIT_TEST(Escape) {
        TJsonEnvelope env(R"json({
            "a": "%message%"
        })json");

        UNIT_ASSERT_JSONS_EQUAL(env.ApplyJsonEnvelope("msg"), R"json({"a":"msg"})json");
        UNIT_ASSERT_JSONS_EQUAL(env.ApplyJsonEnvelope("\"\n\""), R"json({"a":"\"\n\""})json");
    }

    Y_UNIT_TEST(BinaryData) {
        TJsonEnvelope env(R"json({
            "a": "%message%"
        })json");

        const ui64 binaryData = 0xABCDEFFF87654321;
        const TStringBuf data(reinterpret_cast<const char*>(&binaryData), sizeof(binaryData));
        UNIT_ASSERT_EXCEPTION(env.ApplyJsonEnvelope(data), std::exception);
        UNIT_ASSERT_JSONS_EQUAL(env.ApplyJsonEnvelope("text"), R"json({"a":"text"})json");
    }
}

} // namespace NKikimr
