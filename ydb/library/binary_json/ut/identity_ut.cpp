#include "test_base.h"

#include <ydb/library/binary_json/write.h>
#include <ydb/library/binary_json/read.h>

#include <ydb/library/yql/minikql/dom/json.h>

using namespace NKikimr;

class TBinaryJsonIdentityTest : public TBinaryJsonTestBase {
public:
    TBinaryJsonIdentityTest()
        : TBinaryJsonTestBase()
    {
    }

    UNIT_TEST_SUITE(TBinaryJsonIdentityTest);
        UNIT_TEST(TestReadToJsonDom);
        UNIT_TEST(TestSerializeToJson);
        UNIT_TEST(TestSerializeDomToBinaryJson);
    UNIT_TEST_SUITE_END();

    const TVector<TString> TestCases = {
            "false",
            "true",
            "null",
            "\"test string\"",
            "\"\"",
            "1.2345",
            "1",
            "-23",
            "0",
            "0.12345",
            "{}",
            "{\"a\":1}",
            "[]",
            "[1]",
            R"([{"key":[true,false,null,"first","second","second","third"]},"fourth",0.34])",
        };

    void TestReadToJsonDom() {
        for (const TStringBuf json : TestCases) {
            const auto binaryJson = *NBinaryJson::SerializeToBinaryJson(json);
            const auto value = NBinaryJson::ReadToJsonDom(binaryJson, &ValueBuilder);
            const auto jsonAfterBinaryJson = NDom::SerializeJsonDom(value);

            UNIT_ASSERT_VALUES_EQUAL(json, jsonAfterBinaryJson);
        }
    }

    void TestSerializeToJson() {
        for (const TStringBuf json : TestCases) {
            const auto binaryJson = *NBinaryJson::SerializeToBinaryJson(json);
            const auto jsonAfterBinaryJson = NBinaryJson::SerializeToJson(binaryJson);

            UNIT_ASSERT_VALUES_EQUAL(json, jsonAfterBinaryJson);
        }
    }

    void TestSerializeDomToBinaryJson() {
        for (const TStringBuf json : TestCases) {
            const auto dom = NDom::TryParseJsonDom(json, &ValueBuilder);
            const auto binaryJson = NBinaryJson::SerializeToBinaryJson(dom);
            const auto jsonAfterBinaryJson = NBinaryJson::SerializeToJson(binaryJson);

            UNIT_ASSERT_VALUES_EQUAL(json, jsonAfterBinaryJson);
        }
    }
};

UNIT_TEST_SUITE_REGISTRATION(TBinaryJsonIdentityTest);
