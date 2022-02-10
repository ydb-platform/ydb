#include "test_base.h"

#include <ydb/library/binary_json/write.h>
#include <ydb/library/binary_json/read.h>
#include <ydb/library/binary_json/format.h>

#include <ydb/library/yql/minikql/dom/json.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/random.h>

using namespace NKikimr::NBinaryJson;

class TBinaryJsonValidnessTest : public TBinaryJsonTestBase {
public:
    TBinaryJsonValidnessTest()
        : TBinaryJsonTestBase()
    {
    }

    UNIT_TEST_SUITE(TBinaryJsonValidnessTest);
        UNIT_TEST(TestValidness);
        UNIT_TEST(TestRandom);
        UNIT_TEST(TestVersionCheck);
    UNIT_TEST_SUITE_END();

    void TestValidness() {
        const TVector<TString> testCases = {
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

        for (const TStringBuf json : testCases) {
            const auto binaryJson = *SerializeToBinaryJson(json);
            const TStringBuf buffer(binaryJson.Data(), binaryJson.Size());
            const auto error = IsValidBinaryJsonWithError(buffer);
            UNIT_ASSERT_C(!error.Defined(), TStringBuilder() << "BinaryJson for '" << json << "' is invalid because of '" << *error << "'");
        }
    }

    void TestRandom() {
        for (ui32 i = 0; i < 1000000; i++) {
            const auto fakeBinaryJson = NUnitTest::RandomString(RandomNumber<size_t>(1000));
            UNIT_ASSERT(!IsValidBinaryJson(fakeBinaryJson));
        }
    }

    void TestVersionCheck() {
        TBinaryJson binaryJson;
        THeader header(EVersion::Draft, 0);
        binaryJson.Append(reinterpret_cast<char*>(&header), sizeof(header));

        UNIT_ASSERT_EXCEPTION_CONTAINS([&]() {
            TBinaryJsonReader::Make(binaryJson);
        }(), yexception, "does not match current version");
    }
};

UNIT_TEST_SUITE_REGISTRATION(TBinaryJsonValidnessTest);
