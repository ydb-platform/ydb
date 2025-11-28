#include <ydb/library/yql/providers/s3/compressors/lz4io.h>

#include <library/cpp/scheme/scheme.h>
#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/udfs/common/clickhouse/client/src/IO/ReadBufferFromFile.h>

namespace NYql::NCompressors {

namespace {
    TString GetResourcePath(const TString& path) {
        return ArcadiaFromCurrentLocation(__SOURCE_FILE__, "test_compression_data/" + path);
    }
}

Y_UNIT_TEST_SUITE(TCompressorTests) {
    Y_UNIT_TEST(SuccessLz4) {
        NDB::ReadBufferFromFile buffer(GetResourcePath("test.json.lz4"));
        auto decompressorBuffer = NLz4::MakeDecompressor(buffer);

        char str[256] = {};
        decompressorBuffer->read(str, 256);
        UNIT_ASSERT_VALUES_EQUAL(NSc::TValue::FromJsonThrow(str), NSc::TValue::FromJsonThrow(R"([
            {
                "id": 0,
                "description": "yq",
                "info": "abc"
            }
            ])"));
    }

    Y_UNIT_TEST(WrongMagicLz4) {
        NDB::ReadBufferFromFile buffer(GetResourcePath("test.json"));
        UNIT_ASSERT_EXCEPTION_CONTAINS(NLz4::MakeDecompressor(buffer), yexception, "Wrong magic.");
    }

    Y_UNIT_TEST(ErrorLz4) {
        NDB::ReadBufferFromFile buffer(GetResourcePath("test.broken.lz4"));
        auto decompressorBuffer = NLz4::MakeDecompressor(buffer);
        char str[256] = {};
        UNIT_ASSERT_EXCEPTION_CONTAINS(decompressorBuffer->read(str, 256), yexception, "Decompression error: ERROR_reservedFlag_set");
    }
}

}
