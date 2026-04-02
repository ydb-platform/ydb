#include <ydb/library/yql/providers/pq/async_io/codec_string.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NDq {

Y_UNIT_TEST_SUITE(TCodecStringTest) {
    Y_UNIT_TEST(TestParseCodecString) {
        using NYdb::NTopic::ECodec;

        // codec name only тЖТ nullopt level
        {
            auto [codec, level] = ParseCodecString("zstd");
            UNIT_ASSERT_EQUAL(codec, ECodec::ZSTD);
            UNIT_ASSERT(!level.has_value());
        }
        // codec + level
        {
            auto [codec, level] = ParseCodecString("zstd_6");
            UNIT_ASSERT_EQUAL(codec, ECodec::ZSTD);
            UNIT_ASSERT(level.has_value());
            UNIT_ASSERT_EQUAL(*level, 6);
        }
        // case-insensitive
        {
            auto [codec, level] = ParseCodecString("GZIP_3");
            UNIT_ASSERT_EQUAL(codec, ECodec::GZIP);
            UNIT_ASSERT(level.has_value());
            UNIT_ASSERT_EQUAL(*level, 3);
        }
        // raw, no level
        {
            auto [codec, level] = ParseCodecString("raw");
            UNIT_ASSERT_EQUAL(codec, ECodec::RAW);
            UNIT_ASSERT(!level.has_value());
        }
        // lzop
        {
            auto [codec, level] = ParseCodecString("lzop");
            UNIT_ASSERT_EQUAL(codec, ECodec::LZOP);
            UNIT_ASSERT(!level.has_value());
        }
        // gzip without level
        {
            auto [codec, level] = ParseCodecString("gzip");
            UNIT_ASSERT_EQUAL(codec, ECodec::GZIP);
            UNIT_ASSERT(!level.has_value());
        }
    }
}

} // namespace NYql::NDq
