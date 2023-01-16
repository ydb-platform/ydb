#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/persqueue/codecs/pqv1.h>

Y_UNIT_TEST_SUITE(PersQueueCodecs) {
    Y_UNIT_TEST(ToV1Codec) {
        {
            NPersQueueCommon::ECodec codec = NPersQueueCommon::ECodec::RAW;
            Ydb::PersQueue::V1::Codec got = NKikimr::NPQ::ToV1Codec(codec);
            UNIT_ASSERT_EQUAL(got, Ydb::PersQueue::V1::Codec::CODEC_RAW);
        }
        {
            NPersQueueCommon::ECodec codec = NPersQueueCommon::ECodec::GZIP;
            Ydb::PersQueue::V1::Codec got = NKikimr::NPQ::ToV1Codec(codec);
            UNIT_ASSERT_EQUAL(got, Ydb::PersQueue::V1::Codec::CODEC_GZIP);
        }
        {
            NPersQueueCommon::ECodec codec = NPersQueueCommon::ECodec::LZOP;
            Ydb::PersQueue::V1::Codec got = NKikimr::NPQ::ToV1Codec(codec);
            UNIT_ASSERT_EQUAL(got, Ydb::PersQueue::V1::Codec::CODEC_LZOP);
        }
        {
            NPersQueueCommon::ECodec codec = NPersQueueCommon::ECodec::ZSTD;
            Ydb::PersQueue::V1::Codec got = NKikimr::NPQ::ToV1Codec(codec);
            UNIT_ASSERT_EQUAL(got, Ydb::PersQueue::V1::Codec::CODEC_ZSTD);
        }
        {
            NPersQueueCommon::ECodec codec = NPersQueueCommon::ECodec::DEFAULT;
            Ydb::PersQueue::V1::Codec got = NKikimr::NPQ::ToV1Codec(codec);
            UNIT_ASSERT_EQUAL(got, Ydb::PersQueue::V1::Codec::CODEC_UNSPECIFIED);
        }
    }

    Y_UNIT_TEST(FromV1Codec) {
        {
            NYdb::NPersQueue::ECodec codec = NYdb::NPersQueue::ECodec::RAW;
            std::optional<NPersQueueCommon::ECodec> got = NKikimr::NPQ::FromV1Codec(codec);
            UNIT_ASSERT_VALUES_EQUAL(got.value(), NPersQueueCommon::ECodec::RAW);
        }
        {
            NYdb::NPersQueue::ECodec codec = NYdb::NPersQueue::ECodec::GZIP;
            std::optional<NPersQueueCommon::ECodec> got = NKikimr::NPQ::FromV1Codec(codec);
            UNIT_ASSERT_VALUES_EQUAL(got.value(), NPersQueueCommon::ECodec::GZIP);
        }
        {
            NYdb::NPersQueue::ECodec codec = NYdb::NPersQueue::ECodec::LZOP;
            std::optional<NPersQueueCommon::ECodec> got = NKikimr::NPQ::FromV1Codec(codec);
            UNIT_ASSERT_VALUES_EQUAL(got.value(), NPersQueueCommon::ECodec::LZOP);
        }
        {
            NYdb::NPersQueue::ECodec codec = NYdb::NPersQueue::ECodec::ZSTD;
            std::optional<NPersQueueCommon::ECodec> got = NKikimr::NPQ::FromV1Codec(codec);
            UNIT_ASSERT_VALUES_EQUAL(got.value(), NPersQueueCommon::ECodec::ZSTD);
        }
        {
            NYdb::NPersQueue::ECodec codec = NYdb::NPersQueue::ECodec(42);
            std::optional<NPersQueueCommon::ECodec> got = NKikimr::NPQ::FromV1Codec(codec);
            UNIT_ASSERT_EQUAL(got, std::nullopt);
        }
    }
}
