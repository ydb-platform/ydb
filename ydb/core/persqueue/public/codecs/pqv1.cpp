#include "pqv1.h"

namespace NKikimr::NPQ {

Ydb::PersQueue::V1::Codec ToV1Codec(const NPersQueueCommon::ECodec codec) {
    switch (codec) {
        case NPersQueueCommon::RAW:
            return Ydb::PersQueue::V1::CODEC_RAW;
        case NPersQueueCommon::GZIP:
            return Ydb::PersQueue::V1::CODEC_GZIP;
        case NPersQueueCommon::LZOP:
            return Ydb::PersQueue::V1::CODEC_LZOP;
        case NPersQueueCommon::ZSTD:
            return Ydb::PersQueue::V1::CODEC_ZSTD;
        default:
            return Ydb::PersQueue::V1::CODEC_UNSPECIFIED;
    }
}

std::optional<NPersQueueCommon::ECodec> FromV1Codec(const NYdb::NPersQueue::ECodec codec) {
    switch (codec) {
        case NYdb::NPersQueue::ECodec::RAW:
            return NPersQueueCommon::RAW;
        case NYdb::NPersQueue::ECodec::GZIP:
            return NPersQueueCommon::GZIP;
        case NYdb::NPersQueue::ECodec::LZOP:
            return NPersQueueCommon::LZOP;
        case NYdb::NPersQueue::ECodec::ZSTD:
            return NPersQueueCommon::ZSTD;
        default:
            return std::nullopt;
    }
}

i32 FromTopicCodec(const NYdb::NTopic::ECodec codec) {
    return (ui32)(codec) - 1;
}

} // NKikimr::NPQ
