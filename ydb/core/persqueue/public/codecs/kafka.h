#pragma once

#include <ydb/public/api/protos/draft/persqueue_common.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>

namespace NKikimr::NPQ {

inline NPersQueueCommon::ECodec KafkaBatchCodec() {
    return static_cast<NPersQueueCommon::ECodec>(static_cast<int>(Ydb::Topic::CODEC_KAFKA_BATCH) - 1);
}

} // namespace NKikimr::NPQ
