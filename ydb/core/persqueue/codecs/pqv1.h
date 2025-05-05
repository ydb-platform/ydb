#pragma once

#include <ydb/public/api/protos/draft/persqueue_common.pb.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/persqueue.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

namespace NKikimr::NPQ {

Ydb::PersQueue::V1::Codec ToV1Codec(const NPersQueueCommon::ECodec codec);
std::optional<NPersQueueCommon::ECodec> FromV1Codec(const NYdb::NPersQueue::ECodec codec);

i32 FromTopicCodec(const NYdb::NTopic::ECodec codec);

} // NKikimr::NPQ
