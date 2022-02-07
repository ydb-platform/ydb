#pragma once

#include <ydb/public/api/protos/draft/persqueue_common.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/persqueue.h>

namespace NKikimr::NPQ {

Ydb::PersQueue::V1::Codec ToV1Codec(const NPersQueueCommon::ECodec codec);
std::optional<NPersQueueCommon::ECodec> FromV1Codec(const NYdb::NPersQueue::ECodec codec);

} // NKikimr::NPQ
