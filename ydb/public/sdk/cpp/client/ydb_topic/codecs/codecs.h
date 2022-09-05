#pragma once
#include <util/stream/output.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>


namespace NYdb::NTopic {
namespace NCompressionDetails {

extern TString Decompress(const Ydb::Topic::StreamReadMessage::ReadResponse::MessageData& data, Ydb::Topic::Codec codec);

THolder<IOutputStream> CreateCoder(ECodec codec, TBuffer& result, int quality);

} // namespace NDecompressionDetails

} // namespace NYdb::NTopic
