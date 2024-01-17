#pragma once

#include <ydb/public/api/protos/ydb_persqueue_v1.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/services/lib/sharding/sharding.h>

#include <util/generic/size_literals.h>

namespace NKikimr::NGRpcProxy::V1 {

static constexpr ui64 READ_BLOCK_SIZE = 8_KB; // metering

using namespace Ydb;

bool RemoveEmptyMessages(PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch& data);

bool RemoveEmptyMessages(Topic::StreamReadMessage::ReadResponse& data);

}
