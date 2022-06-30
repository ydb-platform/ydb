#pragma once

#include <ydb/public/api/protos/ydb_persqueue_v1.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>

namespace NKikimr::NGRpcProxy::V1 {

using namespace Ydb;

bool RemoveEmptyMessages(PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch& data);

bool RemoveEmptyMessages(Topic::StreamReadMessage::ReadResponse& data);

}
