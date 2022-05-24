#pragma once

#include <ydb/public/api/protos/ydb_persqueue_v1.pb.h>

namespace NKikimr::NGRpcProxy::V1 {

using namespace Ydb;

bool RemoveEmptyMessages(PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch& data);

// TODO: remove after grpc refactor
bool RemoveEmptyMessages(PersQueue::V1::StreamingReadServerMessage::ReadResponse& data);

}
