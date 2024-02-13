#pragma once

#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/public/api/protos/ydb_persqueue_v1.pb.h>

#include "rpc_calls.h"

namespace NKikimr::NGRpcService {

using TEvDropTopicRequest = TGrpcRequestOperationCall<Ydb::Topic::DropTopicRequest, Ydb::Topic::DropTopicResponse>; 
using TEvCreateTopicRequest = TGrpcRequestOperationCall<Ydb::Topic::CreateTopicRequest, Ydb::Topic::CreateTopicResponse>; 
using TEvAlterTopicRequest = TGrpcRequestOperationCall<Ydb::Topic::AlterTopicRequest, Ydb::Topic::AlterTopicResponse>;
using TEvDescribeTopicRequest = TGrpcRequestOperationCall<Ydb::Topic::DescribeTopicRequest, Ydb::Topic::DescribeTopicResponse>;
using TEvDescribeConsumerRequest = TGrpcRequestOperationCall<Ydb::Topic::DescribeConsumerRequest, Ydb::Topic::DescribeConsumerResponse>;
using TEvDescribePartitionRequest = TGrpcRequestOperationCall<Ydb::Topic::DescribePartitionRequest, Ydb::Topic::DescribePartitionResponse>;

using TEvPQDropTopicRequest = TGrpcRequestOperationCall<Ydb::PersQueue::V1::DropTopicRequest, Ydb::PersQueue::V1::DropTopicResponse>;
using TEvPQCreateTopicRequest = TGrpcRequestOperationCall<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>; 
using TEvPQAlterTopicRequest = TGrpcRequestOperationCall<Ydb::PersQueue::V1::AlterTopicRequest, Ydb::PersQueue::V1::AlterTopicResponse>; 
using TEvPQDescribeTopicRequest = TGrpcRequestOperationCall<Ydb::PersQueue::V1::DescribeTopicRequest, Ydb::PersQueue::V1::DescribeTopicResponse>;
using TEvPQAddReadRuleRequest = TGrpcRequestOperationCall<Ydb::PersQueue::V1::AddReadRuleRequest, Ydb::PersQueue::V1::AddReadRuleResponse>;
using TEvPQRemoveReadRuleRequest = TGrpcRequestOperationCall<Ydb::PersQueue::V1::RemoveReadRuleRequest, Ydb::PersQueue::V1::RemoveReadRuleResponse>;

}
