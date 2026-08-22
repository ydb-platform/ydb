#pragma once

#include "ydb_replication.h"
#include "ydb_view.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <ydb/public/api/protos/draft/ydb_replication.pb.h>
#include <ydb/public/api/protos/draft/ydb_view.pb.h>

namespace NYdb::inline Dev::NDraft {

//! Provides access to raw protobuf values of YDB API entities. It is not recommended to use this
//! class in client applications as it add dependency on API protobuf format which is subject to
//! change. Use functionality provided by YDB SDK classes.
class TProtoAccessor : public NYdb::TProtoAccessor {
public:
    using NYdb::TProtoAccessor::GetProto;
    using NYdb::TProtoAccessor::GetProtoMap;
    using NYdb::TProtoAccessor::GetProtoMapPtr;
    using NYdb::TProtoAccessor::FromProto;

    static const Ydb::Replication::DescribeReplicationResult& GetProto(const NYdb::NReplication::TDescribeReplicationResult& desc);
    static const Ydb::Replication::DescribeTransferResult& GetProto(const NYdb::NReplication::TDescribeTransferResult& desc);
    static const Ydb::View::DescribeViewResult& GetProto(const NYdb::NView::TDescribeViewResult& desc);
};

} // namespace NYdb
