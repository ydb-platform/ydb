#pragma once

#include <ydb/public/api/grpc/ydb_scheme_v1.grpc.pb.h>

namespace NYdb::NScheme {

class TSchemeDummyService : public Ydb::Scheme::V1::SchemeService::Service {
    Ydb::Scheme::DescribePathResult DummyPathDescription;

public:
    TSchemeDummyService(const Ydb::Scheme::DescribePathResult& dummyPathDescription)
        : DummyPathDescription(dummyPathDescription)
    {
    }

    grpc::Status DescribePath(
        grpc::ServerContext* context,
        const Ydb::Scheme::DescribePathRequest* request,
        Ydb::Scheme::DescribePathResponse* response) override;
};

}
