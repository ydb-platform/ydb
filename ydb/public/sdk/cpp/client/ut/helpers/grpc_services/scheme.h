#pragma once

#include <ydb/public/api/grpc/ydb_scheme_v1.grpc.pb.h>

namespace NYdb::NScheme {

class TSchemeDummyService : public Ydb::Scheme::V1::SchemeService::Service {
    Ydb::Scheme::DescribeSchemeObjectResult DummySchemeObjectDescription;

public:
    TSchemeDummyService(const Ydb::Scheme::DescribeSchemeObjectResult& dummySchemeObjectDescription)
        : DummySchemeObjectDescription(dummySchemeObjectDescription)
    {
    }

    grpc::Status DescribeSchemeObject(
        grpc::ServerContext* context,
        const Ydb::Scheme::DescribeSchemeObjectRequest* request,
        Ydb::Scheme::DescribeSchemeObjectResponse* response) override;
};

}
