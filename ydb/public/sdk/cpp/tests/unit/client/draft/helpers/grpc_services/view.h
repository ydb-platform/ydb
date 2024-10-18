#pragma once

#include <ydb/public/api/grpc/draft/ydb_view_v1.grpc.pb.h>

namespace NYdb::NView {

constexpr const char* DummyQueryText = "select 42";

class TViewDummyService : public Ydb::View::V1::ViewService::Service
{
public:
    grpc::Status DescribeView(
        grpc::ServerContext* context,
        const Ydb::View::DescribeViewRequest* request,
        Ydb::View::DescribeViewResponse* response) override;
};

}
