#pragma once

#include <ydb/public/api/grpc/draft/ydb_view_v1.grpc.pb.h>
#include <ydb-cpp-sdk/type_switcher.h>

namespace NYdb::inline V3::NView {

constexpr const char* DummyQueryText = "select 42";

class TViewDummyService : public NYdbProtos::View::V1::ViewService::Service
{
public:
    grpc::Status DescribeView(
        grpc::ServerContext* context,
        const NYdbProtos::View::DescribeViewRequest* request,
        NYdbProtos::View::DescribeViewResponse* response) override;
};

}
