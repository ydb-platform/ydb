#include "view.h"

namespace NYdb::inline Dev::NView {

grpc::Status TViewDummyService::DescribeView(
    [[maybe_unused]] grpc::ServerContext* context,
    [[maybe_unused]] const Ydb::View::DescribeViewRequest* request,
    Ydb::View::DescribeViewResponse* response
) {
    auto* op = response->mutable_operation();
    op->set_ready(true);
    op->set_status(Ydb::StatusIds::SUCCESS);
    Ydb::View::DescribeViewResult describeResult;
    describeResult.set_query_text(DummyQueryText);
    op->mutable_result()->PackFrom(describeResult);
    return grpc::Status::OK;
}

}
