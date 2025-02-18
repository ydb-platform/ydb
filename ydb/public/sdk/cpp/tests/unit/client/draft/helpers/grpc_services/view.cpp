#include "view.h"

namespace NYdb::inline V3::NView {

grpc::Status TViewDummyService::DescribeView(
    [[maybe_unused]] grpc::ServerContext* context,
    [[maybe_unused]] const NYdbProtos::View::DescribeViewRequest* request,
    NYdbProtos::View::DescribeViewResponse* response
) {
    auto* op = response->mutable_operation();
    op->set_ready(true);
    op->set_status(NYdbProtos::StatusIds::SUCCESS);
    NYdbProtos::View::DescribeViewResult describeResult;
    describeResult.set_query_text(DummyQueryText);
    op->mutable_result()->PackFrom(describeResult);
    return grpc::Status::OK;
}

}
