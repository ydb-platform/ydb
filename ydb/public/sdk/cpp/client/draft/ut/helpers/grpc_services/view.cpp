#include "view.h"

namespace NYdb::NView {

grpc::Status TViewDummyService::DescribeView(
    grpc::ServerContext* context,
    const Ydb::View::DescribeViewRequest* request,
    Ydb::View::DescribeViewResponse* response
) {
    Y_UNUSED(context);
    Y_UNUSED(request);

    auto* op = response->mutable_operation();
    op->set_ready(true);
    op->set_status(Ydb::StatusIds::SUCCESS);

    Ydb::View::DescribeViewResult describeResult;
    describeResult.set_query_text(DummyQueryText);
    op->mutable_result()->PackFrom(describeResult);

    return grpc::Status::OK;
}

}
