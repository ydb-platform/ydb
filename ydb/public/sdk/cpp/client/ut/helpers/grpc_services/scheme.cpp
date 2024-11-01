#include "scheme.h"

namespace NYdb::NScheme {

grpc::Status TSchemeDummyService::DescribePath(
    grpc::ServerContext* context,
    const Ydb::Scheme::DescribePathRequest* request,
    Ydb::Scheme::DescribePathResponse* response
) {
    Y_UNUSED(context);
    Y_UNUSED(request);

    auto* op = response->mutable_operation();
    op->set_ready(true);
    op->set_status(Ydb::StatusIds::SUCCESS);

    op->mutable_result()->PackFrom(DummyPathDescription);

    return grpc::Status::OK;
}

}
