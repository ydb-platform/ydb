#include "scheme.h"

namespace NYdb::NScheme {

grpc::Status TSchemeDummyService::DescribeSchemeObject(
    grpc::ServerContext* context,
    const Ydb::Scheme::DescribeSchemeObjectRequest* request,
    Ydb::Scheme::DescribeSchemeObjectResponse* response
) {
    Y_UNUSED(context);
    Y_UNUSED(request);

    auto* op = response->mutable_operation();
    op->set_ready(true);
    op->set_status(Ydb::StatusIds::SUCCESS);

    op->mutable_result()->PackFrom(DummySchemeObjectDescription);

    return grpc::Status::OK;
}

}
