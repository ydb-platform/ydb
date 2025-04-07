#include "scripting.h"

namespace NYdb::inline Dev::NScripting {

grpc::Status TMockSlyDbProxy::ExecuteYql(
    grpc::ServerContext* context,
    [[maybe_unused]] const Ydb::Scripting::ExecuteYqlRequest* request,
    Ydb::Scripting::ExecuteYqlResponse* response
) {
    context->AddInitialMetadata("key", "value");

    // Just to make sdk core happy
    auto* op = response->mutable_operation();
    op->set_ready(true);
    op->set_status(Ydb::StatusIds::SUCCESS);
    op->mutable_result();

    return grpc::Status::OK;
}

}
