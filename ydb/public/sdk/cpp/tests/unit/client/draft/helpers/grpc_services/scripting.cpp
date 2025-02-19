#include "scripting.h"

namespace NYdb::inline V3::NScripting {

grpc::Status TMockSlyDbProxy::ExecuteYql(
    grpc::ServerContext* context,
    [[maybe_unused]] const NYdbProtos::Scripting::ExecuteYqlRequest* request,
    NYdbProtos::Scripting::ExecuteYqlResponse* response
) {
    context->AddInitialMetadata("key", "value");

    // Just to make sdk core happy
    auto* op = response->mutable_operation();
    op->set_ready(true);
    op->set_status(NYdbProtos::StatusIds::SUCCESS);
    op->mutable_result();

    return grpc::Status::OK;
}

}
