#pragma once

#include <ydb/public/api/client/yc_private/iam/operation_service.grpc.pb.h>

#include <util/generic/hash.h>
#include <util/system/mutex.h>

#include <atomic>

// Mock of yandex.cloud.priv.iam.v1.OperationService: an operation becomes done after GetsUntilDone calls of Get.
class TOperationServiceMock : public yandex::cloud::priv::iam::v1::OperationService::Service {
public:
    TMutex Mutex;
    ui32 GetsUntilDone = 1;
    std::atomic<ui32> GetCalls = 0;
    // when set, done operations carry this error
    TString OperationError;
    THashMap<TString, ui32> GetsByOperation;

    grpc::Status Get(grpc::ServerContext*,
                     const yandex::cloud::priv::iam::v1::GetOperationRequest* request,
                     ydb::yc::priv::operation::Operation* response) override
    {
        with_lock (Mutex) {
            ++GetCalls;
            const ui32 gets = ++GetsByOperation[request->operation_id()];
            response->set_id(request->operation_id());
            if (gets >= GetsUntilDone) {
                response->set_done(true);
                if (!OperationError.empty()) {
                    response->mutable_error()->set_code(grpc::StatusCode::PERMISSION_DENIED);
                    response->mutable_error()->set_message(OperationError);
                }
            } else {
                response->set_done(false);
            }
            return grpc::Status::OK;
        }
    }
};
