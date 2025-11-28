#include "ydb_object_storage.h"

#include <ydb/core/grpc_services/service_object_storage.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcYdbObjectStorageService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::ObjectStorage;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef SETUP_STORAGE_METHOD
#error SETUP_STORAGE_METHOD macro already defined
#endif

#define SETUP_STORAGE_METHOD(methodName, inputType, outputType, methodCallback, rlMode, requestType, auditMode) \
    SETUP_RUNTIME_EVENT_METHOD(                                         \
        methodName,                                                     \
        inputType,                                                      \
        outputType,                                                     \
        methodCallback,                                                 \
        rlMode,                                                         \
        requestType,                                                    \
        YDB_API_DEFAULT_COUNTER_BLOCK(object-storage-list, methodName), \
        auditMode,                                                      \
        COMMON,                                                         \
        TGrpcRequestNoOperationCall,                                    \
        GRpcRequestProxyId_,                                            \
        CQ_,                                                            \
        nullptr,                                                        \
        nullptr)

    SETUP_STORAGE_METHOD(List, ListingRequest, ListingResponse, DoObjectStorageListingRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying());

#undef SETUP_STORAGE_METHOD
}

} // namespace NGRpcService
} // namespace NKikimr
