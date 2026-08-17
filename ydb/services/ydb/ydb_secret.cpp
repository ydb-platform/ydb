#include "ydb_secret.h"

#include <ydb/core/grpc_services/grpc_helper.h>

#include <ydb/core/grpc_services/service_secret.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcYdbSecretService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::Secret;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#define SETUP_SECRET_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    SETUP_METHOD(methodName, methodCallback, rlMode, requestType, secret, auditMode, EEmptyDatabaseMode::EmptyDatabaseForbidden)

    SETUP_SECRET_METHOD(DescribeSecret, DoDescribeSecretRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying());

#undef SETUP_SECRET_METHOD
}

} // namespace NGRpcService
} // namespace NKikimr
