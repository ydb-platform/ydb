#include "grpc_service.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/service_auth.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcAuthService::SetServerOptions(const NYdbGrpc::TServerOptions& options) {
    // !!! WARN: The login request should be available without auth token !!!
    // Until we have only one rpc call in this service we can just disable auth
    // for service. In case of adding other rpc calls consider this and probably
    // implement switch via TRequestAuxSettings for particular call
    auto op = options;
    op.UseAuth = false;
    TBase::SetServerOptions(op);
}

void TGRpcAuthService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::Auth;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
    ReportSdkBuildInfo();

#ifdef SETUP_LOGIN_METHOD
#error SETUP_LOGIN_METHOD macro already defined
#endif

#define SETUP_LOGIN_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    SETUP_METHOD(methodName, methodCallback, rlMode, requestType, login, auditMode)

    SETUP_LOGIN_METHOD(Login, DoLoginRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Login));

#undef SETUP_LOGIN_METHOD
}

} // namespace NGRpcService
} // namespace NKikimr
