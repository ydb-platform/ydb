#include "grpc_service.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/service_view.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr::NGRpcService {

void TGRpcViewService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::View;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef SETUP_VIEW_METHOD
#error SETUP_VIEW_METHOD macro already defined
#endif

#define SETUP_VIEW_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    SETUP_METHOD(methodName, methodCallback, rlMode, requestType, view, auditMode)

    SETUP_VIEW_METHOD(DescribeView, DoDescribeView, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying());

#undef SETUP_VIEW_METHOD
}

}
