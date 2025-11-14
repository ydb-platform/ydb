#include "grpc_service.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/grpc_services/service_replication.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr::NGRpcService {

void TGRpcReplicationService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::Replication;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef SETUP_REPLICATION_METHOD
#error SETUP_REPLICATION_METHOD macro already defined
#endif

#define SETUP_REPLICATION_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    SETUP_METHOD(methodName, methodCallback, rlMode, requestType, replication, auditMode)

    SETUP_REPLICATION_METHOD(DescribeReplication, DoDescribeReplication, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_REPLICATION_METHOD(DescribeTransfer, DoDescribeTransfer, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying());

#undef SETUP_REPLICATION_METHOD
}

}
