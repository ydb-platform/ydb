#include "ydb_logstore.h"

#include <ydb/core/grpc_services/service_logstore.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr::NGRpcService {

void TGRpcYdbLogStoreService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::LogStore;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef SETUP_LOGSTORE_METHOD
#error SETUP_LOGSTORE_METHOD macro already defined
#endif

#define SETUP_LOGSTORE_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    SETUP_METHOD(methodName, methodCallback, rlMode, requestType, logstore, auditMode)

    SETUP_LOGSTORE_METHOD(CreateLogStore, DoCreateLogStoreRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_LOGSTORE_METHOD(DescribeLogStore, DoDescribeLogStoreRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_LOGSTORE_METHOD(DropLogStore, DoDropLogStoreRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_LOGSTORE_METHOD(AlterLogStore, DoAlterLogStoreRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));

    SETUP_LOGSTORE_METHOD(CreateLogTable, DoCreateLogTableRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_LOGSTORE_METHOD(DescribeLogTable, DoDescribeLogTableRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_LOGSTORE_METHOD(DropLogTable, DoDropLogTableRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_LOGSTORE_METHOD(AlterLogTable, DoAlterLogTableRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));

#undef SETUP_LOGSTORE_METHOD
}

}
