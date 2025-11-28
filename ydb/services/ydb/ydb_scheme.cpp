#include "ydb_scheme.h"

#include <ydb/core/grpc_services/grpc_helper.h>

#include <ydb/core/grpc_services/service_scheme.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcYdbSchemeService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::Scheme;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef SETUP_SCHEME_METHOD
#error SETUP_SCHEME_METHOD macro already defined
#endif

#define SETUP_SCHEME_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    SETUP_METHOD(methodName, methodCallback, rlMode, requestType, scheme, auditMode)

    SETUP_SCHEME_METHOD(MakeDirectory, DoMakeDirectoryRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_SCHEME_METHOD(RemoveDirectory, DoRemoveDirectoryRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_SCHEME_METHOD(ListDirectory, DoListDirectoryRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_SCHEME_METHOD(DescribePath, DoDescribePathRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_SCHEME_METHOD(ModifyPermissions, DoModifyPermissionsRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Acl));

#undef SETUP_SCHEME_METHOD
}

} // namespace NGRpcService
} // namespace NKikimr
