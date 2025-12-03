#include "ydb_export.h"

#include <ydb/core/grpc_services/service_export.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcYdbExportService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::Export;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef SETUP_EXPORT_METHOD
#error SETUP_EXPORT_METHOD macro already defined
#endif

#define SETUP_EXPORT_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    SETUP_METHOD(methodName, methodCallback, rlMode, requestType, export, auditMode)

    SETUP_EXPORT_METHOD(ExportToYt, DoExportToYtRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ExportImport));
    SETUP_EXPORT_METHOD(ExportToS3, DoExportToS3Request, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ExportImport));
    SETUP_EXPORT_METHOD(ExportToFs, DoExportToFsRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ExportImport));

#undef SETUP_EXPORT_METHOD
}

} // namespace NGRpcService
} // namespace NKikimr
