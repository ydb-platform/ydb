#include "ydb_import.h"

#include <ydb/core/grpc_services/service_import.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcYdbImportService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::Import;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef SETUP_IMPORT_METHOD
#error SETUP_IMPORT_METHOD macro already defined
#endif

#define SETUP_IMPORT_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    SETUP_METHOD(methodName, methodCallback, rlMode, requestType, import, auditMode)

    SETUP_IMPORT_METHOD(ImportFromS3, DoImportFromS3Request, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ExportImport));
    SETUP_IMPORT_METHOD(ListObjectsInS3Export, DoListObjectsInS3ExportRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_IMPORT_METHOD(ImportData, DoImportDataRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ExportImport));

#undef SETUP_IMPORT_METHOD
}

} // namespace NGRpcService
} // namespace NKikimr
