#include "grpc_service.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/service_backup.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcBackupService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::Backup;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef SETUP_BACKUP_METHOD
#error SETUP_BACKUP_METHOD macro already defined
#endif

#define SETUP_BACKUP_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    SETUP_METHOD(methodName, methodCallback, rlMode, requestType, backup, auditMode)

    SETUP_BACKUP_METHOD(FetchBackupCollections, DoFetchBackupCollectionsRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_BACKUP_METHOD(ListBackupCollections, DoListBackupCollectionsRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_BACKUP_METHOD(CreateBackupCollection, DoCreateBackupCollectionRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::DatabaseAdmin));
    SETUP_BACKUP_METHOD(ReadBackupCollection, DoReadBackupCollectionRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_BACKUP_METHOD(UpdateBackupCollection, DoUpdateBackupCollectionRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::DatabaseAdmin));
    SETUP_BACKUP_METHOD(DeleteBackupCollection, DoDeleteBackupCollectionRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::DatabaseAdmin));

#undef SETUP_BACKUP_METHOD
}

} // namespace NGRpcService
} // namespace NKikimr
