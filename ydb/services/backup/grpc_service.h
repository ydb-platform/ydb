#pragma once

#include <ydb/public/api/grpc/draft/ydb_backup_v1.grpc.pb.h>
#include <ydb/core/grpc_services/base/base_service.h>

namespace NKikimr {
namespace NGRpcService {

 class TGRpcBackupService : public TGrpcServiceBase<Ydb::Backup::V1::BackupService>
 {
     using TBase = TGrpcServiceBase<Ydb::Backup::V1::BackupService>;
 public:
     using TGrpcServiceBase<Ydb::Backup::V1::BackupService>::TGrpcServiceBase;

 private:
     void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) override;
 };

} // namespace NGRpcService
} // namespace NKikimr
