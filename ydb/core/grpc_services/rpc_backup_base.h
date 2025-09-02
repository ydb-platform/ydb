#pragma once

#include "rpc_operation_conv_base.h"

#include <ydb/core/protos/backup.pb.h>
#include <ydb/public/api/protos/draft/ydb_backup.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>
#include <ydb/public/sdk/cpp/src/library/operation_id/protos/operation_id.pb.h>

#include <util/string/cast.h>

namespace NKikimr::NGRpcService {

struct TIncrementalBackupConv: public TOperationConv<NKikimrBackup::TIncrementalBackup> {
    static Ydb::TOperationId MakeOperationId(const ui64 id) {
        Ydb::TOperationId operationId;
        operationId.SetKind(Ydb::TOperationId::INCREMENTAL_BACKUP);
        NOperationId::AddOptionalValue(operationId, "id", ToString(id));
        return operationId;
    }

    static Operation ToOperation(const NKikimrBackup::TIncrementalBackup& in) {
        auto operation = TOperationConv::ToOperation(in);

        if (operation.status() == Ydb::StatusIds::SUCCESS) {
            operation.set_ready(in.GetProgress() == Ydb::Backup::BackupProgress::PROGRESS_DONE);
        } else if (operation.status() != Ydb::StatusIds::CANCELLED) {
            return operation;
        }

        operation.set_id(NOperationId::ProtoToString(MakeOperationId(in.GetId())));
        Ydb::Backup::IncrementalBackupMetadata metadata;
        metadata.set_progress(in.GetProgress());
        metadata.set_progress_percent(in.GetProgressPercent());
        operation.mutable_metadata()->PackFrom(metadata);

        Ydb::Backup::IncrementalBackupResult result;
        operation.mutable_result()->PackFrom(result);

        return operation;
    }

}; // TBackupConv

} // namespace NKikimr::NGRpcService
