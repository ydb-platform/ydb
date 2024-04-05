#include "storage.h"
#include <ydb/core/tx/columnshard/export/session/storage/s3/storage.h>
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr::NOlap::NExport {

NKikimr::TConclusion<TStorageInitializerContainer> TStorageInitializerContainer::BuildFromProto(const NKikimrTxColumnShard::TBackupTxBody& proto) {
    if (!proto.GetBackupTask().HasS3Settings()) {
        return TConclusionStatus::Fail("s3 settings not found in backup task");
    }
    return TStorageInitializerContainer(std::make_shared<TS3StorageInitializer>("BACKUP", proto.GetBackupTask().GetS3Settings()));
}

}