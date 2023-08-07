#ifndef KIKIMR_DISABLE_S3_OPS

#include "export_s3_base_uploader.h"

#include "backup_restore_common.h"

namespace NKikimr {
namespace NDataShard {

class TS3Uploader: public TS3UploaderBase<TS3Uploader> {
protected:
    bool NeedToResolveProxy() const override {
        return false;
    }

    void ResolveProxy() override {
        Y_FAIL("unreachable");
    }

public:
    using TS3UploaderBase::TS3UploaderBase;

}; // TS3Uploader

IActor* TS3Export::CreateUploader(const TActorId& dataShard, ui64 txId) const {
    auto scheme = (Task.GetShardNum() == 0)
        ? GenYdbScheme(Columns, Task.GetTable())
        : Nothing();

    NBackupRestore::TMetadata metadata;

    NBackupRestore::TFullBackupMetadata::TPtr backup = new NBackupRestore::TFullBackupMetadata{
        .SnapshotVts = NBackupRestore::TVirtualTimestamp(
            Task.GetSnapshotStep(),
            Task.GetSnapshotTxId())
    };
    metadata.AddFullBackup(backup);

    return new TS3Uploader(
        dataShard, txId, Task, std::move(scheme), metadata.Serialize());
}

} // NDataShard
} // NKikimr

#endif // KIKIMR_DISABLE_S3_OPS
