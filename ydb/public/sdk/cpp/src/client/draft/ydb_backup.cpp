#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_backup.h>

#include <ydb/public/api/protos/draft/ydb_backup.pb.h>

namespace NYdb::inline Dev::NBackup {

namespace {
    NBackup::EBackupProgress FromProto(Ydb::Backup::BackupProgress::Progress value) {
        switch (value) {
        case Ydb::Backup::BackupProgress::PROGRESS_UNSPECIFIED:
            return NBackup::EBackupProgress::Unspecified;
        case Ydb::Backup::BackupProgress::PROGRESS_PREPARING:
            return NBackup::EBackupProgress::Preparing;
        case Ydb::Backup::BackupProgress::PROGRESS_TRANSFER_DATA:
            return NBackup::EBackupProgress::TransferData;
        case Ydb::Backup::BackupProgress::PROGRESS_DONE:
            return NBackup::EBackupProgress::Done;
        case Ydb::Backup::BackupProgress::PROGRESS_CANCELLATION:
            return NBackup::EBackupProgress::Cancellation;
        case Ydb::Backup::BackupProgress::PROGRESS_CANCELLED:
            return NBackup::EBackupProgress::Cancelled;
        default:
            return NBackup::EBackupProgress::Unknown;
        }
    }

} // namespace anonymous

TIncrementalBackupResponse::TIncrementalBackupResponse(TStatus&& status, Ydb::Operations::Operation&& operation)
     : TOperation(std::move(status), std::move(operation))
{
    Ydb::Backup::IncrementalBackupMetadata metadata;
    GetProto().metadata().UnpackTo(&metadata);

    Metadata_.Progress = FromProto(metadata.progress());
    Metadata_.ProgressPercent = metadata.progress_percent();
}

const TIncrementalBackupResponse::TMetadata& TIncrementalBackupResponse::Metadata() const {
    return Metadata_;
}

} // namespace NYdb::inline Dev::NBackup
