#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_backup.h>

#include <ydb/public/api/protos/draft/ydb_backup.pb.h>

#include <ydb/public/sdk/cpp/src/client/operation/impl.h>


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

    NBackup::ERestoreProgress FromProto(Ydb::Backup::RestoreProgress::Progress value) {
        switch (value) {
        case Ydb::Backup::RestoreProgress::PROGRESS_UNSPECIFIED:
            return NBackup::ERestoreProgress::Unspecified;
        case Ydb::Backup::RestoreProgress::PROGRESS_PREPARING:
            return NBackup::ERestoreProgress::Preparing;
        case Ydb::Backup::RestoreProgress::PROGRESS_TRANSFER_DATA:
            return NBackup::ERestoreProgress::TransferData;
        case Ydb::Backup::RestoreProgress::PROGRESS_DONE:
            return NBackup::ERestoreProgress::Done;
        case Ydb::Backup::RestoreProgress::PROGRESS_CANCELLATION:
            return NBackup::ERestoreProgress::Cancellation;
        case Ydb::Backup::RestoreProgress::PROGRESS_CANCELLED:
            return NBackup::ERestoreProgress::Cancelled;
        default:
            return NBackup::ERestoreProgress::Unknown;
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

TBackupCollectionRestoreResponse::TBackupCollectionRestoreResponse(TStatus&& status, Ydb::Operations::Operation&& operation)
     : TOperation(std::move(status), std::move(operation))
{
    Ydb::Backup::RestoreMetadata metadata;
    GetProto().metadata().UnpackTo(&metadata);

    Metadata_.Progress = FromProto(metadata.progress());
    Metadata_.ProgressPercent = metadata.progress_percent();
}

const TBackupCollectionRestoreResponse::TMetadata& TBackupCollectionRestoreResponse::Metadata() const {
    return Metadata_;
}

} // namespace NYdb::inline Dev::NBackup

namespace NYdb::inline Dev::NOperation {

template NThreading::TFuture<NBackup::TIncrementalBackupResponse> TOperationClient::Get(const TOperation::TOperationId& id);
template <>
NThreading::TFuture<TOperationsList<NBackup::TIncrementalBackupResponse>> TOperationClient::List(std::uint64_t pageSize, const std::string& pageToken) {
    return List<NBackup::TIncrementalBackupResponse>("incbackup", pageSize, pageToken);
}

template NThreading::TFuture<NBackup::TBackupCollectionRestoreResponse> TOperationClient::Get(const TOperation::TOperationId& id);
template <>
NThreading::TFuture<TOperationsList<NBackup::TBackupCollectionRestoreResponse>> TOperationClient::List(std::uint64_t pageSize, const std::string& pageToken) {
    return List<NBackup::TBackupCollectionRestoreResponse>("restore", pageSize, pageToken);
}

}
