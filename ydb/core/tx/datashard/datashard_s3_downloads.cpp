#include "datashard_s3_downloads.h"
#include "datashard_impl.h"

namespace NKikimr {
namespace NDataShard {

bool TS3DownloadsManager::Load(NIceDb::TNiceDb& db) {
    using Schema = TDataShard::Schema;

    bool ready = true;
    auto rowset = db.Table<Schema::S3Downloads>().Range().Select();
    if (rowset.IsReady()) {
        while (!rowset.EndOfSet()) {
            ui64 txId = rowset.GetValue<Schema::S3Downloads::TxId>();

            Y_ENSURE(!Downloads.contains(txId), "Unexpected duplicate s3 download: " << txId);
            auto& info = Downloads[txId];

            if (rowset.HaveValue<Schema::S3Downloads::DataETag>()) {
                info.DataETag = rowset.GetValue<Schema::S3Downloads::DataETag>();
            }

            info.ProcessedBytes = rowset.GetValueOrDefault<Schema::S3Downloads::ProcessedBytes>(0);
            info.WrittenBytes = rowset.GetValueOrDefault<Schema::S3Downloads::WrittenBytes>(0);
            info.WrittenRows = rowset.GetValueOrDefault<Schema::S3Downloads::WrittenRows>(0);

            if (rowset.HaveValue<Schema::S3Downloads::ChecksumState>()) {
                info.ChecksumState = rowset.GetValue<Schema::S3Downloads::ChecksumState>();
            }

            if (rowset.HaveValue<Schema::S3Downloads::DownloadState>()) {
                info.DownloadState = rowset.GetValue<Schema::S3Downloads::DownloadState>();
            }

            if (!rowset.Next()) {
                ready = false;
                break;
            }
        }
    } else {
        ready = false;
    }

    return ready;
}

void TS3DownloadsManager::Reset() {
    Downloads.clear();
}

const TS3Download* TS3DownloadsManager::Find(ui64 txId) const {
    return Downloads.FindPtr(txId);
}

const TS3Download& TS3DownloadsManager::Store(NIceDb::TNiceDb& db, ui64 txId, const TS3Download& newInfo) {
    auto& info = Downloads[txId];

    Y_ENSURE(newInfo.DataETag);
    Y_ENSURE(info.DataETag.GetOrElse(*newInfo.DataETag) == *newInfo.DataETag);
    info = newInfo;

    using Schema = TDataShard::Schema;
    db.Table<Schema::S3Downloads>().Key(txId).Update(
        NIceDb::TUpdate<Schema::S3Downloads::DataETag>(*newInfo.DataETag),
        NIceDb::TUpdate<Schema::S3Downloads::ProcessedBytes>(newInfo.ProcessedBytes),
        NIceDb::TUpdate<Schema::S3Downloads::WrittenBytes>(newInfo.WrittenBytes),
        NIceDb::TUpdate<Schema::S3Downloads::WrittenRows>(newInfo.WrittenRows),
        NIceDb::TUpdate<Schema::S3Downloads::ChecksumState>(newInfo.ChecksumState),
        NIceDb::TUpdate<Schema::S3Downloads::DownloadState>(newInfo.DownloadState)
    );

    return info;
}

}   // namespace NDataShard
}   // namespace NKikimr
