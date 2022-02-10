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
 
            Y_VERIFY_S(!Downloads.contains(txId), "Unexpected duplicate s3 download: " << txId); 
            auto& info = Downloads[txId]; 
 
            if (rowset.HaveValue<Schema::S3Downloads::DataETag>()) { 
                info.DataETag = rowset.GetValue<Schema::S3Downloads::DataETag>(); 
            } 
 
            info.ProcessedBytes = rowset.GetValueOrDefault<Schema::S3Downloads::ProcessedBytes>(0); 
            info.WrittenBytes = rowset.GetValueOrDefault<Schema::S3Downloads::WrittenBytes>(0); 
            info.WrittenRows = rowset.GetValueOrDefault<Schema::S3Downloads::WrittenRows>(0); 
 
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
 
const TS3DownloadsManager::TInfo* TS3DownloadsManager::Find(ui64 txId) const { 
    return Downloads.FindPtr(txId); 
} 
 
const TS3DownloadsManager::TInfo& TS3DownloadsManager::Store(NIceDb::TNiceDb& db, const TEvDataShard::TEvStoreS3DownloadInfo& msg) { 
    auto& info = Downloads[msg.TxId]; 
 
    Y_VERIFY(info.DataETag.GetOrElse(msg.DataETag) == msg.DataETag); 
    info.DataETag = msg.DataETag; 
    info.ProcessedBytes = msg.ProcessedBytes; 
    info.WrittenBytes = msg.WrittenBytes; 
    info.WrittenRows = msg.WrittenRows; 
 
    using Schema = TDataShard::Schema;
    db.Table<Schema::S3Downloads>().Key(msg.TxId).Update( 
        NIceDb::TUpdate<Schema::S3Downloads::DataETag>(msg.DataETag), 
        NIceDb::TUpdate<Schema::S3Downloads::ProcessedBytes>(msg.ProcessedBytes), 
        NIceDb::TUpdate<Schema::S3Downloads::WrittenBytes>(msg.WrittenBytes), 
        NIceDb::TUpdate<Schema::S3Downloads::WrittenRows>(msg.WrittenRows)); 
 
    return info; 
} 
 
}   // namespace NDataShard
}   // namespace NKikimr 
