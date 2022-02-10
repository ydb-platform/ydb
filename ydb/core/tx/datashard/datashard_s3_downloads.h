#pragma once 
 
#include "datashard.h" 
 
#include <ydb/core/tablet_flat/flat_cxx_database.h>
 
#include <util/generic/hash.h> 
 
namespace NKikimr { 
namespace NDataShard {
 
class TS3DownloadsManager { 
    using TInfo = TEvDataShard::TEvS3DownloadInfo::TInfo; 
 
public: 
    bool Load(NIceDb::TNiceDb& db); 
    void Reset(); 
 
    const TInfo* Find(ui64 txId) const; 
    const TInfo& Store(NIceDb::TNiceDb& db, const TEvDataShard::TEvStoreS3DownloadInfo& msg); 
 
private: 
    THashMap<ui64, TInfo> Downloads; 
 
}; // TS3DownloadsManager 
 
}   // namespace NDataShard
}   // namespace NKikimr 
