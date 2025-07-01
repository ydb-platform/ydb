#pragma once

#include <ydb/core/protos/datashard_backup.pb.h>

#include <util/generic/maybe.h>

namespace NKikimr {
namespace NDataShard {

struct TS3Download {
    TMaybe<TString> DataETag;
    ui64 ProcessedBytes = 0;
    ui64 WrittenBytes = 0;
    ui64 WrittenRows = 0;
    NKikimrBackup::TChecksumState ChecksumState;
    NKikimrBackup::TS3DownloadState DownloadState; // Can hold secure encryption key

    void Out(IOutputStream& out) const;
};

} // namespace NDataShard
} // namespace NKikimr
