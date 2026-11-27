#pragma once

#include <ydb/core/protos/datashard_backup.pb.h>

#include <util/generic/maybe.h>

namespace NKikimr {

struct TRowVersion;

namespace NDataShard {

struct TS3Download {
    TMaybe<TString> DataETag;
    ui64 ProcessedBytes = 0;
    ui64 WrittenBytes = 0;
    ui64 WrittenRows = 0;
    NKikimrBackup::TChecksumState ProcessedChecksumState;
    NKikimrBackup::TS3DownloadState DownloadState; // Can hold secure encryption key

    bool HasWrittenAtVersion(const TRowVersion& version) const;
    void Out(IOutputStream& out) const;
};

} // namespace NDataShard
} // namespace NKikimr
