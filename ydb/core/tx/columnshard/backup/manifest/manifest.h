#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>

#include <ydb/library/conclusion/result.h>

#include <util/generic/string.h>

namespace NKikimr::NColumnShard::NBackup {

// The export data of a shard can be split into several data files. The first one is data_NN at the backup item prefix,
// data file KKKKKK > 0 is data_NN at the prefix data_NN_files/KKKKKK of the item. The datashard uploader and downloader
// work with a single data file of a shard, so the task of a data file gets the prefix and the encryption IV of the file.
// The uploader of a data file of shard 0 also writes the item files (scheme, metadata, ...) at the prefix of the data file:
// only the ones at the item prefix are used.
// The encryption IV of a data file has the file index in the high half of the shard number
constexpr ui32 MaxDataFileShardNumber = 1 << 16;
constexpr ui32 MaxDataFileIndex = 1 << 16;

TString DataFileObjectKeyPattern(const TString& objectKeyPattern, ui32 shardNum, ui32 fileIdx);
// The base IV of a data file. The data file IV made from it by the datashard, Combine(result, TableData, 0, shardNumber),
// differs from the IVs of all other files of the backup item for shardNumber < MaxDataFileShardNumber.
// The first data file keeps the base IV.
TString DataFileIV(const TString& baseIV, ui32 fileIdx);
void SetDataFile(NKikimrSchemeOp::TBackupTask& task, ui32 fileIdx);
void SetDataFile(NKikimrSchemeOp::TRestoreTask& task, ui32 fileIdx);

TString DataManifestKeySuffix(ui32 shardNum);

// Lists the data files of a shard whose export data is split into several files.
// Without a manifest the shard has a single data_NN file.
struct TDataManifest {
    static constexpr ui32 CurrentVersion = 1;

    ui32 Version = CurrentVersion;
    ui32 FilesCount = 0;

    TString Serialize() const;
    static TConclusion<TDataManifest> Parse(TStringBuf data);
};

}   // namespace NKikimr::NColumnShard::NBackup
