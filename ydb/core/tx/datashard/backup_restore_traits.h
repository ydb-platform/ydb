#pragma once

#include <util/generic/string.h>
#include <util/string/printf.h>

namespace NKikimrSchemeOp {
    class TBackupTask;
}

namespace NKikimr {
namespace NDataShard {
namespace NBackupRestoreTraits {

enum class EDataFormat: int {
    Invalid /* "invalid" */,
    Csv /* "csv" */,
};

enum class ECompressionCodec: int {
    Invalid /* "invalid" */,
    None /* "none" */,
    Zstd /* "zstd" */,
};

bool TryCodecFromTask(const NKikimrSchemeOp::TBackupTask& task, ECompressionCodec& codec);
ECompressionCodec CodecFromTask(const NKikimrSchemeOp::TBackupTask& task);

EDataFormat NextDataFormat(EDataFormat cur);
ECompressionCodec NextCompressionCodec(ECompressionCodec cur);

TString DataFileExtension(EDataFormat format, ECompressionCodec codec);

TString PermissionsKeySuffix();
TString SchemeKeySuffix();
TString MetadataKeySuffix();
TString DataKeySuffix(ui32 n, EDataFormat format, ECompressionCodec codec);

TString ChecksumKey(const TString& objKey);

} // NBackupRestoreTraits
} // NDataShard
} // NKikimr
