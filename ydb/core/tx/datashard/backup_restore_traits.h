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

inline TString SchemeKey(const TString& objKeyPattern) {
    return Sprintf("%s/scheme.pb", objKeyPattern.c_str());
}

inline TString PermissionsKey(const TString& objKeyPattern) {
    return Sprintf("%s/permissions.pb", objKeyPattern.c_str());
}

inline TString MetadataKey(const TString& objKeyPattern) {
    return Sprintf("%s/metadata.json", objKeyPattern.c_str());
}

inline TString DataKey(const TString& objKeyPattern, ui32 n, EDataFormat format, ECompressionCodec codec) {
    const auto ext = DataFileExtension(format, codec);
    return Sprintf("%s/data_%02d%s", objKeyPattern.c_str(), n, ext.c_str());
}

} // NBackupRestoreTraits
} // NDataShard
} // NKikimr
