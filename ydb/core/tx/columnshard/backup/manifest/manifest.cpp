#include "manifest.h"

#include <ydb/core/backup/common/encryption.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

namespace NKikimr::NColumnShard::NBackup {

namespace {

template <class TTask>
void SetDataFileImpl(TTask& task, const ui32 fileIdx) {
    if (fileIdx == 0) {
        return;
    }
    auto& s3Settings = *task.MutableS3Settings();
    s3Settings.SetObjectKeyPattern(DataFileObjectKeyPattern(s3Settings.GetObjectKeyPattern(), task.GetShardNum(), fileIdx));
    if (task.HasEncryptionSettings() && task.GetEncryptionSettings().HasIV()) {
        auto& encryption = *task.MutableEncryptionSettings();
        encryption.SetIV(DataFileIV(encryption.GetIV(), fileIdx));
    }
}

}   // namespace

TString DataFileObjectKeyPattern(const TString& objectKeyPattern, const ui32 shardNum, const ui32 fileIdx) {
    if (fileIdx == 0) {
        return objectKeyPattern;
    }
    return TStringBuilder() << objectKeyPattern << Sprintf("/data_%02u_files/%06u", shardNum, fileIdx);
}

TString DataFileIV(const TString& baseIV, const ui32 fileIdx) {
    Y_ENSURE(fileIdx < MaxDataFileIndex);
    if (fileIdx == 0) {
        return baseIV;
    }
    // Metadata is the zero file type: only the shard number field changes
    const auto base = NKikimr::NBackup::TEncryptionIV::FromBinaryString(baseIV);
    return NKikimr::NBackup::TEncryptionIV::Combine(base, NKikimr::NBackup::EBackupFileType::Metadata, 0, fileIdx << 16).GetBinaryString();
}

void SetDataFile(NKikimrSchemeOp::TBackupTask& task, const ui32 fileIdx) {
    SetDataFileImpl(task, fileIdx);
}

void SetDataFile(NKikimrSchemeOp::TRestoreTask& task, const ui32 fileIdx) {
    SetDataFileImpl(task, fileIdx);
}

TString DataManifestKeySuffix(const ui32 shardNum) {
    return Sprintf("data_%02u.manifest.json", shardNum);
}

TString TDataManifest::Serialize() const {
    NJson::TJsonValue json;
    json["version"] = Version;
    json["files_count"] = FilesCount;
    return NJson::WriteJson(json, false);
}

TConclusion<TDataManifest> TDataManifest::Parse(TStringBuf data) {
    NJson::TJsonValue json;
    if (!NJson::ReadJsonTree(data, &json)) {
        return TConclusionStatus::Fail("data manifest is not a valid json");
    }
    unsigned long long version = 0;
    if (!json["version"].GetUInteger(&version)) {
        return TConclusionStatus::Fail("data manifest has no version");
    }
    if (version > CurrentVersion) {
        return TConclusionStatus::Fail(TStringBuilder() << "unsupported data manifest version: " << version);
    }
    unsigned long long filesCount = 0;
    if (!json["files_count"].GetUInteger(&filesCount) || filesCount == 0 || filesCount > Max<ui32>()) {
        return TConclusionStatus::Fail("data manifest has an invalid files_count");
    }
    TDataManifest result;
    result.Version = version;
    result.FilesCount = filesCount;
    return result;
}

}   // namespace NKikimr::NColumnShard::NBackup
