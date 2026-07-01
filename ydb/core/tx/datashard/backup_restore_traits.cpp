#include "backup_restore_traits.h"

#include <ydb/core/protos/data_format_settings.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/fs_settings.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <util/generic/hash.h>
#include <util/string/cast.h>

namespace NKikimr {
namespace NDataShard {
namespace NBackupRestoreTraits {

namespace {

EDataFormat DataFormatFromExportData(const NKikimrSchemeOp::TExportDataSettings& exportData) {
    switch (exportData.GetFormatCase()) {
    case NKikimrSchemeOp::TExportDataSettings::FORMAT_NOT_SET:
    case NKikimrSchemeOp::TExportDataSettings::kYdbDump:
        return EDataFormat::YdbDump;
    case NKikimrSchemeOp::TExportDataSettings::kParquet:
        return EDataFormat::Parquet;
    }
    return EDataFormat::Invalid;
}

} // namespace

bool TryCodecFromTask(const NKikimrSchemeOp::TBackupTask& task, ECompressionCodec& codec) {
    if (!task.HasCompression()) {
        codec = ECompressionCodec::None;
        return true;
    }

    if (!TryFromString<ECompressionCodec>(task.GetCompression().GetCodec(), codec)) {
        return false;
    }

    if (codec == ECompressionCodec::Invalid) {
        return false;
    }

    return true;
}

ECompressionCodec CodecFromTask(const NKikimrSchemeOp::TBackupTask& task) {
    ECompressionCodec codec;
    Y_ENSURE(TryCodecFromTask(task, codec));
    return codec;
}

EDataFormat DataFormatFromTask(const NKikimrSchemeOp::TBackupTask& task) {
    switch (task.GetSettingsCase()) {
    case NKikimrSchemeOp::TBackupTask::kS3Settings:
        return DataFormatFromExportData(task.GetS3Settings().GetExportDataSettings());
    case NKikimrSchemeOp::TBackupTask::kFSSettings:
        return DataFormatFromExportData(task.GetFSSettings().GetExportDataSettings());
    case NKikimrSchemeOp::TBackupTask::SETTINGS_NOT_SET:
    case NKikimrSchemeOp::TBackupTask::kYTSettings:
        return EDataFormat::Invalid;
    }
    return EDataFormat::Invalid;
}

EDataFormat NextDataFormat(EDataFormat cur) {
    switch (cur) {
    case EDataFormat::YdbDump:
        return EDataFormat::Parquet;
    case EDataFormat::Parquet:
        return EDataFormat::Invalid;
    case EDataFormat::Invalid:
        return EDataFormat::Invalid;
    }
}

ECompressionCodec NextCompressionCodec(ECompressionCodec cur) {
    switch (cur) {
    case ECompressionCodec::None:
        return ECompressionCodec::Zstd;
    case ECompressionCodec::Zstd:
        return ECompressionCodec::Invalid;
    case ECompressionCodec::Invalid:
        return ECompressionCodec::Invalid;
    }
}

TParquetExportSettings ParquetExportSettingsFromTask(const NKikimrSchemeOp::TBackupTask& task) {
    NKikimrSchemeOp::TParquetFormat taskParquetSettings;
    switch(task.GetSettingsCase()) {
    case NKikimrSchemeOp::TBackupTask::kS3Settings: {        
        auto& taskParquetSettings = task.GetS3Settings().GetExportDataSettings().GetParquet();
        return TParquetExportSettings().WithRowGroupSize(taskParquetSettings.GetRowGroupSize());
    }
    case NKikimrSchemeOp::TBackupTask::kFSSettings: {
        auto& taskParquetSettings = task.GetFSSettings().GetExportDataSettings().GetParquet();
        return TParquetExportSettings().WithRowGroupSize(taskParquetSettings.GetRowGroupSize());
    }
    case NKikimrSchemeOp::TBackupTask::SETTINGS_NOT_SET:
    case NKikimrSchemeOp::TBackupTask::kYTSettings:
        return TParquetExportSettings();
    }
    return TParquetExportSettings();
}

TString DataFileExtension(EDataFormat format, ECompressionCodec codec) {
    static THashMap<EDataFormat, TString> formats = {
        {EDataFormat::YdbDump, ".csv"},
        {EDataFormat::Parquet, ".parquet"},
    };

    static THashMap<ECompressionCodec, TString> codecs = {
        {ECompressionCodec::None, ""},
        {ECompressionCodec::Zstd, ".zst"},
    };

    auto fit = formats.find(format);
    Y_ENSURE(fit != formats.end(), "Unexpected format: " << format);

    const char* codecExt = "";
    if (fit->first != EDataFormat::Parquet) {
        auto cit = codecs.find(codec);
        Y_ENSURE(cit != codecs.end(), "Unexpected codec: " << codec);
        codecExt = cit->second.c_str();
    }

    return Sprintf("%s%s", fit->second.c_str(), codecExt);
}

static TString AddEncryptedSuffix(TString name, bool encryptedBackup) {
    if (encryptedBackup) {
        name.append(".enc");
    }
    return name;
}

TString PermissionsKeySuffix(bool encryptedBackup) {
    return AddEncryptedSuffix("permissions.pb", encryptedBackup);
}

TString TopicKeySuffix(bool encryptedBackup) {
    return AddEncryptedSuffix("topic_description.pb", encryptedBackup);
}

TString ChangefeedKeySuffix(bool encryptedBackup) {
    return AddEncryptedSuffix("changefeed_description.pb", encryptedBackup);
}

TString SchemeKeySuffix(bool encryptedBackup) {
    return AddEncryptedSuffix("scheme.pb", encryptedBackup);
}

TString MetadataKeySuffix(bool encryptedBackup) {
    return AddEncryptedSuffix("metadata.json", encryptedBackup);
}

TString DataKeySuffix(ui32 n, EDataFormat format, ECompressionCodec codec, bool encryptedBackup) {
    const auto ext = DataFileExtension(format, codec);
    return AddEncryptedSuffix(Sprintf("data_%02d%s", n, ext.c_str()), encryptedBackup);
}

} // NBackupRestoreTraits
} // NDataShard
} // NKikimr
