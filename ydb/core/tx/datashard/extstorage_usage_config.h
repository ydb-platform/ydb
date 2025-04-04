#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "backup_restore_traits.h"

#include <ydb/core/backup/common/encryption.h>
#include <ydb/core/base/events.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/public/api/protos/ydb_export.pb.h>

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/StorageClass.h>
#include <util/string/printf.h>
#include <util/string/builder.h>

namespace NKikimr::NDataShard {

class TS3Settings {
public:
    struct TEncryptionSettings {
        const bool EncryptedBackup;
        const TString EncryptionAlgorithm;
        const TMaybe<NBackup::TEncryptionKey> Key;
        const TMaybe<NBackup::TEncryptionIV> IV;

        static TEncryptionSettings FromBackupTask(const NKikimrSchemeOp::TBackupTask& task) {
            return FromProto(task);
        }

        static TEncryptionSettings FromRestoreTask(const NKikimrSchemeOp::TRestoreTask& task) {
            return FromProto(task);
        }

        TMaybe<NBackup::TEncryptionIV> GetMetadataIV() const {
            return IV;
        }

        TMaybe<NBackup::TEncryptionIV> GetSchemeIV() const {
            return GetIV(NBackup::EBackupFileType::TableSchema);
        }

        TMaybe<NBackup::TEncryptionIV> GetPermissionsIV() const {
            return GetIV(NBackup::EBackupFileType::Permissions);
        }

        TMaybe<NBackup::TEncryptionIV> GetTopicIV() const {
            return GetIV(NBackup::EBackupFileType::TableTopic);
        }

        TMaybe<NBackup::TEncryptionIV> GetChangefeedIV() const {
            return GetIV(NBackup::EBackupFileType::TableChangefeed);
        }

    private:
        TMaybe<NBackup::TEncryptionIV> GetIV(NBackup::EBackupFileType fileType) const {
            TMaybe<NBackup::TEncryptionIV> iv;
            if (IV) {
                iv = NBackup::TEncryptionIV::Combine(*IV, fileType, 0 /* backupItemNumber is already mixed in */, 0);
            }
            return iv;
        }

        template <class TProto>
        static TEncryptionSettings FromProto(const TProto& task) {
            if (task.HasEncryptionSettings()) {
                TString algorithm;
                if constexpr (std::is_same_v<TProto, NKikimrSchemeOp::TBackupTask>) {
                    algorithm = task.GetEncryptionSettings().GetEncryptionAlgorithm();
                }
                return TEncryptionSettings{
                    .EncryptedBackup = true,
                    .EncryptionAlgorithm = algorithm,
                    .Key = NBackup::TEncryptionKey(task.GetEncryptionSettings().GetSymmetricKey().key()),
                    .IV = NBackup::TEncryptionIV::FromBinaryString(task.GetEncryptionSettings().GetIV()),
                };
            } else {
                return TEncryptionSettings{
                    .EncryptedBackup = false,
                    .EncryptionAlgorithm = {},
                    .Key = Nothing(),
                    .IV = Nothing(),
                };
            }
        }
    };
public:
    const TString Bucket;
    const TString ObjectKeyPattern;
    const ui32 Shard;
    const Ydb::Export::ExportToS3Settings::StorageClass StorageClass;
    const TEncryptionSettings EncryptionSettings;

    explicit TS3Settings(const NKikimrSchemeOp::TS3Settings& settings, ui32 shard, const TEncryptionSettings& encryptionSettings)
        : Bucket(settings.GetBucket())
        , ObjectKeyPattern(settings.GetObjectKeyPattern())
        , Shard(shard)
        , StorageClass(settings.GetStorageClass())
        , EncryptionSettings(encryptionSettings)
    {
    }

public:
    static TS3Settings FromBackupTask(const NKikimrSchemeOp::TBackupTask& task) {
        return TS3Settings(task.GetS3Settings(), task.GetShardNum(), TEncryptionSettings::FromBackupTask(task));
    }

    static TS3Settings FromRestoreTask(const NKikimrSchemeOp::TRestoreTask& task) {
        return TS3Settings(task.GetS3Settings(), task.GetShardNum(), TEncryptionSettings::FromRestoreTask(task));
    }

    inline const TString& GetBucket() const { return Bucket; }
    inline const TString& GetObjectKeyPattern() const { return ObjectKeyPattern; }

    Aws::S3::Model::StorageClass GetStorageClass() const;

    inline TString GetPermissionsKey() const {
        return ObjectKeyPattern + '/' + NBackupRestoreTraits::PermissionsKeySuffix(EncryptionSettings.EncryptedBackup);
    }

    inline TString GetTopicKey(const TString& changefeedName) const {
        return TStringBuilder() << ObjectKeyPattern << '/'<< changefeedName << '/' << NBackupRestoreTraits::TopicKeySuffix(EncryptionSettings.EncryptedBackup);
    }

     inline TString GetChangefeedKey(const TString& changefeedName) const {
        return TStringBuilder() << ObjectKeyPattern << '/' << changefeedName << '/' << NBackupRestoreTraits::ChangefeedKeySuffix(EncryptionSettings.EncryptedBackup);
    }

    inline TString GetMetadataKey() const {
        return ObjectKeyPattern + '/' + NBackupRestoreTraits::MetadataKeySuffix(EncryptionSettings.EncryptedBackup);
    }

    inline TString GetSchemeKey() const {
        return ObjectKeyPattern + '/' + NBackupRestoreTraits::SchemeKeySuffix(EncryptionSettings.EncryptedBackup);
    }

    inline TString GetDataKey(
        NBackupRestoreTraits::EDataFormat format,
        NBackupRestoreTraits::ECompressionCodec codec) const {
        return ObjectKeyPattern + '/' + NBackupRestoreTraits::DataKeySuffix(Shard, format, codec, EncryptionSettings.EncryptedBackup);
    }

    inline TString GetDataFile(
        NBackupRestoreTraits::EDataFormat format,
        NBackupRestoreTraits::ECompressionCodec codec) const {
        return NBackupRestoreTraits::DataKeySuffix(Shard, format, codec, EncryptionSettings.EncryptedBackup);
    }

}; // TS3Settings
}

#endif // KIKIMR_DISABLE_S3_OPS
