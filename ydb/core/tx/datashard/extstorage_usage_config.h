#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "backup_restore_traits.h"

#include <ydb/core/backup/common/encryption.h>
#include <ydb/core/base/events.h>
#include <ydb/core/base/path.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/fs_settings.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/StorageClass.h>
#include <util/folder/path.h>
#include <util/string/printf.h>
#include <util/string/builder.h>

namespace NKikimr::NDataShard {

class TStorageSettings {
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

        TMaybe<NBackup::TEncryptionIV> GetChangefeedTopicIV(ui32 changefeedIndex) const {
            return GetIV(NBackup::EBackupFileType::TableTopic, changefeedIndex);
        }

        TMaybe<NBackup::TEncryptionIV> GetChangefeedIV(ui32 changefeedIndex) const {
            return GetIV(NBackup::EBackupFileType::TableChangefeed, changefeedIndex);
        }

    private:
        TMaybe<NBackup::TEncryptionIV> GetIV(NBackup::EBackupFileType fileType, ui32 shardNumber = 0) const {
            TMaybe<NBackup::TEncryptionIV> iv;
            if (IV) {
                iv = NBackup::TEncryptionIV::Combine(*IV, fileType, 0 /* backupItemNumber is already mixed in */, shardNumber);
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
    const TString ObjectKeyPattern;
    const ui32 Shard;
    const TEncryptionSettings EncryptionSettings;

    template <class TSettings>
    static TStorageSettings FromBackupTask(const NKikimrSchemeOp::TBackupTask& task);

    template <class TSettings>
    static TStorageSettings FromRestoreTask(const NKikimrSchemeOp::TRestoreTask& task);

    template <>
    static TStorageSettings FromBackupTask<NKikimrSchemeOp::TS3Settings>(const NKikimrSchemeOp::TBackupTask& task) {
        return TStorageSettings(task.GetS3Settings().GetObjectKeyPattern(), task.GetShardNum(), TEncryptionSettings::FromBackupTask(task));
    }

    template <>
    static TStorageSettings FromBackupTask<NKikimrSchemeOp::TFSSettings>(const NKikimrSchemeOp::TBackupTask& task) {
        return TStorageSettings(CanonizePath(TStringBuilder() << task.GetFSSettings().GetBasePath() << "/" << task.GetFSSettings().GetPath()), task.GetShardNum(), TEncryptionSettings::FromBackupTask(task));
    }

    template <>
    static TStorageSettings FromRestoreTask<NKikimrSchemeOp::TS3Settings>(const NKikimrSchemeOp::TRestoreTask& task) {
        return TStorageSettings(task.GetS3Settings().GetObjectKeyPattern(), task.GetShardNum(), TEncryptionSettings::FromRestoreTask(task));
    }

    template <>
    static TStorageSettings FromRestoreTask<NKikimrSchemeOp::TFSSettings>(const NKikimrSchemeOp::TRestoreTask& task) {
        TString path = task.GetFSSettings().GetPath();
        // Absolute path in the prefix is possible if the backup with SchemaMapping
        if (!path.empty() && path[0] != '/') {
            path = CanonizePath(TStringBuilder() << task.GetFSSettings().GetBasePath() << "/" << path);
        }
        return TStorageSettings(path, task.GetShardNum(), TEncryptionSettings::FromRestoreTask(task));
    }

    explicit TStorageSettings(const TString& objectKeyPattern, ui32 shard, const TEncryptionSettings& encryptionSettings)
        : ObjectKeyPattern(objectKeyPattern)
        , Shard(shard)
        , EncryptionSettings(encryptionSettings)
    {
    }

public:
    inline const TString& GetObjectKeyPattern() const { return ObjectKeyPattern; }

    inline TString GetPermissionsKey() const {
        return ObjectKeyPattern + '/' + NBackupRestoreTraits::PermissionsKeySuffix(EncryptionSettings.EncryptedBackup);
    }

    inline TString GetTopicKey(const TString& changefeedPrefix) const {
        return TStringBuilder() << ObjectKeyPattern << '/'<< changefeedPrefix << '/' << NBackupRestoreTraits::TopicKeySuffix(EncryptionSettings.EncryptedBackup);
    }

    inline TString GetChangefeedKey(const TString& changefeedPrefix) const {
        return TStringBuilder() << ObjectKeyPattern << '/' << changefeedPrefix << '/' << NBackupRestoreTraits::ChangefeedKeySuffix(EncryptionSettings.EncryptedBackup);
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

};

} // NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
