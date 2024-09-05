#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "backup_restore_traits.h"

#include <ydb/core/base/events.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/public/api/protos/ydb_export.pb.h>

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/StorageClass.h>
#include <util/string/printf.h>
#include <util/string/builder.h>

namespace NKikimr::NDataShard {

class TS3Settings {
public:
    const TString Bucket;
    const TString ObjectKeyPattern;
    const ui32 Shard;
    const Ydb::Export::ExportToS3Settings::StorageClass StorageClass;

    explicit TS3Settings(const NKikimrSchemeOp::TS3Settings& settings, ui32 shard)
        : Bucket(settings.GetBucket())
        , ObjectKeyPattern(settings.GetObjectKeyPattern())
        , Shard(shard)
        , StorageClass(settings.GetStorageClass()) {
    }

public:
    static TS3Settings FromBackupTask(const NKikimrSchemeOp::TBackupTask& task) {
        return TS3Settings(task.GetS3Settings(), task.GetShardNum());
    }

    static TS3Settings FromRestoreTask(const NKikimrSchemeOp::TRestoreTask& task) {
        return TS3Settings(task.GetS3Settings(), task.GetShardNum());
    }

    inline const TString& GetBucket() const { return Bucket; }
    inline const TString& GetObjectKeyPattern() const { return ObjectKeyPattern; }

    Aws::S3::Model::StorageClass GetStorageClass() const;

    inline TString GetPermissionsKey() const {
        return NBackupRestoreTraits::PermissionsKey(ObjectKeyPattern);
    }

    inline TString GetMetadataKey() const {
        return NBackupRestoreTraits::MetadataKey(ObjectKeyPattern);
    }

    inline TString GetSchemeKey() const {
        return NBackupRestoreTraits::SchemeKey(ObjectKeyPattern);
    }

    inline TString GetDataKey(
        NBackupRestoreTraits::EDataFormat format,
        NBackupRestoreTraits::ECompressionCodec codec) const {
        return NBackupRestoreTraits::DataKey(ObjectKeyPattern, Shard, format, codec);
    }

}; // TS3Settings
}

#endif // KIKIMR_DISABLE_S3_OPS
