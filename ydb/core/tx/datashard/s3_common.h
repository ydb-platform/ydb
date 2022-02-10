#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "defs.h"

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/auth/AWSCredentials.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/client/ClientConfiguration.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/StorageClass.h>

#include <ydb/core/protos/flat_scheme_op.pb.h> 

#include <util/string/builder.h>
#include <util/string/printf.h>

namespace NKikimr {
namespace NDataShard { 

inline Aws::Client::ClientConfiguration ConfigFromSettings(const NKikimrSchemeOp::TS3Settings& settings) {
    Aws::Client::ClientConfiguration config;

    config.endpointOverride = settings.GetEndpoint();
    config.connectTimeoutMs = 10000;
    config.maxConnections = 5;

    switch (settings.GetScheme()) {
    case NKikimrSchemeOp::TS3Settings::HTTP:
        config.scheme = Aws::Http::Scheme::HTTP;
        break;
    case NKikimrSchemeOp::TS3Settings::HTTPS:
        config.scheme = Aws::Http::Scheme::HTTPS;
        break;
    default:
        Y_FAIL("Unknown scheme");
    }

    return config;
}

inline Aws::Auth::AWSCredentials CredentialsFromSettings(const NKikimrSchemeOp::TS3Settings& settings) {
    return Aws::Auth::AWSCredentials(settings.GetAccessKey(), settings.GetSecretKey());
}

struct TS3Settings {
    Aws::Client::ClientConfiguration Config;
    Aws::Auth::AWSCredentials Credentials;
    TString Bucket;
    TString SchemeKey;
    TString DataKey;
    Aws::S3::Model::StorageClass StorageClass;

private:
    explicit TS3Settings(const NKikimrSchemeOp::TS3Settings& settings, ui32 shard)
        : Config(ConfigFromSettings(settings))
        , Credentials(CredentialsFromSettings(settings))
        , Bucket(settings.GetBucket())
        , SchemeKey(TStringBuilder() << settings.GetObjectKeyPattern() << "/scheme.pb")
        , DataKey(TStringBuilder() << settings.GetObjectKeyPattern() << Sprintf("/data_%02d.csv", shard))
        , StorageClass(ConvertStorageClass(settings.GetStorageClass()))
    {
        Config.caPath = "/etc/ssl/certs";
    }

    static Aws::S3::Model::StorageClass ConvertStorageClass(Ydb::Export::ExportToS3Settings::StorageClass value) {
        using ExportToS3Settings = Ydb::Export::ExportToS3Settings;
        using AwsStorageClass = Aws::S3::Model::StorageClass;

        switch (value) {
        case ExportToS3Settings::STORAGE_CLASS_UNSPECIFIED:
            return AwsStorageClass::NOT_SET;
        case ExportToS3Settings::STANDARD:
            return AwsStorageClass::STANDARD;
        case ExportToS3Settings::REDUCED_REDUNDANCY:
            return AwsStorageClass::REDUCED_REDUNDANCY;
        case ExportToS3Settings::STANDARD_IA:
            return AwsStorageClass::STANDARD_IA;
        case ExportToS3Settings::ONEZONE_IA:
            return AwsStorageClass::ONEZONE_IA;
        case ExportToS3Settings::INTELLIGENT_TIERING:
            return AwsStorageClass::INTELLIGENT_TIERING;
        case ExportToS3Settings::GLACIER:
            return AwsStorageClass::GLACIER;
        case ExportToS3Settings::DEEP_ARCHIVE:
            return AwsStorageClass::DEEP_ARCHIVE;
        case ExportToS3Settings::OUTPOSTS:
            return AwsStorageClass::OUTPOSTS;
        default:
            return AwsStorageClass::NOT_SET;
        }
    }

public:
    static TS3Settings FromBackupTask(const NKikimrSchemeOp::TBackupTask& task) {
        return TS3Settings(task.GetS3Settings(), task.GetShardNum());
    }

    static TS3Settings FromRestoreTask(const NKikimrSchemeOp::TRestoreTask& task) {
        return TS3Settings(task.GetS3Settings(), task.GetShardNum());
    }

}; // TS3Settings

} // NDataShard 
} // NKikimr

#endif // KIKIMR_DISABLE_S3_OPS
