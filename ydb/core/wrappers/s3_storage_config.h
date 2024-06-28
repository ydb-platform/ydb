#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "abstract.h"

#include <ydb/core/base/events.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/public/api/protos/ydb_import.pb.h>
#include <ydb/public/api/protos/ydb_export.pb.h>

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/auth/AWSCredentials.h>

#include <util/string/builder.h>
#include <util/string/printf.h>

namespace NKikimr::NWrappers::NExternalStorage {

class TS3ExternalStorageConfig: public IExternalStorageConfig {
private:
    YDB_READONLY_DEF(TString, Bucket);
    Aws::Client::ClientConfiguration Config;
    const Aws::Auth::AWSCredentials Credentials;
    YDB_READONLY(Aws::S3::Model::StorageClass, StorageClass, Aws::S3::Model::StorageClass::STANDARD);
    YDB_READONLY(bool, UseVirtualAddressing, true);

    static Aws::Client::ClientConfiguration ConfigFromSettings(const NKikimrSchemeOp::TS3Settings& settings);
    static Aws::Auth::AWSCredentials CredentialsFromSettings(const NKikimrSchemeOp::TS3Settings& settings);
    static Aws::Client::ClientConfiguration ConfigFromSettings(const Ydb::Import::ImportFromS3Settings& settings);
    static Aws::Auth::AWSCredentials CredentialsFromSettings(const Ydb::Import::ImportFromS3Settings& settings);
    static Aws::Client::ClientConfiguration ConfigFromSettings(const Ydb::Export::ExportToS3Settings& settings);
    static Aws::Auth::AWSCredentials CredentialsFromSettings(const Ydb::Export::ExportToS3Settings& settings);
protected:
    virtual TString DoGetStorageId() const override;
    virtual IExternalStorageOperator::TPtr DoConstructStorageOperator(bool verbose) const override;
public:
    static Aws::S3::Model::StorageClass ConvertStorageClass(const Ydb::Export::ExportToS3Settings::StorageClass storage);

    const Aws::Client::ClientConfiguration& GetConfig() const {
        return Config;
    }

    Aws::Client::ClientConfiguration& ConfigRef() {
        return Config;
    }

    TS3ExternalStorageConfig(const NKikimrSchemeOp::TS3Settings& settings);
    TS3ExternalStorageConfig(const Ydb::Import::ImportFromS3Settings& settings);
    TS3ExternalStorageConfig(const Ydb::Export::ExportToS3Settings& settings);
    TS3ExternalStorageConfig(const Aws::Auth::AWSCredentials& credentials, const Aws::Client::ClientConfiguration& config, const TString& bucket);
};
} // NKikimr::NWrappers::NExternalStorage

#endif // KIKIMR_DISABLE_S3_OPS
