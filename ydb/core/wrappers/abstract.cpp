#include "abstract.h"
#include "fake_storage_config.h"
#include "fs_storage_config.h"
#include "s3_storage_config.h"

#include <ydb/core/protos/s3_settings.pb.h>

namespace NKikimr::NWrappers::NExternalStorage {

IExternalStorageOperator::TPtr IExternalStorageConfig::ConstructStorageOperator(bool verbose) const {
    return DoConstructStorageOperator(verbose);
}

template <>
IExternalStorageConfig::TPtr IExternalStorageConfig::Construct(const NKikimrConfig::TAwsClientConfig& defaultAwsClientSettings, const NKikimrSchemeOp::TS3Settings& settings) {
    if (settings.GetEndpoint() == "fake.fake") {
        return std::make_shared<TFakeExternalStorageConfig>(settings.GetBucket(), settings.GetSecretKey());
    } else {
        return std::make_shared<TS3ExternalStorageConfig>(defaultAwsClientSettings, settings);
    }
}

template <>
IExternalStorageConfig::TPtr IExternalStorageConfig::Construct(const NKikimrConfig::TAwsClientConfig& defaultAwsClientSettings, const Ydb::Export::ExportToS3Settings& settings) {
    return std::make_shared<TS3ExternalStorageConfig>(defaultAwsClientSettings, settings);
}

template <>
IExternalStorageConfig::TPtr IExternalStorageConfig::Construct(const NKikimrConfig::TAwsClientConfig&, const NKikimrSchemeOp::TFSSettings& settings) {
    return std::make_shared<TFsExternalStorageConfig>(settings);
}

template <>
IExternalStorageConfig::TPtr IExternalStorageConfig::Construct(const NKikimrConfig::TAwsClientConfig&, const Ydb::Export::ExportToFsSettings& settings) {
    return std::make_shared<TFsExternalStorageConfig>(settings);
}
}
