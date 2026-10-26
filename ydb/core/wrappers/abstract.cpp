#include "abstract.h"
#include "fake_storage_config.h"
#include "fs_storage_config.h"
#include "s3_storage_config.h"

#include <ydb/core/protos/s3_settings.pb.h>

#include <util/generic/singleton.h>

namespace NKikimr::NWrappers::NExternalStorage {

IExternalStorageOperator::TPtr IExternalStorageConfig::ConstructStorageOperator(bool verbose) const {
    return DoConstructStorageOperator(verbose);
}

namespace {

struct TS3EndpointOverrideHolder {
    TMutex Mutex;
    IExternalStorageConfig::TS3EndpointOverride Override;
};

IExternalStorageConfig::TPtr OverrideS3Endpoint(const TString& endpoint, const TString& bucket, bool useVirtualAddressing) {
    auto& holder = *Singleton<TS3EndpointOverrideHolder>();
    TGuard<TMutex> guard(holder.Mutex);
    return holder.Override ? holder.Override(endpoint, bucket, useVirtualAddressing) : nullptr;
}

} // anonymous

void IExternalStorageConfig::SetS3EndpointOverride(TS3EndpointOverride override) {
    auto& holder = *Singleton<TS3EndpointOverrideHolder>();
    TGuard<TMutex> guard(holder.Mutex);
    holder.Override = std::move(override);
}

template <>
IExternalStorageConfig::TPtr IExternalStorageConfig::Construct(const NKikimrConfig::TAwsClientConfig& defaultAwsClientSettings, const NKikimrSchemeOp::TS3Settings& settings, NMonitoring::TDynamicCounterPtr rootCounters) {
    if (settings.GetEndpoint() == "fake.fake") {
        return std::make_shared<TFakeExternalStorageConfig>(settings.GetBucket(), settings.GetSecretKey());
    } else if (auto config = OverrideS3Endpoint(settings.GetEndpoint(), settings.GetBucket(), settings.GetUseVirtualAddressing())) {
        return config;
    } else {
        return std::make_shared<TS3ExternalStorageConfig>(defaultAwsClientSettings, settings, std::move(rootCounters));
    }
}

template <>
IExternalStorageConfig::TPtr IExternalStorageConfig::Construct(const NKikimrConfig::TAwsClientConfig& defaultAwsClientSettings, const Ydb::Export::ExportToS3Settings& settings, NMonitoring::TDynamicCounterPtr rootCounters) {
    if (auto config = OverrideS3Endpoint(settings.endpoint(), settings.bucket(), !settings.disable_virtual_addressing())) {
        return config;
    }
    return std::make_shared<TS3ExternalStorageConfig>(defaultAwsClientSettings, settings, std::move(rootCounters));
}

template <>
IExternalStorageConfig::TPtr IExternalStorageConfig::Construct(const NKikimrConfig::TAwsClientConfig& defaultAwsClientSettings, const Ydb::Import::ImportFromS3Settings& settings, NMonitoring::TDynamicCounterPtr rootCounters) {
    if (auto config = OverrideS3Endpoint(settings.endpoint(), settings.bucket(), !settings.disable_virtual_addressing())) {
        return config;
    }
    return std::make_shared<TS3ExternalStorageConfig>(defaultAwsClientSettings, settings, std::move(rootCounters));
}

template <>
IExternalStorageConfig::TPtr IExternalStorageConfig::Construct(const NKikimrConfig::TAwsClientConfig&, const NKikimrSchemeOp::TFSSettings& settings, NMonitoring::TDynamicCounterPtr /*rootCounters*/) {
    return std::make_shared<TFsExternalStorageConfig>(settings);
}

template <>
IExternalStorageConfig::TPtr IExternalStorageConfig::Construct(const NKikimrConfig::TAwsClientConfig&, const Ydb::Export::ExportToFsSettings& settings, NMonitoring::TDynamicCounterPtr /*rootCounters*/) {
    return std::make_shared<TFsExternalStorageConfig>(settings);
}

template <>
IExternalStorageConfig::TPtr IExternalStorageConfig::Construct(const NKikimrConfig::TAwsClientConfig&, const Ydb::Import::ImportFromFsSettings& settings, NMonitoring::TDynamicCounterPtr /*rootCounters*/) {
    return std::make_shared<TFsExternalStorageConfig>(settings);
}
}
