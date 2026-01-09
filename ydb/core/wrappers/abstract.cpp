#include "abstract.h"
#include "fake_storage_config.h"
#include "fs_storage_config.h"
#include "s3_storage_config.h"

namespace NKikimr::NWrappers::NExternalStorage {

IExternalStorageOperator::TPtr IExternalStorageConfig::ConstructStorageOperator(bool verbose) const {
    return DoConstructStorageOperator(verbose);
}

template <>
IExternalStorageConfig::TPtr IExternalStorageConfig::Construct(const NKikimrSchemeOp::TS3Settings& settings) {
    if (settings.GetEndpoint() == "fake.fake") {
        return std::make_shared<TFakeExternalStorageConfig>(settings.GetBucket(), settings.GetSecretKey());
    } else {
        return std::make_shared<TS3ExternalStorageConfig>(settings);
    }
}

template <>
IExternalStorageConfig::TPtr IExternalStorageConfig::Construct(const NKikimrSchemeOp::TFSSettings& settings) {
    return std::make_shared<TFsExternalStorageConfig>(settings);
}

}
