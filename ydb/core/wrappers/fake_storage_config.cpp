#include "fake_storage.h"
#include "fake_storage_config.h"
#include <ydb/library/actors/core/log.h>

#ifndef KIKIMR_DISABLE_S3_OPS
namespace NKikimr::NWrappers::NExternalStorage {

TString TFakeExternalStorageConfig::DoGetStorageId() const {
    return "fake";
}

IExternalStorageOperator::TPtr TFakeExternalStorageConfig::DoConstructStorageOperator(bool verbose) const {
    Y_UNUSED(verbose);
    return std::make_shared<TFakeExternalStorageOperator>(Bucket, SecretKey);
}
}

#endif // KIKIMR_DISABLE_S3_OPS
