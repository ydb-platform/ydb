#include "extstorage_usage_config.h"
#include <ydb/core/wrappers/s3_storage_config.h>

#ifndef KIKIMR_DISABLE_S3_OPS
namespace NKikimr::NDataShard {

Aws::S3::Model::StorageClass TS3Settings::GetStorageClass() const {
    return NKikimr::NWrappers::NExternalStorage::TS3ExternalStorageConfig::ConvertStorageClass(StorageClass);
}

}

#endif // KIKIMR_DISABLE_S3_OPS
