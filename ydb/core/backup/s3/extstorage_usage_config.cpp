#include "extstorage_usage_config.h"
#include <ydb/core/wrappers/s3_storage_config.h>

#ifndef KIKIMR_DISABLE_S3_OPS
namespace NKikimr::NBackup::NS3 {

Aws::S3::Model::StorageClass TS3Settings::GetStorageClass() const {
    return NKikimr::NWrappers::NExternalStorage::TS3ExternalStorageConfig::ConvertStorageClass(StorageClass);
}

} // namespace NKikimr::NBackup::NS3

#endif // KIKIMR_DISABLE_S3_OPS
