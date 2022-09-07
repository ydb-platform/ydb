#include "extstorage_usage_config.h"

#ifndef KIKIMR_DISABLE_S3_OPS
namespace NKikimr::NDataShard {

Aws::S3::Model::StorageClass TS3Settings::GetStorageClass() const {
    using ExportToS3Settings = Ydb::Export::ExportToS3Settings;
    using AwsStorageClass = Aws::S3::Model::StorageClass;

    switch (StorageClass) {
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

}

#endif // KIKIMR_DISABLE_S3_OPS
