#include "fs_storage.h"
#include "fs_storage_config.h"

#include <ydb/core/protos/fs_settings.pb.h>
#include <ydb/public/api/protos/ydb_export.pb.h>

namespace NKikimr::NWrappers::NExternalStorage {

TString TFsExternalStorageConfig::DoGetStorageId() const {
    return BasePath;
}

IExternalStorageOperator::TPtr TFsExternalStorageConfig::DoConstructStorageOperator(bool) const {
    return std::make_shared<TFsExternalStorage>(BasePath);
}

TFsExternalStorageConfig::TFsExternalStorageConfig(const NKikimrSchemeOp::TFSSettings& settings)
    : BasePath(settings.GetBasePath())
{
}

TFsExternalStorageConfig::TFsExternalStorageConfig(const Ydb::Export::ExportToFsSettings& settings)
    : BasePath(settings.base_path())
{
}

} // NKikimr::NWrappers::NExternalStorage
