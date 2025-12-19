#include "fs_storage.h"
#include "fs_storage_config.h"

namespace NKikimr::NWrappers::NExternalStorage {

TString TFsExternalStorageConfig::DoGetStorageId() const {
    return BasePath;
}

IExternalStorageOperator::TPtr TFsExternalStorageConfig::DoConstructStorageOperator(bool verbose) const {
    return std::make_shared<TFsExternalStorage>(BasePath, verbose);
}

TFsExternalStorageConfig::TFsExternalStorageConfig(const NKikimrSchemeOp::TFSSettings& settings)
    : BasePath(settings.GetBasePath())
{
}

} // NKikimr::NWrappers::NExternalStorage
