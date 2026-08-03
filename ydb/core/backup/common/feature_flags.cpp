#include "feature_flags.h"

#include <ydb/core/base/appdata.h>

namespace NKikimr::NBackup {

bool IsExportFilteringEnabled(const TAppData& appData) {
    // Backward compatibility: clusters with only EnableEncryptedExport keep filtering API enabled.
    return appData.FeatureFlags.GetEnableExportFiltering()
        || appData.FeatureFlags.GetEnableEncryptedExport();
}

bool IsEncryptedExportEnabled(const TAppData& appData) {
    return appData.FeatureFlags.GetEnableEncryptedExport();
}

} // namespace NKikimr::NBackup
