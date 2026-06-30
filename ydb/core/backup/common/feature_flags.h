#pragma once

#include <ydb/core/base/appdata_fwd.h>

namespace NKikimr::NBackup {

inline bool IsExportFilteringEnabled(const TAppData& appData) {
    // Backward compatibility: clusters with only EnableEncryptedExport keep filtering API enabled.
    return appData.FeatureFlags.GetEnableExportFiltering()
        || appData.FeatureFlags.GetEnableEncryptedExport();
}

inline bool IsEncryptedExportEnabled(const TAppData& appData) {
    return appData.FeatureFlags.GetEnableEncryptedExport();
}

} // namespace NKikimr::NBackup
