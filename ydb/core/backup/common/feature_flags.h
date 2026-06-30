#pragma once

#include <ydb/core/base/appdata_fwd.h>

namespace NKikimr::NBackup {

inline bool IsExportFilteringEnabled(const TAppData& appData) {
    return appData.FeatureFlags.GetEnableExportFiltering();
}

inline bool IsEncryptedExportEnabled(const TAppData& appData) {
    return appData.FeatureFlags.GetEnableEncryptedExport();
}

} // namespace NKikimr::NBackup
