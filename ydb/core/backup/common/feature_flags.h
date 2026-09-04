#pragma once

#include <ydb/core/base/appdata_fwd.h>

namespace NKikimr::NBackup {

bool IsExportFilteringEnabled(const TAppData& appData);

bool IsEncryptedExportEnabled(const TAppData& appData);

} // namespace NKikimr::NBackup
