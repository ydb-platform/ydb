#pragma once
#include "defs.h"

#include <ydb/core/base/tablet_types.h>
#include <ydb/core/protos/config.pb.h>

namespace NKikimr::NTabletFlatExecutor {

IActor* CreateBackupWriter(const NKikimrConfig::TSystemTabletBackupConfig& config,
                           TTabletTypes::EType tabletType, ui64 tabletId, ui32 generation);

} // NKikimr::NTabletFlatExecutor
