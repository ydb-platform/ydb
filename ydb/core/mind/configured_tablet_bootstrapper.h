#pragma once
#include "defs.h"
#include <ydb/core/tablet/tablet_setup.h>
#include <ydb/core/base/tablet_types.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/bootstrap.pb.h>

namespace NKikimr {

    // would subscribe to boot config and instantiate tablet bootstrapper if configured for this node
    IActor* CreateConfiguredTabletBootstrapper(const ::NKikimrConfig::TBootstrap::TTablet &defaultConfig);

    TTabletTypes::EType BootstrapperTypeToTabletType(ui32 type);
    TIntrusivePtr<TTabletSetupInfo> MakeTabletSetupInfo(TTabletTypes::EType tabletType, ui32 poolId, ui32 tabletPoolId);
}
