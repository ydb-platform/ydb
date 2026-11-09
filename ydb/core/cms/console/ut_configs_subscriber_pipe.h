#pragma once

#include "console_configs_subscriber.h"
#include <ydb/core/base/tablet_pipe.h>

namespace NKikimr::NConsole {

// The test uses actual subscriber protocol handling with local actor routing.
// Tablet discovery, pipe retries and inter-node transport are outside its scope.
namespace NQueueTestPipe {
using TClientConfig = NTabletPipe::TClientConfig;
TActorId ServiceId(ui32 nodeId);
IActor* CreateClient(const TActorId& owner, ui64 tabletId, const TClientConfig& config);
void SendData(const TActorContext& ctx, const TActorId& pipe, IEventBase* event, ui64 cookie = 0);
void CloseClient(const TActorContext& ctx, const TActorId& pipe);
}

IActor* CreateQueueTestConfigsSubscriber(const TActorId& owner, const TVector<ui32>& kinds,
    const NKikimrConfig::TAppConfig& currentConfig, ui64 cookie, bool processYaml, ui64 version,
    const TString& yaml, const TMap<ui64, TString>& volatileConfigs, const std::optional<TNodeInfo> nodeInfo);

} // namespace NKikimr::NConsole
