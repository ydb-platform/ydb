#include "bridge.h"

#include <ydb/core/protos/config.pb.h>

namespace NKikimr {

bool IsBridgeMode(const TActorContext &ctx) {
    const auto *bridgeConfig = AppData(ctx)->BridgeConfig;
    return bridgeConfig && bridgeConfig->PilesSize() > 0;
}

} // NKikimr
