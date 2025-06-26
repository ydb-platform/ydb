#include "appdata_fwd.h"
#include "defs.h"
#include "blobstorage_common.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/bridge.pb.h>

namespace NKikimr {

    bool IsBridgeMode(const TActorContext &ctx) {
        const auto *bridgeConfig = AppData(ctx)->BridgeConfig;
        return bridgeConfig && bridgeConfig->PilesSize() > 0;
    }

} // NKikimr
