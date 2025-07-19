#include "bridge.h"

#include <ydb/core/protos/config.pb.h>

namespace NKikimr {

bool IsBridgeMode(const TActorContext &ctx) {
    return AppData(ctx)->BridgeModeEnabled;
}

} // NKikimr
