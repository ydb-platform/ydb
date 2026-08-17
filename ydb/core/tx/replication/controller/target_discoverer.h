#pragma once

#include <ydb/core/base/defs.h>

namespace NKikimrReplication {
    class TReplicationConfig;
}

namespace NKikimr::NReplication::NController {

IActor* CreateTargetDiscoverer(const TActorId& parent, ui64 rid, const TActorId& proxy,
    const NKikimrReplication::TReplicationConfig& config);

}
