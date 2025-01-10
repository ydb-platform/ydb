#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/protos/replication.pb.h>

#include <util/generic/vector.h>

namespace NKikimr::NReplication::NController {

IActor* CreateTargetDiscoverer(const TActorId& parent, ui64 rid, const TActorId& proxy,
    TVector<std::pair<TString, TString>>&& specificPaths, NKikimrReplication::EReplicationType type);

}
