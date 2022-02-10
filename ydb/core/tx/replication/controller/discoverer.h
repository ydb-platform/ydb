#pragma once

#include <ydb/core/base/defs.h>

#include <util/generic/vector.h>

namespace NKikimr {
namespace NReplication {
namespace NController {

IActor* CreateDiscoverer(const TActorId& parent, ui64 rid, const TActorId& proxy,
    TVector<std::pair<TString, TString>>&& specificPaths);

} // NController
} // NReplication
} // NKikimr
