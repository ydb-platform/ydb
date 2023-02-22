#pragma once

#include <ydb/core/base/defs.h>

namespace NKikimr::NReplication {

namespace NService {

inline TString MakeDiscoveryPath(const TString& tenant) {
    return "rs+" + tenant;
}

} // NService

inline TActorId MakeReplicationServiceId(ui32 nodeId) {
    return TActorId(nodeId, TStringBuf("ReplictnSvc"));
}

IActor* CreateReplicationService();

}
