#pragma once
#include <util/generic/strbuf.h>
#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NKqp {

const TStringBuf DefaultKikimrPublicClusterName = "db";

inline NActors::TActorId MakeKqpProxyID(ui32 nodeId) {
    const char name[12] = "kqp_proxy";
    return NActors::TActorId(nodeId, TStringBuf(name, 12));
}

inline NActors::TActorId MakeKqpCompileServiceID(ui32 nodeId) {
    const char name[12] = "kqp_compile";
    return NActors::TActorId(nodeId, TStringBuf(name, 12));
}

inline NActors::TActorId MakeKqpResourceManagerServiceID(ui32 nodeId) {
    const char name[12] = "kqp_resman";
    return NActors::TActorId(nodeId, TStringBuf(name, 12));
}

inline NActors::TActorId MakeKqpRmServiceID(ui32 nodeId) {
    const char name[12] = "kqp_rm";
    return NActors::TActorId(nodeId, TStringBuf(name, 12));
}

inline NActors::TActorId MakeKqpNodeServiceID(ui32 nodeId) {
    const char name[12] = "kqp_node";
    return NActors::TActorId(nodeId, TStringBuf(name, 12));
}

inline NActors::TActorId MakeKqpCompileComputationPatternServiceID(ui32 nodeId) {
    const char name[12] = "kqp_comp_cp";
    return NActors::TActorId(nodeId, TStringBuf(name, 12));
}

inline NActors::TActorId MakeKqpFinalizeScriptServiceId(ui32 nodeId) {
    const char name[12] = "kqp_sfinal";
    return NActors::TActorId(nodeId, TStringBuf(name, 12));
}

inline NActors::TActorId MakeKqpWorkloadServiceId(ui32 nodeId) {
    const char name[12] = "kqp_workld";
    return NActors::TActorId(nodeId, TStringBuf(name, 12));
}

} // namespace NKikimr::NKqp
