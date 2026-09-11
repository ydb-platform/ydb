#pragma once

#include <ydb/services/udf_store/compile_controller/protos/compile_controller.pb.h>

#include <ydb/core/base/events.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/events.h>

namespace NKikimr::NUdfStore {

//! The controller tablet and the dinode services live on different nodes, so
//! this protocol has to survive a tablet pipe. The process-local replies of the
//! compile actors stay in `udf_store/events.h` as TEventLocal.
struct TEvCompileController {
    enum EEv {
        EvRegister = EventSpaceBegin(TKikimrEvents::ES_WASM_COMPILE_CTL),
        EvRegisterResult,
        EvHeartbeat,
        EvNeedArtifact,
        EvAssignCompile,
        EvCompileDone,
        EvCompileFailed,
        EvArtifactReady,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_WASM_COMPILE_CTL),
        "expected EvEnd < EventSpaceEnd(TKikimrEvents::ES_WASM_COMPILE_CTL)");

    struct TEvRegister
        : public NActors::TEventPB<TEvRegister, NKikimrUdfStore::TEvRegister, EvRegister>
    {};

    struct TEvRegisterResult
        : public NActors::TEventPB<TEvRegisterResult, NKikimrUdfStore::TEvRegisterResult, EvRegisterResult>
    {};

    struct TEvHeartbeat
        : public NActors::TEventPB<TEvHeartbeat, NKikimrUdfStore::TEvHeartbeat, EvHeartbeat>
    {};

    struct TEvNeedArtifact
        : public NActors::TEventPB<TEvNeedArtifact, NKikimrUdfStore::TEvNeedArtifact, EvNeedArtifact>
    {};

    struct TEvAssignCompile
        : public NActors::TEventPB<TEvAssignCompile, NKikimrUdfStore::TEvAssignCompile, EvAssignCompile>
    {};

    struct TEvCompileDone
        : public NActors::TEventPB<TEvCompileDone, NKikimrUdfStore::TEvCompileDone, EvCompileDone>
    {};

    struct TEvCompileFailed
        : public NActors::TEventPB<TEvCompileFailed, NKikimrUdfStore::TEvCompileFailed, EvCompileFailed>
    {};

    struct TEvArtifactReady
        : public NActors::TEventPB<TEvArtifactReady, NKikimrUdfStore::TEvArtifactReady, EvArtifactReady>
    {};
};

} // namespace NKikimr::NUdfStore
