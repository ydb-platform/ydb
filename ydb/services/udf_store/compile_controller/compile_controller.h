#pragma once

#include <ydb/core/base/blobstorage.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NUdfStore {

//! Per-tenant scheduler of WASM AOT compiles. The tablet never runs LLVM
//! itself: object code is only valid on the cpu_spec that produced it, so
//! codegen stays on the assigned dinode and the leader only decides who does
//! it and when.
NActors::IActor* CreateWasmCompileController(
    const NActors::TActorId& tablet,
    TTabletStorageInfo* info);

} // namespace NKikimr::NUdfStore
