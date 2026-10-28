#pragma once

#include <ydb/core/tx/schemeshard/schemeshard_identificators.h>
#include <ydb/library/actors/core/actor.h>

#include <functional>

namespace NKikimr::NSchemeShard {

struct TIndexBuildInfo;

// Single-threaded actor-runtime tests may enqueue events at a preparation prefix.
// The callback must never dispatch events or execute transactions reentrantly.
struct TColumnBuildTestHooks {
    static std::function<void(const TActorContext&, TIndexBuildId, size_t)> OnColumnPrepared;
    // Returning true pauses progress between transactions, after Filling is persisted.
    static std::function<bool(const TIndexBuildInfo&)> BeforeFillingProgress;
};

} // namespace NKikimr::NSchemeShard
