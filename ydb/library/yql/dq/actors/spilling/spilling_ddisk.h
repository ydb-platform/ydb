#pragma once

#include "spilling_counters.h"

#include <ydb/library/yql/dq/common/dq_common.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>

#include <util/system/types.h>

#include <optional>

namespace NYql::NDq {

enum class EDqSpillingBackend {
    LocalFile = 0,
    DDisk = 1,
};

struct TDDiskSpillingConfig {
    bool Enable = false;
};

// Process-wide backend selection. Called once from KQP proxy on startup.
void ConfigureDqSpillingBackend(
    EDqSpillingBackend backend,
    TDDiskSpillingConfig ddiskConfig = {},
    TIntrusivePtr<TSpillingCounters> counters = nullptr);
EDqSpillingBackend GetDqSpillingBackend();
const TDDiskSpillingConfig& GetDqDDiskSpillingConfig();
TIntrusivePtr<TSpillingCounters> GetDqSpillingCounters();

// Lightweight mon actor that serves /actors/kqp_spilling_ddisk.
NActors::IActor* CreateDqDDiskSpillingMonActor(TIntrusivePtr<TSpillingCounters> counters);

// Same contract as CreateDqLocalFileSpillingActor: accepts TEvDqSpilling::{Write,Read},
// replies with WriteResult / ReadResult / Error to `client`.
// If pbActorIdOverride is set, skips NodeWarden discovery (used by tests).
NActors::IActor* CreateDqDDiskSpillingActor(
    TTxId txId,
    const TString& details,
    const NActors::TActorId& client,
    bool removeBlobsAfterRead,
    ESpillingType spillingType,
    TDDiskSpillingConfig config,
    std::optional<NActors::TActorId> pbActorIdOverride = std::nullopt);

// Picks LocalFile or DDisk based on ConfigureDqSpillingBackend().
NActors::IActor* CreateDqSpillingActor(
    TTxId txId,
    const TString& details,
    const NActors::TActorId& client,
    bool removeBlobsAfterRead,
    ESpillingType spillingType);

} // namespace NYql::NDq
