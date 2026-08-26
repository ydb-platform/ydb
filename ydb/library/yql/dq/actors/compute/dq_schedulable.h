#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <library/cpp/time_provider/monotonic.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>

#include <memory>

namespace NYql::NDq {

using TPoolId = TString;
using TDatabaseId = TString;

// Fully-qualified resource pool identifier. Pool names are not unique across
// databases on a shared node — the (database, pool) pair disambiguates.
struct TPoolKey {
    TDatabaseId DatabaseId;
    TPoolId PoolId;

    bool operator==(const TPoolKey&) const = default;
};

///
/// Per work-unit gate + attribution.
///
struct IDqSchedulableWork {
    virtual ~IDqSchedulableWork() = default;

    // Gate before a unit of work. Returns false when quota is exhausted.
    virtual bool StartExecution(TMonotonic now) = 0;

    // Called after the unit finishes. `forcedResume` is passed through to
    // signal whether the actor was woken up by resume rather than natural
    // completion.
    virtual void StopExecution(bool& forcedResume) = 0;

    // Estimated delay until quota is likely available again. Use after a
    // failed StartExecution, e.g. WaitForEvent(now + CalculateDelay(now)).
    virtual TDuration CalculateDelay(TMonotonic now) const = 0;

    // Subscribe on wake-up when quota frees up. The actor will receive
    // TEvWakeup from the scheduler.
    virtual void RegisterForResume(const NActors::TActorId& actorId) = 0;

    virtual TPoolKey GetPoolKey() const = 0;
};

///
/// Factory carried through TSourceArguments. Implementation lives in kqp
/// scheduler; sources create per-actor/coroutine schedulable-work wrappers
/// from it without pulling in kqp types.
///
struct IDqSchedulerContext {
    virtual ~IDqSchedulerContext() = default;

    // Each caller must own its own IDqSchedulableWork instance — Start/Stop
    // state is not thread-safe and not shareable across actors.
    virtual std::unique_ptr<IDqSchedulableWork> CreateSchedulableWork() = 0;

    // Pool identity without creating a Work object. Use when a caller only
    // needs the (database, pool) tag (e.g. for HTTP request routing).
    virtual TPoolKey GetPoolKey() const = 0;
};

using IDqSchedulerContextPtr = std::shared_ptr<IDqSchedulerContext>;

} // namespace NYql::NDq

template <>
struct THash<NYql::NDq::TPoolKey> {
    size_t operator()(const NYql::NDq::TPoolKey& k) const {
        return CombineHashes(THash<TString>{}(k.DatabaseId), THash<TString>{}(k.PoolId));
    }
};
