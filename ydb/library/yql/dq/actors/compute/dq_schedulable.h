#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <library/cpp/time_provider/monotonic.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>

#include <memory>
#include <optional>

namespace NYql::NDq {

///
/// Opaque two-part identifier of a scheduling scope.
///
struct TWorkScope {
    TString Namespace;
    TString Name;

    bool operator==(const TWorkScope&) const = default;
};

///
/// Per work-unit gate + attribution.
///
struct IDqSchedulableWork {
    virtual ~IDqSchedulableWork() = default;

    // Gate before a unit of work. Returns nullopt on success — the caller may
    // proceed with the unit. On failure returns the estimated delay until
    // quota is likely available again, e.g. WaitForEvent(now + *delay).
    virtual std::optional<TDuration> TryStartExecution(TMonotonic now) = 0;

    // Called after the unit finishes; releases the quota.
    virtual void StopExecution() = 0;

    // Reports how a throttling wait ended: a wake-up delivered by the scheduler
    // (byScheduler = true) is accounted separately from a self-scheduled retry
    // after the delay returned by TryStartExecution has expired. The value is
    // consumed and reset by the next StopExecution().
    virtual void NotifyResumed(bool byScheduler) = 0;

    // Subscribe on wake-up when quota frees up. The actor will receive
    // TEvWakeup from the scheduler.
    virtual void RegisterForResume(const NActors::TActorId& actorId) = 0;

    virtual TWorkScope GetWorkScope() const = 0;
};

// Produces schedulable work for one fixed scope.
struct IDqSchedulableWorkFactory {
    virtual ~IDqSchedulableWorkFactory() = default;

    // Each caller must own its own IDqSchedulableWork instance — Start/Stop
    // state is not thread-safe and not shareable across actors. Whatever the
    // instances share (a query, a rate limiter) stays behind this factory.
    virtual std::unique_ptr<IDqSchedulableWork> CreateSchedulableWork() = 0;

    // Scope identity without creating a work object. Use when a caller only
    // needs the routing/tagging token (e.g. for HTTP request routing).
    virtual TWorkScope GetWorkScope() const = 0;
};

using IDqSchedulableWorkFactoryPtr = std::shared_ptr<IDqSchedulableWorkFactory>;

} // namespace NYql::NDq

template <>
struct THash<NYql::NDq::TWorkScope> {
    size_t operator()(const NYql::NDq::TWorkScope& k) const {
        return CombineHashes(THash<TString>{}(k.Namespace), THash<TString>{}(k.Name));
    }
};
