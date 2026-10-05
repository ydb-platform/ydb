#pragma once

#include <ydb/services/workload_manager/gateway_internal.h>

#include <util/datetime/base.h>


namespace NKikimr::NWorkloadManager::NPrivate {

///
/// Tracks the node-wide classifier metadata request for the workload manager state actor:
/// Pending until the first TEvRefreshSubscriberData, TimedOut if it does not arrive in time.
/// Plain class (no actor system); the caller asks the metadata provider and rebuilds the snapshot.
///
class TClassifierMetadataTracker {
public:
    explicit TClassifierMetadataTracker(TDuration requestTimeout);

    /// Ready if the metadata service is disabled (no classifiers will ever arrive), otherwise Pending.
    void Init(bool metadataServiceEnabled, TInstant now);

    EMetadataState GetState() const;

    /// True while waiting for metadata with resource pools enabled.
    bool IsInFlight() const;

    /// Returns true if the request became in flight (pools enabled while Pending);
    /// the caller asks for classifiers and arms the timeout check.
    bool OnPoolsEnabled(bool enabled, TInstant now);

    /// Classifier snapshot arrived. Returns true if the state changed.
    bool OnReady(TInstant now);

    /// Moves an in-flight request past the timeout to TimedOut. Returns true if changed.
    bool TimeOutPending(TInstant now);

    /// Returns true if TimedOut past the timeout; resets the timer, the caller asks again.
    bool NeedsRequery(TInstant now);

private:
    void SetState(EMetadataState state, TInstant now);

private:
    const TDuration RequestTimeout_;
    EMetadataState State_ = EMetadataState::Pending;
    TInstant StateAt_;
    bool PoolsEnabled_ = false;
};

}
