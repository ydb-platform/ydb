#pragma once

#include <util/generic/ptr.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/spinlock.h>

#include <optional>

namespace NKikimr {

// Coherent value, default, and override-presence snapshot of an ICB control.
struct TControlState {
    // Effective value returned by the control at the snapshot point.
    TAtomicBase Value;

    // Registry default used when no explicit override is active.
    TAtomicBase Default;

    // Override state distinguishing an explicit value from the registry default.
    bool Overridden;
};

// State transition produced by one serialized control mutation.
struct TControlMutation {
    // Control state immediately before the mutation.
    TControlState Before;

    // Control state immediately after the mutation.
    TControlState After;
};

// Immediate control with a fast effective-value read and coherent state snapshots.
class TControl : public TThrRefBase {
    // Effective value returned by the numeric Get() API.
    TAtomic Value;

    // Registry default used when no explicit override is active.
    TAtomic Default;

    // Inclusive lower bound used for explicit overrides.
    TAtomicBase LowerBound;

    // Inclusive upper bound used for explicit overrides.
    TAtomicBase UpperBound;

    // Override presence stored as zero or one.
    TAtomic Overridden;

    // Serialization for mutations and complete state snapshots.
    mutable TAdaptiveLock StateLock;

    // State version: odd during a mutation and even after publication.
    TAtomic Sequence;

    // Read the atomic fields without validating cross-field coherence.
    TControlState GetStateUnsafe() const;

public:
    TControl(TAtomicBase defaultValue, TAtomicBase lowerBound, TAtomicBase upperBound);

    void Set(TAtomicBase newValue);
    void Reset(TAtomicBase defaultValue, TAtomicBase lowerBound, TAtomicBase upperBound);

    // Update the registry default without replacing an active override.
    void UpdateDefault(TAtomicBase newDefault);

    // Set a bounded override, even when equal to the default, and return its transition.
    TControlMutation SetOverride(TAtomicBase newValue);

    // Set an HTML override; accept the current default outside bounds and return the transition.
    TControlMutation SetFromHtmlRequestWithState(TAtomicBase newValue);

    TAtomicBase SetFromHtmlRequest(TAtomicBase newValue);

    TAtomicBase Get() const;

    TAtomicBase GetDefault() const;

    // Return one coherent snapshot of the complete control state.
    TControlState GetState() const;

    // Return the explicit override or an empty value when the default is active.
    std::optional<TAtomicBase> GetOverride() const;

    // Restore the current default, clear the override, and return the transition.
    TControlMutation RestoreDefault();

    bool IsDefault() const;

    TString RangeAsString() const;
};

}
