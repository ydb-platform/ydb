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
public:
    TControl(TAtomicBase defaultValue, TAtomicBase lowerBound, TAtomicBase upperBound);

    void Set(TAtomicBase newValue);
    void Reset(TAtomicBase defaultValue, TAtomicBase lowerBound, TAtomicBase upperBound);

    // Update the registry default without replacing an active override.
    void UpdateDefault(TAtomicBase newDefault);

    // Apply an HTML value and return its complete state transition.
    TControlMutation SetFromHtmlRequestWithState(TAtomicBase newValue);

    TAtomicBase SetFromHtmlRequest(TAtomicBase newValue);

    TAtomicBase Get() const;

    TAtomicBase GetDefault() const;

    // Return one coherent snapshot of the control state.
    TControlState GetState() const;

    // Return the explicit override or an empty value when the default is active.
    std::optional<TAtomicBase> GetOverride() const;

    // Clear the explicit override and return its complete state transition.
    TControlMutation ClearOverride();

    void RestoreDefault();

    bool HasOverride() const;

    bool IsDefault() const;

    TString RangeAsString() const;

private:
    TControlState GetStateUnsafe() const;

    // Writer serialization for compound state transitions.
    TAdaptiveLock WriteLock;

    // Snapshot sequence: even values are stable and odd values mark a writer.
    TAtomic Sequence;

    // Effective value returned by the compatibility `Get()` API.
    TAtomic Value;

    // Registry default used when no explicit override is active.
    TAtomic Default;

    // Override presence stored as zero or one.
    TAtomic Overridden;

    // Inclusive lower bound used for explicit overrides.
    TAtomicBase LowerBound;

    // Inclusive upper bound used for explicit overrides.
    TAtomicBase UpperBound;
};

}
