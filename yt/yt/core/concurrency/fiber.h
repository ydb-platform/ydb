#pragma once

#include "private.h"
#include "propagating_storage.h"
#include "fls.h"

#include <yt/yt/core/misc/intrusive_mpsc_stack.h>

#include <library/cpp/yt/memory/function_view.h>

#include <util/system/context.h>

#include <atomic>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TFiberRegistry;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

struct TFiberRegisterTag
{ };

struct TFiberUnregisterTag
{ };

////////////////////////////////////////////////////////////////////////////////

// Basic invariants:
// Some SetRunning is in hb with SetWaiting/SetIdle/SetFinished.
// Some SetWaiting is in hb with a successful TryStartIntrospection.
// SetRunning stalls while observed state is Introspecting.

// You can think about this state machine in the following way:
// This is a SpinLock where
// SetRunning is Acquire
// SetWaiting is Release
// SetIdle/SetFinished are basically no-op
// TryLockForIntrospection is TryAcquire + Release on success.
// Thus same sync logic applies.
class TFiberIntrospectionBase
{
public:
    // Used by introspector on any fiber.
    bool TryLockForIntrospection(EFiberState* state, TFunctionView<void()> successHandler);
    TFiberId GetFiberId() const;

    // Used by introspector on waiting fibers.
    TInstant GetWaitingSince() const;
    TFls* GetFls();

    // Used by fiber_schedulers on running fibers.
    void OnCallbackExecutionStarted(TFiberId fiberId, TFls* fls);
    void OnCallbackExecutionFinished();
    void SetWaiting();
    void SetIdle();

protected:
    // Used by fiber itself.

    // Returns duration of stalling.
    Y_FORCE_INLINE std::optional<TDuration> SetRunning();
    Y_FORCE_INLINE void SetFinished();

    // NB(arkady-e1ppa): This does a relaxed
    // load. We expect that synchronisation
    // is done via external means.
    Y_FORCE_INLINE EFiberState GetState() const;

private:
    std::atomic<EFiberState> State_ = EFiberState::Created;
    std::atomic<TFiberId> FiberId_ = InvalidFiberId;

    // Guarded by State_.
    TInstant WaitingSince_ = TInstant::Zero();
    TFls* Fls_ = nullptr;

    EFiberState TryLockAsIntrospector() noexcept;
};

////////////////////////////////////////////////////////////////////////////////

class TFiberBase
    : public TIntrusiveListItem<TFiberBase, NDetail::TFiberRegisterTag>
    , public TIntrusiveListItem<TFiberBase, NDetail::TFiberUnregisterTag>
    , public TFiberIntrospectionBase
{
public:
    TFiber* AsFiber() noexcept;

protected:
    using TRegisterBase = TIntrusiveListItem<TFiberBase, NDetail::TFiberRegisterTag>;
    using TUnregisterBase = TIntrusiveListItem<TFiberBase, NDetail::TFiberUnregisterTag>;
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

// Do not change inheritence order or layout.
// Some offsets are hardcoded at devtools/gdb/yt_fibers_printer.py.
class TFiber
    : public NDetail::TFiberBase
    , public ITrampoLine
{
public:
    using TFiberList = TIntrusiveList<TFiberBase, NDetail::TFiberRegisterTag>;

    static TFiber* CreateFiber(EExecutionStackKind stackKind = EExecutionStackKind::Small);

    // Set this as AfterSwitch to release fiber's resources.
    static void ReleaseFiber(TFiber* fiber);

    // TODO(arkady-e1ppa): Make fiber be in charge of context switches
    // so that these methods are automatically inlined instead of manually called.
    void SetRunning();

    bool CheckFreeStackSpace(size_t space) const;

    TExceptionSafeContext* GetMachineContext();
    static void ReadFibers(TFunctionView<void(TFiberList&)> callback);

private:
    const std::shared_ptr<TExecutionStack> Stack_;
    TExceptionSafeContext MachineContext_;

    explicit TFiber(EExecutionStackKind stackKind = EExecutionStackKind::Small);
    ~TFiber();

    void DoRunNaked() override;

    void DeleteFiber() noexcept;

    friend class ::NYT::NConcurrency::TFiberRegistry;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
