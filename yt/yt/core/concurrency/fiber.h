#pragma once

#include "private.h"
#include "propagating_storage.h"
#include "fls.h"

#include <yt/yt/core/misc/intrusive_mpsc_stack.h>

#include <library/cpp/yt/misc/function_view.h>

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

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

// Do not change inheritence order or layout.
// Some offsets are hardcoded at devtools/gdb/yt_fibers_printer.py.
class TFiber
    : public TIntrusiveNode<TFiber, NDetail::TFiberRegisterTag>
    , public TIntrusiveNode<TFiber, NDetail::TFiberUnregisterTag>
    , public ITrampoLine
{
    using TRegisterBase = TIntrusiveNode<TFiber, NDetail::TFiberRegisterTag>;
    using TUnregisterBase = TIntrusiveNode<TFiber, NDetail::TFiberUnregisterTag>;

public:
    using TFiberList = TSimpleIntrusiveList<TFiber, NDetail::TFiberRegisterTag>;

    static TFiber* CreateFiber(EExecutionStackKind stackKind = EExecutionStackKind::Small);

    // Set this as AfterSwitch to release fiber's resources.
    static void ReleaseFiber(TFiber* fiber);

    ~TFiber();

    void Recreate();

    bool CheckFreeStackSpace(size_t space) const;

    TExceptionSafeContext* GetMachineContext();
    TFiberId GetFiberId() const;
    EFiberState GetState() const;

    void SetRunning();
    void SetWaiting();
    void SetIdle();

    bool TryIntrospectWaiting(EFiberState& state, const std::function<void()>& func);

    TInstant GetWaitingSince() const;
    const TPropagatingStorage& GetPropagatingStorage() const;
    TFls* GetFls() const;

    static void ReadFibers(TFunctionView<void(TFiberList&)> callback);

private:
    const std::shared_ptr<TExecutionStack> Stack_;

    TExceptionSafeContext MachineContext_;

    std::atomic<TFiberId> FiberId_ = InvalidFiberId;
    std::atomic<EFiberState> State_ = EFiberState::Created;
    std::atomic<TCpuInstant> WaitingSince_ = 0;

    std::unique_ptr<TFls> Fls_;

    explicit TFiber(EExecutionStackKind stackKind = EExecutionStackKind::Small);

    void SetFinished();
    void Clear();

    void DoRunNaked() override;

    void UnregisterAndDelete() noexcept;

    friend class ::NYT::NConcurrency::TFiberRegistry;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
