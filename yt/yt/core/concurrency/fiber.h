#pragma once

#include "private.h"
#include "propagating_storage.h"
#include "fls.h"

#include <util/system/context.h>

#include <atomic>
#include <list>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TFiber
    : public TRefCounted
    , public ITrampoLine
{
public:
    using TCookie = std::list<TFiber*>::iterator;

    explicit TFiber(EExecutionStackKind stackKind = EExecutionStackKind::Small);
    ~TFiber();

    void Recreate();

    bool CheckFreeStackSpace(size_t space) const;

    TExceptionSafeContext* GetMachineContext();
    TFiberId GetFiberId() const;
    EFiberState GetState() const;

    void SetRunning();
    void SetWaiting();
    void SetFinished();
    void SetIdle();

    bool TryIntrospectWaiting(EFiberState& state, const std::function<void()>& func);

    TInstant GetWaitingSince() const;
    const TPropagatingStorage& GetPropagatingStorage() const;
    TFls* GetFls() const;

    static std::vector<TFiberPtr> List();

private:
    const std::shared_ptr<TExecutionStack> Stack_;
    const TCookie RegistryCookie_;
    TExceptionSafeContext MachineContext_;

    std::atomic<TFiberId> FiberId_ = InvalidFiberId;
    std::atomic<EFiberState> State_ = EFiberState::Created;
    std::atomic<TCpuInstant> WaitingSince_ = 0;

    std::unique_ptr<TFls> Fls_;

    void Clear();

    void DoRunNaked() override;
};

DEFINE_REFCOUNTED_TYPE(TFiber)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
