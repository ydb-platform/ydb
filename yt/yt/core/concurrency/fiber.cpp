#include "fiber.h"

#include "execution_stack.h"

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/misc/intrusive_mpsc_stack.h>
#include <yt/yt/core/misc/singleton.h>
#include <yt/yt/core/misc/shutdown.h>
#include <yt/yt/core/misc/finally.h>

#include <yt/yt/library/profiling/producer.h>

#include <library/cpp/yt/threading/fork_aware_spin_lock.h>

#include <util/system/yield.h>

#include <util/random/random.h>

#if defined(_asan_enabled_)
    #include <yt/yt/core/misc/shutdown.h>
#endif

namespace NYT::NConcurrency {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = ConcurrencyLogger;

////////////////////////////////////////////////////////////////////////////////

class TFiberProfiler
    : public ISensorProducer
{
public:
    void OnStackAllocated(i64 stackSize)
    {
        StackBytesAllocated_.fetch_add(stackSize, std::memory_order::relaxed);
        StackBytesAlive_.fetch_add(stackSize, std::memory_order::relaxed);
    }

    void OnStackFreed(i64 stackSize)
    {
        StackBytesFreed_.fetch_add(stackSize, std::memory_order::relaxed);
        StackBytesAlive_.fetch_sub(stackSize, std::memory_order::relaxed);
    }

    void OnFiberCreated()
    {
        FibersCreated_.fetch_add(1, std::memory_order::relaxed);
    }

    static TFiberProfiler* Get()
    {
        return LeakyRefCountedSingleton<TFiberProfiler>().Get();
    }

private:
    std::atomic<i64> StackBytesAllocated_ = 0;
    std::atomic<i64> StackBytesFreed_ = 0;
    std::atomic<i64> StackBytesAlive_ = 0;
    std::atomic<i64> FibersCreated_ = 0;

    DECLARE_LEAKY_REF_COUNTED_SINGLETON_FRIEND()

    TFiberProfiler()
    {
        TProfiler("").AddProducer("/fiber", MakeStrong(this));
    }

    void CollectSensors(ISensorWriter* writer) override
    {
        writer->AddCounter("/created", FibersCreated_.load(std::memory_order::relaxed));

        writer->AddCounter("/stack/bytes_allocated", StackBytesAllocated_.load(std::memory_order::relaxed));
        writer->AddCounter("/stack/bytes_freed", StackBytesFreed_.load(std::memory_order::relaxed));
        writer->AddGauge("/stack/bytes_alive", StackBytesAlive_.load(std::memory_order::relaxed));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFiberRegistry
{
public:
    //! Do not rename, change the signature, or drop Y_NO_INLINE.
    //! Used in devtools/gdb/yt_fibers_printer.py.
    static Y_NO_INLINE TFiberRegistry* Get()
    {
        return LeakySingleton<TFiberRegistry>();
    }

    void Register(TFiber* fiber) noexcept
    {
        RegisterQueue_.Push(fiber);

        if (auto guard = TTryGuard(Lock_)) {
            GuardedProcessQueues();
        }
    }

    void Unregister(TFiber* fiber) noexcept
    {
        UnregisterQueue_.Push(fiber);

        if (auto guard = TTryGuard(Lock_)) {
            GuardedProcessQueues();
        }
    }

    void ReadFibers(TFunctionView<void(TFiber::TFiberList&)> callback)
    {
        auto guard = Guard(Lock_);

        GuardedProcessQueues();

        callback(Fibers_);

        GuardedProcessQueues();
    }

private:
    template <class Tag>
    using TFiberStack = TIntrusiveMpscStack<NDetail::TFiberBase, Tag>;

    TFiberStack<NDetail::TFiberRegisterTag> RegisterQueue_;
    TFiberStack<NDetail::TFiberUnregisterTag> UnregisterQueue_;

    YT_DECLARE_SPIN_LOCK(NThreading::TForkAwareSpinLock, Lock_);
    TFiber::TFiberList Fibers_;

// NB(arkady-e1ppa): This shutdown logic
// is only here to prevent potential memory
// leak caused by some fibers being stuck in
// registry. We don't really care about them
// cause realistically this is a "problem"
// only during the shutdown which means that
// process is going to be killed shortly after.
// In for asan we cleanup properly so that
// there are no actual leaks.
#if defined(_asan_enabled_)
    TShutdownCookie ShutdownCookie_;

    void InitializeShutdownCookie()
    {
        ShutdownCookie_ = RegisterShutdownCallback(
            "TFiberRegistry",
            BIND([this] {
                auto guard = Guard(Lock_);
                while(GuardedProcessQueues());
            }),
            /*priority*/ std::numeric_limits<int>::min());
    }

#endif

    // Returns |false| iff both queues
    // were observed empty.
    bool GuardedProcessQueues()
    {
#if defined(_asan_enabled_)
        if (!ShutdownCookie_) {
            InitializeShutdownCookie();
        }
#endif

        // NB(arkady-e1ppa): One thread can quickly
        // Register and then Unregister some fiber1.
        // Another thread running the GuardedProcessQueues
        // call has two options:
        // 1) Read RegisterQueue and then UnregisterQueue
        // 2) Inverse of (1)
        // In case of (1) we might miss fiber1 registration
        // event but still observe fiber1 unregistration event.
        // In this case we would unlink fiber and delete it.
        // Unlinking fiber which is still in RegisterQueue
        // Is almost guaranteed to cause a segfault, so
        // we cannot afford such scenario.
        // In case of (2) we might miss fiber1 unregistration
        // event but observe fiber1 registration event.
        // This would not cause a leak since we
        // clean up fibers during the shutdown anyway.
        auto toUnregister = UnregisterQueue_.PopAll();
        auto toRegister = RegisterQueue_.PopAll();

        if (toRegister.Empty() && toUnregister.Empty()) {
            return false;
        }

        Fibers_.Append(std::move(toRegister));

        // NB: util intrusive list does not return
        // nullptr in case of empty!
        // We have to check ourselves that
        // PopBack return is a valid one.
        while (!toUnregister.Empty()) {
            toUnregister.PopBack()->AsFiber()->DeleteFiber();
        }

        // NB: Around this line guard is released. We do not properly double check
        // if queues are actually empty after this.
        // We are okay with this since we expect to have occasional calls of this method
        // which would unstuck most of the fibers. In dtor of this singleton we
        // release the last batch of stuck fibers.

        return true;
    };

    void DebugPrint()
    {
        Cerr << "Debug print begin\n";
        Cerr << "---------------------------------------------------------------" << '\n';
        for (auto& iter : Fibers_) {
            auto* fiber = iter.AsFiber();
            auto* regNode = static_cast<TIntrusiveListItem<NDetail::TFiberBase, NDetail::TFiberRegisterTag>*>(fiber);
            auto* delNode = static_cast<TIntrusiveListItem<NDetail::TFiberBase, NDetail::TFiberUnregisterTag>*>(fiber);

            Cerr << Format("Fiber address after cast is %v", fiber) << '\n';
            Cerr << Format("Fiber registration queue status: Next: %v, Prev: %v", regNode->Next(), regNode->Prev()) << '\n';
            // NB: Reading deletion queue is data race. Don't do this under tsan.
            Cerr << Format("Fiber deletion queue status: Next: %v, Prev: %v", delNode->Next(), delNode->Prev()) << '\n';
            Cerr << "---------------------------------------------------------------" << '\n';
        }

        Cerr << "Debug print end\n";
    }
};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

EFiberState TFiberIntrospectionBase::TryLockAsIntrospector() noexcept
{
    auto state = GetState();
    if (state != EFiberState::Waiting) {
        // Locked by running fiber.
        return state;
    }

    State_.compare_exchange_strong(
        state,
        EFiberState::Introspecting,
        std::memory_order::acquire,
        std::memory_order::relaxed);

    return state;
}

bool TFiberIntrospectionBase::TryLockForIntrospection(EFiberState* state, TFunctionView<void()> successHandler)
{
    auto& stateRef = *state;
    stateRef = TryLockAsIntrospector();
    if (stateRef != EFiberState::Waiting) {
        // Locked by running fiber.
        return false;
    }

    auto guard = Finally([&] {
        // Release lock held by introspector.
        YT_VERIFY(State_.load(std::memory_order::relaxed) == EFiberState::Introspecting);
        State_.store(EFiberState::Waiting, std::memory_order::release);
    });

    successHandler();

    return true;
}

TFiberId TFiberIntrospectionBase::GetFiberId() const
{
    return FiberId_.load(std::memory_order::relaxed);
}

TInstant TFiberIntrospectionBase::GetWaitingSince() const
{
    // Locked by introspector
    YT_VERIFY(GetState() == EFiberState::Introspecting);
    return WaitingSince_;
}

TFls* TFiberIntrospectionBase::GetFls()
{
    // Locked by introspector
    YT_VERIFY(GetState() == EFiberState::Introspecting);
    return Fls_;
}

void TFiberIntrospectionBase::OnCallbackExecutionStarted(TFiberId fiberId, TFls* fls)
{
    // Locked by running fiber.
    YT_VERIFY(GetState() == EFiberState::Running);

    FiberId_.store(fiberId, std::memory_order::relaxed);
    Fls_ = fls;
}

void TFiberIntrospectionBase::OnCallbackExecutionFinished()
{
    // Locked by running fiber.
    YT_VERIFY(GetState() == EFiberState::Running);

    FiberId_.store(InvalidFiberId, std::memory_order::relaxed);
    Fls_ = nullptr;
}

void TFiberIntrospectionBase::SetWaiting()
{
    WaitingSince_ = CpuInstantToInstant(GetApproximateCpuInstant());

    // Release lock that should be acquired by running fiber.
    YT_VERIFY(State_.load(std::memory_order::relaxed) == EFiberState::Running);
    State_.store(EFiberState::Waiting, std::memory_order::release);
}

void TFiberIntrospectionBase::SetIdle()
{
    // 1) Locked by running fiber.
    // 2) Reading this doesn't cause anything to happen.
    // This state is never checked for, so just relaxed.
    YT_VERIFY(State_.load(std::memory_order::relaxed) == EFiberState::Running);
    State_.store(EFiberState::Idle, std::memory_order::relaxed);
}

std::optional<TDuration> TFiberIntrospectionBase::SetRunning()
{
    auto observed = GetState();
    std::optional<NProfiling::TWallTimer> lockedTimer;

    do {
        // NB(arkady-e1ppa): We expect SetRunning to have
        // either SetIdle, SetWaiting or Ctor in HB relation to this call.
        // If we have SetIdle in HB relation then we must
        // observed |Idle|.
        // If we have SetWaiting in HB then we either observer
        // |Waiting| by w-r coherence or |Introspecting|
        // written by RWM from TryLockForIntrospection
        // which observed mentioned SetWaiting write.
        // If Ctor is in HB then this is the first op
        // on State_ and thus guaranteed to read |Created|.
        // This leaves |Running| and |Finished| impossible.
        YT_VERIFY(observed != EFiberState::Running);
        if (observed == EFiberState::Introspecting) {
            if (!lockedTimer) {
                lockedTimer.emplace();
            }
            ThreadYield();
            observed = GetState();
            continue;
        }
        // TryAcquire lock.
    } while (!State_.compare_exchange_weak(
        observed,
        EFiberState::Running,
        std::memory_order::acquire,
        std::memory_order::relaxed));

    return lockedTimer.has_value()
        ? std::optional(lockedTimer->GetElapsedTime())
        : std::nullopt;
}

void TFiberIntrospectionBase::SetFinished()
{
    // NB(arkady-e1ppa): This state is relevant for
    // two reasons:
    // 1) For dtor of fiber -- see ~Fiber for details
    // 2) For verifying that fiber is attempted to be
    // deleted exactly one. In such case (though we
    // check it only in debug mode) exchange reads
    // the last modification and if this function
    // is called twice regardless of situation
    // one of the calls will crash as it should.
    YT_VERIFY(State_.load(std::memory_order::relaxed) == EFiberState::Running);
    State_.store(EFiberState::Finished, std::memory_order::relaxed);
}

EFiberState TFiberIntrospectionBase::GetState() const
{
    return State_.load(std::memory_order::relaxed);
}

TFiber* TFiberBase::AsFiber() noexcept
{
    return static_cast<TFiber*>(this);
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TFiber* TFiber::CreateFiber(EExecutionStackKind stackKind)
{
    auto* fiber = new TFiber(stackKind);
    TFiberRegistry::Get()->Register(fiber);
    return fiber;
}

void TFiber::ReleaseFiber(TFiber* fiber)
{
    YT_VERIFY(fiber);
    fiber->SetFinished();
    TFiberRegistry::Get()->Unregister(fiber);
}

void TFiber::SetRunning()
{
    if (auto delay = TFiberBase::SetRunning()) {
        YT_LOG_WARNING(
            "Fiber execution was delayed due to introspection (FiberId: %x, Delay: %v)",
            GetFiberId(),
            *delay);
    }
}

bool TFiber::CheckFreeStackSpace(size_t space) const
{
    return reinterpret_cast<char*>(Stack_->GetStack()) + space < __builtin_frame_address(0);
}

TExceptionSafeContext* TFiber::GetMachineContext()
{
    return &MachineContext_;
}

void TFiber::ReadFibers(TFunctionView<void(TFiberList&)> callback)
{
    return TFiberRegistry::Get()->ReadFibers(callback);
}

TFiber::TFiber(EExecutionStackKind stackKind)
    : Stack_(CreateExecutionStack(stackKind))
    , MachineContext_({
        .TrampoLine = this,
        .Stack = TArrayRef(static_cast<char*>(Stack_->GetStack()), Stack_->GetSize()),
    })
{
    TFiberProfiler::Get()->OnFiberCreated();
    TFiberProfiler::Get()->OnStackAllocated(Stack_->GetSize());
}

TFiber::~TFiber()
{
    // What happens:
    // SetFinished
    // Sequenced-before
    // Enqueue in deletion queue
    // Synchronizes-with
    // Dequeue from deletion queue (can be from another thread)
    // Sequenced-before
    // DeleteFiber
    // Sequence-before
    // GetState
    // Since this is the only possible chain of events
    // we are safe to use |GetState| (which does
    // relaxed load).
    YT_VERIFY(GetState() == EFiberState::Finished);
    TFiberProfiler::Get()->OnStackFreed(Stack_->GetSize());
}

void TFiber::DeleteFiber() noexcept
{
    YT_VERIFY(static_cast<TUnregisterBase*>(this)->Empty());
    YT_VERIFY(!static_cast<TRegisterBase*>(this)->Empty());

    // NB(arkady-e1ppa): Pair of events such as
    // RegisterFiber and UnregisterFiber are always
    // processes in the mentioned order.
    // Since this is the case, every fiber
    // is supposed to be inserted in the registry
    // exactly once. And then removed from the registry
    // also exactly once. This means that at this point
    // we must be linked in the FiberList_.
    YT_VERIFY(!static_cast<TRegisterBase*>(this)->Empty());

    static_cast<TRegisterBase*>(this)->Unlink();
    delete this;
}

namespace NDetail {

void FiberTrampoline();

} // namespace NDetail

void TFiber::DoRunNaked()
{
    NDetail::FiberTrampoline();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
