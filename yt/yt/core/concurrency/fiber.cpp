#include "fiber.h"

#include "execution_stack.h"

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/misc/intrusive_mpsc_stack.h>
#include <yt/yt/core/misc/singleton.h>
#include <yt/yt/core/misc/finally.h>

#include <yt/yt/library/profiling/producer.h>

#include <library/cpp/yt/threading/fork_aware_spin_lock.h>

#include <util/system/yield.h>

#include <util/random/random.h>

namespace NYT::NConcurrency {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ConcurrencyLogger;

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

class TFiberIdGenerator
{
public:
    static TFiberIdGenerator* Get()
    {
        return LeakySingleton<TFiberIdGenerator>();
    }

    TFiberId Generate()
    {
        const TFiberId Factor = std::numeric_limits<TFiberId>::max() - 173864;
        YT_ASSERT(Factor % 2 == 1); // Factor must be coprime with 2^n.

        while (true) {
            auto seed = Seed_++;
            auto id = seed * Factor;
            if (id != InvalidFiberId) {
                return id;
            }
        }
    }

private:
    std::atomic<TFiberId> Seed_;

    DECLARE_LEAKY_SINGLETON_FRIEND()

    TFiberIdGenerator()
    {
        Seed_.store(static_cast<TFiberId>(::time(nullptr)));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFiberRegistry
{
    template <class Tag>
    using TFiberStack = TIntrusiveMPSCStack<TFiber, Tag>;

public:
    //! Do not rename, change the signature, or drop Y_NO_INLINE.
    //! Used in devtools/gdb/yt_fibers_printer.py.
    static Y_NO_INLINE TFiberRegistry* Get()
    {
        return LeakySingleton<TFiberRegistry>();
    }

    void Register(TFiber* fiber)
    {
        RegisterQueue_.Push(fiber);

        if (auto guard = TTryGuard(Lock_)) {
            GuardedProcessQueues();
        }
    }

    void Unregister(TFiber* fiber)
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

    ~TFiberRegistry()
    {
        GuardedProcessQueues();
    }

private:
    TFiberStack<NDetail::TFiberRegisterTag> RegisterQueue_;
    TFiberStack<NDetail::TFiberUnregisterTag> UnregisterQueue_;

    NThreading::TForkAwareSpinLock Lock_;
    TFiber::TFiberList Fibers_;

    void GuardedProcessQueues()
    {
        Fibers_.Append(RegisterQueue_.PopAll());

        auto toUnregister = UnregisterQueue_.PopAll();

        while (auto fiber = toUnregister.PopBack()) {
            fiber->UnregisterAndDelete();
        }

        // NB: Around this line guard is released. We do not properly double check
        // if queues are actually empty after this.
        // We are okay with this since we expect to have occasional calls of this method
        // which would unstuck most of the fibers. In dtor of this singleton we
        // release the last batch of stuck fibers.
    };

    void DebugPrint()
    {
        Cerr << "Debug print begin\n";
        Cerr << "---------------------------------------------------------------" << '\n';
        for (auto* iter = Fibers_.Begin(); iter != Fibers_.End(); iter = iter->Next) {
            auto* fiber = static_cast<TFiber*>(iter);
            auto* regNode = static_cast<TIntrusiveNode<TFiber, NDetail::TFiberRegisterTag>*>(fiber);
            auto* delNode = static_cast<TIntrusiveNode<TFiber, NDetail::TFiberUnregisterTag>*>(fiber);

            Cerr << Format("Fiber node at %v. Next is %v, Prev is %v", iter, iter->Next, iter->Prev) << '\n';
            Cerr << Format("Fiber address after cast is %v", fiber) << '\n';
            Cerr << Format("Fiber registration queue status: Next: %v, Prev: %v", regNode->Next, regNode->Prev) << '\n';
            // NB: Reading deletion queue is data race. Don't do this under tsan.
            Cerr << Format("Fiber deletion queue status: Next: %v, Prev: %v", delNode->Next, delNode->Prev) << '\n';
            Cerr << "---------------------------------------------------------------" << '\n';
        }
        Cerr << "Debug print end\n";
    }
};

////////////////////////////////////////////////////////////////////////////////

TFiber* TFiber::CreateFiber(EExecutionStackKind stackKind)
{
    return new TFiber(stackKind);
}

void TFiber::ReleaseFiber(TFiber* fiber)
{
    YT_VERIFY(fiber);
    fiber->SetFinished();
    fiber->Clear();
    TFiberRegistry::Get()->Unregister(fiber);
}

TFiber::TFiber(EExecutionStackKind stackKind)
    : Stack_(CreateExecutionStack(stackKind))
    , MachineContext_({
        this,
        TArrayRef(static_cast<char*>(Stack_->GetStack()), Stack_->GetSize()),
    })
{
    TFiberRegistry::Get()->Register(this);
    TFiberProfiler::Get()->OnFiberCreated();
    TFiberProfiler::Get()->OnStackAllocated(Stack_->GetSize());
}

TFiber::~TFiber()
{
    YT_VERIFY(GetState() == EFiberState::Finished);
    TFiberProfiler::Get()->OnStackFreed(Stack_->GetSize());
}

bool TFiber::CheckFreeStackSpace(size_t space) const
{
    return reinterpret_cast<char*>(Stack_->GetStack()) + space < __builtin_frame_address(0);
}

TExceptionSafeContext* TFiber::GetMachineContext()
{
    return &MachineContext_;
}

TFiberId TFiber::GetFiberId() const
{
    return FiberId_.load(std::memory_order::relaxed);
}


EFiberState TFiber::GetState() const
{
    return State_.load(std::memory_order::relaxed);
}

void TFiber::SetRunning()
{
    auto expectedState = State_.load(std::memory_order::relaxed);
    std::optional<NProfiling::TWallTimer> lockedTimer;
    do {
        YT_VERIFY(expectedState != EFiberState::Running);
        if (expectedState == EFiberState::Introspecting) {
            if (!lockedTimer) {
                lockedTimer.emplace();
            }
            ThreadYield();
            expectedState = State_.load();
            continue;
        }
    } while (!State_.compare_exchange_weak(expectedState, EFiberState::Running));

    if (lockedTimer) {
        YT_LOG_WARNING("Fiber execution was delayed due to introspection (FiberId: %x, Delay: %v)",
            GetFiberId(),
            lockedTimer->GetElapsedTime());
    }
}

void TFiber::SetWaiting()
{
    WaitingSince_.store(GetApproximateCpuInstant(), std::memory_order::release);
    State_.store(EFiberState::Waiting, std::memory_order::release);
}

void TFiber::SetFinished()
{
    State_.store(EFiberState::Finished);
}

void TFiber::SetIdle()
{
    State_.store(EFiberState::Idle);
    Clear();
}

bool TFiber::TryIntrospectWaiting(EFiberState& state, const std::function<void()>& func)
{
    state = State_.load();
    if (state != EFiberState::Waiting) {
        return false;
    }
    if (!State_.compare_exchange_strong(state, EFiberState::Introspecting)) {
        return false;
    }
    auto guard = Finally([&] {
        YT_VERIFY(State_.exchange(state) == EFiberState::Introspecting);
    });
    func();
    return true;
}

TInstant TFiber::GetWaitingSince() const
{
    YT_VERIFY(State_.load() == EFiberState::Introspecting);
    return CpuInstantToInstant(WaitingSince_.load());
}

const TPropagatingStorage& TFiber::GetPropagatingStorage() const
{
    YT_VERIFY(State_.load() == EFiberState::Introspecting);
    return NConcurrency::GetPropagatingStorage(*Fls_);
}

TFls* TFiber::GetFls() const
{
    return Fls_.get();
}

void TFiber::Recreate()
{
    FiberId_.store(TFiberIdGenerator::Get()->Generate(), std::memory_order::release);
    Fls_ = std::make_unique<TFls>();
}

void TFiber::Clear()
{
    FiberId_.store(InvalidFiberId);
    Fls_.reset();
}

void TFiber::ReadFibers(TFunctionView<void(TFiberList&)> callback)
{
    return TFiberRegistry::Get()->ReadFibers(callback);
}

void TFiber::UnregisterAndDelete() noexcept
{
    YT_VERIFY(!static_cast<TUnregisterBase*>(this)->IsLinked());

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
