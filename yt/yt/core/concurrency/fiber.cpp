#include "fiber.h"

#include "execution_stack.h"

#include <yt/yt/core/profiling/timing.h>

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
public:
    //! Do not rename, change the signature, or drop Y_NO_INLINE.
    //! Used in devtools/gdb/yt_fibers_printer.py.
    static Y_NO_INLINE TFiberRegistry* Get()
    {
        return LeakySingleton<TFiberRegistry>();
    }

    TFiber::TCookie Register(TFiber* fiber)
    {
        auto guard = Guard(Lock_);
        return Fibers_.insert(Fibers_.begin(), fiber);
    }

    void Unregister(TFiber::TCookie cookie)
    {
        auto guard = Guard(Lock_);
        Fibers_.erase(cookie);
    }

    std::vector<TFiberPtr> List()
    {
        auto guard = Guard(Lock_);
        std::vector<TFiberPtr> fibers;
        for (const auto& fiber : Fibers_) {
            fibers.push_back(fiber);
        }
        return fibers;
    }

private:
    NThreading::TForkAwareSpinLock Lock_;
    std::list<TFiber*> Fibers_;
};

////////////////////////////////////////////////////////////////////////////////

TFiber::TFiber(EExecutionStackKind stackKind)
    : Stack_(CreateExecutionStack(stackKind))
    , RegistryCookie_(TFiberRegistry::Get()->Register(this))
    , MachineContext_({
        this,
        TArrayRef(static_cast<char*>(Stack_->GetStack()), Stack_->GetSize()),
    })
{
    TFiberProfiler::Get()->OnFiberCreated();
    TFiberProfiler::Get()->OnStackAllocated(Stack_->GetSize());
}

TFiber::~TFiber()
{
    YT_VERIFY(GetState() == EFiberState::Finished);
    TFiberProfiler::Get()->OnStackFreed(Stack_->GetSize());
    TFiberRegistry::Get()->Unregister(RegistryCookie_);
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
    Clear();
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

std::vector<TFiberPtr> TFiber::List()
{
    return TFiberRegistry::Get()->List();
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
