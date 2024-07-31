#include "fiber_scheduler_thread.h"

#include "fiber.h"
#include "moody_camel_concurrent_queue.h"
#include "private.h"

#include <yt/yt/library/profiling/producer.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/shutdown.h>
#include <yt/yt/core/misc/singleton.h>

#include <library/cpp/yt/threading/spin_lock_count.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <library/cpp/yt/memory/memory_tag.h>

#include <library/cpp/yt/memory/function_view.h>

#include <library/cpp/yt/threading/fork_aware_spin_lock.h>

#include <util/thread/lfstack.h>

#include <thread>

#if defined(_linux_) && !defined(NDEBUG)
    #define YT_ENABLE_TLS_ADDRESS_TRACKING
#endif

#ifdef YT_ENABLE_TLS_ADDRESS_TRACKING
    #include <unistd.h>
    #include <sys/types.h>
    #include <sys/syscall.h>
#endif

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

using namespace NLogging;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = ConcurrencyLogger;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

DECLARE_REFCOUNTED_CLASS(TRefCountedGauge)

class TRefCountedGauge
    : public TRefCounted
    , public NProfiling::TGauge
{
public:
    TRefCountedGauge(const NProfiling::TRegistry& profiler, const TString& name)
        : NProfiling::TGauge(profiler.Gauge(name))
    { }

    void Increment(i64 delta)
    {
        auto value = Value_.fetch_add(delta, std::memory_order::relaxed) + delta;
        NProfiling::TGauge::Update(value);
    }

private:
    std::atomic<i64> Value_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TRefCountedGauge)

////////////////////////////////////////////////////////////////////////////////

// TODO(arkady-e1ppa): Add noexcept perhaps?
using TAfterSwitch = TFunctionView<void()>;

// NB: We use MakeAfterSwitch to wrap our lambda in order to move closure
// on the caller's stack (initially it is on the suspended fiber's stack).
// We do that because callback can resume fiber which will destroy the
// closure on its stack creating the risk of stack-use-after-scope.
// The only safe place at that moment is caller's stack frame.
template <CInvocable<void()> T>
auto MakeAfterSwitch(T&& lambda)
{
    class TMoveOnCall
    {
    public:
        explicit TMoveOnCall(T&& lambda)
            : Lambda_(std::move(lambda))
        { }

        void operator()()
        {
            auto lambda = std::move(Lambda_);
            lambda();
        }

    private:
        T Lambda_;
    };

    return TMoveOnCall(std::move(lambda));
}

////////////////////////////////////////////////////////////////////////////////

void RunInFiberContext(TFiber* fiber, TClosure callback);
void SwitchFromThread(TFiber* targetFiber);

////////////////////////////////////////////////////////////////////////////////

// Non POD TLS sometimes does not work correctly in dynamic library.
struct TFiberContext
{
    TFiberContext() = default;

    TFiberContext(
        TFiberSchedulerThread* fiberThread,
        const TString& threadGroupName)
        : FiberThread(fiberThread)
        , WaitingFibersCounter(New<NDetail::TRefCountedGauge>(
            NProfiling::TRegistry("/fiber").WithTag("thread", threadGroupName).WithHot(),
            "/waiting"))
    { }

    TFiberSchedulerThread* const FiberThread = nullptr;
    const TRefCountedGaugePtr WaitingFibersCounter;

    TExceptionSafeContext MachineContext;
    TAfterSwitch AfterSwitch;
    TFiber* ResumerFiber = nullptr;
    TFiber* CurrentFiber = nullptr;
};

YT_DEFINE_THREAD_LOCAL(TFiberContext*, FiberContext, nullptr);

// Forbid inlining these accessors to prevent the compiler from
// miss-optimizing TLS access in presence of fiber context switches.
TFiberContext* TryGetFiberContext()
{
    return FiberContext();
}

void SetFiberContext(TFiberContext* context)
{
    FiberContext() = context;
}

////////////////////////////////////////////////////////////////////////////////

class TFiberContextGuard
{
public:
    explicit TFiberContextGuard(TFiberContext* context)
        : Prev_(TryGetFiberContext())
    {
        SetFiberContext(context);
    }

    ~TFiberContextGuard()
    {
        SetFiberContext(Prev_);
    }

    TFiberContextGuard(const TFiberContextGuard&) = delete;
    TFiberContextGuard operator=(const TFiberContextGuard&) = delete;

private:
    TFiberContext* Prev_;
};

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TMemoryTag SwapMemoryTag(TMemoryTag tag)
{
    auto result = GetCurrentMemoryTag();
    SetCurrentMemoryTag(tag);
    return result;
}

Y_FORCE_INLINE TFiberId SwapCurrentFiberId(TFiberId fiberId)
{
    auto result = GetCurrentFiberId();
    SetCurrentFiberId(fiberId);
    return result;
}

Y_FORCE_INLINE ELogLevel SwapMinLogLevel(ELogLevel minLogLevel)
{
    auto result = GetThreadMinLogLevel();
    SetThreadMinLogLevel(minLogLevel);
    return result;
}

Y_FORCE_INLINE TExceptionSafeContext* GetMachineContext()
{
    return &FiberContext()->MachineContext;
}

Y_FORCE_INLINE void SetAfterSwitch(TAfterSwitch afterSwitch)
{
    auto* context = TryGetFiberContext();
    YT_VERIFY(!context->AfterSwitch.IsValid());
    context->AfterSwitch = afterSwitch;
}

Y_FORCE_INLINE TAfterSwitch ExtractAfterSwitch()
{
    auto* context = FiberContext();
    return context->AfterSwitch.Release();
}

Y_FORCE_INLINE void SetResumerFiber(TFiber* fiber)
{
    auto* context = FiberContext();
    YT_VERIFY(!context->ResumerFiber);
    context->ResumerFiber = fiber;
}

Y_FORCE_INLINE TFiber* ExtractResumerFiber()
{
    return std::exchange(FiberContext()->ResumerFiber, nullptr);
}

Y_FORCE_INLINE TFiber* TryGetResumerFiber()
{
    return FiberContext()->ResumerFiber;
}

Y_FORCE_INLINE TFiber* SwapCurrentFiber(TFiber* fiber)
{
    return std::exchange(FiberContext()->CurrentFiber, fiber);
}

Y_FORCE_INLINE TFiber* TryGetCurrentFiber()
{
    auto* context = FiberContext();
    return context ? context->CurrentFiber : nullptr;
}

Y_FORCE_INLINE TFiber* GetCurrentFiber()
{
    auto* fiber = FiberContext()->CurrentFiber;
    YT_VERIFY(fiber);
    return fiber;
}

Y_FORCE_INLINE TFiberSchedulerThread* TryGetFiberThread()
{
    return FiberContext()->FiberThread;
}

Y_FORCE_INLINE TRefCountedGaugePtr GetWaitingFibersCounter()
{
    return FiberContext()->WaitingFibersCounter;
}

////////////////////////////////////////////////////////////////////////////////

void RunAfterSwitch()
{
    if (auto afterSwitch = ExtractAfterSwitch()) {
        afterSwitch();
    }
}

void SwitchMachineContext(TExceptionSafeContext* from, TExceptionSafeContext* to)
{
    from->SwitchTo(to);

    RunAfterSwitch();

    // TODO(lukyan): Allow to set after switch inside itself
    YT_VERIFY(!ExtractAfterSwitch());
}

void SwitchFromThread(TFiber* targetFiber)
{
    YT_ASSERT(targetFiber);

    targetFiber->SetRunning();

    auto* targetContext = targetFiber->GetMachineContext();
    auto currentFiber = SwapCurrentFiber(targetFiber);
    YT_VERIFY(!currentFiber);

    SwitchMachineContext(GetMachineContext(), targetContext);

    YT_VERIFY(!TryGetCurrentFiber());
}

[[noreturn]] void SwitchToThread(TAfterSwitch afterSwitch)
{
    auto currentFiber = SwapCurrentFiber(nullptr);
    auto* currentContext = currentFiber->GetMachineContext();

    SetAfterSwitch(afterSwitch);
    SwitchMachineContext(currentContext, GetMachineContext());

    YT_ABORT();
}

void SwitchFromFiber(TFiber* targetFiber, TAfterSwitch afterSwitch)
{
    YT_ASSERT(targetFiber);

    targetFiber->SetRunning();
    auto* targetContext = targetFiber->GetMachineContext();

    auto currentFiber = SwapCurrentFiber(targetFiber);
    auto* currentContext = currentFiber->GetMachineContext();

    SetAfterSwitch(afterSwitch);
    SwitchMachineContext(currentContext, targetContext);

    YT_VERIFY(TryGetCurrentFiber() == currentFiber);
}

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

class TIdleFiberPool
{
public:
    static TIdleFiberPool* Get()
    {
        return LeakySingleton<TIdleFiberPool>();
    }

    // NB(lukyan): Switch out and add fiber to idle fibers.
    // Save fiber in AfterSwitch because it can be immediately concurrently reused.
    void SwichFromFiberAndMakeItIdle(TFiber* currentFiber, TFiber* targetFiber)
    {
        RemoveOverdrawnIdleFibers();

        auto afterSwitch = MakeAfterSwitch([currentFiber, this] {
            currentFiber->SetIdle();
            EnqueueIdleFiber(currentFiber);
        });

        SwitchFromFiber(targetFiber, afterSwitch);
    }

    TFiber* GetFiber()
    {
        if (auto* fiber = TryDequeueIdleFiber()) {
            return fiber;
        }

        return TFiber::CreateFiber();
    }

    void UpdateMaxIdleFibers(int maxIdleFibers)
    {
        MaxIdleFibers_.store(maxIdleFibers, std::memory_order::relaxed);
    }

private:
    moodycamel::ConcurrentQueue<TFiber*> IdleFibers_;
    std::atomic<int> MaxIdleFibers_ = DefaultMaxIdleFibers;

    // NB(arkady-e1ppa): Construct this last so that every other
    // field is initialized if this callback is ran concurrently.
    const TShutdownCookie ShutdownCookie_ = RegisterShutdownCallback(
        "IdleFiberPool",
        BIND_NO_PROPAGATE(&TIdleFiberPool::Shutdown, this),
        /*priority*/ std::numeric_limits<int>::min() + 1);

    void Shutdown()
    {
    #ifdef _unix_
        // The current thread could be already exiting and MacOS has some issues
        // with registering new thread-local terminators in this case:
        // https://github.com/lionheart/openradar-mirror/issues/20926
        // As a matter of workaround, we offload all finalization logic to a separate
        // temporary thread.
        std::thread thread([&] {
            ::TThread::SetCurrentThreadName("IdleFiberDtor");

            JoinAllFibers();
        });
        thread.join();
    #else
        // Starting threads in exit handlers on Windows causes immediate calling exit
        // so the routine will not be executed. Moreover, if we try to join this thread we'll get deadlock
        // because this thread will try to acquire atexit lock which is owned by this thread.
        JoinAllFibers();
    #endif
    }

    void JoinAllFibers()
    {
        std::vector<TFiber*> fibers;

        while (true) {
            auto size = std::max<size_t>(1, IdleFibers_.size_approx());

            DequeueFibersBulk(&fibers, size);
            if (fibers.empty()) {
                break;
            }
            JoinFibers(std::move(fibers));
        }
    }

    void JoinFibers(std::vector<TFiber*>&& fibers)
    {
        TFiberContext fiberContext;
        TFiberContextGuard fiberContextGuard(&fiberContext);

        for (auto fiber : fibers) {
            // Fibers will observe nullptr fiberThread
            // and switch back with deleter in afterSwitch.
            SwitchFromThread(fiber);
        }
    }

    void RemoveOverdrawnIdleFibers()
    {
        // NB: size_t to int conversion.
        int size = IdleFibers_.size_approx();
        if (size <= MaxIdleFibers_.load(std::memory_order::relaxed)) {
            return;
        }

        auto targetSize = std::max<size_t>(1, MaxIdleFibers_ / 2);

        std::vector<TFiber*> fibers;
        DequeueFibersBulk(&fibers, size - targetSize);
        if (fibers.empty()) {
            return;
        }
        JoinFibers(std::move(fibers));
    }

    void DequeueFibersBulk(std::vector<TFiber*>* fibers, int count)
    {
        fibers->resize(count);
        auto dequeued = IdleFibers_.try_dequeue_bulk(std::begin(*fibers), count);
        fibers->resize(dequeued);
    }

    void EnqueueIdleFiber(TFiber* fiber)
    {
        IdleFibers_.enqueue(fiber);
    }

    TFiber* TryDequeueIdleFiber()
    {
        TFiber* fiber = nullptr;
        IdleFibers_.try_dequeue(fiber);
        return fiber;
    }

    DECLARE_LEAKY_SINGLETON_FRIEND()
};

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TClosure PickCallback(TFiberSchedulerThread* fiberThread)
{
    TCallback<void()> callback;
    // We wrap fiberThread->OnExecute() into a propagating storage guard to ensure
    // that the propagating storage created there won't spill into the fiber callbacks.

    TNullPropagatingStorageGuard guard;
    YT_VERIFY(guard.GetOldStorage().IsEmpty());
    callback = fiberThread->OnExecute();

    return callback;
}

////////////////////////////////////////////////////////////////////////////////

void FiberTrampoline()
{
    RunAfterSwitch();

    YT_LOG_DEBUG("Fiber started");

    auto* currentFiber = GetCurrentFiber();
    TFiber* successorFiber = nullptr;

    // Break loop to terminate fiber
    while (auto* fiberThread = TryGetFiberThread()) {
        YT_VERIFY(!TryGetResumerFiber());
        YT_VERIFY(CurrentFls() == nullptr);

        YT_VERIFY(GetCurrentPropagatingStorage().IsEmpty());

        auto callback = PickCallback(fiberThread);

        if (!callback) {
            break;
        }

        try {
            RunInFiberContext(currentFiber, std::move(callback));
        } catch (const TFiberCanceledException&) {
            // Just swallow.
        }

        // Trace context can be restored for resumer fiber, so current trace context and memory tag are
        // not necessarily null. Check them after switch from and returning into current fiber.
        if (successorFiber = ExtractResumerFiber()) {
            // Suspend current fiber.
            TIdleFiberPool::Get()->SwichFromFiberAndMakeItIdle(currentFiber, successorFiber);
        }
    }

    YT_LOG_DEBUG("Fiber finished");

    auto afterSwitch = MakeAfterSwitch([currentFiber] () mutable {
        TFiber::ReleaseFiber(currentFiber);
    });

    // All allocated objects in this frame must be destroyed here.
    SwitchToThread(afterSwitch);
}

void YieldFiber(TAfterSwitch afterSwitch)
{
    YT_VERIFY(TryGetCurrentFiber());

    // Try to get resumer.
    auto targetFiber = ExtractResumerFiber();

    // If there is no resumer switch to idle fiber. Or switch to thread main.
    if (!targetFiber) {
        targetFiber = TIdleFiberPool::Get()->GetFiber();
    }

    auto waitingFibersCounter = GetWaitingFibersCounter();
    waitingFibersCounter->Increment(1);

    SwitchFromFiber(targetFiber, afterSwitch);
    YT_VERIFY(TryGetResumerFiber());

    waitingFibersCounter->Increment(-1);
}

void ResumeFiber(TFiber* targetFiber)
{
    auto currentFiber = GetCurrentFiber();

    SetResumerFiber(currentFiber);
    auto afterSwitch = MakeAfterSwitch([currentFiber] {
        currentFiber->SetWaiting();
    });

    SwitchFromFiber(targetFiber, afterSwitch);

    YT_VERIFY(!TryGetResumerFiber());
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TCanceler)

class TCanceler
    : public ::NYT::NDetail::TBindStateBase
{
public:
    explicit TCanceler(TFiberId id)
        : TBindStateBase(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
            TSourceLocation("", 0)
#endif
        )
        , FiberId_(id)
    { }

    bool IsCanceled() const
    {
        return Canceled_.load(std::memory_order::relaxed);
    }

    void SetFuture(TFuture<void> awaitable)
    {
        auto guard = Guard(Lock_);
        Future_ = std::move(awaitable);
    }

    void ResetFuture()
    {
        auto guard = Guard(Lock_);
        Future_.Reset();
    }

    void Cancel(const TError& error)
    {
        bool expected = false;
        if (!Canceled_.compare_exchange_strong(expected, true, std::memory_order::relaxed)) {
            return;
        }

        TFuture<void> future;
        {
            auto guard = Guard(Lock_);
            CancelationError_ = error;
            future = std::move(Future_);
        }

        if (future) {
            YT_LOG_DEBUG("Sending cancelation to fiber, propagating to the awaited future (TargetFiberId: %x)",
                FiberId_);
            future.Cancel(error);
        } else {
            YT_LOG_DEBUG("Sending cancelation to fiber (TargetFiberId: %x)",
                FiberId_);
        }
    }

    TError GetCancelationError() const
    {
        auto guard = Guard(Lock_);
        return CancelationError_;
    }

    void Run(const TError& error)
    {
        Cancel(error);
    }

    static void StaticInvoke(const TError& error, NYT::NDetail::TBindStateBase* stateBase)
    {
        auto* state = static_cast<TCanceler*>(stateBase);
        return state->Run(error);
    }

    TFiberId GetFiberId() const
    {
        return FiberId_;
    }

private:
    const TFiberId FiberId_;

    std::atomic<bool> Canceled_ = false;
    NThreading::TSpinLock Lock_;
    TError CancelationError_;
    TFuture<void> Future_;
};

DEFINE_REFCOUNTED_TYPE(TCanceler)

////////////////////////////////////////////////////////////////////////////////

class TContextSwitchManager
{
public:
    static TContextSwitchManager* Get()
    {
        return Singleton<TContextSwitchManager>();
    }

    void RegisterGlobalHandlers(
        TGlobalContextSwitchHandler outHandler,
        TGlobalContextSwitchHandler inHandler)
    {
        auto guard = Guard(Lock_);
        int index = HandlerCount_.load();
        YT_VERIFY(index < MaxHandlerCount);
        Handlers_[index] = {outHandler, inHandler};
        ++HandlerCount_;
    }

    void OnOut()
    {
        int count = HandlerCount_.load(std::memory_order::acquire);
        for (int index = 0; index < count; ++index) {
            if (const auto& handler = Handlers_[index].Out) {
                handler();
            }
        }
    }

    void OnIn()
    {
        int count = HandlerCount_.load();
        for (int index = count - 1; index >= 0; --index) {
            if (const auto& handler = Handlers_[index].In) {
                handler();
            }
        }
    }

private:
    NThreading::TForkAwareSpinLock Lock_;

    struct TGlobalContextSwitchHandlers
    {
        TGlobalContextSwitchHandler Out;
        TGlobalContextSwitchHandler In;
    };

    static constexpr int MaxHandlerCount = 16;
    std::array<TGlobalContextSwitchHandlers, MaxHandlerCount> Handlers_;
    std::atomic<int> HandlerCount_ = 0;

    TContextSwitchManager() = default;
    Y_DECLARE_SINGLETON_FRIEND()
};

////////////////////////////////////////////////////////////////////////////////

//! This class wraps reads of TLS saving the reader thread id.
//! Rereads of the TLS compare the thread id and crash if
//! TLS was cached when it shouldn't have been.
template <class T>
class TTlsAddressStorage
{
public:
    TTlsAddressStorage() = default;

    template <CInvocable<T*()> TTlsReader>
    Y_FORCE_INLINE explicit TTlsAddressStorage(TTlsReader reader)
    {
#ifdef YT_ENABLE_TLS_ADDRESS_TRACKING
        Tid_ = GetTid();
#endif

        Address_ = reader();
    }

    Y_FORCE_INLINE T& operator*()
    {
        return *Address_;
    }

    template <CInvocable<T*()> TTlsReader>
    Y_FORCE_INLINE void ReReadAddress(TTlsReader reader)
    {
        auto* address = reader();

#ifdef YT_ENABLE_TLS_ADDRESS_TRACKING
        auto newTid = GetTid();
        if (newTid != Tid_) {
            YT_VERIFY(address != Address_);
        }
        Tid_ = newTid;
#endif

        Address_ = address;
    }

private:
#ifdef YT_ENABLE_TLS_ADDRESS_TRACKING
    pid_t Tid_;
#endif

    T* Address_;

#ifdef YT_ENABLE_TLS_ADDRESS_TRACKING
    Y_FORCE_INLINE static pid_t GetTid()
    {
        return ::syscall(__NR_gettid);
    }
#endif
};

////////////////////////////////////////////////////////////////////////////////

//! All context thread local variables which must be preserved for each fiber are listed here.
class TBaseSwitchHandler
{
protected:
    void OnSwitch()
    {
        FiberId_ = SwapCurrentFiberId(FiberId_);
        MemoryTag_ = SwapMemoryTag(MemoryTag_);
        Fls_ = SwapCurrentFls(Fls_);
        MinLogLevel_ = SwapMinLogLevel(MinLogLevel_);
    }

    ~TBaseSwitchHandler()
    {
        YT_VERIFY(FiberId_ == InvalidFiberId);
        YT_VERIFY(MemoryTag_ == NullMemoryTag);
        YT_VERIFY(!Fls_);
        YT_VERIFY(MinLogLevel_ == ELogLevel::Minimum);
    }

private:
    TMemoryTag MemoryTag_ = NullMemoryTag;
    TFls* Fls_ = nullptr;
    TFiberId FiberId_ = InvalidFiberId;
    ELogLevel MinLogLevel_ = ELogLevel::Minimum;
};

class TFiberSwitchHandler;

YT_DEFINE_THREAD_LOCAL(TFiberSwitchHandler*, CurrentFiberSwitchHandler);

class TFiberSwitchHandler
    : public TBaseSwitchHandler
{
public:
    // On start fiber running.
    explicit TFiberSwitchHandler(TFiber* fiber)
        : Fiber_(fiber)
        , FiberId_(TFiberIdGenerator::Get()->Generate())
    {
        AddressStorage_ = TTlsAddressStorage<TFiberSwitchHandler*>([] {
            return std::addressof(CurrentFiberSwitchHandler());
        });

        SavedThis_ = std::exchange(*AddressStorage_, this);

        Fiber_->OnCallbackExecutionStarted(FiberId_, &Fls_);

        YT_VERIFY(SwapCurrentFiberId(FiberId_) == InvalidFiberId);
        YT_VERIFY(!SwapCurrentFls(&Fls_));
    }

    // On finish fiber running.
    ~TFiberSwitchHandler()
    {
        AddressStorage_.ReReadAddress([] {
            return std::addressof(CurrentFiberSwitchHandler());
        });

        YT_VERIFY(*AddressStorage_ == this);
        YT_VERIFY(UserHandlers_.empty());

        Fiber_->OnCallbackExecutionFinished();

        YT_VERIFY(SwapCurrentFiberId(InvalidFiberId) == FiberId_);
        YT_VERIFY(SwapCurrentFls(nullptr) == &Fls_);

        // Support case when current fiber has been resumed, but finished without WaitFor.
        // There is preserved context of resumer fiber saved in switchHandler. Restore it.
        // If there are no values for resumer the following call will swap null with null.
        OnSwitch();
    }

    TFiberSwitchHandler(const TFiberSwitchHandler&) = delete;
    TFiberSwitchHandler(TFiberSwitchHandler&&) = delete;

    TCancelerPtr& Canceler()
    {
        return Canceler_;
    }

    class TGuard
    {
    public:
        TGuard(const TGuard&) = delete;
        TGuard(TGuard&&) = delete;

        TGuard()
            : SwitchHandler_(CurrentFiberSwitchHandler())
        {
            YT_VERIFY(SwitchHandler_);
            SwitchHandler_->OnOut();
        }

        ~TGuard()
        {
            SwitchHandler_->OnIn();
        }

    private:
        TFiberSwitchHandler* const SwitchHandler_;
    };

private:
    friend TContextSwitchGuard;

    TFiber* const Fiber_;
    const TFiberId FiberId_;
    TFls Fls_;
    TTlsAddressStorage<TFiberSwitchHandler*> AddressStorage_;

    TFiberSwitchHandler* SavedThis_;

    struct TContextSwitchHandlers
    {
        TContextSwitchHandler Out;
        TContextSwitchHandler In;
    };

    TCompactVector<TContextSwitchHandlers, 16> UserHandlers_;

    TCancelerPtr Canceler_;

    void OnSwitch()
    {
        // In user defined context switch callbacks (ContextSwitchGuard) Swap must be used. It preserves context
        // from fiber resumer.
        // In internal SwitchIn/SwitchOut Get/Set must be used.

        TBaseSwitchHandler::OnSwitch();

        std::swap(SavedThis_, *AddressStorage_);
    }

    // On finish fiber running.
    void OnOut()
    {
        TContextSwitchManager::Get()->OnOut();

        for (auto it = UserHandlers_.begin(); it != UserHandlers_.end(); ++it) {
            if (it->Out) {
                it->Out();
            }
        }

        OnSwitch();
    }

    // On start fiber running.
    void OnIn()
    {
        AddressStorage_.ReReadAddress([] {
            return std::addressof(CurrentFiberSwitchHandler());
        });
        OnSwitch();

        for (auto it = UserHandlers_.rbegin(); it != UserHandlers_.rend(); ++it) {
            if (it->In) {
                it->In();
            }
        }

        TContextSwitchManager::Get()->OnIn();
    }
};

TFiberSwitchHandler* GetFiberSwitchHandler()
{
    auto* switchHandler = CurrentFiberSwitchHandler();
    YT_VERIFY(switchHandler);
    return switchHandler;
}

// Prevent inlining for backtrace examination.
// See devtools/gdb/yt_fibers_printer.py.
Y_NO_INLINE void RunInFiberContext(TFiber* fiber, TClosure callback)
{
    TFiberSwitchHandler switchHandler(fiber);
    TNullPropagatingStorageGuard nullPropagatingStorageGuard;
    callback();
    // To ensure callback is destroyed before switchHandler.
    callback.Reset();
}

////////////////////////////////////////////////////////////////////////////////

// Compared to GuardedInvoke TResumeGuard reduces frame count in backtrace.
class TResumeGuard
{
public:
    TResumeGuard(TFiber* fiber, TCancelerPtr canceler) noexcept
        : Fiber_(fiber)
        , Canceler_(std::move(canceler))
    { }

    explicit TResumeGuard(TResumeGuard&& other) noexcept
        : Fiber_(other.Release())
        , Canceler_(std::move(other.Canceler_))
    { }

    TResumeGuard(const TResumeGuard&) = delete;

    TResumeGuard& operator=(const TResumeGuard&) = delete;
    TResumeGuard& operator=(TResumeGuard&&) = delete;

    void operator()()
    {
        YT_VERIFY(Fiber_);
        Canceler_.Reset();
        NDetail::ResumeFiber(Release());
    }

    ~TResumeGuard()
    {
        if (Fiber_) {
            YT_LOG_TRACE("Unwinding fiber (TargetFiberId: %x)", Canceler_->GetFiberId());

            Canceler_->Run(TError("Fiber resumer is lost"));
            Canceler_.Reset();

            GetFinalizerInvoker()->Invoke(
                BIND_NO_PROPAGATE([fiber = Release()] {
                    NDetail::ResumeFiber(fiber);
                }));
        }
    }

private:
    TFiber* Fiber_;
    TCancelerPtr Canceler_;

    TFiber* Release()
    {
        return std::exchange(Fiber_, nullptr);
    }
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TFiberSchedulerThread::TFiberSchedulerThread(
    TString threadGroupName,
    TString threadName,
    NThreading::TThreadOptions options)
    : TThread(std::move(threadName), std::move(options))
    , ThreadGroupName_(std::move(threadGroupName))
{ }


void TFiberSchedulerThread::ThreadMain()
{
    // Hold this strongly.
    auto this_ = MakeStrong(this);

    EnsureSafeShutdown();

    try {
        YT_LOG_DEBUG("Thread started (Name: %v)",
            GetThreadName());

        NDetail::TFiberContext fiberContext(this, ThreadGroupName_);
        NDetail::TFiberContextGuard fiberContextGuard(&fiberContext);

        NDetail::SwitchFromThread(TFiber::CreateFiber());

        YT_LOG_DEBUG("Thread stopped (Name: %v)",
            GetThreadName());
    } catch (const std::exception& ex) {
        YT_LOG_FATAL(ex, "Unhandled exception in thread main (Name: %v)",
            GetThreadName());
    }
}

////////////////////////////////////////////////////////////////////////////////

void UpdateMaxIdleFibers(int maxIdleFibers)
{
    NDetail::TIdleFiberPool::Get()->UpdateMaxIdleFibers(maxIdleFibers);
}

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_THREAD_LOCAL(TFiberId, CurrentFiberId);

TFiberId GetCurrentFiberId()
{
    return CurrentFiberId();
}

void SetCurrentFiberId(TFiberId id)
{
    CurrentFiberId() = id;
}

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_THREAD_LOCAL(bool, ContextSwitchForbidden);

bool IsContextSwitchForbidden()
{
    return ContextSwitchForbidden();
}

TForbidContextSwitchGuard::TForbidContextSwitchGuard()
    : OldValue_(std::exchange(ContextSwitchForbidden(), true))
{ }

TForbidContextSwitchGuard::~TForbidContextSwitchGuard()
{
    ContextSwitchForbidden() = OldValue_;
}

////////////////////////////////////////////////////////////////////////////////

bool CheckFreeStackSpace(size_t space)
{
    auto* currentFiber = NDetail::TryGetCurrentFiber();
    return !currentFiber || currentFiber->CheckFreeStackSpace(space);
}

TFiberCanceler GetCurrentFiberCanceler()
{
    auto* switchHandler = NDetail::CurrentFiberSwitchHandler();
    if (!switchHandler) {
        // Not in fiber context.
        return {};
    }

    if (!switchHandler->Canceler()) {
        TMemoryTagGuard guard(NullMemoryTag);
        switchHandler->Canceler() = New<NDetail::TCanceler>(GetCurrentFiberId());
    }

    return TFiberCanceler(switchHandler->Canceler(), &NDetail::TCanceler::StaticInvoke);
}

////////////////////////////////////////////////////////////////////////////////

void WaitUntilSet(TFuture<void> future, IInvokerPtr invoker)
{
    YT_VERIFY(!IsContextSwitchForbidden());
    YT_VERIFY(future);
    YT_ASSERT(invoker);

    TMemoryTagGuard memoryTagGuard(NullMemoryTag);

    auto* currentFiber = NDetail::TryGetCurrentFiber();
    if (!currentFiber) {
        // When called from a fiber-unfriendly context, we fallback to blocking wait.
        YT_VERIFY(invoker == GetCurrentInvoker());
        YT_VERIFY(invoker == GetSyncInvoker());
        YT_VERIFY(future.Wait());
        return;
    }

    NThreading::VerifyNoSpinLockAffinity();
    YT_VERIFY(invoker != GetSyncInvoker());

    // Ensure canceler created.
    GetCurrentFiberCanceler();

    const auto& canceler = NDetail::GetFiberSwitchHandler()->Canceler();
    if (canceler->IsCanceled()) {
        future.Cancel(canceler->GetCancelationError());
    }

    canceler->SetFuture(future);
    auto finally = Finally([&] {
        canceler->ResetFuture();
    });

    // TODO(lukyan): transfer resumer as argument of AfterSwitch.
    // Use CallOnTop like in boost.
    auto afterSwitch = NDetail::MakeAfterSwitch([
            canceler,
            invoker = std::move(invoker),
            future = std::move(future),
            currentFiber
        ] () mutable {
            currentFiber->SetWaiting();
            future.Subscribe(BIND_NO_PROPAGATE([
                invoker = std::move(invoker),
                currentFiber,
                canceler = std::move(canceler)
            ] (const TError&) mutable {
                YT_LOG_DEBUG("Waking up fiber (TargetFiberId: %x)",
                    canceler->GetFiberId());

                invoker->Invoke(
                    BIND_NO_PROPAGATE(NDetail::TResumeGuard(currentFiber, std::move(canceler))));
            }));
        });

    {
        NDetail::TFiberSwitchHandler::TGuard switchGuard;
        NDetail::YieldFiber(afterSwitch);
    }

    if (canceler->IsCanceled()) {
        YT_LOG_DEBUG("Throwing fiber cancelation exception");
        throw TFiberCanceledException();
    }
}

////////////////////////////////////////////////////////////////////////////////

void InstallGlobalContextSwitchHandlers(
    TGlobalContextSwitchHandler outHandler,
    TGlobalContextSwitchHandler inHandler)
{
    NDetail::TContextSwitchManager::Get()->RegisterGlobalHandlers(
        outHandler,
        inHandler);
}

////////////////////////////////////////////////////////////////////////////////

TContextSwitchGuard::TContextSwitchGuard(
    TContextSwitchHandler outHandler,
    TContextSwitchHandler inHandler)
{
    if (auto* context = NDetail::CurrentFiberSwitchHandler()) {
        context->UserHandlers_.push_back({std::move(outHandler), std::move(inHandler)});
    }
}

TContextSwitchGuard::~TContextSwitchGuard()
{
    if (auto* context = NDetail::CurrentFiberSwitchHandler()) {
        YT_VERIFY(!context->UserHandlers_.empty());
        context->UserHandlers_.pop_back();
    }
}

TOneShotContextSwitchGuard::TOneShotContextSwitchGuard(TContextSwitchHandler outHandler)
    : TContextSwitchGuard(
        [this, handler = std::move(outHandler)] () noexcept {
            if (!Active_) {
                return;
            }
            Active_ = false;
            handler();
        },
        nullptr)
    , Active_(true)
{ }

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency
