#include "fiber_scheduler_thread.h"

#include "private.h"
#include "fiber.h"

#include <yt/yt/library/profiling/producer.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/shutdown.h>
#include <yt/yt/core/misc/singleton.h>

#include <library/cpp/yt/threading/spin_lock_count.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <library/cpp/yt/memory/memory_tag.h>

#include <library/cpp/yt/threading/fork_aware_spin_lock.h>

#include <util/thread/lfstack.h>

#include <thread>

namespace NYT::NConcurrency {

using namespace NLogging;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ConcurrencyLogger;

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

void RunInFiberContext(TFiber* fiber, TClosure callback);
void SwitchFromThread(TFiberPtr targetFiber);

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
    TClosure AfterSwitch;
    TFiberPtr ResumerFiber;
    TFiberPtr CurrentFiber;
};

YT_THREAD_LOCAL(TFiberContext*) FiberContext;

// Forbid inlining these accessors to prevent the compiler from
// miss-optimizing TLS access in presence of fiber context switches.
Y_NO_INLINE TFiberContext* TryGetFiberContext()
{
    return FiberContext;
}

Y_NO_INLINE void SetFiberContext(TFiberContext* context)
{
    FiberContext = context;
}

////////////////////////////////////////////////////////////////////////////////

class TFiberContextGuard
{
public:
    explicit TFiberContextGuard(TFiberContext* context)
    {
        SetFiberContext(context);
    }

    ~TFiberContextGuard()
    {
        SetFiberContext(nullptr);
    }

    TFiberContextGuard(const TFiberContextGuard&) = delete;
    TFiberContextGuard operator=(const TFiberContextGuard&) = delete;
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
    return &TryGetFiberContext()->MachineContext;
}

Y_FORCE_INLINE void SetAfterSwitch(TClosure&& closure)
{
    auto* context = TryGetFiberContext();
    YT_VERIFY(!context->AfterSwitch);
    context->AfterSwitch = std::move(closure);
}

Y_FORCE_INLINE TClosure ExtractAfterSwitch()
{
    auto* context = TryGetFiberContext();
    return std::move(context->AfterSwitch);
}

Y_FORCE_INLINE void SetResumerFiber(TFiberPtr fiber)
{
    auto* context = TryGetFiberContext();
    YT_VERIFY(!context->ResumerFiber);
    context->ResumerFiber = std::move(fiber);
}

Y_FORCE_INLINE TFiberPtr ExtractResumerFiber()
{
    return std::move(TryGetFiberContext()->ResumerFiber);
}

Y_FORCE_INLINE TFiber* TryGetResumerFiber()
{
    return TryGetFiberContext()->ResumerFiber.Get();
}

Y_FORCE_INLINE TFiberPtr SwapCurrentFiber(TFiberPtr fiber)
{
    return std::exchange(TryGetFiberContext()->CurrentFiber, std::move(fiber));
}

Y_FORCE_INLINE TFiber* TryGetCurrentFiber()
{
    auto* context = TryGetFiberContext();
    return context ? context->CurrentFiber.Get() : nullptr;
}

Y_FORCE_INLINE TFiber* GetCurrentFiber()
{
    auto* fiber = TryGetFiberContext()->CurrentFiber.Get();
    YT_VERIFY(fiber);
    return fiber;
}

Y_FORCE_INLINE TFiberSchedulerThread* TryGetFiberThread()
{
    return TryGetFiberContext()->FiberThread;
}

Y_FORCE_INLINE TRefCountedGaugePtr GetWaitingFibersCounter()
{
    return TryGetFiberContext()->WaitingFibersCounter;
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

void SwitchFromThread(TFiberPtr targetFiber)
{
    YT_ASSERT(targetFiber);

    targetFiber->SetRunning();

    auto* targetContext = targetFiber->GetMachineContext();

    auto currentFiber = SwapCurrentFiber(std::move(targetFiber));
    YT_VERIFY(!currentFiber);

    SwitchMachineContext(GetMachineContext(), targetContext);

    YT_VERIFY(!TryGetCurrentFiber());
}

[[noreturn]] void SwitchToThread()
{
    auto currentFiber = SwapCurrentFiber(nullptr);
    auto* currentContext = currentFiber->GetMachineContext();
    currentFiber.Reset();

    SwitchMachineContext(currentContext, GetMachineContext());

    YT_ABORT();
}

void SwitchFromFiber(TFiberPtr targetFiber)
{
    YT_ASSERT(targetFiber);

    targetFiber->SetRunning();
    auto* targetContext = targetFiber->GetMachineContext();

    auto currentFiber = SwapCurrentFiber(std::move(targetFiber));
    YT_VERIFY(currentFiber->GetState() != EFiberState::Waiting);
    auto* currentContext = currentFiber->GetMachineContext();

    SwitchMachineContext(currentContext, targetContext);

    YT_VERIFY(TryGetCurrentFiber() == currentFiber);
}

////////////////////////////////////////////////////////////////////////////////

#ifdef YT_REUSE_FIBERS

class TIdleFiberPool
{
public:
    static TIdleFiberPool* Get()
    {
        return LeakySingleton<TIdleFiberPool>();
    }

    void EnqueueIdleFiber(TFiberPtr fiber)
    {
        IdleFibers_.Enqueue(std::move(fiber));
        if (DestroyingIdleFibers_.load()) {
            DoDestroyIdleFibers();
        }
    }

    TFiberPtr TryDequeueIdleFiber()
    {
        TFiberPtr fiber;
        IdleFibers_.Dequeue(&fiber);
        return fiber;
    }

private:
    const TShutdownCookie ShutdownCookie_ = RegisterShutdownCallback(
        "FiberManager",
        BIND_NO_PROPAGATE(&TIdleFiberPool::DestroyIdleFibers, this),
        /*priority*/ -100);

    TLockFreeStack<TFiberPtr> IdleFibers_;
    std::atomic<bool> DestroyingIdleFibers_ = false;


    void DestroyIdleFibers()
    {
        DestroyingIdleFibers_.store(true);
        DoDestroyIdleFibers();
    }

    void DoDestroyIdleFibers()
    {
        auto destroyFibers = [&] {
            TFiberContext fiberContext;
            TFiberContextGuard fiberContextGuard(&fiberContext);

            std::vector<TFiberPtr> fibers;
            IdleFibers_.DequeueAll(&fibers);

            for (const auto& fiber : fibers) {
                SwitchFromThread(std::move(fiber));
            }
        };

    #ifdef _unix_
        // The current thread could be already exiting and MacOS has some issues
        // with registering new thread-local terminators in this case:
        // https://github.com/lionheart/openradar-mirror/issues/20926
        // As a matter of workaround, we offload all finalization logic to a separate
        // temporary thread.
        std::thread thread([&] {
            ::TThread::SetCurrentThreadName("IdleFiberDtor");

            destroyFibers();
        });
        thread.join();
    #else
        // Starting threads in exit handlers on Windows causes immediate calling exit
        // so the routine will not be executed. Moreover, if we try to join this thread we'll get deadlock
        // because this thread will try to acquire atexit lock which is owned by this thread.
        destroyFibers();
    #endif
    }

    DECLARE_LEAKY_SINGLETON_FRIEND()
};

#endif

////////////////////////////////////////////////////////////////////////////////

void FiberTrampoline()
{
    RunAfterSwitch();

    YT_LOG_DEBUG("Fiber started");

    auto* currentFiber = GetCurrentFiber();

    // Break loop to terminate fiber
    while (auto* fiberThread = TryGetFiberThread()) {
        YT_VERIFY(!TryGetResumerFiber());

        TCallback<void()> callback;
        {
            // We wrap fiberThread->OnExecute() into a propagating storage guard to ensure
            // that the propagating storage created there won't spill into the fiber callbacks.
            TNullPropagatingStorageGuard guard;
            YT_VERIFY(guard.GetOldStorage().IsNull());
            callback = fiberThread->OnExecute();
        }

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
        if (auto resumerFiber = ExtractResumerFiber()) {
            // Suspend current fiber.
#ifdef YT_REUSE_FIBERS
            {
                // TODO(lukyan): Use simple callbacks without memory allocation.
                // Make TFiber::MakeIdle method instead of lambda function.
                // Switch out and add fiber to idle fibers.
                // Save fiber in AfterSwitch because it can be immediately concurrently reused.
                SetAfterSwitch(BIND_NO_PROPAGATE([currentFiber = MakeStrong(currentFiber)] () mutable {
                    currentFiber->SetIdle();
                    TIdleFiberPool::Get()->EnqueueIdleFiber(std::move(currentFiber));
                }));
            }

            // Switched to ResumerFiber or thread main.
            SwitchFromFiber(std::move(resumerFiber));
#else
            SetAfterSwitch(BIND_NO_PROPAGATE([
                currentFiber = MakeStrong(currentFiber),
                resumerFiber = std::move(resumerFiber)
            ] () mutable {
                currentFiber.Reset();
                SwitchFromThread(std::move(resumerFiber));
            }));
            break;
#endif
        }
    }

    YT_LOG_DEBUG("Fiber finished");

    SetAfterSwitch(BIND_NO_PROPAGATE([currentFiber = MakeStrong(currentFiber)] () mutable {
        currentFiber->SetFinished();
        currentFiber.Reset();
    }));

    // All allocated objects in this frame must be destroyed here.
    SwitchToThread();
}

void YieldFiber(TClosure afterSwitch)
{
    YT_VERIFY(TryGetCurrentFiber());

    SetAfterSwitch(std::move(afterSwitch));

    // Try to get resumer.
    auto targetFiber = ExtractResumerFiber();

    // If there is no resumer switch to idle fiber. Or switch to thread main.
#ifdef YT_REUSE_FIBERS
    if (!targetFiber) {
        targetFiber = TIdleFiberPool::Get()->TryDequeueIdleFiber();
    }
#endif

    if (!targetFiber) {
        targetFiber = New<TFiber>();
    }

    auto waitingFibersCounter = GetWaitingFibersCounter();
    waitingFibersCounter->Increment(1);

    SwitchFromFiber(std::move(targetFiber));
    YT_VERIFY(TryGetResumerFiber());

    waitingFibersCounter->Increment(-1);
}

void ResumeFiber(TFiberPtr targetFiber)
{
    TMemoryTagGuard guard(NullMemoryTag);

    auto currentFiber = MakeStrong(GetCurrentFiber());

    SetResumerFiber(currentFiber);
    SetAfterSwitch(BIND_NO_PROPAGATE([currentFiber = std::move(currentFiber)] {
        currentFiber->SetWaiting();
    }));

    SwitchFromFiber(std::move(targetFiber));
    YT_VERIFY(!TryGetResumerFiber());
}

class TFiberSwitchHandler;
TFiberSwitchHandler* TryGetFiberSwitchHandler();

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

class TFiberSwitchHandler
    : public TBaseSwitchHandler
{
public:
    // On start fiber running.
    explicit TFiberSwitchHandler(TFiber* fiber)
        : Fiber_(fiber)
    {
        SavedThis_ = std::exchange(This_, this);

        YT_VERIFY(SwapCurrentFiberId(fiber->GetFiberId()) == InvalidFiberId);
        YT_VERIFY(!SwapCurrentFls(fiber->GetFls()));
    }

    // On finish fiber running.
    ~TFiberSwitchHandler()
    {
        YT_VERIFY(This_ == this);
        YT_VERIFY(UserHandlers_.empty());

        YT_VERIFY(SwapCurrentFiberId(InvalidFiberId) == Fiber_->GetFiberId());
        YT_VERIFY(SwapCurrentFls(nullptr) == Fiber_->GetFls());

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
            : SwitchHandler_(This_)
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
    friend TFiberSwitchHandler* TryGetFiberSwitchHandler();

    const TFiber* const Fiber_;

    TFiberSwitchHandler* SavedThis_;
    static YT_THREAD_LOCAL(TFiberSwitchHandler*) This_;

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

        std::swap(SavedThis_, GetTlsRef(This_));
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
        OnSwitch();

        for (auto it = UserHandlers_.rbegin(); it != UserHandlers_.rend(); ++it) {
            if (it->In) {
                it->In();
            }
        }

        TContextSwitchManager::Get()->OnIn();
    }
};

YT_THREAD_LOCAL(TFiberSwitchHandler*) TFiberSwitchHandler::This_;

TFiberSwitchHandler* TryGetFiberSwitchHandler()
{
    return TFiberSwitchHandler::This_;
}

TFiberSwitchHandler* GetFiberSwitchHandler()
{
    auto* switchHandler = TryGetFiberSwitchHandler();
    YT_VERIFY(switchHandler);
    return switchHandler;
}

// Prevent inlining for backtrace examination.
// See devtools/gdb/yt_fibers_printer.py.
Y_NO_INLINE void RunInFiberContext(TFiber* fiber, TClosure callback)
{
    fiber->Recreate();
    TFiberSwitchHandler switchHandler(fiber);
    TNullPropagatingStorageGuard nullPropagatingStorageGuard;
    callback();
}

////////////////////////////////////////////////////////////////////////////////

// Compared to GuardedInvoke TResumeGuard reduces frame count in backtrace.
class TResumeGuard
{
public:
    TResumeGuard(TFiberPtr fiber, TCancelerPtr canceler)
        : Fiber_(std::move(fiber))
        , Canceler_(std::move(canceler))
    { }

    explicit TResumeGuard(TResumeGuard&& other)
        : Fiber_(std::move(other.Fiber_))
        , Canceler_(std::move(other.Canceler_))
    { }

    TResumeGuard(const TResumeGuard&) = delete;

    TResumeGuard& operator=(const TResumeGuard&) = delete;
    TResumeGuard& operator=(TResumeGuard&&) = delete;

    void operator()()
    {
        YT_VERIFY(Fiber_);
        Canceler_.Reset();
        NDetail::ResumeFiber(std::move(Fiber_));
    }

    ~TResumeGuard()
    {
        if (Fiber_) {
            YT_LOG_TRACE("Unwinding fiber (TargetFiberId: %x)", Canceler_->GetFiberId());

            Canceler_->Run(TError("Fiber resumer is lost"));
            Canceler_.Reset();

            GetFinalizerInvoker()->Invoke(
                BIND_NO_PROPAGATE(&NDetail::ResumeFiber, Passed(std::move(Fiber_))));
        }
    }

private:
    TFiberPtr Fiber_;
    TCancelerPtr Canceler_;
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

    try {
        YT_LOG_DEBUG("Thread started (Name: %v)",
            GetThreadName());

        NDetail::TFiberContext fiberContext(this, ThreadGroupName_);
        NDetail::TFiberContextGuard fiberContextGuard(&fiberContext);

        NDetail::SwitchFromThread(New<TFiber>());

        YT_LOG_DEBUG("Thread stopped (Name: %v)",
            GetThreadName());
    } catch (const std::exception& ex) {
        YT_LOG_FATAL(ex, "Unhandled exception in thread main (Name: %v)",
            GetThreadName());
    }
}

////////////////////////////////////////////////////////////////////////////////

YT_THREAD_LOCAL(TFiberId) CurrentFiberId;

TFiberId GetCurrentFiberId()
{
    return CurrentFiberId;
}

void SetCurrentFiberId(TFiberId id)
{
    CurrentFiberId = id;
}

////////////////////////////////////////////////////////////////////////////////

YT_THREAD_LOCAL(bool) ContextSwitchForbidden;

bool IsContextSwitchForbidden()
{
    return ContextSwitchForbidden;
}

TForbidContextSwitchGuard::TForbidContextSwitchGuard()
    : OldValue_(std::exchange(ContextSwitchForbidden, true))
{ }

TForbidContextSwitchGuard::~TForbidContextSwitchGuard()
{
    ContextSwitchForbidden = OldValue_;
}

////////////////////////////////////////////////////////////////////////////////

bool CheckFreeStackSpace(size_t space)
{
    auto* currentFiber = NDetail::TryGetCurrentFiber();
    return !currentFiber || currentFiber->CheckFreeStackSpace(space);
}

TFiberCanceler GetCurrentFiberCanceler()
{
    auto* switchHandler = NDetail::TryGetFiberSwitchHandler();
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
    auto afterSwitch = BIND_NO_PROPAGATE([
            canceler,
            invoker = std::move(invoker),
            future = std::move(future),
            currentFiber = MakeStrong(currentFiber)
        ] () mutable {
            currentFiber->SetWaiting();
            future.Subscribe(BIND_NO_PROPAGATE([
                invoker = std::move(invoker),
                currentFiber = std::move(currentFiber),
                canceler = std::move(canceler)
            ] (const TError&) mutable {
                YT_LOG_DEBUG("Waking up fiber (TargetFiberId: %x)",
                    canceler->GetFiberId());

                invoker->Invoke(
                    BIND_NO_PROPAGATE(NDetail::TResumeGuard(std::move(currentFiber), std::move(canceler))));
            }));
        });

    {
        NDetail::TFiberSwitchHandler::TGuard switchGuard;
        NDetail::YieldFiber(std::move(afterSwitch));
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
    if (auto* context = NDetail::TryGetFiberSwitchHandler()) {
        context->UserHandlers_.push_back({std::move(outHandler), std::move(inHandler)});
    }
}

TContextSwitchGuard::~TContextSwitchGuard()
{
    if (auto* context = NDetail::TryGetFiberSwitchHandler()) {
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
