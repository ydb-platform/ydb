#include "async_looper.h"

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/misc/finally.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TAsyncLooper::TAsyncLooper(
    IInvokerPtr invoker,
    TCallback<TFuture<void>()> asyncStart,
    TClosure syncFinish,
    const NLogging::TLogger& logger)
    : Invoker_(std::move(invoker))
    , AsyncStart_(std::move(asyncStart))
    , SyncFinish_(std::move(syncFinish))
    , Logger(logger)
{
    YT_VERIFY(Invoker_);
    YT_VERIFY(Invoker_ != GetSyncInvoker());
    YT_VERIFY(AsyncStart_);
    YT_VERIFY(SyncFinish_);
}

void TAsyncLooper::Start()
{
    auto traceContext = NTracing::GetOrCreateTraceContext("LooperStart");
    auto traceGuard = NTracing::TCurrentTraceContextGuard(traceContext);
    YT_LOG_DEBUG("Requesting looper to start");

    Invoker_->Invoke(
        BIND(&TAsyncLooper::DoStart, MakeStrong(this)));
}

void TAsyncLooper::DoStart()
{
    auto guard = Guard(StateLock_);
    switch (State_) {
        case EState::Running:
            // Already Running.
        case EState::Restarting:
            // Soon to be Running.
            YT_LOG_DEBUG("Looper is either already running or restarting");
            return;

        case EState::NotRunning:
            break;

        default:
            YT_ABORT();
    }

    switch (Stage_) {
        case EStage::AsyncBusy:
            // Has been stopped during async step
            // -> request restart.
        case EStage::Busy:
            // Has been stopped during sync step
            // -> request restart.
            YT_LOG_DEBUG("Looper is busy but was stopped. Requesting restart");
            State_ = EState::Restarting;
            return;

        case EStage::Idle:
            // Two possibilities:
            // 1. Nothing is happening -> we just start as normal
            // 2. We have been stopped during intermission between
            // async step and the sync one. Call to stop must have
            // incremented the epoch and therefore imminent sync step
            // will bail out due to epoch mismatch -> we are free to
            // ignore this case on our side.
            State_ = EState::Running;
            YT_LOG_DEBUG("Starting looper");
            StartLoop(guard);
            break;

        default:
            YT_ABORT();
    }
}

void TAsyncLooper::StartLoop(const TGuard& guard)
{
    YT_ASSERT_SPINLOCK_AFFINITY(StateLock_);
    YT_VERIFY(State_ == EState::Running);
    YT_VERIFY(Stage_ == EStage::Idle);

    Future_.Reset();

    auto traceContext = NTracing::GetOrCreateTraceContext("StartLoop");
    auto traceGuard = NTracing::TCurrentTraceContextGuard(traceContext);

    Stage_ = EStage::AsyncBusy;

    TFuture<void> future;

    try {
        auto cleanup = Finally([&] {
            Stage_ = EStage::Idle;
        });
        auto unguard = Unguard(guard);
        future = AsyncStart_();

    } catch (const TFiberCanceledException&) {
        // We got canceled -- this is normal.
        throw;
    } catch (const std::exception& ex) {
        if (TError(ex).GetCode() == NYT::EErrorCode::Canceled) {
            throw;
        }
        YT_LOG_FATAL(ex, "Unexpected error encountered during the async step");
    } catch (...) {
        YT_LOG_FATAL("Unexpected error encountered during the async step");
    }

    switch (State_) {
        case EState::NotRunning:
            // We have been stopped during the async step
            // -> cancel the future if there is one.
            YT_LOG_DEBUG("Looper stop occured during the async part");

            if (future) {
                future.Cancel(TError("Looper stopped"));
            }
            return;

        case EState::Restarting:
            // We have been restarted during the async step
            // -> convert to running.
            YT_LOG_DEBUG("Looper restart occured during the async part. Next loop will be a clean start");

            State_ = EState::Running;
            break;

        case EState::Running:
            // Nothing happened during async step
            // -> proceed as normal.
            break;

        default:
            YT_ABORT();
    }

    if (future) {
        // NB(arkady-e1ppa): We read epoch here (and not earlier)
        // because we could have been restarted during the async step
        // in which case we take up the role of the restarted loop.
        // Thus we have to have get the most up to date epoch here.
        Future_ = future
            .Apply(BIND(
                &TAsyncLooper::AfterStart,
                MakeWeak(this),
                EpochNumber_)
                    .AsyncVia(Invoker_));
    }
}

void TAsyncLooper::AfterStart(ui64 epochNumber, const TError& error)
{
    if (!error.IsOK()) {
        YT_LOG_WARNING(error, "Async start failed unexpectedly. Stopping looper");
        return;
    }

    {
        auto guard = Guard(StateLock_);

        YT_ASSERT(Stage_ == EStage::Idle);

        switch (State_) {
            case EState::NotRunning:
                YT_LOG_DEBUG("Looper stop occured during the intermission between async and sync steps");
                // We have been stopped -> bail out.
                return;

            case EState::Running:
                if (epochNumber != EpochNumber_) {
                    // We got restarted during the intermission.
                    // Caller of |Start| will start the new chain
                    // and we just bail out.
                    YT_LOG_DEBUG("Looper restart occured during the intermission between async and sync steps");
                    return;
                }
                break;

            default:
                YT_ABORT();
        }

        Stage_ = EStage::Busy;
    }

    DoStep();
}

void TAsyncLooper::DoStep()
{
    auto cleanup = Finally([this] {
        FinishStep();
    });

    try {
        SyncFinish_();
    } catch (const TFiberCanceledException&) {
        Stop();
        throw;
    } catch (const std::exception& ex) {
        if (TError(ex).GetCode() == NYT::EErrorCode::Canceled) {
            throw;
        }
        YT_LOG_FATAL(ex, "Unexpected error encountered during the sync step");
    } catch (...) {
        YT_LOG_FATAL("Unexpected error encountered during the sync step");
    }
}

void TAsyncLooper::FinishStep()
{
    auto guard = Guard(StateLock_);

    YT_ASSERT(Stage_ == EStage::Busy);
    // NB(arkady-e1ppa): Since this transition happens
    // under spinlock and below we would go to AsyncBusy before the release
    // this stage switch will not be observed by users.
    Stage_ = EStage::Idle;

    switch (State_) {
        case EState::NotRunning:
            // We have been stopped
            // -> bail out.
            YT_LOG_DEBUG("Looper stop occured during the sync step");

            return;

        case EState::Restarting:
            // We have been restarted
            // -> start the new chain of loops.
            YT_LOG_DEBUG("Looper restart occured during the sync step");

            State_ = EState::Running;
            StartLoop(guard);
            return;

        case EState::Running:
            // Nothing has happened
            // -> continue the current chain of loops.
            StartLoop(guard);
            return;

        default:
            YT_ABORT();
    }
}

void TAsyncLooper::Stop()
{
    TFuture<void> future;

    {
        auto guard = Guard(StateLock_);

        if (State_ == EState::NotRunning) {
            // Already stopping
            // -> bail out.
            YT_LOG_DEBUG("Trying to stop looper that is already stopped");
            return;
        }

        State_ = EState::NotRunning;
        ++EpochNumber_;
        YT_LOG_DEBUG("Stopping the looper");

        // We could be in one of three possible situations (for each stage):
        // 1. EStage::AsyncBusy -- |StartLoop| caller will observe
        // state |NotRunning| eventually and will cancel the future for us
        // (or there will be a restart which is also handled by caller above).
        // 2. EStage::Idle -- intermission between async and sync parts
        // -- if there is not restart, sync part will just bail out
        // if there is a restart, sync part will observe a different epoch
        // and bail out as well.
        // 3. EStage::Busy -- sync part is running and will simply not
        // continue the chain once it observes NotRunning state.

        if (!Future_) {
            // If there was a null future produced for async part
            // or simply while Stage_ == EStage::AsyncBusy.
            return;
        }

        future = std::exchange(Future_, TFuture<void>());
    }

    future.Cancel(TError("Looper stopped"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
