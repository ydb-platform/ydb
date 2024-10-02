#include "blobstorage_pdisk_blockdevice.h"
#include <ydb/library/pdisk_io/buffers.h>
#include "blobstorage_pdisk_completion_impl.h"
#include "blobstorage_pdisk_impl.h"
#include "blobstorage_pdisk_log_cache.h"
#include "blobstorage_pdisk_mon.h"
#include "blobstorage_pdisk_util_atomicblockcounter.h"
#include "blobstorage_pdisk_util_countedqueuemanyone.h"
#include "blobstorage_pdisk_util_countedqueueoneone.h"
#include "blobstorage_pdisk_util_flightcontrol.h"
#include "blobstorage_pdisk_util_idlecounter.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/library/pdisk_io/aio.h>
#include <ydb/library/pdisk_io/spdk_state.h>
#include <ydb/library/pdisk_io/wcache.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/util/thread.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/generic/deque.h>
#include <util/generic/bitops.h>
#include <util/system/file.h>
#include <util/system/mutex.h>
#include <util/system/sanitizers.h>
#include <util/system/spinlock.h>
#include <util/system/thread.h>

namespace NKikimr {
namespace NPDisk {

LWTRACE_USING(BLOBSTORAGE_PROVIDER);

constexpr ui64 MaxWaitingNoops = 256;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TRealBlockDevice
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TRealBlockDevice : public IBlockDevice {
    ////////////////////////////////////////////////////////
    // TCompletionThread
    ////////////////////////////////////////////////////////
    class TCompletionThread : public TThread {
        static constexpr ui32 NumOfWriters = 2;
    public:
        TCompletionThread(TRealBlockDevice &device, ui32 maxQueuedActions)
            : TThread(&ThreadProc, this)
            , Device(device)
            , QueuedActions(0)
            , MaxQueuedActions(maxQueuedActions)
        {}

        static void* ThreadProc(void* _this) {
            SetCurrentThreadName("PdCmpl");
            static_cast<TCompletionThread*>(_this)->Exec();
            return nullptr;
        }

        void Exec() {
            ui32 exitSignalsReceived = 0;
            Device.Mon.L7.Set(false, AtomicGetAndIncrement(SeqnoL7));
            auto prevCycleEnd = HPNow();
            bool isWorking = true;
            bool stateError = false;

            while(isWorking) {
                TAtomicBase actionCount = CompletionActions.GetWaitingSize();

                if (actionCount > 0) {
                    for (TAtomicBase idx = 0; idx < actionCount; ++idx) {
                        TCompletionAction *action = CompletionActions.Pop();
                        AtomicDecrement(QueuedActions);
                        if (action == nullptr) {
                            ++exitSignalsReceived;
                            if (exitSignalsReceived == NumOfWriters) {
                                isWorking = false;
                            }
                        } else {
                            if (!stateError && action->CanHandleResult()) {
                                action->Exec(Device.PCtx->ActorSystem);
                            } else {
                                TString errorReason = action->ErrorReason;

                                action->Release(Device.PCtx->ActorSystem);

                                if (!stateError) {
                                    stateError = true;
                                    Device.BecomeErrorState(TStringBuilder()
                                            << " CompletionAction error, operation info# " << errorReason);
                                }
                            }
                        }
                    }
                } else {
                    *Device.Mon.CompletionThreadCPU = ThreadCPUTime();
                    CompletionActions.ProducedWaitI();
                }

                const auto cycleEnd = HPNow();
                if (actionCount > 0) {
                    *Device.Mon.DeviceCompletionThreadBusyTimeNs += HPNanoSeconds(cycleEnd - prevCycleEnd);
                }
                prevCycleEnd = cycleEnd;
            }
        }

        // Schedule action execution
        // pass action = nullptr to quit
        void Schedule(TCompletionAction *action) noexcept {
            TAtomicBase queueActions = AtomicIncrement(QueuedActions);
            if (queueActions >= MaxQueuedActions) {
                Device.Mon.L7.Set(true, AtomicGetAndIncrement(SeqnoL7));
                while (AtomicGet(QueuedActions) >= MaxQueuedActions) {
                    SpinLockPause();
                }
                Device.Mon.L7.Set(false, AtomicGetAndIncrement(SeqnoL7));
            }
            CompletionActions.Push(action);
            return;
        }

        // Schedule action execution
        // pass action = nullptr to quit
        void ScheduleHackForLogReader(TCompletionAction *action) noexcept {
            AtomicIncrement(QueuedActions);
            action->Result = EIoResult::Ok;
            CompletionActions.Push(action);
            return;
        }

    private:
        TCountedQueueManyOne<TCompletionAction, 4 << 10> CompletionActions;
        TRealBlockDevice &Device;
        TAtomic QueuedActions;
        const TAtomicBase MaxQueuedActions;
        TAtomic SeqnoL7 = 0;
    };

    class TSubmitThreadBase : public TThread {
    protected:
        TRealBlockDevice &Device;
        std::shared_ptr<TPDiskCtx> &PCtx;
        TCountedQueueOneOne<IAsyncIoOperation*, 4 << 10> OperationsToBeSubmit;
        static constexpr TAtomicBase SubmitInFlightBytesMax = 1ull << 15;
        TMutex SubmitMtx;
        TCondVar SubmitCondVar;
        TAtomicBlockCounter SubmitQuitCounter;

    public:
        TAtomic SubmitInFlightBytes = 0;

    public:
        TSubmitThreadBase(TRealBlockDevice &device, TThread::TThreadProc threadProc, void *_this)
            : TThread(threadProc, _this)
            , Device(device)
            , PCtx(device.PCtx)
        {}

        // Schedule op execution
        // pass op = nullptr to quit
        void Schedule(IAsyncIoOperation *op) noexcept {
            if (!op) {
                SubmitQuitCounter.Increment();
                SubmitQuitCounter.BlockA();
                OperationsToBeSubmit.Push(op);
                return;
            }
            if (!SubmitQuitCounter.Increment()) {
                Device.FreeOperation(op);
                return;
            }
            ui64 size = op->GetSize();
            OperationsToBeSubmit.Push(op);
            NHPTimer::STime start;
            if (AtomicGetAndAdd(SubmitInFlightBytes, size) > SubmitInFlightBytesMax) {
                TGuard<TMutex> guard(SubmitMtx);
                start = HPNow();
                while (AtomicGet(SubmitInFlightBytes) > SubmitInFlightBytesMax) {
                    if (SubmitCondVar.WaitT(SubmitMtx, TDuration::Seconds(1))) {
                        return;
                    } else {
                        P_LOG(PRI_WARN, BPD01, "Exceed 1 second deadline in SubmitThreadQueue",
                                    (PDiskId, Device.PCtx->PDiskId),
                                    (Path, Device.Path),
                                    (TotalTimeInWaitingSec, NHPTimer::GetSeconds(HPNow() - start)),
                                    (SubmitInFlightBytes, AtomicGet(SubmitInFlightBytes)),
                                    (SubmitInFlightBytesMax, SubmitInFlightBytesMax));
                    }
                }
            }
        }
    };

    ////////////////////////////////////////////////////////
    // TSubmitThread
    ////////////////////////////////////////////////////////
    class TSubmitThread : public TSubmitThreadBase {
    public:
        TSubmitThread(TRealBlockDevice &device)
            : TSubmitThreadBase(device, &ThreadProc, this)
        {}

        static void* ThreadProc(void* _this) {
            SetCurrentThreadName("PdSbmEv");
            static_cast<TSubmitThread*>(_this)->Exec();
            return nullptr;
        }

        void ReleaseOp(IAsyncIoOperation *op) {
            Device.DecrementMonInFlight(op->GetType(), op->GetSize());
            Device.FreeOperation(op);
            Device.QuitCounter.Decrement();
            Device.IdleCounter.Decrement();
        }

        void Submit(IAsyncIoOperation *op) {
            TCompletionAction *action = static_cast<TCompletionAction*>(op->GetCookie());

            if (!Device.QuitCounter.Increment()) {
                Device.FreeOperation(op);
                TGuard<TMutex> guard(SubmitMtx);
                SubmitCondVar.Signal();
                return;
            }
            Device.IdleCounter.Increment();

            Device.IncrementMonInFlight(op->GetType(), op->GetSize());

            double blockedMs = 0;
            action->OperationIdx = Device.FlightControl.Schedule(blockedMs);

            *Device.Mon.DeviceWaitTimeMs += blockedMs;

            if (action->FlushAction) {
                action->FlushAction->OperationIdx = action->OperationIdx;
            }

            EIoResult ret = EIoResult::TryAgain;
            while (ret == EIoResult::TryAgain) {
                action->SubmitTime = HPNow();
                if (action->FlushAction) {
                    action->FlushAction->SubmitTime = action->SubmitTime;
                }

                if (op->GetType() == IAsyncIoOperation::EType::PWrite) {
                    PDISK_FAIL_INJECTION(1);
                }
                ret = Device.IoContext->Submit(op, Device.SharedCallback.Get());

                if (ret == EIoResult::Ok) {
                    return;
                }
                if (Device.QuitCounter.IsBlocked()) {
                    ReleaseOp(op);
                    return;
                }
            }
            // IoError happend
            ReleaseOp(op);
            Device.BecomeErrorState(TStringBuilder() << " Submit error, reason# " << ret);
        }

        void Exec() {
            auto prevCycleEnd = HPNow();
            while(!SubmitQuitCounter.IsBlocked() || SubmitQuitCounter.Get()) {
                TAtomicBase ops = OperationsToBeSubmit.GetWaitingSize();
                if (ops > 0) {
                    for (TAtomicBase idx = 0; idx < ops; ++idx) {
                        IAsyncIoOperation *op = OperationsToBeSubmit.Pop();
                        SubmitQuitCounter.Decrement();
                        if (op) {
                            ui64 size = op->GetSize(); // op may be deleted after submit
                            Submit(op);
                            TGuard<TMutex> guard(SubmitMtx);
                            if (AtomicSub(SubmitInFlightBytes, size) <= SubmitInFlightBytesMax) {
                                SubmitCondVar.Signal();
                            }
                        }
                    }
                } else {
                    *Device.Mon.SubmitThreadCPU = ThreadCPUTime();
                    OperationsToBeSubmit.ProducedWaitI();
                }
                auto cycleEnd = HPNow();
                // LWPROBE(PDiskDeviceSubmitThreadIdle, Device.GetPDiskId(), ops,
                //         HPMilliSecondsFloat(cycleEnd - prevCycleEnd));
                if (ops) {
                    *Device.Mon.DeviceSubmitThreadBusyTimeNs += HPNanoSeconds(cycleEnd - prevCycleEnd);
                }
                prevCycleEnd = cycleEnd;
            }
            Y_ABORT_UNLESS(OperationsToBeSubmit.GetWaitingSize() == 0);
        }
    };

    ////////////////////////////////////////////////////////
    // TGetThread
    ////////////////////////////////////////////////////////
    class TGetThread : public TThread {
    private:
        TRealBlockDevice &Device;

    public:
        TGetThread(TRealBlockDevice &device)
            : TThread(&ThreadProc, this)
            , Device(device)
        {}

        static void* ThreadProc(void* _this) {
            SetCurrentThreadName("PdGetEv");
            static_cast<TGetThread*>(_this)->Exec();
            return nullptr;
        }

        void Exec() {
            bool isOk = SetHighestThreadPriority();
            // TODO: ckeck isOk
            Y_UNUSED(isOk);

            TAsyncIoOperationResult events[MaxEvents];

            while(!Device.QuitCounter.IsBlocked() || Device.QuitCounter.Get()) {
                i64 ret = Device.IoContext->GetEvents(1, MaxEvents, events, TDuration::MilliSeconds(WaitTimeoutMs));
                if (ret == -static_cast<i64>(EIoResult::InterruptedSystemCall)) {
                    Device.Mon.DeviceInterruptedSystemCalls->Inc();
                } else if (ret < 0) {
                    Device.BecomeErrorState(TStringBuilder() << " Get error, reason# " << (EIoResult)-ret);
                }
            }
        }
    };


    class TSharedCallback : public ICallback {
        ui64 NextPossibleNoop = 0;
        ui64 EndOffset = 0;
        NHPTimer::STime PrevEventGotAtCycle = HPNow();
        NHPTimer::STime PrevEstimationAtCycle = HPNow();
        ui64 PrevEstimatedCostNs = 0;
        ui64 PrevActualCostNs = 0;

        TCompletionAction* WaitingNoops[MaxWaitingNoops] = {nullptr};
        TRealBlockDevice &Device;
        std::shared_ptr<TPDiskCtx> &PCtx;

    public:
        TSharedCallback(TRealBlockDevice &device)
            : Device(device)
            , PCtx(device.PCtx)
        {}

        void FillCompletionAction(TCompletionAction *action, IAsyncIoOperation *op, EIoResult result) {
            action->TraceId = std::move(*op->GetTraceIdPtr());
            action->SetResult(result);
            if (result != EIoResult::Ok) {
                // Previously seen errors: OutOfMemory, IOError;
                action->SetErrorReason(TStringBuilder()
                        << " type# " << op->GetType()
                        << " offset# " << op->GetOffset()
                        << " size# " << op->GetSize()
                        << " Result# " << result);
                P_LOG(PRI_ERROR, BPD01, "IAsyncIoOperation error",  (Reason, action->ErrorReason));
                ++*Device.Mon.DeviceIoErrors;
            }
        }

        void ExecuteOrScheduleCompletion(TCompletionAction *action) {
            if (action->ShouldBeExecutedInCompletionThread) {
                Device.CompletionThread->Schedule(action);
            } else {
                if (action->CanHandleResult()) {
                    action->Exec(Device.PCtx->ActorSystem);
                } else {
                    TString errorReason = action->ErrorReason;

                    action->Release(Device.PCtx->ActorSystem);

                    if (!Device.QuitCounter.IsBlocked()) {
                        Device.BecomeErrorState(TStringBuilder()
                                << " CompletionAction error, operation info# " << errorReason);
                    }
                }
            }
        }

        void Exec(TAsyncIoOperationResult *event) {
            IAsyncIoOperation *op = event->Operation;
            // Add up the execution time of all the events
            NHPTimer::STime eventGotAtCycle = HPNow();
            AtomicSet(Device.Mon.LastDoneOperationTimestamp, eventGotAtCycle);

            TCompletionAction *completionAction = static_cast<TCompletionAction*>(op->GetCookie());
            FillCompletionAction(completionAction, op, event->Result);
            completionAction->GetTime = eventGotAtCycle;
            LWTRACK(PDiskDeviceGetFromDevice, completionAction->Orbit);
            if (completionAction->FlushAction) {
                completionAction->FlushAction->GetTime = eventGotAtCycle;
                LWTRACK(PDiskDeviceGetFromDevice, completionAction->FlushAction->Orbit);
            }

            Device.QuitCounter.Decrement();
            Device.IdleCounter.Decrement();
            Device.FlightControl.MarkComplete(completionAction->OperationIdx);

            NHPTimer::STime startCycle = Max(completionAction->SubmitTime, (i64)PrevEventGotAtCycle);
            NHPTimer::STime durationCycles = (eventGotAtCycle > startCycle) ? eventGotAtCycle - startCycle : 0;
            NHPTimer::STime totalExecutionCycles = durationCycles;
            NHPTimer::STime totalCostNs = completionAction->CostNs;

            bool isSeekExpected = (completionAction->SubmitTime + (NHPTimer::STime)Device.SeekCostNs / 25ll >= PrevEventGotAtCycle);

            const ui64 opSize = op->GetSize();
            Device.DecrementMonInFlight(op->GetType(), opSize);
            if (opSize == 0) { // Special case for flush operation, which is a read operation with 0 bytes size
                if (op->GetType() == IAsyncIoOperation::EType::PRead) {
                    Y_ABORT_UNLESS(WaitingNoops[completionAction->OperationIdx % MaxWaitingNoops] == nullptr);
                    WaitingNoops[completionAction->OperationIdx % MaxWaitingNoops] = completionAction;
                } else {
                    Y_DEBUG_ABORT("Threre must not be writes of size 0 in TRealBlockDevice");
                }
            } else {
                if ((ui64)op->GetOffset() != EndOffset) {
                    isSeekExpected = true;
                }
                EndOffset = op->GetOffset() + opSize;

                double duration = HPMilliSecondsFloat(HPNow() - completionAction->SubmitTime);
                if (op->GetType() == IAsyncIoOperation::EType::PRead) {
                    NSan::Unpoison(op->GetData(), opSize);
                    REQUEST_VALGRIND_MAKE_MEM_DEFINED(op->GetData(), opSize);
                    Device.Mon.DeviceReadDuration.Increment(duration);
                    LWPROBE(PDiskDeviceReadDuration, Device.GetPDiskId(), duration, opSize);
                } else {
                    Device.Mon.DeviceWriteDuration.Increment(duration);
                    LWPROBE(PDiskDeviceWriteDuration, Device.GetPDiskId(), duration, opSize);
                }
                if (completionAction->FlushAction) {
                    ui64 idx = completionAction->FlushAction->OperationIdx;
                    Y_ABORT_UNLESS(WaitingNoops[idx % MaxWaitingNoops] == nullptr);
                    WaitingNoops[idx % MaxWaitingNoops] = completionAction->FlushAction;
                    completionAction->FlushAction = nullptr;
                }
                ExecuteOrScheduleCompletion(completionAction);
                auto seqnoL6 = AtomicGetAndIncrement(Device.Mon.SeqnoL6);
                Device.Mon.L6.Set(duration > Device.Reordering, seqnoL6);
            }

            if (isSeekExpected) {
                Device.Mon.DeviceExpectedSeeks->Inc();
                totalCostNs += Device.SeekCostNs;
            }

            Device.IoContext->DestroyAsyncIoOperation(op);
            ui64 firstIncompleteIdx = Device.FlightControl.FirstIncompleteIdx();
            while (NextPossibleNoop < firstIncompleteIdx) {
                ui64 i = NextPossibleNoop % MaxWaitingNoops;
                if (WaitingNoops[i] && WaitingNoops[i]->OperationIdx == NextPossibleNoop) {
                    LWTRACK(PDiskDeviceGetFromWaiting, WaitingNoops[i]->Orbit);
                    double durationMs = HPMilliSecondsFloat(HPNow() - WaitingNoops[i]->GetTime);
                    Device.Mon.DeviceFlushDuration.Increment(durationMs);
                    ExecuteOrScheduleCompletion(WaitingNoops[i]);
                    WaitingNoops[i] = nullptr;
                }
                ++NextPossibleNoop;
            }
            *Device.Mon.DeviceEstimatedCostNs += totalCostNs;
            *Device.Mon.DeviceActualCostNs += HPNanoSeconds(totalExecutionCycles);

            if (PrevEstimationAtCycle > eventGotAtCycle) {
                PrevEstimationAtCycle = eventGotAtCycle;
            }
            if (HPMilliSeconds(eventGotAtCycle - PrevEstimationAtCycle) >= 15000) {
                ui64 estimated = (*Device.Mon.DeviceEstimatedCostNs - PrevEstimatedCostNs);
                ui64 actual = (*Device.Mon.DeviceActualCostNs - PrevActualCostNs + 30000000ull);
                if (estimated != 0) {
                    *Device.Mon.DeviceOverestimationRatio = 1000ull * actual / (estimated + 30000000ull);
                    if (actual > estimated) {
                        if (actual - estimated < 15000000000ull) {
                            *Device.Mon.DeviceNonperformanceMs = (actual - estimated) / 15000000ull;
                        } else {
                            *Device.Mon.DeviceNonperformanceMs = 1000;
                        }
                    } else {
                        *Device.Mon.DeviceNonperformanceMs = 0;
                    }
                } else {
                    *Device.Mon.DeviceOverestimationRatio = 1000ull;
                    *Device.Mon.DeviceNonperformanceMs = 0ull;
                }

                PrevEstimatedCostNs = *Device.Mon.DeviceEstimatedCostNs;
                PrevActualCostNs = *Device.Mon.DeviceActualCostNs;
                PrevEstimationAtCycle = eventGotAtCycle;
                *Device.Mon.GetThreadCPU = ThreadCPUTime();
            }

            PrevEventGotAtCycle = eventGotAtCycle;
        }

        void Destroy() {
            // There are no Schedule() calls in progress
            for (ui64 idx = 0; idx < MaxWaitingNoops; ++idx) {
                if (WaitingNoops[idx]) {
                    WaitingNoops[idx]->Release(Device.PCtx->ActorSystem);
                }
            }
            // Stop the completion thread
            Device.CompletionThread->Schedule(nullptr);
        }
    };

    ////////////////////////////////////////////////////////
    // TSubmitGetThread
    ////////////////////////////////////////////////////////
    class TSubmitGetThread : public TSubmitThreadBase {
        NHPTimer::STime OpScheduleFailedTime = 0;

    public:
        TSubmitGetThread(TRealBlockDevice &device)
            : TSubmitThreadBase(device, &ThreadProc, this)
        {}

        static int ThreadProcSpdk(void* _this) {
            SetCurrentThreadName("PdSbmGet");
            static_cast<TSubmitGetThread*>(_this)->Exec();
            return 0;
        }

        static void* ThreadProc(void* _this) {
            ThreadProcSpdk(_this);
            return nullptr;
        }

        void ReleaseOp(IAsyncIoOperation *op) {
            Device.DecrementMonInFlight(op->GetType(), op->GetSize());
            Device.FreeOperation(op);
            Device.QuitCounter.Decrement();
            Device.IdleCounter.Decrement();
        }

        bool Submit(IAsyncIoOperation *op, i64 *inFlight) {
            TCompletionAction *action = static_cast<TCompletionAction*>(op->GetCookie());

            action->OperationIdx = Device.FlightControl.TrySchedule();
            if (action->OperationIdx == 0) {
                if (OpScheduleFailedTime == 0) {
                    // If failed to schedule, remember the time to use it when scheduling succeeds.
                    OpScheduleFailedTime = HPNow();
                }
                return false;
            }

            if (OpScheduleFailedTime != 0) {
                // Scheduling failed previously, calculate how much time operation had to wait for scheduling.
                *Device.Mon.DeviceWaitTimeMs += HPMilliSecondsFloat(OpScheduleFailedTime - HPNow());

                OpScheduleFailedTime = 0;
            }

            if (!Device.QuitCounter.Increment()) {
                Device.FreeOperation(op);
                TGuard<TMutex> guard(SubmitMtx);
                SubmitCondVar.Signal();
                (*inFlight)--;
                return true;
            }
            Device.IdleCounter.Increment();

            if (action->FlushAction) {
                action->FlushAction->OperationIdx = action->OperationIdx;
            }

            if (op->GetSize() == 0) {
                TAsyncIoOperationResult result;
                result.Operation = op;
                result.Result = EIoResult::Ok;
                Device.SharedCallback->Exec(&result);
                (*inFlight)--;
                return true;
            }

            Device.IncrementMonInFlight(op->GetType(), op->GetSize());

            EIoResult ret = EIoResult::TryAgain;
            while (ret == EIoResult::TryAgain) {
                action->SubmitTime = HPNow();
                if (action->FlushAction) {
                    action->FlushAction->SubmitTime = action->SubmitTime;
                }
                ret = Device.IoContext->Submit(op, Device.SharedCallback.Get());
                if (ret == EIoResult::Ok) {
                    return true;
                }
                if (Device.QuitCounter.IsBlocked()) {
                    ReleaseOp(op);
                    (*inFlight)--;
                    return true;
                }
            }
            ReleaseOp(op);
            (*inFlight)--;
            Device.BecomeErrorState(TStringBuilder() << " SpdkSubmit error, reason# " << ret);
            return true;
        }

        void Exec() {
            i64 inFlight = 0;
            bool isExiting = false;

            TAsyncIoOperationResult events[MaxEvents];

            while(!SubmitQuitCounter.IsBlocked() || SubmitQuitCounter.Get()) {
                // Submit events
                TAtomicBase ops = OperationsToBeSubmit.GetWaitingSize();
                if (inFlight < (i64)Device.DeviceInFlight && ops > 0) {
                    for (TAtomicBase idx = 0; idx < ops; ++idx) {
                        IAsyncIoOperation *op = OperationsToBeSubmit.Head();
                        if (op) {
                            const ui64 opSize = op->GetSize();
                            if (isExiting) {
                                OperationsToBeSubmit.Pop();
                                SubmitQuitCounter.Decrement();
                                Device.FreeOperation(op);
                            } else if (Submit(op, &inFlight)) {
                                OperationsToBeSubmit.Pop();
                                SubmitQuitCounter.Decrement();
                                ++inFlight;
                                TGuard<TMutex> guard(SubmitMtx);
                                AtomicSub(SubmitInFlightBytes, opSize);
                                SubmitCondVar.Signal();
                            } else {
                                break;
                            }
                        } else {
                            OperationsToBeSubmit.Pop();
                            SubmitQuitCounter.Decrement();
                            isExiting = true;
                        }
                    }
                } else if (inFlight == 0) {
                    if (isExiting) {
                        break;
                    } else {
                        OperationsToBeSubmit.ProducedWaitI();
                    }
                }

                // Get events
                do {
                    i64 ret = Device.IoContext->GetEvents(0, MaxEvents, events, TDuration::MilliSeconds(WaitTimeoutMs));
                    // TODO Stop working here in case of error
                    if (ret < 0) {
                        Device.BecomeErrorState(TStringBuilder() << " error in IoContext->GetEvents, reason# "
                                << (EIoResult)-ret);
                    }
                    inFlight -= ret;
                    Y_VERIFY_S(inFlight >= 0, "Error in inFlight# " << inFlight);
                } while (inFlight == (i64)Device.DeviceInFlight || isExiting && inFlight > 0);
            }

            Y_ABORT_UNLESS(OperationsToBeSubmit.GetWaitingSize() == 0);
        }
    };

    ////////////////////////////////////////////////////////
    // TTrimThread
    ////////////////////////////////////////////////////////
    class TTrimThread : public TThread {
        TCountedQueueOneOne<IAsyncIoOperation*, 4 << 10> TrimOperations;
        TRealBlockDevice &Device;
        std::shared_ptr<TPDiskCtx> &PCtx;

    public:
        TTrimThread(TRealBlockDevice &device)
            : TThread(&ThreadProc, this)
            , Device(device)
            , PCtx(device.PCtx)
        {}

        static void* ThreadProc(void* _this) {
            SetCurrentThreadName("PdTrim");
            static_cast<TTrimThread*>(_this)->Exec();
            return nullptr;
        }

        void Exec() {
            while(true) {
                TAtomicBase actionCount = TrimOperations.GetWaitingSize();
                if (actionCount > 0) {
                    for (TAtomicBase idx = 0; idx < actionCount; ++idx) {
                        IAsyncIoOperation *op = TrimOperations.Pop();
                        if (op == nullptr) {
                            Device.CompletionThread->Schedule(nullptr);
                            return;
                        }
                        Y_ABORT_UNLESS(op->GetType() == IAsyncIoOperation::EType::PTrim);
                        auto *completion = static_cast<TCompletionAction*>(op->GetCookie());
                        if (Device.IsTrimEnabled) {
                            Device.IdleCounter.Increment();
                            NHPTimer::STime startTime = HPNow();
                            Device.IsTrimEnabled = Device.IoContext->DoTrim(op);
                            NHPTimer::STime endTime = HPNow();
                            Device.IdleCounter.Decrement();
                            const double duration = HPMilliSecondsFloat(endTime - startTime);
                            Device.Mon.DeviceTrimDuration.Increment(duration);
                            *Device.Mon.DeviceEstimatedCostNs += completion->CostNs;
                            if (Device.PCtx->ActorSystem && Device.IsTrimEnabled) {
                                P_LOG(PRI_DEBUG, BPD01, "trim is done",
                                        (ReqId, op->GetReqId()),
                                        (TrimDurationMs, HPMilliSeconds(endTime - startTime)),
                                        (Path, Device.Path),
                                        (Offset, op->GetOffset()),
                                        (Size, op->GetSize()));
                            }
                            LWPROBE(PDiskDeviceTrimDuration, Device.GetPDiskId(), duration, op->GetOffset());
                        }
                        completion->SetResult(EIoResult::Ok);
                        Device.CompletionThread->Schedule(completion);
                        Device.IoContext->DestroyAsyncIoOperation(op);
                    }
                } else {
                    *Device.Mon.TrimThreadCPU = ThreadCPUTime();
                    TrimOperations.ProducedWaitI();
                }
            }
        }

        // Schedule action execution
        // pass action = nullptr to quit
        void Schedule(IAsyncIoOperation *op) noexcept {
            TrimOperations.Push(op);
        }
    };


protected:
    std::shared_ptr<TPDiskCtx> PCtx;
    TPDiskMon &Mon;
    TString Path;

private:
    THolder<TCompletionThread> CompletionThread;
    THolder<TTrimThread> TrimThread;
    THolder<TGetThread> GetEventsThread;
    THolder<TSubmitGetThread> SpdkSubmitGetThread;

    THolder<TSharedCallback> SharedCallback;
    THolder<TSubmitThreadBase> SubmitThread;

    bool IsFileOpened;
    bool IsInitialized;
    ui64 Reordering;
    ui64 SeekCostNs;
    bool IsTrimEnabled;
    ui32 MaxQueuedCompletionActions;

    TIdleCounter IdleCounter; // Includes reads, writes and trims

    TDeviceMode::TFlags Flags;
    TIntrusivePtr<TSectorMap> SectorMap;
    std::unique_ptr<IAsyncIoContext> IoContext;
    ISpdkState *SpdkState = nullptr;

    static constexpr int WaitTimeoutMs = 1;
    static constexpr int MaxEvents = 32;

    ui64 DeviceInFlight;
    TFlightControl FlightControl;
    TAtomicBlockCounter QuitCounter;
    TString LastWarning;
    TDeque<IAsyncIoOperation*> Trash;
    TMutex TrashMutex;

    std::optional<TDriveData> DriveData;

public:
    TRealBlockDevice(const TString &path, TPDiskMon &mon, ui64 reorderingCycles,
            ui64 seekCostNs, ui64 deviceInFlight, TDeviceMode::TFlags flags, ui32 maxQueuedCompletionActions,
            TIntrusivePtr<TSectorMap> sectorMap)
        : Mon(mon)
        , Path(path)
        , CompletionThread(nullptr)
        , TrimThread(nullptr)
        , GetEventsThread(nullptr)
        , SharedCallback(nullptr)
        , SubmitThread(nullptr)
        , IsFileOpened(false)
        , IsInitialized(false)
        , Reordering(reorderingCycles)
        , SeekCostNs(seekCostNs)
        , IsTrimEnabled(true)
        , MaxQueuedCompletionActions(maxQueuedCompletionActions)
        , IdleCounter(Mon.IdleLight)
        , Flags(flags)
        , SectorMap(sectorMap)
        , DeviceInFlight(FastClp2(deviceInFlight))
        , FlightControl(CountTrailingZeroBits(DeviceInFlight))
        , LastWarning(IsPowerOf2(deviceInFlight) ? "" : "Device inflight must be a power of 2")
    {
        if (sectorMap) {
            DriveData = TDriveData();
            DriveData->Path = path;
            DriveData->SerialNumber = sectorMap->Serial;
            DriveData->FirmwareRevision = "rev 1.0";
            DriveData->ModelNumber = "SectorMap";
        }
    }

protected:
    void Initialize(std::shared_ptr<TPDiskCtx> pCtx) override {
        PCtx = std::move(pCtx);
        Y_ABORT_UNLESS(PCtx);

        TString errStr = TDeviceMode::Validate(Flags);
        if (errStr) {
            Y_FAIL_S(IoContext->GetPDiskInfo() << " Error in device flags: " << errStr);
        }

        Y_ABORT_UNLESS(PCtx->ActorSystem->AppData<TAppData>());
        Y_ABORT_UNLESS(PCtx->ActorSystem->AppData<TAppData>()->IoContextFactory);
        auto *factory = PCtx->ActorSystem->AppData<TAppData>()->IoContextFactory;
        IoContext = factory->CreateAsyncIoContext(Path, PCtx->PDiskId, Flags, SectorMap);
        if (Flags & TDeviceMode::UseSpdk) {
            SpdkState = factory->CreateSpdkState();
        }

        while (true) {
            EIoResult ret = IoContext->Setup(MaxEvents, Flags & TDeviceMode::LockFile);
            if (ret == EIoResult::Ok) {
                IsFileOpened = true;
                break;
            } else if (ret == EIoResult::FileOpenError || ret == EIoResult::FileLockError) {
                IsFileOpened = false;
                if (ret == EIoResult::FileOpenError) {
                    LastWarning = "got EIoResult::FileOpenError from IoContext->Setup";
                } else if (ret == EIoResult::FileLockError) {
                    LastWarning = "got EIoResult::FileLockError from IoContext->Setup";
                }
                break;
            } else if (ret == EIoResult::TryAgain) {
                continue;
            } else {
                Y_FAIL_S(IoContext->GetPDiskInfo() << " Error initing IoContext: " << ret);
            }
        }

        IoContext->InitializeMonitoring(Mon);
        //IoContext->InitializeMonitoring(Mon.DeviceOperationPoolTotalAllocations, Mon.DeviceOperationPoolFreeObjectsMin);
        if (!LastWarning.empty() && PCtx->ActorSystem) {
            P_LOG(PRI_WARN, BPD01, "", (Warning, LastWarning));
        }
        if (IsFileOpened) {
            IoContext->SetActorSystem(PCtx->ActorSystem);
            CompletionThread = MakeHolder<TCompletionThread>(*this, MaxQueuedCompletionActions);
            TrimThread = MakeHolder<TTrimThread>(*this);
            SharedCallback = MakeHolder<TSharedCallback>(*this);
            if (Flags & TDeviceMode::UseSpdk) {
                SpdkSubmitGetThread = MakeHolder<TSubmitGetThread>(*this);
                SpdkState->LaunchThread(TSubmitGetThread::ThreadProcSpdk, SpdkSubmitGetThread.Get());
            } else {
                if (Flags & TDeviceMode::UseSubmitGetThread) {
                    SubmitThread = MakeHolder<TSubmitGetThread>(*this);
                    SubmitThread->Start();
                } else {
                    SubmitThread = MakeHolder<TSubmitThread>(*this);
                    SubmitThread->Start();
                    GetEventsThread = MakeHolder<TGetThread>(*this);
                    GetEventsThread->Start();
                }
            }
            CompletionThread->Start();
            TrimThread->Start();
            IsInitialized = true;
        }
    }

    bool IsGood() override {
        return IsFileOpened && IsInitialized;
    }

    int GetLastErrno() override {
        return IoContext->GetLastErrno();
    }

    TString DebugInfo() override {
        TStringStream str;
        str << " Path# " << Path.Quote();
        str << " IsFileOpened# " << IsFileOpened;
        str << " IsInitialized# " << IsInitialized;
        str << " LastWarning# " << LastWarning.Quote();
        str << " LastErrno# " << IoContext->GetLastErrno();
        return str.Str();
    }

    void IncrementMonInFlight(IAsyncIoOperation::EType type, ui32 size) {
        switch (type) {
            case IAsyncIoOperation::EType::PWrite:
                (*Mon.DeviceInFlightBytesWrite) += size;
                Mon.DeviceInFlightWrites->Inc();
                break;
            case IAsyncIoOperation::EType::PRead:
                (*Mon.DeviceInFlightBytesRead) += size;
                Mon.DeviceInFlightReads->Inc();
                break;
            default:
                break;
        }
        Mon.DeviceTakeoffs->Inc();
    }

    void DecrementMonInFlight(IAsyncIoOperation::EType type, ui32 size) {
        switch (type) {
            case IAsyncIoOperation::EType::PWrite:
                (*Mon.DeviceInFlightBytesWrite) -= size;
                Mon.DeviceInFlightWrites->Dec();
                (*Mon.DeviceBytesWritten) += size;
                Mon.DeviceWrites->Inc();
                break;
            case IAsyncIoOperation::EType::PRead:
                (*Mon.DeviceInFlightBytesRead) -= size;
                Mon.DeviceInFlightReads->Dec();
                (*Mon.DeviceBytesRead) += size;
                Mon.DeviceReads->Inc();
                break;
            default:
                break;
        }
        Mon.DeviceLandings->Inc();
    }

    void FreeOperation(IAsyncIoOperation *op) {
        TCompletionAction *action = static_cast<TCompletionAction*>(op->GetCookie());

        if (action->FlushAction) {
            action->FlushAction->Release(PCtx->ActorSystem);
        }
        action->Release(PCtx->ActorSystem);
        {
            TGuard<TMutex> guard(TrashMutex);
            Trash.push_back(op);
        }
    }

    void Submit(IAsyncIoOperation *op) {
        if (QuitCounter.IsBlocked()) {
            FreeOperation(op);
            return;
        }

        const ui64 size = op->GetSize();
        const ui64 type = static_cast<ui64>(op->GetType());
        LWPROBE(PDiskDeviceOperationSizeAndType, GetPDiskId(), size, type);

        if (Flags & TDeviceMode::UseSpdk) {
            SpdkSubmitGetThread->Schedule(op);
        } else {
            SubmitThread->Schedule(op);
        }
    }

    void PreadSync(void *data, ui32 size, ui64 offset, TReqId reqId, NWilson::TTraceId *traceId) override {
        TSignalEvent doneEvent;
        PreadAsync(data, size, offset, new TCompletionSignal(&doneEvent), reqId, traceId);
        doneEvent.WaitI();
    }

    void PwriteSync(const void *data, ui64 size, ui64 offset, TReqId reqId, NWilson::TTraceId *traceId) override {
        TSignalEvent doneEvent;
        PwriteAsync(data, size, offset, new TCompletionSignal(&doneEvent), reqId, traceId);
        doneEvent.WaitI();
    }

    void TrimSync(ui32 size, ui64 offset) override {
        IAsyncIoOperation* op = IoContext->CreateAsyncIoOperation(nullptr, {}, nullptr);
        IoContext->PreparePTrim(op, size, offset);
        IsTrimEnabled = IoContext->DoTrim(op);
        IoContext->DestroyAsyncIoOperation(op);
    }

    void PreadAsync(void *data, ui32 size, ui64 offset, TCompletionAction *completionAction, TReqId reqId,
            NWilson::TTraceId *traceId) override {
        Y_ABORT_UNLESS(completionAction);
        if (!IsInitialized) {
            completionAction->Release(PCtx->ActorSystem);
            return;
        }
        if (data && size) {
            Y_ABORT_UNLESS(intptr_t(data) % 512 == 0);
            REQUEST_VALGRIND_CHECK_MEM_IS_ADDRESSABLE(data, size);
        }

        IAsyncIoOperation* op = IoContext->CreateAsyncIoOperation(completionAction, reqId, traceId);
        IoContext->PreparePRead(op, data, size, offset);
        Submit(op);
    }

    void PwriteAsync(const void *data, ui64 size, ui64 offset, TCompletionAction *completionAction, TReqId reqId,
            NWilson::TTraceId *traceId) override {
        Y_ABORT_UNLESS(completionAction);
        if (!IsInitialized) {
            completionAction->Release(PCtx->ActorSystem);
            return;
        }
        if (data && size) {
            Y_ABORT_UNLESS(intptr_t(data) % 512 == 0);
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(data, size);
        }

        IAsyncIoOperation* op = IoContext->CreateAsyncIoOperation(completionAction, reqId, traceId);
        IoContext->PreparePWrite(op, const_cast<void*>(data), size, offset);
        Submit(op);
    }

    void FlushAsync(TCompletionAction *completionAction, TReqId reqId) override {
        Y_ABORT_UNLESS(completionAction);
        if (!IsInitialized) {
            completionAction->Release(PCtx->ActorSystem);
            return;
        }

        IAsyncIoOperation* op = IoContext->CreateAsyncIoOperation(completionAction, reqId, nullptr);
        IoContext->PreparePRead(op, nullptr, 0, 0);
        Submit(op);
    }

    void NoopAsync(TCompletionAction *completionAction, TReqId /*reqId*/) override {
        Y_ABORT_UNLESS(completionAction);
        if (!IsInitialized) {
            completionAction->Release(PCtx->ActorSystem);
            return;
        }
        if (QuitCounter.IsBlocked()) {
            completionAction->Release(PCtx->ActorSystem);
            return;
        }

        completionAction->SetResult(EIoResult::Ok);
        CompletionThread->Schedule(completionAction);
    }

    void NoopAsyncHackForLogReader(TCompletionAction *completionAction, TReqId /*reqId*/) override {
        Y_ABORT_UNLESS(completionAction);
        if (!IsInitialized) {
            completionAction->Release(PCtx->ActorSystem);
            return;
        }
        if (QuitCounter.IsBlocked()) {
            completionAction->Release(PCtx->ActorSystem);
            return;
        }

        completionAction->SetResult(EIoResult::Ok);
        CompletionThread->ScheduleHackForLogReader(completionAction);
    }

    void TrimAsync(ui32 size, ui64 offset, TCompletionAction *completionAction, TReqId reqId) override {
        Y_ABORT_UNLESS(completionAction);
        if (!IsInitialized || QuitCounter.IsBlocked()) {
            return;
        }

        IAsyncIoOperation* op = IoContext->CreateAsyncIoOperation(completionAction, reqId, nullptr);
        IoContext->PreparePTrim(op, size, offset);
        TrimThread->Schedule(op);
    }

    bool GetIsTrimEnabled() override {
        return IsTrimEnabled;
    }

    TDriveData GetDriveData() override {
        if (!DriveData) {
            TStringStream details;
            if (DriveData = ::NKikimr::NPDisk::GetDriveData(Path, &details)) {
                P_LOG(PRI_NOTICE, BPD01, "Gathered DriveData", (Data, DriveData->ToString(false)),
                    (Details, details.Str()));
            } else {
                P_LOG(PRI_WARN, BPD01, "Error on gathering DriveData", (Details, details.Str()));
            }
        }
        return DriveData.value_or(TDriveData());
    }

    void SetWriteCache(bool isEnable) override {
        if (TFileHandle *handle = IoContext->GetFileHandle()) {
            TStringStream details;
            EWriteCacheResult res = NKikimr::NPDisk::SetWriteCache(*handle, Path, isEnable, &details);
            if (res != WriteCacheResultOk) {
                P_LOG(PRI_WARN, BPD01, "Error on setting write cache", (Details, details.Str()));
            }
        }
    }

    ui32 GetPDiskId() override {
        return PCtx->PDiskId;
    }

    virtual ~TRealBlockDevice() {
        Stop();
        while (Trash.size() > 0) {
            IAsyncIoOperation *op = Trash.front();
            Trash.pop_front();
            IoContext->DestroyAsyncIoOperation(op);
        }
    }

    void BecomeErrorState(const TString& info) {
        // Block only B flag so device will not be working but when Stop() will be called AFlag will be toggled
        QuitCounter.BlockB();
        TString fullInfo = TStringBuilder() << IoContext->GetPDiskInfo() << info;
        if (PCtx) {
            PCtx->ActorSystem->Send(PCtx->PDiskActor, new TEvDeviceError(fullInfo));
        } else {
            Y_FAIL_S(fullInfo);
        }
    }

    void Stop() override {
        TAtomicBlockCounter::TResult res;
        QuitCounter.BlockA(res);
        if (res.PrevA ^ res.A) { // res.ToggledA()
            if (IsInitialized) {
                Y_ABORT_UNLESS(TrimThread);
                Y_ABORT_UNLESS(CompletionThread);
                TrimThread->Schedule(nullptr); // Stop the Trim thread
                if (Flags & TDeviceMode::UseSpdk) {
                    Y_ABORT_UNLESS(SpdkSubmitGetThread);
                    SpdkSubmitGetThread->Schedule(nullptr); // Stop the SpdkSubmitGetEvents thread
                    SpdkState->WaitAllThreads();
                } else {
                    Y_ABORT_UNLESS(SubmitThread);
                    SubmitThread->Schedule(nullptr); // Stop the SubminEvents thread
                    SubmitThread->Join();

                    if (!(Flags & TDeviceMode::UseSubmitGetThread)) {
                        Y_ABORT_UNLESS(GetEventsThread);
                        GetEventsThread->Join();
                    }
                }
                SharedCallback->Destroy();
                TrimThread->Join();
                CompletionThread->Join();
                IsInitialized = false;
            } else {
                Y_ABORT_UNLESS(SubmitThread.Get() == nullptr);
                Y_ABORT_UNLESS(GetEventsThread.Get() == nullptr);
                Y_ABORT_UNLESS(TrimThread.Get() == nullptr);
                Y_ABORT_UNLESS(CompletionThread.Get() == nullptr);
            }
            if (IsFileOpened) {
                EIoResult ret = IoContext->Destroy();
                if (ret != EIoResult::Ok) {
                    BecomeErrorState(TStringBuilder() << " Error in IoContext desctruction, reason# " << ret);
                }
                IsFileOpened = false;
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TCachedBlockDevice
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TCachedBlockDevice : public TRealBlockDevice {

    class TCachedReadCompletion : public TCompletionAction {
        TCachedBlockDevice &CachedBlockDevice;
        void *Data;
        ui32 Size;
        ui64 Offset;
        TReqId ReqId;

    public:
        TCachedReadCompletion(TCachedBlockDevice &cachedBlockDevice, void *data, ui32 size, ui64 offset, TReqId reqId)
            : CachedBlockDevice(cachedBlockDevice)
            , Data(data)
            , Size(size)
            , Offset(offset)
            , ReqId(reqId)
        {}

        ui64 GetOffset() {
            return Offset;
        }

        ui32 GetSize() {
            return Size;
        }

        void* GetData() {
            return Data;
        }

        TVector<ui64>& GetBadOffsets() {
            return BadOffsets;
        }

        void Exec(TActorSystem *actorSystem) override {
            if (actorSystem) {
                STLOGX(*actorSystem, PRI_DEBUG, BS_PDISK, BPD01, "Exec TCachedReadCompletion", (ReqId, ReqId), (Offset, Offset));
            }
            CachedBlockDevice.ExecRead(this, actorSystem);
        }

        void Release(TActorSystem *actorSystem) override {
            if (actorSystem) {
                STLOGX(*actorSystem, PRI_DEBUG, BS_PDISK, BPD01, "Release TCachedReadCompletion", (ReqId, ReqId), (Offset, Offset));
            }
            CachedBlockDevice.ReleaseRead(this, actorSystem);
        }
    };

    struct TRead {
        void *Data;
        ui32 Size;
        ui64 Offset;
        TCompletionAction *CompletionAction;
        TReqId ReqId;
        NWilson::TTraceId *TraceId;

        TRead(void *data, ui32 size, ui64 offset, TCompletionAction *completionAction, TReqId reqId,
                NWilson::TTraceId *traceId)
            : Data(data)
            , Size(size)
            , Offset(offset)
            , CompletionAction(completionAction)
            , ReqId(reqId)
            , TraceId(traceId)
        {
        }
    };

    static constexpr ui64 MaxCount = 500ull;
    static constexpr ui64 MaxReadsInFly = 2;

    TMutex CacheMutex;
    TLogCache Cache; // cache records MUST NOT cross chunk boundaries
    TMultiMap<ui64, TRead> ReadsForOffset;
    TMap<ui64, std::shared_ptr<TCachedReadCompletion>> CurrentReads;
    ui64 ReadsInFly;
    TPDisk * const PDisk;

    void UpdateReads() {
        auto nextIt = ReadsForOffset.begin();
        for (auto it = ReadsForOffset.begin(); it != ReadsForOffset.end(); it = nextIt) {
            nextIt++;
            TRead &read = it->second;
            const TLogCache::TCacheRecord* cached = Cache.Find(read.Offset);
            if (cached) {
                if (read.Size <= cached->Data.Size()) {
                    memcpy(read.Data, cached->Data.GetData(), read.Size);
                    Mon.DeviceReadCacheHits->Inc();
                    Y_ABORT_UNLESS(read.CompletionAction);
                    for (size_t i = 0; i < cached->BadOffsets.size(); ++i) {
                        read.CompletionAction->RegisterBadOffset(cached->BadOffsets[i]);
                    }
                    NoopAsyncHackForLogReader(read.CompletionAction, read.ReqId);
                    ReadsForOffset.erase(it);
                }
            }
        }
        if (ReadsInFly >= MaxReadsInFly) {
            return;
        }

        for (auto it = ReadsForOffset.begin(); it != ReadsForOffset.end(); it++) {
            TRead &read = it->second;
            auto currentIt = CurrentReads.find(read.Offset);
            if (currentIt == CurrentReads.end()) {
                auto ptr = std::make_shared<TCachedReadCompletion>(*this, read.Data, read.Size, read.Offset, read.ReqId);
                CurrentReads[read.Offset] = ptr;
                PCtx->ActorSystem->Send(PCtx->PDiskActor, new TEvReadLogContinue(read.Data, read.Size, read.Offset, ptr, read.ReqId));
                ReadsInFly++;
                if (ReadsInFly >= MaxReadsInFly) {
                    return;
                }
            }
        }
    }

public:
    TCachedBlockDevice(const TString &path, TPDiskMon &mon, ui64 reorderingCycles,
            ui64 seekCostNs, ui64 deviceInFlight, TDeviceMode::TFlags flags, ui32 maxQueuedCompletionActions,
            TIntrusivePtr<TSectorMap> sectorMap, TPDisk * const pdisk)
        : TRealBlockDevice(path, mon, reorderingCycles, seekCostNs, deviceInFlight, flags,
                maxQueuedCompletionActions, sectorMap)
        , ReadsInFly(0)
        , PDisk(pdisk)
    {}

    void ExecRead(TCachedReadCompletion *completion, TActorSystem *) {
        Y_ASSERT(PDisk);
        TStackVec<TCompletionAction*, 32> pendingActions;
        {
            TGuard<TMutex> guard(CacheMutex);
            ui64 offset = completion->GetOffset();
            auto currentReadIt = CurrentReads.find(offset);
            Y_ABORT_UNLESS(currentReadIt != CurrentReads.end());
            auto range = ReadsForOffset.equal_range(offset);

            ui64 chunkIdx = offset / PDisk->Format.ChunkSize;
            Y_ABORT_UNLESS(chunkIdx < PDisk->ChunkState.size());
            if (TChunkState::DATA_COMMITTED == PDisk->ChunkState[chunkIdx].CommitState) {
                if ((offset % PDisk->Format.ChunkSize) + completion->GetSize() > PDisk->Format.ChunkSize) {
                    // TODO: split buffer if crossing chunk boundary instead of completely discarding it
                    P_LOG(PRI_INFO, BPD01, "Skip caching log read due to chunk boundary crossing");
                } else {
                    if (Cache.Size() >= MaxCount) {
                        Cache.Pop();
                    }
                    const char* dataPtr = static_cast<const char*>(completion->GetData());
                    Cache.Insert(
                        TLogCache::TCacheRecord(
                            completion->GetOffset(),
                            TRcBuf(TString(dataPtr, dataPtr + completion->GetSize())),
                            completion->GetBadOffsets()));
                }
            }

            auto nextIt = range.first;
            for (auto it = range.first; it != range.second; it = nextIt) {
                nextIt++;
                TRead &read = it->second;
                if (read.Size <= completion->GetSize()) {
                    if (read.Data != completion->GetData()) {
                        memcpy(read.Data, completion->GetData(), read.Size);
                        Mon.DeviceReadCacheHits->Inc();
                    } else {
                        Mon.DeviceReadCacheMisses->Inc();
                    }
                    Y_ABORT_UNLESS(read.CompletionAction);
                    for (ui64 badOffset : completion->GetBadOffsets()) {
                        read.CompletionAction->RegisterBadOffset(badOffset);
                    }
                    pendingActions.push_back(read.CompletionAction);
                    ReadsForOffset.erase(it);
                }
            }
            CurrentReads.erase(currentReadIt);
            ReadsInFly--;
            UpdateReads();
        }

        for (size_t i = 0; i < pendingActions.size(); ++i) {
            pendingActions[i]->Exec(PCtx->ActorSystem);
        }

        {
            TGuard<TMutex> guard(CacheMutex);
            if (ReadsInFly == 0) {
                ClearCache();
            }
        }
    }

    void ReleaseRead(TCachedReadCompletion *completion, TActorSystem *) {
        TGuard<TMutex> guard(CacheMutex);

        if (!completion->CanHandleResult()) {
            // If error, notify all underlying reads of that error.
            // Notice that reads' CompletionActions are not released here.
            // This should happen on device stop.
            ui64 offset = completion->GetOffset();
            auto range = ReadsForOffset.equal_range(offset);

            for (auto it = range.first; it != range.second; ++it) {
                TRead &read = it->second;

                Y_ABORT_UNLESS(read.CompletionAction);

                read.CompletionAction->SetResult(completion->Result);
                read.CompletionAction->SetErrorReason(completion->ErrorReason);
            }
        }

        auto it = CurrentReads.find(completion->GetOffset());
        Y_ABORT_UNLESS(it != CurrentReads.end());
        CurrentReads.erase(it);
        ReadsInFly--;
    }

    virtual ~TCachedBlockDevice() {
        Stop();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // cache related methods implementation
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    // Can be called from completion Exec
    virtual void CachedPreadAsync(void *data, ui32 size, ui64 offset, TCompletionAction *completionAction,
            TReqId reqId, NWilson::TTraceId *traceId) override {
        TGuard<TMutex> guard(CacheMutex);
        ReadsForOffset.emplace(offset, TRead(data, size, offset, completionAction, reqId, traceId));
        UpdateReads();
    }

    virtual void ClearCache() override {
        TGuard<TMutex> guard(CacheMutex);
        Cache.Clear();
    }

    virtual void EraseCacheRange(ui64 begin, ui64 end) override {
        TGuard<TMutex> guard(CacheMutex);
        Cache.EraseRange(begin, end);
    }

    void Stop() override {
        TRealBlockDevice::Stop();
        CurrentReads.clear();
        for (auto it = ReadsForOffset.begin(); it != ReadsForOffset.end(); ++it) {
            if (it->second.CompletionAction) {
                it->second.CompletionAction->Release(PCtx->ActorSystem);
            }
        }
        ReadsForOffset.clear();
    }
};

IBlockDevice* CreateRealBlockDevice(const TString &path, TPDiskMon &mon, ui64 reorderingCycles,
        ui64 seekCostNs, ui64 deviceInFlight, TDeviceMode::TFlags flags, ui32 maxQueuedCompletionActions,
        TIntrusivePtr<TSectorMap> sectorMap, TPDisk * const pdisk) {
    return new TCachedBlockDevice(path, mon, reorderingCycles, seekCostNs, deviceInFlight, flags,
            maxQueuedCompletionActions, sectorMap, pdisk);
}

IBlockDevice* CreateRealBlockDeviceWithDefaults(const TString &path, TPDiskMon &mon, TDeviceMode::TFlags flags,
        TIntrusivePtr<TSectorMap> sectorMap, TActorSystem *actorSystem, TPDisk * const pdisk) {
    IBlockDevice *device = CreateRealBlockDevice(path, mon, 0, 0, 4, flags, 8, sectorMap, pdisk);
    device->Initialize(std::make_shared<TPDiskCtx>(actorSystem));
    return device;
}

} // NPDisk
} // NKikimr
