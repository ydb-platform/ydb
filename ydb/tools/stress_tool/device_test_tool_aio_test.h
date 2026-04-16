#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_mon.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_util_flightcontrol.h>
#include <ydb/library/pdisk_io/aio.h>
#include <ydb/library/pdisk_io/buffers.h>
#include <ydb/library/pdisk_io/file_params.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/monlib/dynamic_counters/percentile/percentile_lg.h>

#include <util/generic/string.h>
#include <util/random/entropy.h>
#include <util/random/mersenne64.h>
#include <util/stream/format.h>
#include <util/string/printf.h>
#include <util/system/condvar.h>
#include <util/system/hp_timer.h>
#include <util/system/mutex.h>

#include <cstdlib>

#include "device_test_tool.h"

#if defined(_tsan_enabled_)
#define IS_SLOW_SYSTEM 1
#else
#define IS_SLOW_SYSTEM 0
#endif


namespace NKikimr {

struct TAlignedDataBuffers {
    static constexpr ui64 Alignment = 4096;
    static constexpr ui64 PoolTotalSizeBytes = 2ull << 30;

    ui32 Size;
    ui32 BuffersPerPool;
    ui32 Count;
    TVector<NPDisk::TAlignedData> Pools;

    TAlignedDataBuffers(ui32 size, ui32 count, bool useHugePages)
        : Size(size)
        , BuffersPerPool(PoolTotalSizeBytes / Size)
        , Count(AlignUp(count, BuffersPerPool))
    {
        Pools.reserve(Count / BuffersPerPool);
        for (ui32 i = 0; i < Count / BuffersPerPool; ++i) {
            Pools.emplace_back((ui64)PoolTotalSizeBytes, useHugePages);
        }
        Y_ASSERT(intptr_t(Size) % Alignment == 0);
    }

    void FillRandom() {
         NPrivate::TMersenne64 randGen(Seed());
         for (ui32 i = 0; i < Pools.size(); i++) {
             ui64 *buff64 = (ui64*)Pools[i].Get();
             ui32 size64 = Pools[i].Size() / sizeof(ui64);
             for (ui32 i = 0; i < size64; ++i) {
                 buff64[i] = randGen.GenRand();
             }
         }
    }

    ui64 TotalSize() {
        return Count * Size;
    }

    ui8 *operator[](ui32 index) {
        return Pools[index / BuffersPerPool].Get() + Size * (index % BuffersPerPool);
    }
};

class TFlightControlMutex {
    TAtomicBase QueueDepth;

    TAtomic InFlight;
    TMutex SchedulerMutex;
    TCondVar SchedulerCondVar;

public:

    TFlightControlMutex(ui32 qd_bits)
        : QueueDepth(1 << qd_bits)
        , InFlight(0)
    {
    }

    TAtomicBase Schedule() {
        if (QueueDepth == AtomicGet(InFlight)) {
            TGuard<TMutex> grd(SchedulerMutex);
            while (QueueDepth == AtomicGet(InFlight)) {
                if (SchedulerCondVar.WaitT(SchedulerMutex, TDuration::Seconds(1))) {
                    break;
                }
            }
        }
        AtomicIncrement(InFlight);
        return 1;
    }

    void MarkComplete(ui64 idx) {
        Y_UNUSED(idx);
        AtomicDecrement(InFlight);
        TGuard<TMutex> grd(SchedulerMutex);
        SchedulerCondVar.Signal();
    }

    TAtomicBase GetFreeSize() {
        return QueueDepth - AtomicGet(InFlight);
    }
};

static constexpr ui32 MaxEvents = 128;

class TAioTest : public TPerfTest {
    struct TTestJob {
        NPDisk::IAsyncIoOperation *Op;
        NHPTimer::STime Start;
        ui64 Idx;
    };

    class TSharedCallback : public NPDisk::ICallback {
        TAioTest *Test;

    public:
        TSharedCallback(TAioTest *test)
            : Test(test)
        {}

        void Exec(NPDisk::TAsyncIoOperationResult *res) {
            Y_VERIFY_S(res->Result == NPDisk::EIoResult::Ok, "Operation ended on device with error# " << res->Result);
            TTestJob *doneJob = static_cast<TTestJob*>(res->Operation->GetCookie());
            Test->FlightControl.MarkComplete(doneJob->Idx, doneJob->Op->GetSize());
            AtomicDecrement(Test->CurrentInFlight);
            const ui64 now = HPNow();
            const ui64 durationUs = HPMicroSeconds(now - doneJob->Start);
            if (now > Test->StartTime + NHPTimer::GetCyclesPerSecond()) {
                switch (doneJob->Op->GetType()) {
                    case NPDisk::IAsyncIoOperation::EType::PRead:
                        Test->ReadLatencyUs.Increment(durationUs);
                        ++Test->ReadEventsDone;
                        break;
                    case NPDisk::IAsyncIoOperation::EType::PWrite:
                        Test->WriteLatencyUs.Increment(durationUs);
                        ++Test->WriteEventsDone;
                        break;
                    default:
                        Y_FAIL_S("Unexpected operation type# " << doneJob->Op->GetType());
                }
            }
        }
    };

    const ui32 QueueDepth;
    std::unique_ptr<NPDisk::IAsyncIoContext> IoContext;
    NPDisk::TFlightControl FlightControl;
    const ui32 DurationSec;
    const ui32 PrepareDurationSec;
    const ui32 BuffSize;
    const float ReadProportion;
    const ui64 RequestsPoolSizeMax;
    const ui64 DeviceSectorSize = 4096;
    const ui32 NumberOfRandomRefills;
    TAlignedDataBuffers Buffers;
    ui64 DeviceSizeBytes;
    TVector<TTestJob> ReqPool;
    NHPTimer::STime NextPrintTime;
    NHPTimer::STime StartTime;
    ui64 CurrentJob = 0;
    ui64 ReadEventsDone = 0;
    ui64 WriteEventsDone = 0;
    ui64 LastReadEventsDone = 0;
    ui64 LastWriteEventsDone = 0;
    NMonitoring::TPercentileTrackerLg<10, 5, 1> ReadLatencyUs;
    NMonitoring::TPercentileTrackerLg<10, 5, 1> WriteLatencyUs;
    TSharedCallback Callback;
    TAtomic IsRunning = 1;
    TAtomic CurrentInFlight = 0;
    TThread SubmitJobsThread;
    TThread GetJobsThread;


public:
    TAioTest(const TPerfTestConfig& cfg, const NDevicePerfTest::TAioTest& testProto)
        : TPerfTest(cfg)
        , QueueDepth(testProto.GetQueueDepth() != 0 ? FastClp2(testProto.GetQueueDepth()) : 4)
        , IoContext(NPDisk::CreateAsyncIoContextReal(cfg.Path, 0, 0))
        , FlightControl(CountTrailingZeroBits(QueueDepth))
        , DurationSec(testProto.GetDurationSeconds() != 0 ? testProto.GetDurationSeconds() : 180)
        , PrepareDurationSec(DurationSec >= 60 ? 30 : DurationSec / 2)
        , BuffSize(testProto.GetRequestSize() != 0 ? testProto.GetRequestSize() : 4096)
        , ReadProportion(testProto.GetReadProportion())
        , RequestsPoolSizeMax((1u << 30) / BuffSize)
        , NumberOfRandomRefills(testProto.GetNumberOfRandomRefills())
        , Buffers(BuffSize, RequestsPoolSizeMax, false)
        , ReqPool()
        , Callback(this)
        , SubmitJobsThread(&StartSubmitJobsThread, this)
        , GetJobsThread(&StartGetJobsThread, this)
    {
        ReqPool.reserve(RequestsPoolSizeMax);
    }

    static void *StartSubmitJobsThread(void *this_) {
        TThread::SetCurrentThreadName("SubmitJobsThread");
        static_cast<TAioTest*>(this_)->SubmitJobs();
        return nullptr;
    }

    static void *StartGetJobsThread(void *this_) {
        TThread::SetCurrentThreadName("GetJobsThread");
        static_cast<TAioTest*>(this_)->GetJobs();
        return nullptr;
    }

    void Init() override {
        bool isBlockDevice = false;
        DetectFileParameters(Cfg.Path, DeviceSizeBytes, isBlockDevice);

        NPDisk::EIoResult res = IoContext->Setup(MaxEvents, Cfg.DoLockFile);
        if (res != NPDisk::EIoResult::Ok) {
            if (res == NPDisk::EIoResult::FileLockError) {
                // find out the reason of the lock error
                TString lsofCommand = "lsof " + Cfg.Path;
                int returnCode = system(lsofCommand.c_str());
                if (returnCode == 0) {
                    Y_FAIL_S("File is locked by another process, see lsof output above");
                } else {
                    Y_FAIL_S("Error in IoContext->Setup, error# " << res);
                }
            }
            Y_VERIFY_S(res == NPDisk::EIoResult::Ok, "Error initializing IoContext, error# " << res
                    << "; path# " << Cfg.Path.Quote());
        }

        THolder<TFileHandle> file(new TFileHandle(Cfg.Path.c_str(), OpenExisting | RdWr | DirectAligned | Sync));

        if (NumberOfRandomRefills > 0) {
            THPTimer start;
            start.Reset();
            Buffers.FillRandom();
            TEST_COUT_LN("Speed of random generator# " << Buffers.TotalSize() / 1e6 / start.Passed() << " MB/s");
        } else {
            Buffers.FillRandom();
        }

        NPrivate::TMersenne64 randGen(Seed());
        ui32 i = 0;
        auto start = HPNow();
        while (i < RequestsPoolSizeMax && HPSecondsFloat(HPNow() - start) < PrepareDurationSec) {
            long long offset = randGen.GenRand() % (DeviceSizeBytes - BuffSize);
            offset = (offset / DeviceSectorSize) * DeviceSectorSize;
            Y_ASSERT((intptr_t)Buffers[i] % 512 == 0);
            ReqPool.emplace_back();
            ReqPool.back().Op = IoContext->CreateAsyncIoOperation(&ReqPool.back(), NPDisk::TReqId(), nullptr);
            if (double(randGen.GenRand()) / Max<ui64>() < ReadProportion) {
                IoContext->PreparePRead(ReqPool.back().Op, Buffers[i], BuffSize, offset);
                file->Pwrite(Buffers[i], BuffSize, offset);
            } else {
                IoContext->PreparePWrite(ReqPool.back().Op, Buffers[i], BuffSize, offset);
            }
            ++i;
        }

        NextPrintTime = HPNow();
        StartTime = HPNow();
    }

    void TryFillRandom(void *buff, ui64 size) {
        if (NumberOfRandomRefills == 0) {
            return;
        }
        NPrivate::TMersenne64 randGen(Seed());
        for (ui32 i = 0; i < NumberOfRandomRefills; ++i) {
            ui32 size64 = size / sizeof(ui64);
            ui64 *buff64 = (ui64*)buff;
            for (ui32 i = 0; i < size64; ++i) {
                buff64[i] = randGen.GenRand();
            }
        }
    }

    void SubmitJobs() {
        NPrivate::TMersenne64 randGen(Seed());
        ui64 start = HPNow();
        while (HPSecondsFloat(HPNow() - start) < DurationSec) {
            NPDisk::IAsyncIoOperation *op = ReqPool[CurrentJob].Op;
            if (op->GetType() == NPDisk::IAsyncIoOperation::EType::PWrite) {
                long long offset = randGen.GenRand() % (DeviceSizeBytes - BuffSize);
                offset = (offset / DeviceSectorSize) * DeviceSectorSize;
                IoContext->PreparePWrite(op, op->GetData(), op->GetSize(), offset);
                TryFillRandom(op->GetData(), BuffSize);
            }

            double blockedMs = 0;
            ReqPool[CurrentJob].Idx = FlightControl.Schedule(blockedMs, op->GetSize());
            ReqPool[CurrentJob].Start = HPNow();
            CurrentJob = (CurrentJob + 1) % ReqPool.size();
            NHPTimer::STime submitStartTime = HPNow();
            NPDisk::EIoResult res = IoContext->Submit(op, &Callback);
            while (res == NPDisk::EIoResult::TryAgain) {
                Cerr << "TryAgain error in IoContext->Submit" << Endl;
                res = IoContext->Submit(op, &Callback);
                if (HPSecondsFloat(HPNow() - submitStartTime) > 60.0) {
                    Y_ABORT("Device is not working, could not submit a requets in 60 seconds");
                }
            }
            Y_VERIFY_S(res == NPDisk::EIoResult::Ok, "Error in IoContext->Submit, error# " << res);
            AtomicIncrement(CurrentInFlight);
        }
    }

    void ScheduledPrint() {
        if (HPNow() > NextPrintTime) {
            NextPrintTime += NHPTimer::GetCyclesPerSecond();
            if (ReadProportion != 0.) {
                TEST_COUT_LN("\rRead speed# " << Sprintf("%.1f MB/s", double(ReadEventsDone - LastReadEventsDone)
                            * BuffSize / 1e6));
                LastReadEventsDone = ReadEventsDone;
            }
            if (ReadProportion != 1.) {
                TEST_COUT_LN("\rWrite speed# " << Sprintf("%.1f MB/s", double(WriteEventsDone - LastWriteEventsDone)
                            * BuffSize / 1e6));
                LastWriteEventsDone = WriteEventsDone;
            }
        }
    }

    void GetJobs() {
        TStackVec<NPDisk::TAsyncIoOperationResult, MaxEvents> ioEvents(MaxEvents);
        const auto ioTimeout = TDuration::Seconds(IS_SLOW_SYSTEM ? 5 : 1);
        const i64 MaxAttempts = 60;
        i64 attemptsRemaining = MaxAttempts;
        while (AtomicGet(IsRunning) || AtomicGet(CurrentInFlight)) {
            int res = IoContext->GetEvents(1, MaxEvents, &ioEvents[0], ioTimeout);
            if (res < 0) {
                auto ioRes = static_cast<NPDisk::EIoResult>(-res);
                if (ioRes == NPDisk::EIoResult::InterruptedSystemCall
                        || ioRes == NPDisk::EIoResult::TryAgain) {
                    continue;
                }
            } else {
                if (res == 0) {
                    attemptsRemaining--;
                    Y_VERIFY_S(attemptsRemaining > 0, "IoContext->GetEvents takes too long");
                } else {
                    attemptsRemaining = MaxAttempts;
                }
            }
            //ScheduledPrint();
        }
    }

    void Run() override {
        GetJobsThread.Start();
        SubmitJobsThread.Start();
        SubmitJobsThread.Join();
        AtomicSet(IsRunning, 0);
        GetJobsThread.Join();
        Y_ASSERT(CurrentInFlight == 0);
        if (ReadProportion != 0.) {
            PrintReadReport();
        }
        if (ReadProportion != 1.) {
            PrintWriteReport();
        }
    }

    void Finish() override {
        for (size_t i = 0; i < ReqPool.size(); ++i) {
            IoContext->DestroyAsyncIoOperation(ReqPool[i].Op);
        }
        NPDisk::EIoResult res = IoContext->Destroy();
        Y_VERIFY_S(res == NPDisk::EIoResult::Ok, "Error in IoContext->Destroy(), error# " << res);
    }

    void PrintReadReport() {
        double speedMBps = double(ReadEventsDone * BuffSize) / 1e6 / DurationSec;
        double iops = double(ReadEventsDone) / DurationSec;
        Printer->SetTestType("AioRead");
        Printer->SetInFlight(QueueDepth);
        Printer->AddResult("Type", Sprintf("Read %.0f%%", ReadProportion * 100));
        Printer->AddResult("Size", ToString(HumanReadableSize(BuffSize, SF_BYTES)));
        Printer->AddResult("QueueDepth", QueueDepth);
        Printer->AddResult("Speed", Sprintf("%.1f MB/s", speedMBps));
        Printer->AddResult("IOPS", ui64(iops));
        Printer->AddSpeedAndIops(TSpeedAndIops(speedMBps, iops));
        for (float percentile : {1., 0.999, 0.99, 0.9, 0.5, 0.1}) {
            TString perc_name = Sprintf("%.1f perc", percentile * 100);
            Printer->AddResult(perc_name, Sprintf("%lu us", ReadLatencyUs.GetPercentile(percentile)));
        }
        Printer->PrintResults();
    }

    void PrintWriteReport() {
        double speedMBps = double(WriteEventsDone * BuffSize) / 1e6 / DurationSec;
        double iops = double(WriteEventsDone) / DurationSec;
        Printer->SetTestType("AioWrite");
        Printer->SetInFlight(QueueDepth);
        Printer->AddResult("Type", Sprintf("Write %.0f%%", (1 - ReadProportion) * 100));
        Printer->AddResult("Size", ToString(HumanReadableSize(BuffSize, SF_BYTES)));
        Printer->AddResult("QueueDepth", QueueDepth);
        Printer->AddResult("Speed", Sprintf("%.1f MB/s", speedMBps));
        Printer->AddResult("IOPS", ui64(iops));
        Printer->AddSpeedAndIops(TSpeedAndIops(speedMBps, iops));
        for (float percentile : {1., 0.999, 0.99, 0.9, 0.5, 0.1}) {
            TString perc_name = Sprintf("%.1f perc", percentile * 100);
            Printer->AddResult(perc_name, Sprintf("%lu us", WriteLatencyUs.GetPercentile(percentile)));
        }
        Printer->PrintResults();
    }

    ~TAioTest() override {
    }
};

} // namespace NKikimr
