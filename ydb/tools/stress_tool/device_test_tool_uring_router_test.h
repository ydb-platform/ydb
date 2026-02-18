#pragma once

#include "defs.h"

#include <ydb/library/pdisk_io/buffers.h>
#include <ydb/library/pdisk_io/file_params.h>
#include <ydb/library/pdisk_io/uring_router.h>
#include <library/cpp/monlib/dynamic_counters/percentile/percentile_lg.h>

#include <util/random/entropy.h>
#include <util/random/mersenne64.h>
#include <util/string/printf.h>
#include <util/system/condvar.h>
#include <util/system/file.h>
#include <util/system/hp_timer.h>
#include <util/system/mutex.h>

#include <atomic>
#include <cstring>
#include <sys/uio.h>
#include <unistd.h>

#include "device_test_tool.h"

namespace NKikimr {

class TUringRouterTest : public TPerfTest {
    static constexpr ui64 Alignment = 4096;

    struct TOp : NPDisk::TUringOperation {
        TUringRouterTest* Owner = nullptr;
        ui32 Slot = 0;
        NHPTimer::STime Start = 0;

        static void OnCompleteThunk(NPDisk::TUringOperation* op, NActors::TActorSystem*) noexcept {
            auto* self = static_cast<TOp*>(op);
            self->Owner->OnIoComplete(*self);
        }
    };

    const ui32 QueueDepth;
    const ui32 DurationSec;
    const ui32 BuffSize;
    const bool UseAlignedData;
    const ui32 NumberOfRandomRefills;
    const bool UseWriteFixed;

    ui64 DeviceSizeBytes = 0;
    THolder<TFileHandle> File;
    THolder<NPDisk::TUringRouter> Router;

    TVector<TOp> Ops;
    TVector<ui8*> Buffers;
    TVector<NPDisk::TAlignedData> AlignedBuffers;
    TVector<TVector<ui8>> UnalignedBuffers;
    TVector<ui32> FreeSlots;
    TMutex SlotsMutex;
    TMutex CompletionMutex;
    TCondVar CompletionCondVar;
    std::atomic<ui32> InFlight = 0;

    NPrivate::TMersenne64 OffsetRandGen;
    NPrivate::TMersenne64 FillRandGen;
    NHPTimer::STime StartTime = 0;
    ui64 WriteEventsDone = 0;
    NMonitoring::TPercentileTrackerLg<10, 5, 1> WriteLatencyUs;

public:
    TUringRouterTest(const TPerfTestConfig& cfg, const NDevicePerfTest::TUringRouterTest& testProto)
        : TPerfTest(cfg)
        , QueueDepth(testProto.GetQueueDepth() != 0 ? FastClp2(testProto.GetQueueDepth()) : 128)
        , DurationSec(testProto.GetDurationSeconds() != 0 ? testProto.GetDurationSeconds() : 180)
        , BuffSize(testProto.GetRequestSize() != 0 ? testProto.GetRequestSize() : 4096)
        , UseAlignedData(testProto.GetUseAlignedData())
        , NumberOfRandomRefills(testProto.GetNumberOfRandomRefills())
        , UseWriteFixed(testProto.GetUseWriteFixed())
        , OffsetRandGen(Seed())
        , FillRandGen(Seed())
    {
    }

    void Init() override {
        bool isBlockDevice = false;
        DetectFileParameters(Cfg.Path, DeviceSizeBytes, isBlockDevice);
        Y_VERIFY_S(DeviceSizeBytes > BuffSize, "Device size is too small for request size");
        Y_VERIFY_S(BuffSize > 0, "Request size must be positive");

        if (UseAlignedData) {
            Y_VERIFY_S(BuffSize % Alignment == 0, "Aligned mode requires request size to be a multiple of 4096");
        }
        if (UseWriteFixed) {
            Y_VERIFY_S(UseAlignedData, "UseWriteFixed requires UseAlignedData=true");
            Y_VERIFY_S(QueueDepth <= Max<ui16>(), "QueueDepth must fit into ui16 for WriteFixed");
        }

        EOpenMode openFlags = static_cast<EOpenMode>(OpenExisting | RdWr | Sync);
        if (UseAlignedData || UseWriteFixed) {
            openFlags = static_cast<EOpenMode>(openFlags | DirectAligned);
        }
        File = THolder<TFileHandle>(new TFileHandle(Cfg.Path.c_str(), openFlags));

        NPDisk::TUringRouterConfig cfg;
        cfg.QueueDepth = QueueDepth;
        cfg.UseSQPoll = true;
        cfg.UseIOPoll = true;
        Router = MakeHolder<NPDisk::TUringRouter>(static_cast<FHANDLE>(*File), nullptr, cfg);
        Y_VERIFY_S(Router->RegisterFile(), "TUringRouter::RegisterFile failed");

        Ops.resize(QueueDepth);
        Buffers.resize(QueueDepth);
        FreeSlots.reserve(QueueDepth);

        if (UseAlignedData) {
            AlignedBuffers.reserve(QueueDepth);
            for (ui32 i = 0; i < QueueDepth; ++i) {
                AlignedBuffers.emplace_back(BuffSize, false);
                Buffers[i] = AlignedBuffers.back().Get();
            }
        } else {
            UnalignedBuffers.resize(QueueDepth);
            for (ui32 i = 0; i < QueueDepth; ++i) {
                // Intentionally misalign by 1 byte to exercise non-zero-copy mode.
                UnalignedBuffers[i].resize(BuffSize + 1);
                Buffers[i] = UnalignedBuffers[i].data() + 1;
            }
        }

        for (ui32 i = 0; i < QueueDepth; ++i) {
            Ops[i].Owner = this;
            Ops[i].Slot = i;
            Ops[i].OnComplete = TOp::OnCompleteThunk;
            FreeSlots.push_back(i);
            TryFillRandom(Buffers[i], BuffSize);
        }

        if (UseWriteFixed) {
            TVector<iovec> iovs(QueueDepth);
            for (ui32 i = 0; i < QueueDepth; ++i) {
                iovs[i].iov_base = Buffers[i];
                iovs[i].iov_len = BuffSize;
            }
            Y_VERIFY_S(Router->RegisterBuffers(iovs.data(), iovs.size()), "TUringRouter::RegisterBuffers failed");
        }
    }

    void Run() override {
        auto cyclesPerSecond = NHPTimer::GetCyclesPerSecond();
        StartTime = Now();
        const NHPTimer::STime endTime = StartTime + DurationSec * cyclesPerSecond;

        while (Now() < endTime) {
            bool submitted = false;
            while (true) {
                ui32 slot = 0;
                if (!TryTakeFreeSlot(slot)) {
                    break;
                }

                TryFillRandom(Buffers[slot], BuffSize);
                TOp& op = Ops[slot];
                op.Start = Now();

                bool ok = false;
                if (UseWriteFixed) {
                    ok = Router->WriteFixed(Buffers[slot], BuffSize, NextOffset(), static_cast<ui16>(slot), &op);
                } else {
                    ok = Router->Write(Buffers[slot], BuffSize, NextOffset(), &op);
                }
                if (!ok) {
                    ReturnFreeSlot(slot);
                    break;
                }

                InFlight.fetch_add(1, std::memory_order_relaxed);
                submitted = true;
            }

            if (submitted) {
                Router->Flush();
            } else {
                // Avoid busy polling when queue is full and wait for completions.
                TGuard<TMutex> guard(CompletionMutex);
                CompletionCondVar.WaitT(CompletionMutex, TDuration::MilliSeconds(1));
            }
        }

        while (InFlight.load(std::memory_order_acquire) > 0) {
            TGuard<TMutex> guard(CompletionMutex);
            CompletionCondVar.WaitT(CompletionMutex, TDuration::Seconds(1));
        }

        PrintReport();
    }

    void Finish() override {
        if (Router) {
            Router->Stop();
            Router.Reset();
        }
        File.Reset();
    }

private:
    bool TryTakeFreeSlot(ui32& slot) {
        TGuard<TMutex> guard(SlotsMutex);
        if (FreeSlots.empty()) {
            return false;
        }
        slot = FreeSlots.back();
        FreeSlots.pop_back();
        return true;
    }

    void ReturnFreeSlot(ui32 slot) {
        TGuard<TMutex> guard(SlotsMutex);
        FreeSlots.push_back(slot);
    }

    ui64 NextOffset() {
        ui64 offset = OffsetRandGen.GenRand() % (DeviceSizeBytes - BuffSize);
        if (UseAlignedData) {
            offset = (offset / Alignment) * Alignment;
        }
        return offset;
    }

    void TryFillRandom(void* buff, ui64 size) {
        if (NumberOfRandomRefills == 0) {
            return;
        }
        ui8* ptr = static_cast<ui8*>(buff);
        for (ui32 refill = 0; refill < NumberOfRandomRefills; ++refill) {
            ui64 pos = 0;
            while (pos + sizeof(ui64) <= size) {
                const ui64 value = FillRandGen.GenRand();
                memcpy(ptr + pos, &value, sizeof(value));
                pos += sizeof(value);
            }
            while (pos < size) {
                ptr[pos++] = static_cast<ui8>(FillRandGen.GenRand());
            }
        }
    }

    void OnIoComplete(TOp& op) {
        const NHPTimer::STime now = Now();
        const ui64 durationUs = static_cast<ui64>(NHPTimer::GetSeconds(now - op.Start) * 1e6);
        const NHPTimer::STime oneSecondCycles = static_cast<NHPTimer::STime>(NHPTimer::GetCyclesPerSecond());
        if (now > StartTime + oneSecondCycles) {
            WriteLatencyUs.Increment(durationUs);
            ++WriteEventsDone;
        }

        ReturnFreeSlot(op.Slot);
        InFlight.fetch_sub(1, std::memory_order_release);
        {
            TGuard<TMutex> guard(CompletionMutex);
            CompletionCondVar.Signal();
        }
        Y_VERIFY_S(op.Result == (i32)BuffSize, "TUringRouter write failed, res# " << op.Result);
    }

    void PrintReport() {
        const double duration = DurationSec;
        const double speedMBps = duration > 0.0 ? double(WriteEventsDone * BuffSize) / 1e6 / duration : 0.0;
        const double iops = duration > 0.0 ? double(WriteEventsDone) / duration : 0.0;

        Printer->SetTestType("UringRouterWrite");
        Printer->SetInFlight(QueueDepth);
        Printer->AddResult("Name", Cfg.Name);
        Printer->AddResult("Type", "Write 100%");
        Printer->AddResult("Size", ToString(HumanReadableSize(BuffSize, SF_BYTES)));
        Printer->AddResult("QueueDepth", QueueDepth);
        Printer->AddResult("AlignedData", UseAlignedData ? "true" : "false");
        Printer->AddResult("WriteFixed", UseWriteFixed ? "true" : "false");
        Printer->AddResult("Speed", Sprintf("%.1f MB/s", speedMBps));
        Printer->AddResult("IOPS", ui64(iops));
        Printer->AddSpeedAndIops(TSpeedAndIops(speedMBps, iops));
        for (float percentile : {1.f, 0.999f, 0.99f, 0.9f, 0.5f, 0.1f}) {
            const TString name = Sprintf("%.1f perc", percentile * 100);
            Printer->AddResult(name, Sprintf("%lu us", WriteLatencyUs.GetPercentile(percentile)));
        }
        Printer->PrintResults();
    }

    static NHPTimer::STime Now() {
        NHPTimer::STime now = 0;
        NHPTimer::GetTime(&now);
        return now;
    }

    ~TUringRouterTest() override = default;
};

} // namespace NKikimr
