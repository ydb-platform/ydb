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
#include <thread>
#include <sys/uio.h>
#include <unistd.h>

#include "device_test_tool.h"

namespace NKikimr {

class TUringRouterTest : public TPerfTest {
    static constexpr ui64 Alignment = 4096;

    struct TDeviceState;

    struct TOp : NPDisk::TUringOperation {
        TDeviceState* DevState = nullptr;
        ui32 Slot = 0;
        NHPTimer::STime Start = 0;

        static void OnCompleteThunk(NPDisk::TUringOperation* op, NActors::TActorSystem*) noexcept {
            auto* self = static_cast<TOp*>(op);
            self->DevState->OnIoComplete(*self);
        }
    };

    struct TDeviceState {
        ui32 BuffSize = 0;
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

        TDeviceState()
            : OffsetRandGen(Seed())
            , FillRandGen(Seed())
        {}

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

        void OnIoComplete(TOp& op) {
            const NHPTimer::STime now = GetNow();
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

        static NHPTimer::STime GetNow() {
            NHPTimer::STime now = 0;
            NHPTimer::GetTime(&now);
            return now;
        }
    };

    const ui32 QueueDepth;
    const ui32 DurationSec;
    const ui32 BuffSize;
    const bool UseAlignedData;
    const ui32 NumberOfRandomRefills;
    const bool UseWriteFixed;

    TVector<THolder<TDeviceState>> DeviceStates;

    // Sync barrier for multi-device
    std::atomic<ui32> ReadyCount{0};
    std::atomic<bool> GoSignal{false};

public:
    TUringRouterTest(const TPerfTestConfig& cfg, const NDevicePerfTest::TUringRouterTest& testProto)
        : TPerfTest(cfg)
        , QueueDepth(testProto.GetQueueDepth() != 0 ? FastClp2(testProto.GetQueueDepth()) : 128)
        , DurationSec(testProto.GetDurationSeconds() != 0 ? testProto.GetDurationSeconds() : 180)
        , BuffSize(testProto.GetRequestSize() != 0 ? testProto.GetRequestSize() : 4096)
        , UseAlignedData(testProto.GetUseAlignedData())
        , NumberOfRandomRefills(testProto.GetNumberOfRandomRefills())
        , UseWriteFixed(testProto.GetUseWriteFixed())
    {
    }

    void Init() override {
        Y_VERIFY_S(BuffSize > 0, "Request size must be positive");
        if (UseAlignedData) {
            Y_VERIFY_S(BuffSize % Alignment == 0, "Aligned mode requires request size to be a multiple of 4096");
        }
        if (UseWriteFixed) {
            Y_VERIFY_S(UseAlignedData, "UseWriteFixed requires UseAlignedData=true");
            Y_VERIFY_S(QueueDepth <= Max<ui16>(), "QueueDepth must fit into ui16 for WriteFixed");
        }

        DeviceStates.resize(Cfg.NumDevices());
        for (ui32 d = 0; d < Cfg.NumDevices(); ++d) {
            DeviceStates[d] = MakeHolder<TDeviceState>();
            InitDevice(d);
        }
    }

    void Run() override {
        if (Cfg.NumDevices() == 1) {
            RunDevice(*DeviceStates[0]);
            PrintDeviceReport(*DeviceStates[0], 0, false);
        } else {
            ReadyCount.store(0, std::memory_order_relaxed);
            GoSignal.store(false, std::memory_order_relaxed);

            TVector<std::thread> threads;
            threads.reserve(Cfg.NumDevices());
            for (ui32 d = 0; d < Cfg.NumDevices(); ++d) {
                threads.emplace_back([this, d]() {
                    ReadyCount.fetch_add(1, std::memory_order_release);
                    while (!GoSignal.load(std::memory_order_acquire)) {
                        std::this_thread::yield();
                    }
                    RunDevice(*DeviceStates[d]);
                });
            }

            while (ReadyCount.load(std::memory_order_acquire) < Cfg.NumDevices()) {
                std::this_thread::yield();
            }
            GoSignal.store(true, std::memory_order_release);

            for (auto& t : threads) {
                t.join();
            }

            Printer->SetSkipStatistics(true);
            for (ui32 d = 0; d < Cfg.NumDevices(); ++d) {
                PrintDeviceReport(*DeviceStates[d], d, true);
            }
            Printer->SetSkipStatistics(false);
            PrintAggregateReport();
        }
    }

    void Finish() override {
        for (auto& dev : DeviceStates) {
            if (dev && dev->Router) {
                dev->Router->Stop();
                dev->Router.Reset();
            }
            if (dev) {
                dev->File.Reset();
            }
        }
    }

private:
    void InitDevice(ui32 deviceIdx) {
        auto& dev = *DeviceStates[deviceIdx];
        dev.BuffSize = BuffSize;
        const TString& path = Cfg.Paths[deviceIdx];

        bool isBlockDevice = false;
        DetectFileParameters(path, dev.DeviceSizeBytes, isBlockDevice);
        Y_VERIFY_S(dev.DeviceSizeBytes > BuffSize, "Device " << deviceIdx << " size is too small for request size");

        EOpenMode openFlags = static_cast<EOpenMode>(OpenExisting | RdWr | Sync);
        if (UseAlignedData || UseWriteFixed) {
            openFlags = static_cast<EOpenMode>(openFlags | DirectAligned);
        }
        dev.File = MakeHolder<TFileHandle>(path.c_str(), openFlags);

        NPDisk::TUringRouterConfig cfg;
        cfg.QueueDepth = QueueDepth;
        cfg.UseSQPoll = true;
        cfg.UseIOPoll = true;
        dev.Router = MakeHolder<NPDisk::TUringRouter>(static_cast<FHANDLE>(*dev.File), nullptr, cfg);
        Y_VERIFY_S(dev.Router->RegisterFile(), "TUringRouter::RegisterFile failed for device " << deviceIdx);

        dev.Ops.resize(QueueDepth);
        dev.Buffers.resize(QueueDepth);
        dev.FreeSlots.reserve(QueueDepth);

        if (UseAlignedData) {
            dev.AlignedBuffers.reserve(QueueDepth);
            for (ui32 i = 0; i < QueueDepth; ++i) {
                dev.AlignedBuffers.emplace_back(BuffSize, false);
                dev.Buffers[i] = dev.AlignedBuffers.back().Get();
            }
        } else {
            dev.UnalignedBuffers.resize(QueueDepth);
            for (ui32 i = 0; i < QueueDepth; ++i) {
                dev.UnalignedBuffers[i].resize(BuffSize + 1);
                dev.Buffers[i] = dev.UnalignedBuffers[i].data() + 1;
            }
        }

        for (ui32 i = 0; i < QueueDepth; ++i) {
            dev.Ops[i].DevState = &dev;
            dev.Ops[i].Slot = i;
            dev.Ops[i].OnComplete = TOp::OnCompleteThunk;
            dev.FreeSlots.push_back(i);
            TryFillRandom(dev, dev.Buffers[i], BuffSize);
        }

        if (UseWriteFixed) {
            TVector<iovec> iovs(QueueDepth);
            for (ui32 i = 0; i < QueueDepth; ++i) {
                iovs[i].iov_base = dev.Buffers[i];
                iovs[i].iov_len = BuffSize;
            }
            Y_VERIFY_S(dev.Router->RegisterBuffers(iovs.data(), iovs.size()), "TUringRouter::RegisterBuffers failed");
        }

        dev.Router->Start();
    }

    void RunDevice(TDeviceState& dev) {
        auto cyclesPerSecond = NHPTimer::GetCyclesPerSecond();
        dev.StartTime = TDeviceState::GetNow();
        const NHPTimer::STime endTime = dev.StartTime + DurationSec * cyclesPerSecond;

        while (TDeviceState::GetNow() < endTime) {
            bool submitted = false;
            while (true) {
                ui32 slot = 0;
                if (!dev.TryTakeFreeSlot(slot)) {
                    break;
                }

                TryFillRandom(dev, dev.Buffers[slot], BuffSize);
                TOp& op = dev.Ops[slot];
                op.Start = TDeviceState::GetNow();

                ui64 offset = NextOffset(dev);
                bool ok = false;
                if (UseWriteFixed) {
                    ok = dev.Router->WriteFixed(dev.Buffers[slot], BuffSize, offset, static_cast<ui16>(slot), &op);
                } else {
                    ok = dev.Router->Write(dev.Buffers[slot], BuffSize, offset, &op);
                }
                if (!ok) {
                    dev.ReturnFreeSlot(slot);
                    break;
                }

                dev.InFlight.fetch_add(1, std::memory_order_relaxed);
                submitted = true;
            }

            if (submitted) {
                dev.Router->Flush();
            } else {
                TGuard<TMutex> guard(dev.CompletionMutex);
                dev.CompletionCondVar.WaitT(dev.CompletionMutex, TDuration::MilliSeconds(1));
            }
        }

        while (dev.InFlight.load(std::memory_order_acquire) > 0) {
            TGuard<TMutex> guard(dev.CompletionMutex);
            dev.CompletionCondVar.WaitT(dev.CompletionMutex, TDuration::Seconds(1));
        }
    }

    ui64 NextOffset(TDeviceState& dev) const {
        ui64 offset = dev.OffsetRandGen.GenRand() % (dev.DeviceSizeBytes - BuffSize);
        if (UseAlignedData) {
            offset = (offset / Alignment) * Alignment;
        }
        return offset;
    }

    void TryFillRandom(TDeviceState& dev, void* buff, ui64 size) const {
        if (NumberOfRandomRefills == 0) {
            return;
        }
        ui8* ptr = static_cast<ui8*>(buff);
        for (ui32 refill = 0; refill < NumberOfRandomRefills; ++refill) {
            ui64 pos = 0;
            while (pos + sizeof(ui64) <= size) {
                const ui64 value = dev.FillRandGen.GenRand();
                memcpy(ptr + pos, &value, sizeof(value));
                pos += sizeof(value);
            }
            while (pos < size) {
                ptr[pos++] = static_cast<ui8>(dev.FillRandGen.GenRand());
            }
        }
    }

    void PrintDeviceReport(TDeviceState& dev, ui32 deviceIdx, bool multiDevice) {
        const double duration = DurationSec;
        const double speedMBps = duration > 0.0 ? double(dev.WriteEventsDone * BuffSize) / 1e6 / duration : 0.0;
        const double iops = duration > 0.0 ? double(dev.WriteEventsDone) / duration : 0.0;

        Printer->SetTestType("UringRouterWrite");
        Printer->SetInFlight(QueueDepth);
        if (multiDevice) {
            Printer->AddResult("Device", deviceIdx);
        }
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
            Printer->AddResult(name, Sprintf("%lu us", dev.WriteLatencyUs.GetPercentile(percentile)));
        }
        Printer->PrintResults();
    }

    void PrintAggregateReport() {
        double totalSpeed = 0.0;
        double totalIops = 0.0;
        NMonitoring::TPercentileTrackerLg<10, 5, 1> mergedLatency;
        for (const auto& dev : DeviceStates) {
            const double duration = DurationSec;
            totalSpeed += duration > 0.0 ? double(dev->WriteEventsDone * BuffSize) / 1e6 / duration : 0.0;
            totalIops += duration > 0.0 ? double(dev->WriteEventsDone) / duration : 0.0;
            for (size_t i = 0; i < mergedLatency.ITEMS_COUNT; ++i) {
                mergedLatency.Items[i].fetch_add(
                    dev->WriteLatencyUs.Items[i].load(std::memory_order_relaxed),
                    std::memory_order_relaxed);
            }
        }

        Printer->SetTestType("UringRouterWrite");
        Printer->SetInFlight(QueueDepth);
        Printer->AddResult("Device", TString("SUM"));
        Printer->AddResult("Name", Cfg.Name);
        Printer->AddResult("Type", "Write 100%");
        Printer->AddResult("Size", ToString(HumanReadableSize(BuffSize, SF_BYTES)));
        Printer->AddResult("QueueDepth", QueueDepth);
        Printer->AddResult("AlignedData", UseAlignedData ? "true" : "false");
        Printer->AddResult("WriteFixed", UseWriteFixed ? "true" : "false");
        Printer->AddResult("Speed", Sprintf("%.1f MB/s", totalSpeed));
        Printer->AddResult("IOPS", ui64(totalIops));
        Printer->AddSpeedAndIops(TSpeedAndIops(totalSpeed, totalIops));
        for (float percentile : {1.f, 0.999f, 0.99f, 0.9f, 0.5f, 0.1f}) {
            const TString name = Sprintf("%.1f perc", percentile * 100);
            Printer->AddResult(name, Sprintf("%lu us", mergedLatency.GetPercentile(percentile)));
        }
        Printer->PrintResults();
    }

    ~TUringRouterTest() override = default;
};

} // namespace NKikimr
