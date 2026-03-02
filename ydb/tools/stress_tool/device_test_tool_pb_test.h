#pragma once

#include "defs.h"
#include "device_test_tool_pdisk_test.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/base/tabletid.h>

#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_crypto.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>

#include <ydb/core/load_test/service_actor.h>

#include <ydb/core/tablet/bootstrapper.h>

#include <ydb/core/protos/blobstorage.pb.h>
#if __has_include("ydb/core/protos/blobstorage.deps.pb.h")
    #include <ydb/core/protos/blobstorage.deps.pb.h> // Y_IGNORE
#endif
#include <ydb/core/protos/blobstorage_pdisk_config.pb.h>
#if __has_include("ydb/core/protos/blobstorage_pdisk_config.deps.pb.h")
    #include <ydb/core/protos/blobstorage_pdisk_config.deps.pb.h> // Y_IGNORE
#endif
#include <ydb/core/protos/blobstorage_vdisk_config.pb.h>
#if __has_include("ydb/core/protos/blobstorage_vdisk_config.deps.pb.h")
    #include <ydb/core/protos/blobstorage_vdisk_config.deps.pb.h> // Y_IGNORE
#endif
#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/protos/services_common.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/executor_pool_io.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/getopt/last_getopt.h>

#include <util/generic/string.h>
#include <util/random/entropy.h>
#include <util/random/mersenne64.h>
#include <util/string/printf.h>
#include <util/system/event.h>

#include "device_test_tool.h"

namespace NKikimr {

static TSystemEvent PersistentBufferDoneEvent(TSystemEvent::rAuto);
static TSystemEvent PersistentBufferResultsPrintedEvent(TSystemEvent::rAuto);

#ifndef ASSERT_YTHROW
#define ASSERT_YTHROW(expr, str) \
do { \
    if (!(expr)) { \
        ythrow TWithBackTrace<yexception>() << str; \
    } \
} while(false)
#endif

struct TPBDeviceInfo {
    ui32 PDiskIdNum;
    ui32 PBSlotId;
};

class TPersistentBufferPerfTestActor : public TActor<TPersistentBufferPerfTestActor> {
    TVector<TPBDeviceInfo> Devices;
    ui64 CurrentTest = 0;
    const TPerfTestConfig& Cfg;
    const NDevicePerfTest::TPersistentBufferTest& TestProto;
    TIntrusivePtr<IResultPrinter> Printer;
    const TIntrusivePtr<NMonitoring::TDynamicCounters>& Counters;
    TString CurrentTestType;

    struct TDeviceResult {
        TIntrusivePtr<TEvLoad::TLoadReport> Report;
        TString ErrorReason;
    };
    TVector<TDeviceResult> PendingResults;
    ui32 ReceivedResults = 0;

protected:
    void FillPrinterWithReport(const TIntrusivePtr<TEvLoad::TLoadReport>& report, i32 deviceIdx) {
        double speedMBps = report->GetAverageSpeed() / 1e6;
        double iops = report->Size ? (report->GetAverageSpeed() / report->Size) : 0.0;
        Printer->SetTestType(CurrentTestType);
        Printer->SetInFlight(report->InFlight);
        if (Devices.size() > 1) {
            Printer->AddResult("Device", deviceIdx);
        }
        Printer->AddResult("Name", Cfg.Name);
        Printer->AddResult("Duration, sec", report->Duration.Seconds());
        Printer->AddResult("Load", report->LoadTypeName());
        Printer->AddResult("Size", ToString(HumanReadableSize(report->Size, SF_BYTES)));
        Printer->AddResult("InFlight", report->InFlight);
        Printer->AddResult("Speed", Sprintf("%.1f MB/s", speedMBps));
        if (report->Size) {
            Printer->AddResult("IOPS", Sprintf("%.0f", iops));
        } else {
            Printer->AddResult("IOPS", TString("N/A"));
        }
        Printer->AddSpeedAndIops(TSpeedAndIops(speedMBps, iops));
        for (double perc : {1.0, 0.9999, 0.999, 0.99, 0.95, 0.9, 0.5, 0.1}) {
            TString perc_name = Sprintf("p%.2f", perc * 100);
            size_t val = report->LatencyUs.GetPercentile(perc);
            Printer->AddResult(perc_name, Sprintf("%zu us", val));
        }
    }

    void FillPrinterWithAggregate() {
        double totalSpeed = 0.0;
        double totalIops = 0.0;
        NMonitoring::TPercentileTrackerLg<10, 4, 1> mergedLatency;
        for (const auto& r : PendingResults) {
            if (r.Report) {
                totalSpeed += r.Report->GetAverageSpeed() / 1e6;
                totalIops += r.Report->Size ? (r.Report->GetAverageSpeed() / r.Report->Size) : 0.0;
                for (size_t i = 0; i < mergedLatency.ITEMS_COUNT; ++i) {
                    mergedLatency.Items[i].fetch_add(
                        r.Report->LatencyUs.Items[i].load(std::memory_order_relaxed),
                        std::memory_order_relaxed);
                }
            }
        }
        const auto& firstReport = !PendingResults.empty() ? PendingResults[0].Report : nullptr;
        Printer->SetTestType(CurrentTestType);
        if (firstReport) {
            Printer->SetInFlight(firstReport->InFlight);
        }
        Printer->AddResult("Device", TString("SUM"));
        Printer->AddResult("Name", Cfg.Name);
        Printer->AddResult("Duration, sec", firstReport ? firstReport->Duration.Seconds() : 0);
        Printer->AddResult("Load", firstReport ? firstReport->LoadTypeName() : TString("-"));
        Printer->AddResult("Size", firstReport ? ToString(HumanReadableSize(firstReport->Size, SF_BYTES)) : TString("-"));
        Printer->AddResult("InFlight", firstReport ? firstReport->InFlight : 0u);
        Printer->AddResult("Speed", Sprintf("%.1f MB/s", totalSpeed));
        Printer->AddResult("IOPS", Sprintf("%.0f", totalIops));
        Printer->AddSpeedAndIops(TSpeedAndIops(totalSpeed, totalIops));
        for (double perc : {1.0, 0.9999, 0.999, 0.99, 0.95, 0.9, 0.5, 0.1}) {
            TString perc_name = Sprintf("p%.2f", perc * 100);
            size_t val = mergedLatency.GetPercentile(perc);
            Printer->AddResult(perc_name, Sprintf("%zu us", val));
        }
    }

    void LaunchTestOnAllDevices(const TActorContext& ctx) {
        auto baseRecord = TestProto.GetPersistentBufferTestList(CurrentTest);

        switch (baseRecord.Command_case()) {
        case NKikimr::TEvLoadTestRequest::CommandCase::kPersistentBufferWriteLoad:
            CurrentTestType = "PersistentBufferWriteLoad";
            break;
        default:
            CurrentTestType = "Unknown";
            Cerr << "Unknown load type" << Endl;
            return;
        }

        PendingResults.clear();
        PendingResults.resize(Devices.size());
        ReceivedResults = 0;

        for (ui32 d = 0; d < Devices.size(); ++d) {
            auto record = baseRecord;
            switch (record.Command_case()) {
            case NKikimr::TEvLoadTestRequest::CommandCase::kPersistentBufferWriteLoad: {
                auto* cfg = record.MutablePersistentBufferWriteLoad();
                auto* ddiskId = cfg->MutableDDiskId();
                ddiskId->SetNodeId(1);
                ddiskId->SetPDiskId(Devices[d].PDiskIdNum);
                ddiskId->SetDDiskSlotId(Devices[d].PBSlotId);
                ctx.Register(CreatePersistentBufferWriterLoadTest(record.GetPersistentBufferWriteLoad(), ctx.SelfID, Counters, d, d));
                break;
            }
            default:
                break;
            }
        }
    }

    void ProcessAllResults() {
        if (Devices.size() == 1) {
            if (PendingResults[0].Report) {
                FillPrinterWithReport(PendingResults[0].Report, 0);
            } else {
                Cerr << "Test error - no report on test finish, reason# " << PendingResults[0].ErrorReason << Endl;
            }
            PersistentBufferDoneEvent.Signal();
            PersistentBufferResultsPrintedEvent.Wait();
        } else {
            Printer->SetSkipStatistics(true);
            for (ui32 d = 0; d < Devices.size(); ++d) {
                if (PendingResults[d].Report) {
                    FillPrinterWithReport(PendingResults[d].Report, d);
                } else {
                    Cerr << "Test error on device " << d << " - no report, reason# " << PendingResults[d].ErrorReason << Endl;
                }
                PersistentBufferDoneEvent.Signal();
                PersistentBufferResultsPrintedEvent.Wait();
            }
            Printer->SetSkipStatistics(false);
            FillPrinterWithAggregate();
            PersistentBufferDoneEvent.Signal();
            PersistentBufferResultsPrintedEvent.Wait();
        }
    }

    void HandleBoot(TEvTablet::TEvBoot::TPtr& ev, const TActorContext& ctx) {
        if (CurrentTest < TestProto.PersistentBufferTestListSize()) {
            LaunchTestOnAllDevices(ctx);
        }
        Y_UNUSED(ev);
    }

    void Handle(TEvLoad::TEvLoadTestFinished::TPtr& ev, const TActorContext& ctx) {
        Y_ABORT_UNLESS(ev);
        Y_ABORT_UNLESS(ev->Get());

        ui32 deviceIdx = static_cast<ui32>(ev->Get()->Tag);
        Y_ABORT_UNLESS(deviceIdx < Devices.size());
        PendingResults[deviceIdx] = {ev->Get()->Report, ev->Get()->ErrorReason};
        ++ReceivedResults;

        if (ReceivedResults == Devices.size()) {
            ProcessAllResults();
            ReceivedResults = 0;
            ++CurrentTest;
            if (CurrentTest < TestProto.PersistentBufferTestListSize()) {
                LaunchTestOnAllDevices(ctx);
            }
        }
    }

public:
    TPersistentBufferPerfTestActor(const TVector<TPBDeviceInfo>& devices, const TPerfTestConfig& perfCfg,
            const NDevicePerfTest::TPersistentBufferTest& testProto,
            const TIntrusivePtr<IResultPrinter>& printer, const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters)
        : TActor(&TThis::StateRegister)
        , Devices(devices)
        , Cfg(perfCfg)
        , TestProto(testProto)
        , Printer(printer)
        , Counters(counters)
    {}

    STFUNC(StateRegister) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvLoad::TEvLoadTestFinished, Handle);
            HFunc(TEvTablet::TEvBoot, HandleBoot);
        }
    }
};

template<ui32 ChunkSize = 128 << 20>
struct TPersistentBufferTest : public TPDiskTest<ChunkSize> {
    using TBase = TPDiskTest<ChunkSize>;
    const NDevicePerfTest::TPersistentBufferTest& TestProto;
    static constexpr ui32 PersistentBufferSlotId = 1;

    TPersistentBufferTest(const TPerfTestConfig& cfg, const NDevicePerfTest::TPersistentBufferTest& testProto)
        : TBase(cfg, DefaultPDiskTestProto())
        , TestProto(testProto)
    {
    }

    static const NDevicePerfTest::TPDiskTest& DefaultPDiskTestProto() {
        static const NDevicePerfTest::TPDiskTest proto;
        return proto;
    }

    void Init() override {
        try {
            TBase::DoBasicSetup();

            auto groupInfo = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::ErasureNone);

            for (ui32 i = 0; i < TBase::Cfg.NumDevices(); ++i) {
                const TActorId ddiskId = MakeBlobStorageDDiskId(1, i + 1, PersistentBufferSlotId);
                const TVDiskID vdiskId(0, 1, 0, 0, i);
                TVDiskConfig::TBaseInfo baseInfo(
                    TVDiskIdShort(vdiskId),
                    TBase::PDiskActorIds[i],
                    TBase::PDiskGuids[i],
                    1,
                    TBase::Cfg.DeviceType,
                    PersistentBufferSlotId,
                    NKikimrBlobStorage::TVDiskKind::Default,
                    1000,
                    "ddisk_pool");
                TActorSetupCmd ddiskSetup(NDDisk::CreateDDiskActor(std::move(baseInfo), groupInfo, TBase::Counters),
                    TMailboxType::Revolving, 1);
                TBase::Setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(ddiskId, std::move(ddiskSetup)));
            }

            TVector<TPBDeviceInfo> pbDevices;
            for (ui32 i = 0; i < TBase::Cfg.NumDevices(); ++i) {
                pbDevices.push_back({i + 1, PersistentBufferSlotId});
            }

            TBase::TestId = MakeBlobStorageProxyID(1);
            TActorSetupCmd testSetup(new TPersistentBufferPerfTestActor(pbDevices, TBase::Cfg, TestProto, TBase::Printer, TBase::Counters),
                    TMailboxType::Revolving, 2);
            TBase::Setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(TBase::TestId, std::move(testSetup)));

            TBase::StartActorSystem();
        } catch (yexception& ex) {
            TBase::IsLastExceptionSet = true;
            VERBOSE_COUT("Error on init state, what# " << ex.what());
        }
    }

    void Run() override {
        if (TBase::IsLastExceptionSet) {
            return;
        }

        try {
            TBase::ActorSystem->Send(TBase::TestId, new TEvTablet::TEvBoot(MakeTabletID(0, 0, 1), 0, nullptr, TActorId(), nullptr));

            const ui32 eventsPerTest = TBase::Cfg.NumDevices() == 1 ? 1 : TBase::Cfg.NumDevices() + 1;
            for (ui32 i = 0; i < TestProto.PersistentBufferTestListSize(); ++i) {
                for (ui32 j = 0; j < eventsPerTest; ++j) {
                    PersistentBufferDoneEvent.Wait();
                    TBase::Printer->PrintResults();
                    PersistentBufferResultsPrintedEvent.Signal();
                }
            }
        } catch (yexception ex) {
            TBase::LastException = ex;
            TBase::IsLastExceptionSet = true;
            VERBOSE_COUT(ex.what());
        }

        if (TBase::ActorSystem.Get()) {
            TBase::ActorSystem->Stop();
            TBase::ActorSystem.Destroy();
        }
        PersistentBufferDoneEvent.Reset();
        if (TBase::IsLastExceptionSet) {
            TBase::IsLastExceptionSet = false;
            ythrow TBase::LastException;
        }
    }

    void Finish() override {
    }

    ~TPersistentBufferTest() override {
    }
};

} // namespace NKikimr
