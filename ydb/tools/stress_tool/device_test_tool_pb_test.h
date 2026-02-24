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

class TPersistentBufferPerfTestActor : public TActor<TPersistentBufferPerfTestActor> {

    ui32 TestStep = 0;
    ui64 CurrentTest = 0;
    const TPerfTestConfig& Cfg;
    const NDevicePerfTest::TPersistentBufferTest& TestProto;
    TIntrusivePtr<IResultPrinter> Printer;
    const TIntrusivePtr<NMonitoring::TDynamicCounters>& Counters;
    TString CurrentTestType; // Current test type name (e.g., "PersistentBufferWriteLoad")

protected:
    void ActTestFSM(const TActorContext& ctx) {
        switch (TestStep) {
        case 0:
            if (CurrentTest < TestProto.PersistentBufferTestListSize()) {
                auto record = TestProto.GetPersistentBufferTestList(CurrentTest);
                switch (record.Command_case()) {
                case NKikimr::TEvLoadTestRequest::CommandCase::kPersistentBufferWriteLoad: {
                    CurrentTestType = "PersistentBufferWriteLoad";
                    const auto cfg = record.GetPersistentBufferWriteLoad();
                    ctx.Register(CreatePersistentBufferWriterLoadTest(cfg, ctx.SelfID, Counters,
                                CurrentTest, cfg.HasTag() ? cfg.GetTag() : 0));
                    break;
                }
                default:
                    CurrentTestType = "Unknown";
                    Cerr << "Unknown load type" << Endl;
                    break;
                }
                ++CurrentTest;
            }
            if (CurrentTest == TestProto.PersistentBufferTestListSize()) {
                TestStep += 10;
            }
            break;
        case 10:
            break;
        }
    }

    void HandleBoot(TEvTablet::TEvBoot::TPtr& ev, const TActorContext& ctx) {
        ASSERT_YTHROW(TestStep == 0, "Error in messages order");
        ActTestFSM(ctx);
        Y_UNUSED(ev);
    }

    void Handle(TEvLoad::TEvLoadTestFinished::TPtr& ev, const TActorContext& ctx) {
        Y_ABORT_UNLESS(ev);
        Y_ABORT_UNLESS(ev->Get());
        TIntrusivePtr<TEvLoad::TLoadReport> report = ev->Get()->Report;
        if (report) {
            double speedMBps = report->GetAverageSpeed() / 1e6;
            double iops = report->Size ? (report->GetAverageSpeed() / report->Size) : 0.0;
            Printer->SetTestType(CurrentTestType);
            Printer->SetInFlight(report->InFlight);
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
        } else {
            Cerr << "Test error - no report on test finish, reason# " << ev->Get()->ErrorReason;
        }

        ActTestFSM(ctx);
        PersistentBufferDoneEvent.Signal();
        PersistentBufferResultsPrintedEvent.Wait();
    }

public:
    TPersistentBufferPerfTestActor(const TPerfTestConfig& perfCfg, const NDevicePerfTest::TPersistentBufferTest& testProto,
            const TIntrusivePtr<IResultPrinter>& printer, const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters)
        : TActor(&TThis::StateRegister)
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

// we derive from TPDiskTest to get ready PDisk (and AS, etc)
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

            // PDisk is ready, setup DDisk
            const TActorId ddiskId = MakeBlobStorageDDiskId(1, 1, PersistentBufferSlotId);
            auto groupInfo = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::ErasureNone);
            const TVDiskID vdiskId(0, 1, 0, 0, 0);
            TVDiskConfig::TBaseInfo baseInfo(
                TVDiskIdShort(vdiskId),
                TBase::PDiskId,
                TBase::PDiskGuid,
                1,
                TBase::Cfg.DeviceType,
                PersistentBufferSlotId,
                NKikimrBlobStorage::TVDiskKind::Default,
                1000,
                "ddisk_pool");
            TActorSetupCmd ddiskSetup(NDDisk::CreateDDiskActor(std::move(baseInfo), groupInfo, TBase::Counters),
                TMailboxType::Revolving, 1);
            TBase::Setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(ddiskId, std::move(ddiskSetup)));

            // DDisk load actor
            TBase::TestId = MakeBlobStorageProxyID(1);
            TActorSetupCmd testSetup(new TPersistentBufferPerfTestActor(TBase::Cfg, TestProto, TBase::Printer, TBase::Counters),
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

            for (ui32 i = 0; i < TestProto.PersistentBufferTestListSize(); ++i) {
                PersistentBufferDoneEvent.Wait();
                TBase::Printer->PrintResults();
                PersistentBufferResultsPrintedEvent.Signal();
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
