#include "keyvalue.h"
#include "keyvalue_flat_impl.h"
#include <ydb/core/base/tablet.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/dsproxy/mock/dsproxy_mock.h>
#include <ydb/core/mind/bscontroller/bsc.h>
#include <ydb/core/mind/bscontroller/indir.h>
#include <ydb/core/mind/bscontroller/types.h>
#include <ydb/core/mind/bscontroller/ut_helpers.h>
#include <ydb/core/mind/bscontroller/vdisk_status_tracker.h>
#include <ydb/core/protos/blobstorage_config.pb.h>
#include <ydb/core/protos/blobstorage_distributed_config.pb.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/wilson/test_util/fake_wilson_uploader.h>

#include <util/datetime/cputimer.h>
#include <util/random/random.h>

#include <google/protobuf/text_format.h>

using namespace NActors;
using namespace NKikimr;

struct TInitialEventsFilter : private TNonCopyable {
    TTestActorRuntime::TEventFilter Prepare() {
        return [](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
            Y_UNUSED(runtime);
            Y_UNUSED(event);
            return false;
        };
    }
};

struct TTestEnvironment {
    THolder<TTestBasicRuntime> Runtime;
    const ui32 NodeCount;
    TActorId Edge;
    const ui64 TabletId = MakeTabletID(0, 0, 1);
    const TTabletTypes::EType TabletType = TTabletTypes::KeyValue;
    NWilson::TFakeWilsonUploader* WilsonUploader = nullptr;
    ui32 NextHostConfigId = 1;

    using TNodeRecord = std::tuple<TString, i32, ui32>;
    using TPDiskDefinition = std::tuple<TString, NKikimrBlobStorage::EPDiskType, bool, bool, ui64>;

    TTestEnvironment(ui32 nodeCount): NodeCount(nodeCount) {
    }

    void Prepare() {
        SetupRuntime();
        InitializeRuntime();

        Edge = Runtime->AllocateEdgeActor();
        CreateTestBootstrapper(*Runtime,
            CreateTestTabletInfo(TabletId, TabletType, TErasureType::Erasure4Plus2Block),
            &CreateKeyValueFlat);
        SetupFakeWilson();

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
        Runtime->DispatchEvents(options);
    }

    void InitializeRuntime() {
        TAppPrepare app;
        app.AddDomain(TDomainsInfo::TDomain::ConstructEmptyDomain("dc-1").Release());
        SetupTabletServices(*Runtime, &app, false, NFake::TStorage {
            .UseDisk = true,
            .SectorSize = 4096,
            .ChunkSize = (ui64)128 << 20, // 128 MiB
            .DiskSize = (ui64)32 << 30, // 1 TiB
        });
    }

    void SetupRuntime() {
        Runtime = MakeHolder<TTestBasicRuntime>(NodeCount, 1u);

        for (ui32 i = 0; i < NodeCount; ++i) {
            SetupStateStorage(*Runtime, i, 0, true);
            SetupTabletResolver(*Runtime, i);
        }
    }

    void SetupFakeWilson() {
        WilsonUploader = new NWilson::TFakeWilsonUploader;
        auto actorId = Runtime->Register(WilsonUploader);
        Runtime->RegisterService(NWilson::MakeWilsonUploaderId(), actorId);
    }

    template<class TRequest>
    auto MakeKVRequest(THolder<TRequest> request) {
        Runtime->SendToPipe(TabletId, Edge, request.Release(), 0, NTabletPipe::TClientConfig(), TActorId(),
                0, NWilson::TTraceId::NewTraceId(15, 4095));
        TAutoPtr<IEventHandle> handle;
        auto response = Runtime->GrabEdgeEventRethrow<typename TRequest::TResponse>(handle);
        UNIT_ASSERT(response);
        auto& record = response->Record;
        UNIT_ASSERT_EQUAL(record.status(), NKikimrKeyValue::Statuses::RSTATUS_OK);

        return std::move(record);
    }

    void Finalize() {
        Runtime.Reset();
    }
};

THolder<TEvKeyValue::TEvExecuteTransaction> CreateWrite(TString key, TString value) {
    auto request = MakeHolder<TEvKeyValue::TEvExecuteTransaction>();
    auto write = request->Record.add_commands()->mutable_write();
    write->set_key(std::move(key));
    write->set_value(std::move(value));
    write->set_priority(NKikimrKeyValue::Priorities_Priority_PRIORITY_UNSPECIFIED);
    write->set_tactic(NKikimrKeyValue::ExecuteTransactionRequest_Command_Write_Tactic_TACTIC_UNSPECIFIED);
    return request;
}

Y_UNIT_TEST_SUITE(TKeyValueTracingTest) {

Y_UNIT_TEST(WriteSmall) {
    TTestEnvironment env(8);
    env.Prepare();

    auto request = CreateWrite("key", "value");
    auto response = env.MakeKVRequest(std::move(request));

    UNIT_ASSERT(env.WilsonUploader->BuildTraceTrees());
    UNIT_ASSERT_EQUAL(env.WilsonUploader->Traces.size(), 1);
    auto& trace = env.WilsonUploader->Traces.begin()->second;

    TString canon = "(KeyValue.Intermediate -> [(KeyValue.StorageRequest -> [(DSProxy.Put -> [(Backpressure.InFlight "
        "-> [(VDisk.Log.Put)])])]) , (Tablet.Transaction -> [(Tablet.Transaction.Execute) , (Tablet.WriteLog -> "
        "[(Tablet.WriteLog.LogEntry -> [(DSProxy.Put -> [(Backpressure.InFlight -> [(VDisk.Log.Put)])])])])])])";
    UNIT_ASSERT_EQUAL(trace.ToString(), canon);
}

Y_UNIT_TEST(WriteHuge) {
    TTestEnvironment env(8);
    env.Prepare();

    auto request = CreateWrite("key", TString(1 << 20, 'v'));
    auto response = env.MakeKVRequest(std::move(request));

    UNIT_ASSERT(env.WilsonUploader->BuildTraceTrees());
    UNIT_ASSERT_EQUAL(env.WilsonUploader->Traces.size(), 1);
    auto& trace = env.WilsonUploader->Traces.begin()->second;

    TString canon = "(KeyValue.Intermediate -> [(KeyValue.StorageRequest -> [(DSProxy.Put -> [(Backpressure.InFlight "
        "-> [(VDisk.HugeBlobKeeper.Write -> [(VDisk.Log.PutHuge)])])])]) , (Tablet.Transaction -> "
        "[(Tablet.Transaction.Execute) , (Tablet.WriteLog -> [(Tablet.WriteLog.LogEntry -> [(DSProxy.Put -> "
        "[(Backpressure.InFlight -> [(VDisk.Log.Put)])])])])])])";
    UNIT_ASSERT_EQUAL(trace.ToString(), canon);

}

}
