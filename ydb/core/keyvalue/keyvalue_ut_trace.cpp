#include <ydb/core/keyvalue/keyvalue.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/keyvalue/protos/events.pb.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/library/actors/wilson/test_util/fake_wilson_uploader.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;
using namespace NKikimr;

struct TTestEnvironment {
    THolder<TTestBasicRuntime> Runtime;
    const ui32 NodeCount;
    TActorId Edge;
    const ui64 TabletId = MakeTabletID(false, 1);
    const TTabletTypes::EType TabletType = TTabletTypes::KeyValue;
    NWilson::TFakeWilsonUploader* WilsonUploader = nullptr;

    TTestEnvironment(ui32 nodeCount): NodeCount(nodeCount) {
    }

    void Prepare() {
        SetupRuntime();
        InitializeRuntime();

        Edge = Runtime->AllocateEdgeActor();
        CreateTestBootstrapper(*Runtime,
            CreateTestTabletInfo(TabletId, TabletType, TErasureType::ErasureNone),
            &CreateKeyValueFlat);
        SetupFakeWilson();

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
        Runtime->DispatchEvents(options);
    }

    void InitializeRuntime() {
        TAppPrepare app;
        app.AddDomain(TDomainsInfo::TDomain::ConstructEmptyDomain("dc-1").Release());
        SetupTabletServices(*Runtime, &app);
    }

    void SetupRuntime() {
        Runtime = MakeHolder<TTestBasicRuntime>(NodeCount, 1u);

        for (ui32 i = 0; i < NodeCount; ++i) {
            SetupStateStorage(*Runtime, i, true);
            SetupTabletResolver(*Runtime, i);
        }
    }

    void SetupFakeWilson() {
        WilsonUploader = new NWilson::TFakeWilsonUploader;
        auto actorId = Runtime->Register(WilsonUploader);
        Runtime->RegisterService(NWilson::MakeWilsonUploaderId(), actorId);
    }

    template<class TRequest>
    auto DoKVRequest(THolder<TRequest> request) {
        Runtime->SendToPipe(TabletId, Edge, request.Release(), 0, NTabletPipe::TClientConfig(), TActorId(),
                0, NWilson::TTraceId::NewTraceId(15, 4095));
        TAutoPtr<IEventHandle> handle;
        auto response = Runtime->GrabEdgeEventRethrow<typename TRequest::TResponse>(handle);
        UNIT_ASSERT(response);
        auto& record = response->Record;
        UNIT_ASSERT_EQUAL(record.status(), NKikimrKeyValue::Statuses::RSTATUS_OK);

        return std::move(record);
    }
};

THolder<TEvKeyValue::TEvExecuteTransaction> CreateWrite(TString key, TString value) {
    auto request = MakeHolder<TEvKeyValue::TEvExecuteTransaction>();
    auto write = request->Record.add_commands()->mutable_write();
    write->set_key(std::move(key));
    write->set_value(std::move(value));
    return request;
}

THolder<TEvKeyValue::TEvRead> CreateRead(TString key) {
    auto request = MakeHolder<TEvKeyValue::TEvRead>();
    auto& record = request->Record;
    record.set_key(std::move(key));
    record.set_offset(0);
    record.set_size(0);
    record.set_limit_bytes(0);
    return request;
}

void TestOneWrite(TString value, TString expectedTrace) {
    TTestEnvironment env(8);
    env.Prepare();

    env.DoKVRequest(CreateWrite("key", std::move(value)));

    UNIT_ASSERT(env.WilsonUploader->BuildTraceTrees());
    UNIT_ASSERT_EQUAL(env.WilsonUploader->Traces.size(), 1);
    auto& trace = env.WilsonUploader->Traces.begin()->second;

    UNIT_ASSERT_EQUAL(trace.ToString(), expectedTrace);
}

void TestOneRead(TString value, TString expectedTrace) {
    TTestEnvironment env(8);
    env.Prepare();

    env.DoKVRequest(CreateWrite("key", value));
    env.WilsonUploader->Clear();

    auto response = env.DoKVRequest(CreateRead("key"));
    UNIT_ASSERT_EQUAL(response.value(), value);

    UNIT_ASSERT(env.WilsonUploader->BuildTraceTrees());
    UNIT_ASSERT_EQUAL(env.WilsonUploader->Traces.size(), 1);
    auto& trace = env.WilsonUploader->Traces.begin()->second;

    UNIT_ASSERT_EQUAL(trace.ToString(), expectedTrace);
}

Y_UNIT_TEST_SUITE(TKeyValueTracingTest) {
    const TString SmallValue = "value";
    const TString HugeValue = TString(1 << 20, 'v');

Y_UNIT_TEST(WriteSmall) {
    TString canon = "(KeyValue.Intermediate -> [(KeyValue.StorageRequest -> [(DSProxy.Put -> [(Backpressure.InFlight "
        "-> [(VDisk.Log.Put)])])]) , (Tablet.Transaction -> [(Tablet.Transaction.Execute) , (Tablet.WriteLog -> "
        "[(Tablet.WriteLog.LogEntry -> [(DSProxy.Put -> [(Backpressure.InFlight -> [(VDisk.Log.Put)])])])])])])";
    TestOneWrite(SmallValue, std::move(canon));
}

Y_UNIT_TEST(WriteHuge) {
    TString canon = "(KeyValue.Intermediate -> [(KeyValue.StorageRequest -> [(DSProxy.Put -> [(Backpressure.InFlight -> "
        "[(VDisk.HullHugeBlobChunkAllocator) , (VDisk.HullHugeKeeper.InWaitQueue -> [(VDisk.HugeBlobKeeper.Write -> "
        "[(VDisk.Log.PutHuge)])])])])]) , (Tablet.Transaction -> [(Tablet.Transaction.Execute) , (Tablet.WriteLog -> "
        "[(Tablet.WriteLog.LogEntry -> [(DSProxy.Put -> [(Backpressure.InFlight -> [(VDisk.Log.Put)])])])])])])";
    TestOneWrite(HugeValue, std::move(canon));
}

Y_UNIT_TEST(ReadSmall) {
    TString canon = "(KeyValue.Intermediate -> [(KeyValue.StorageReadRequest -> [(DSProxy.Get -> [(Backpressure.InFlight -> "
        "[(VDisk.LevelIndexExtremeQueryViaBatcherMergeData)])])])])";
    TestOneRead(SmallValue, std::move(canon));
}

Y_UNIT_TEST(ReadHuge) {
    TString canon = "(KeyValue.Intermediate -> [(KeyValue.StorageReadRequest -> [(DSProxy.Get -> [(Backpressure.InFlight -> "
        "[(VDisk.LevelIndexExtremeQueryViaBatcherMergeData -> [(VDisk.Query.ReadBatcher)])])])])])";
    TestOneRead(HugeValue, std::move(canon));
}

}
