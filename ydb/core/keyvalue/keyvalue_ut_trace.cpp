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
        Runtime->AddAppDataInit([](ui32, NKikimr::TAppData& appData) {
            appData.FeatureFlags.SetEnablePutBatchingForBlobStorage(false);
        });

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

void TestOneWrite(TString value, TVector<TString> &&expectedTraceVariants) {
    TTestEnvironment env(8);
    env.Prepare();

    env.DoKVRequest(CreateWrite("key", std::move(value)));

    UNIT_ASSERT(env.WilsonUploader->BuildTraceTrees());
    UNIT_ASSERT_VALUES_EQUAL(env.WilsonUploader->Traces.size(), 1);
    auto& trace = env.WilsonUploader->Traces.begin()->second;

    bool found = false;
    for (const TString &expectedTrace : expectedTraceVariants) {
        found |= trace.ToString() == expectedTrace;
    }
    UNIT_ASSERT_C(found, trace.ToString());
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


std::string DSProxyPutBlobTemplate =
    "(DSProxy.Put.Blob -> "
        "[(VDisk.Log.MultiPutItem -> "
            "[{PDISK_LOG_WRITE}]"
        ")]"
    ")";

TVector<std::string> PDiskLogWriteVariants = {
    "(PDisk.LogWrite -> "
        "[(PDisk.InScheduler.InLogWriteBatch) , (PDisk.InBlockDevice)]"
    ")",
    "(PDisk.LogWrite -> "
        "[(PDisk.InScheduler) , (PDisk.InBlockDevice)]"
    ")",
};

TVector<std::string> TabletWriteLogTempaltes = {
    "(Tablet.WriteLog -> "
        "[(Tablet.WriteLog.Reference -> "
            "[{DSPROXY_PUT_BLOB1}]"
        ") , "
        "(Tablet.WriteLog.LogEntry -> "
            "[{DSPROXY_PUT_BLOB2}]"
        ")]"
    ")",
    "(Tablet.WriteLog -> "
        "[(Tablet.WriteLog.Reference -> "
            "[{DSPROXY_PUT_BLOB1}]"
        ")]"
    ")",
    "(Tablet.WriteLog -> "
        "[(Tablet.WriteLog.LogEntry -> "
            "[{DSPROXY_PUT_BLOB1}]"
        ")]"
    ")",
};


TVector<TString> MakeCanons(std::string globalTemplate) {
    TVector<std::string> tabletWriteLogVariants;
    TVector<std::string> dsProxyPutBlobVariants;
    for (auto &pdiskLogWrite : PDiskLogWriteVariants) {
        std::string localTemplate = DSProxyPutBlobTemplate;
        std::string templateWord = "{PDISK_LOG_WRITE}";
        auto it = localTemplate.find(templateWord);
        UNIT_ASSERT(it != std::string::npos);
        localTemplate.replace(it, templateWord.size(), pdiskLogWrite);
        dsProxyPutBlobVariants.push_back(localTemplate);
    }
    
    for (auto &tabletWriteLog : TabletWriteLogTempaltes) {
        std::string templateWord1 = "{DSPROXY_PUT_BLOB1}";
        for (auto &dsProxyPutBlob1 : dsProxyPutBlobVariants) {
            std::string template1 = tabletWriteLog;
            auto it1 = template1.find(templateWord1);
            UNIT_ASSERT(it1 != std::string::npos);
            template1.replace(it1, templateWord1.size(), dsProxyPutBlob1);

            std::string templateWord2 = "{DSPROXY_PUT_BLOB2}";
            auto it2 = template1.find(templateWord2);
            if (it2 == std::string::npos) {
                tabletWriteLogVariants.push_back(TString(template1));
            } else {
                for (auto &dsProxyPutBlob2 : dsProxyPutBlobVariants) {
                    std::string template2 = template1;
                    it2 = template2.find(templateWord2);
                    UNIT_ASSERT(it2 != std::string::npos);
                    template2.replace(it2, templateWord2.size(), dsProxyPutBlob2);
                    tabletWriteLogVariants.push_back(TString(template2));
                }
            }
        }
    }

    TVector<TString> result;
    for (auto &tabletLogWrite : tabletWriteLogVariants) {
        std::string localTemplate = globalTemplate;
        std::string templateWord = "{TABLET_LOG_WRITE}";
        auto it = localTemplate.find(templateWord);
        UNIT_ASSERT(it != std::string::npos);
        localTemplate.replace(it, templateWord.size(), tabletLogWrite);
        result.push_back(TString(localTemplate));
    }
    return result;
};



Y_UNIT_TEST(WriteSmall) {
    TVector<TString> canons = MakeCanons(
        "(KeyValue.Intermediate -> "
            "[(KeyValue.StorageRequest -> "
                "[(DSProxy.Put -> "
                    "[(Backpressure.InFlight -> "
                        "[(VDisk.Log.Put)]"
                    ")]"
                ")]"
            ") , "
            "(Tablet.Transaction -> "
                "[(Tablet.Transaction.Execute) , {TABLET_LOG_WRITE}]"
            ")]"
        ")"
    );
    TestOneWrite(SmallValue, std::move(canons));
}

Y_UNIT_TEST(WriteHuge) {
    TVector<TString> canons = MakeCanons(
        "(KeyValue.Intermediate -> "
            "[(KeyValue.StorageRequest -> "
                "[(DSProxy.Put -> "
                    "[(Backpressure.InFlight -> "
                        "[(VDisk.HullHugeBlobChunkAllocator) , "
                        "(VDisk.HullHugeKeeper.InWaitQueue -> "
                            "[(VDisk.HugeBlobKeeper.Write -> "
                                "[(VDisk.Log.PutHuge)]"
                            ")]"
                        ")]"
                    ")]"
                ")]"
            ") , "
            "(Tablet.Transaction -> "
                "[(Tablet.Transaction.Execute) , {TABLET_LOG_WRITE}]"
            ")]"
        ")"
    );
    TestOneWrite(HugeValue, std::move(canons));
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
