#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/system/env.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/mon/sync_http_mon.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/core/sys_view/service/sysview_service.h>
#include <ydb/core/testlib/fake_scheme_shard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
namespace NKikimr::NPQ {

namespace {

TVector<std::pair<ui64, TString>> TestData() {
    TVector<std::pair<ui64, TString>> data;
    TString s{32, 'c'};
    // FIXME: replace magic numbers and add VERIFY on sizes
    const ui32 pp = 8 + 4 + 2 + 9;
    for (ui32 i = 0; i < 10; ++i) {
        data.push_back({i + 1, s.substr(pp)});
    }
    return data;
}

struct THttpRequest : NMonitoring::IHttpRequest {
    HTTP_METHOD Method;
    TCgiParameters CgiParameters;
    THttpHeaders HttpHeaders;

    THttpRequest(HTTP_METHOD method)
        : Method(method)
    {
        CgiParameters.emplace("type", TTabletTypes::TypeToStr(TTabletTypes::PersQueue));
        CgiParameters.emplace("json", "");
    }

    ~THttpRequest() {}

    const char* GetURI() const override {
        return "";
    }

    const char* GetPath() const override {
        return "";
    }

    const TCgiParameters& GetParams() const override {
        return CgiParameters;
    }

    const TCgiParameters& GetPostParams() const override {
        return CgiParameters;
    }

    TStringBuf GetPostContent() const override {
        return TString();
    }

    HTTP_METHOD GetMethod() const override {
        return Method;
    }

    const THttpHeaders& GetHeaders() const override {
        return HttpHeaders;
    }

    TString GetRemoteAddr() const override {
        return TString();
    }
};

} // anonymous namespace
Y_UNIT_TEST_SUITE(PQCountersSimple) {

Y_UNIT_TEST(Partition) {
    TTestContext tc;
    TFinalizer finalizer(tc);
    bool activeZone{false};
    tc.Prepare("", [](TTestActorRuntime&) {}, activeZone, false, true);
    tc.Runtime->SetScheduledLimit(100);

    PQTabletPrepare({}, {}, tc);
    CmdWrite(0, "sourceid0", TestData(), tc, false, {}, true);
    CmdWrite(0, "sourceid1", TestData(), tc, false);
    CmdWrite(0, "sourceid2", TestData(), tc, false);
    CmdWrite(0, "sourceid1", TestData(), tc, false);
    CmdWrite(0, "sourceid2", TestData(), tc, false);
    PQGetPartInfo(0, 30, tc);


    {
        auto counters = tc.Runtime->GetAppData(0).Counters;
        auto dbGroup = GetServiceCounters(counters, "pqproxy");
        TStringStream countersStr;
        dbGroup->OutputHtml(countersStr);
        TString referenceCounters = NResource::Find(TStringBuf("counters_pqproxy.html"));

        UNIT_ASSERT_VALUES_EQUAL(countersStr.Str() + "\n", referenceCounters);
    }

    {
        auto counters = tc.Runtime->GetAppData(0).Counters;
        auto dbGroup = GetServiceCounters(counters, "datastreams");
        TStringStream countersStr;
        dbGroup->OutputHtml(countersStr);
        UNIT_ASSERT_VALUES_EQUAL(countersStr.Str(), "<pre></pre>");
    }
}


Y_UNIT_TEST(PartitionWriteQuota) {
    TTestContext tc;

    TFinalizer finalizer(tc);
    bool activeZone{false};
    tc.Prepare("", [](TTestActorRuntime&) {}, activeZone, false, true);
    tc.Runtime->SetScheduledLimit(100);
    tc.Runtime->GetAppData(0).PQConfig.MutableQuotingConfig()->SetEnableQuoting(true);

    PQTabletPrepare({.partitions = 1, .writeSpeed = 30_KB}, {}, tc);
    TVector<std::pair<ui64, TString>> data;
    TString s{32_KB, 'c'};
    data.push_back({1, s});
    tc.Runtime->SetObserverFunc(
        [&](TAutoPtr<IEventHandle>& ev) {
            if (auto* msg = ev->CastAsLocal<TEvQuota::TEvRequest>()) {
                Cerr << "Captured kesus quota request event from " << ev->Sender.ToString() << Endl;
                tc.Runtime->Send(new IEventHandle(
                    ev->Sender, TActorId{},
                    new TEvQuota::TEvClearance(TEvQuota::TEvClearance::EResult::Success), 0, ev->Cookie)
                );
                return TTestActorRuntimeBase::EEventAction::DROP;
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
    });
    for (auto i = 0u; i < 6; i++) {
        CmdWrite(0, "sourceid0", data, tc);
        data[0].first++;
    }

    {
        auto counters = tc.Runtime->GetAppData(0).Counters;
        Y_ABORT_UNLESS(counters);
        auto dbGroup = GetServiceCounters(counters, "pqproxy");

        auto quotaWait = dbGroup->FindSubgroup("subsystem", "partitionWriteQuotaWait")
                             ->FindSubgroup("Account", "total")
                             ->FindSubgroup("Producer", "total")
                             ->FindSubgroup("Topic", "total")
                             ->FindSubgroup("TopicPath", "total")
                             ->FindSubgroup("OriginDC", "cluster");
        auto histogram = quotaWait->FindSubgroup("sensor", "PartitionWriteQuotaWaitOriginal");
        TStringStream histogramStr;
        histogram->OutputHtml(histogramStr);
        Cerr << "**** Total histogram: **** \n " << histogramStr.Str() << "**** **** **** ****" << Endl;
        auto instant = histogram->FindNamedCounter("Interval", "0ms")->Val();
        auto oneSec = histogram->FindNamedCounter("Interval", "1000ms")->Val();
        auto twoSec = histogram->FindNamedCounter("Interval", "2500ms")->Val();
        UNIT_ASSERT_VALUES_EQUAL(oneSec + twoSec, 5);
        UNIT_ASSERT(twoSec >= 2);
        UNIT_ASSERT(oneSec >= 1);
        UNIT_ASSERT(instant >= 1);
    }
}

Y_UNIT_TEST(PartitionFirstClass) {
    TTestContext tc;
    TFinalizer finalizer(tc);
    bool activeZone{false};
    tc.Prepare("", [](TTestActorRuntime&){}, activeZone, true, true);
    tc.Runtime->SetScheduledLimit(100);

    PQTabletPrepare({}, {}, tc);
    CmdWrite(0, "sourceid0", TestData(), tc, false, {}, true);
    CmdWrite(0, "sourceid1", TestData(), tc, false);
    CmdWrite(0, "sourceid2", TestData(), tc, false);
    CmdWrite(0, "sourceid0", TestData(), tc, false);
    PQGetPartInfo(0, 30, tc);

    {
        auto counters = tc.Runtime->GetAppData(0).Counters;
        auto dbGroup = GetServiceCounters(counters, "pqproxy");
        TStringStream countersStr;
        dbGroup->OutputHtml(countersStr);
        TString referenceCounters = NResource::Find(TStringBuf("counters_pqproxy_firstclass.html"));

        UNIT_ASSERT_EQUAL(countersStr.Str() + "\n", referenceCounters);
    }

    {
        auto counters = tc.Runtime->GetAppData(0).Counters;
        auto dbGroup = GetServiceCounters(counters, "datastreams");

        TStringStream countersStr;
        dbGroup->OutputHtml(countersStr);
        const TString referenceCounters = NResource::Find(TStringBuf("counters_datastreams.html"));
        UNIT_ASSERT_VALUES_EQUAL(countersStr.Str() + "\n", referenceCounters);
    }
}

Y_UNIT_TEST(SupportivePartitionCountersPersist) {
    TTestContext tc;

    TFinalizer finalizer(tc);
    bool activeZone{false};
    tc.Prepare("", [](TTestActorRuntime&) {}, activeZone, false, true);
    tc.Runtime->SetScheduledLimit(100);
    tc.Runtime->GetAppData(0).PQConfig.MutableQuotingConfig()->SetEnableQuoting(true);

    PQTabletPrepare({.partitions = 1, .writeSpeed = 30_KB}, {}, tc);
    TVector<std::pair<ui64, TString>> data;
    TString s{32_KB, 'c'};
    data.push_back({1, s});
    tc.Runtime->SetObserverFunc(
        [&](TAutoPtr<IEventHandle>& ev) {
            if (auto* msg = ev->CastAsLocal<TEvQuota::TEvRequest>()) {
                Cerr << "Captured kesus quota request event from " << ev->Sender.ToString() << Endl;
                tc.Runtime->Send(new IEventHandle(
                    ev->Sender, TActorId{},
                    new TEvQuota::TEvClearance(TEvQuota::TEvClearance::EResult::Success), 0, ev->Cookie)
                );
                return TTestActorRuntimeBase::EEventAction::DROP;
            } else if (auto* msg = ev->CastAsLocal<TEvKeyValue::TEvRequest>()) {
                Cerr << "Captured TEvRequest, cmd write size: " << msg->Record.CmdWriteSize() << Endl;
                for (auto& w : msg->Record.GetCmdWrite()) {
                    if (w.GetKey().StartsWith("J")) {
                        NKikimrPQ::TPartitionMeta meta;
                        bool res = meta.ParseFromString(w.GetValue());
                        UNIT_ASSERT(res);
                        UNIT_ASSERT(meta.HasCounterData());
                        Cerr << "Write meta: " << meta.GetCounterData().ShortDebugString() << Endl;
                    }
                }
                return TTestActorRuntimeBase::EEventAction::PROCESS;
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
    });
    for (auto i = 0u; i < 6; i++) {
        CmdWrite(0, "sourceid0", data, tc);
        data[0].first++;
    }
    PQGetPartInfo(0, 6, tc);
}
} // Y_UNIT_TEST_SUITE(PQCountersSimple)

Y_UNIT_TEST_SUITE(PQCountersLabeled) {

void CompareJsons(const TString& inputStr, const TString& referenceStr) {
    NJson::TJsonValue referenceJson;
    UNIT_ASSERT(NJson::ReadJsonTree(TStringBuf(referenceStr), &referenceJson));

    NJson::TJsonValue inputJson;
    UNIT_ASSERT(NJson::ReadJsonTree(TStringBuf(inputStr), &inputJson));

    Cerr << "Expected: " << referenceStr << Endl;
    Cerr << "Result: " << inputStr << Endl;

    // Run time of test differs as well as counters below.
    // We  set it to 5000 and then compare with reference string.
    auto getByPath = [](const NJson::TJsonValue& msg, TStringBuf path) {
        NJson::TJsonValue ret;
        UNIT_ASSERT_C(msg.GetValueByPath(path, ret), path);
        return ret.GetStringSafe();
    };

    for (auto &sensor : inputJson["sensors"].GetArraySafe()) {
        if (getByPath(sensor, "kind") == "GAUGE" &&
            (getByPath(sensor, "labels.sensor") == "PQ/TimeSinceLastReadMs" ||
            getByPath(sensor, "labels.sensor") == "PQ/PartitionLifeTimeMs" ||
            getByPath(sensor, "labels.sensor") == "PQ/TotalTimeLagMsByLastRead" ||
            getByPath(sensor, "labels.sensor") == "PQ/WriteTimeLagMsByLastReadOld")) {
            sensor.SetValueByPath("value", 5000);
        } else if (getByPath(sensor, "kind") == "GAUGE" &&
            (getByPath(sensor, "labels.sensor") == "PQ/WriteTimeLagMsByLastRead" ||
            getByPath(sensor, "labels.sensor") == "PQ/WriteTimeLagMsByLastWrite")) {
            sensor.SetValueByPath("value", 30);
        }
    }

    Cerr << "Test diff count : " << inputJson["sensors"].GetArraySafe().size()
        << " " << referenceJson["sensors"].GetArraySafe().size() << Endl;

    ui64 inCount = inputJson["sensors"].GetArraySafe().size();
    ui64 refCount = referenceJson["sensors"].GetArraySafe().size();
    for (ui64 i = 0; i < inCount && i < refCount; ++i) {
        auto& in = inputJson["sensors"].GetArraySafe()[i];
        auto& ref = referenceJson["sensors"].GetArraySafe()[i];
        UNIT_ASSERT_VALUES_EQUAL_C(in["labels"], ref["labels"], TStringBuilder() << " at pos #" << i);
    }
    if (inCount > refCount) {
        UNIT_ASSERT_C(false, inputJson["sensors"].GetArraySafe()[refCount].GetStringRobust());
    } else if (refCount > inCount) {
        UNIT_ASSERT_C(false, referenceJson["sensors"].GetArraySafe()[inCount].GetStringRobust());
    }

    //UNIT_ASSERT_VALUES_EQUAL(referenceJson, inputJson);
}

Y_UNIT_TEST(Partition) {
    SetEnv("FAST_UT", "1");
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone, false, true, true);
        tc.Runtime->SetScheduledLimit(1000);

        PQTabletPrepare({}, {}, tc);

        IActor* actor = CreateTabletCountersAggregator(false);
        auto aggregatorId = tc.Runtime->Register(actor);
        tc.Runtime->EnableScheduleForActor(aggregatorId);

        CmdWrite(0, "sourceid0", TestData(), tc, false, {}, true);
        CmdWrite(0, "sourceid1", TestData(), tc, false);
        CmdWrite(0, "sourceid2", TestData(), tc, false);
        PQGetPartInfo(0, 30, tc);

        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTabletCounters::EvTabletAddLabeledCounters);
            auto processedCountersEvent = tc.Runtime->DispatchEvents(options);
            UNIT_ASSERT_VALUES_EQUAL(processedCountersEvent, true);
        }

        IActor* actorX = CreateClusterLabeledCountersAggregatorActor(tc.Edge, TTabletTypes::PersQueue);
        tc.Runtime->Register(actorX);

        TAutoPtr<IEventHandle> handle;
        TEvTabletCounters::TEvTabletLabeledCountersResponse *result;
        result = tc.Runtime->GrabEdgeEvent<TEvTabletCounters::TEvTabletLabeledCountersResponse>(handle);
        UNIT_ASSERT(result);

        THttpRequest httpReq(HTTP_METHOD_GET);
        NMonitoring::TMonService2HttpRequest monReq(nullptr, &httpReq, nullptr, nullptr, "", nullptr);
        tc.Runtime->Send(new IEventHandle(aggregatorId, tc.Edge, new NMon::TEvHttpInfo(monReq)));

        TAutoPtr<IEventHandle> handle1;
        auto resp = tc.Runtime->GrabEdgeEvent<NMon::TEvHttpInfoRes>(handle1);
        const TString countersStr = ((NMon::TEvHttpInfoRes*) resp)->Answer.substr(sizeof("HTTP/1.1 200 Ok Content-Type: application/json Connection: Close "));
        const TString referenceStr = NResource::Find(TStringBuf("counters_labeled.json"));
        CompareJsons(countersStr, referenceStr);


    });
}

Y_UNIT_TEST(PartitionFirstClass) {
    SetEnv("FAST_UT", "1");
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& activeZone) {
        TFinalizer finalizer(tc);
        activeZone = false;
        bool dbRegistered{false};
        bool labeledCountersReceived =false ;

        tc.Prepare(dispatchName, setup, activeZone, true, true, true);
        tc.Runtime->SetScheduledLimit(10000);

        tc.Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == NSysView::TEvSysView::EvRegisterDbCounters) {
                auto database = event.Get()->Get<NSysView::TEvSysView::TEvRegisterDbCounters>()->Database;
                UNIT_ASSERT_VALUES_EQUAL(database, "/Root/PQ");
                dbRegistered = true;
            } else if (event->GetTypeRewrite() == TEvTabletCounters::EvTabletAddLabeledCounters) {
                labeledCountersReceived = true;
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        });
        PQTabletPrepare({.deleteTime=3600, .writeSpeed = 100_KB, .meteringMode = NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS}, {{"client", true}}, tc);
        TFakeSchemeShardState::TPtr state{new TFakeSchemeShardState()};
        ui64 ssId = 325;
        BootFakeSchemeShard(*tc.Runtime, ssId, state);

        PQBalancerPrepare("topic", {{0, {tc.TabletId, 1}}}, ssId, tc);

        IActor* actor = CreateTabletCountersAggregator(false);
        auto aggregatorId = tc.Runtime->Register(actor);
        tc.Runtime->EnableScheduleForActor(aggregatorId);

        CmdWrite(0, "sourceid0", TestData(), tc, false, {}, true);
        CmdWrite(0, "sourceid1", TestData(), tc, false);
        CmdWrite(0, "sourceid2", TestData(), tc, false);
        PQGetPartInfo(0, 30, tc);

        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTabletCounters::EvTabletAddLabeledCounters);
            tc.Runtime->DispatchEvents(options);
        }
        //UNIT_ASSERT(labeledCountersReceived);

        {
            NSchemeCache::TDescribeResult::TPtr result = new NSchemeCache::TDescribeResult{};
            result->SetPath("/Root");
            TVector<TString> attrs = {"folder_id", "cloud_id", "database_id"};
            for (auto& attr : attrs) {
                auto ua = result->MutablePathDescription()->AddUserAttributes();
                ua->SetKey(attr);
                ua->SetValue(attr);
            }
            NSchemeCache::TDescribeResult::TCPtr cres = result;
            auto event = MakeHolder<TEvTxProxySchemeCache::TEvWatchNotifyUpdated>(0, "/Root", TPathId{}, cres);
            TActorId pipeClient = tc.Runtime->ConnectToPipe(tc.BalancerTabletId, tc.Edge, 0, GetPipeConfigWithRetries());
            tc.Runtime->SendToPipe(tc.BalancerTabletId, tc.Edge, event.Release(), 0, GetPipeConfigWithRetries(), pipeClient);

            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTxProxySchemeCache::EvWatchNotifyUpdated);
            auto processedCountersEvent = tc.Runtime->DispatchEvents(options);
            UNIT_ASSERT_VALUES_EQUAL(processedCountersEvent, true);
        }
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvPersQueue::EvPeriodicTopicStats);
            auto processedCountersEvent = tc.Runtime->DispatchEvents(options);
            UNIT_ASSERT_VALUES_EQUAL(processedCountersEvent, true);
        }
        {
            auto counters = tc.Runtime->GetAppData(0).Counters;
            auto dbGroup = GetServiceCounters(counters, "topics_serverless", false);

            auto group = dbGroup->GetSubgroup("host", "")
                                ->GetSubgroup("database", "/Root")
                                ->GetSubgroup("cloud_id", "cloud_id")
                                ->GetSubgroup("folder_id", "folder_id")
                                ->GetSubgroup("database_id", "database_id")->GetSubgroup("topic", "topic");
            group->GetNamedCounter("name", "topic.partition.uptime_milliseconds_min", false)->Set(30000);
            group->GetNamedCounter("name", "topic.partition.write.lag_milliseconds_max", false)->Set(600);
            group->GetNamedCounter("name", "topic.partition.uptime_milliseconds_min", false)->Set(30000);
            group->GetNamedCounter("name", "topic.partition.write.lag_milliseconds_max", false)->Set(600);
            group = group->GetSubgroup("consumer", "client");
            group->GetNamedCounter("name", "topic.partition.end_to_end_lag_milliseconds_max", false)->Set(30000);
            group->GetNamedCounter("name", "topic.partition.read.idle_milliseconds_max", false)->Set(30000);
            group->GetNamedCounter("name", "topic.partition.write.lag_milliseconds_max", false)->Set(200);

            TStringStream countersStr;
            dbGroup->OutputHtml(countersStr);
            const TString referenceCounters = NResource::Find(TStringBuf("counters_topics.html"));
            Cerr << "REF: " << referenceCounters << "\n";
            Cerr << "COUNTERS: " << countersStr.Str() << "\n";
            UNIT_ASSERT_VALUES_EQUAL(countersStr.Str() + "\n", referenceCounters);
        }
    });
}

void CheckLabeledCountersResponse(TTestContext& tc, ui32 count, TVector<TString> mustHave = {}) {
    IActor* actor = CreateClusterLabeledCountersAggregatorActor(tc.Edge, TTabletTypes::PersQueue);
    tc.Runtime->Register(actor);

    TAutoPtr<IEventHandle> handle;
    TEvTabletCounters::TEvTabletLabeledCountersResponse *result;
    result = tc.Runtime->GrabEdgeEvent<TEvTabletCounters::TEvTabletLabeledCountersResponse>(handle);
    UNIT_ASSERT(result);

    THashSet<TString> groups;

    Cerr << "NEW ANS:\n";
    for (ui32 i = 0; i < result->Record.LabeledCountersByGroupSize(); ++i) {
        auto& c = result->Record.GetLabeledCountersByGroup(i);
        groups.insert(c.GetGroup());
        Cerr << "ANS GROUP " << c.GetGroup() << "\n";
    }
    UNIT_ASSERT_VALUES_EQUAL(groups.size(), count);
    for (auto& g : mustHave) {
        Cerr << "CHECKING GROUP " << g << "\n";
        UNIT_ASSERT(groups.contains(g));
    }
}

Y_UNIT_TEST(ImportantFlagSwitching) {
    const TString topicName = "rt3.dc1--asdfgs--topic";

    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        activeZone = false;
        tc.Runtime->SetScheduledLimit(1000);

        auto MakeTopics = [&] (const TVector<TString>& users) {
            TVector<TString> res;
            for (const auto& u : users) {
                res.emplace_back(NKikimr::JoinPath({u, topicName}));
            }
            return res;
        };

        PQTabletPrepare({}, {}, tc);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTabletCounters::EvTabletAddLabeledCounters);
            tc.Runtime->DispatchEvents(options);
        }
        // Topic counters only
        CheckLabeledCountersResponse(tc, 8);

        // Topic counters + important
        PQTabletPrepare({}, {{"user", true}}, tc);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTabletCounters::EvTabletAddLabeledCounters);
            tc.Runtime->DispatchEvents(options);
        }
        CheckLabeledCountersResponse(tc, 8, MakeTopics({"user/1"}));

        PQTabletPrepare({}, {}, tc);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTabletCounters::EvTabletAddLabeledCounters);
            tc.Runtime->DispatchEvents(options);
        }
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTabletCounters::EvTabletAddLabeledCounters);
            tc.Runtime->DispatchEvents(options);
        }
        // Topic counters + not important
        CheckLabeledCountersResponse(tc, 8, MakeTopics({"user/0"}));

        // Topic counters + not important
        PQTabletPrepare({}, {{"user", true}, {"user2", true}}, tc);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTabletCounters::EvTabletAddLabeledCounters);
            tc.Runtime->DispatchEvents(options);
        }
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTabletCounters::EvTabletAddLabeledCounters);
            tc.Runtime->DispatchEvents(options);
        }
        CheckLabeledCountersResponse(tc, 11, MakeTopics({"user/1", "user2/1"}));

        PQTabletPrepare({}, {{"user", true}, {"user2", false}}, tc);
        for (ui32 i = 0 ; i < 2; ++i){
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTabletCounters::EvTabletAddLabeledCounters);
            tc.Runtime->DispatchEvents(options);
        }

        CheckLabeledCountersResponse(tc, 12, MakeTopics({"user/1", "user2/0"}));

        PQTabletPrepare({}, {{"user", true}}, tc);
        for (ui32 i = 0 ; i < 2; ++i){
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTabletCounters::EvTabletAddLabeledCounters);
            tc.Runtime->DispatchEvents(options);
        }

        CheckLabeledCountersResponse(tc, 8, MakeTopics({"user/1"}));
    });
}

} // Y_UNIT_TEST_SUITE(PQCountersLabeled)

} // namespace NKikimr::NPQ
