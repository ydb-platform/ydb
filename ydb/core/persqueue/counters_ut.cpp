#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/mon/sync_http_mon.h>

#include "pq_ut.h"

namespace NKikimr {

Y_UNIT_TEST_SUITE(PQCountersSimple) {

Y_UNIT_TEST(Partition) {
    TTestContext tc;
    TFinalizer finalizer(tc);
    bool activeZone{false};
    tc.Prepare("", [](TTestActorRuntime&) {}, activeZone, false, true);
    tc.Runtime->SetScheduledLimit(100);

    TVector<std::pair<ui64, TString>> data;
    TString s{32, 'c'};
    ui32 pp = 8 + 4 + 2 + 9;
    for (ui32 i = 0; i < 10; ++i) {
        data.push_back({i + 1, s.substr(pp)});
    }
    PQTabletPrepare({}, {}, tc);
    CmdWrite(0, "sourceid0", data, tc, false, {}, true);
    CmdWrite(0, "sourceid1", data, tc, false);
    CmdWrite(0, "sourceid2", data, tc, false);

    {
        auto counters = tc.Runtime->GetAppData(0).Counters;
        auto dbGroup = GetServiceCounters(counters, "pqproxy");
        TStringStream countersStr;
        dbGroup->OutputHtml(countersStr);
        Cerr << "ASDFGS: " << countersStr.Str() << Endl;
        TString referenceCounters = NResource::Find(TStringBuf("counters_pqproxy.html"));
        UNIT_ASSERT_EQUAL(countersStr.Str() + "\n", referenceCounters);
    }

    {
        auto counters = tc.Runtime->GetAppData(0).Counters;
        auto dbGroup = GetServiceCounters(counters, "datastreams");
        TStringStream countersStr;
        dbGroup->OutputHtml(countersStr);
        UNIT_ASSERT_EQUAL(countersStr.Str(), "<pre></pre>");
    }
}

Y_UNIT_TEST(PartitionFirstClass) {
    TTestContext tc;
    TFinalizer finalizer(tc);
    bool activeZone{false};
    tc.Prepare("", [](TTestActorRuntime&){}, activeZone, true, true);
    tc.Runtime->SetScheduledLimit(100);

    TVector<std::pair<ui64, TString>> data;
    TString s{32, 'c'};
    ui32 pp = 8 + 4 + 2 + 9;
    for (ui32 i = 0; i < 10; ++i) {
        data.push_back({i + 1, s.substr(pp)});
    }
    PQTabletPrepare({}, {}, tc);
    CmdWrite(0, "sourceid0", data, tc, false, {}, true);
    CmdWrite(0, "sourceid1", data, tc, false);
    CmdWrite(0, "sourceid2", data, tc, false);

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
        UNIT_ASSERT_EQUAL(countersStr.Str() + "\n", referenceCounters);
    }
}

} // Y_UNIT_TEST_SUITE(PQCountersSimple)

Y_UNIT_TEST_SUITE(PQCountersLabeled) {

struct THttpRequest : NMonitoring::IHttpRequest {
    HTTP_METHOD Method;
    TCgiParameters CgiParameters;
    THttpHeaders HttpHeaders;

    THttpRequest(HTTP_METHOD method)
        : Method(method)
    {
        CgiParameters.emplace("type", TTabletTypes::TypeToStr(TTabletTypes::PersQueue));
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

Y_UNIT_TEST(Partition) {
    TTestContext tc;
    RunTestWithReboots({}, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone, false, true);
        tc.Runtime->SetScheduledLimit(100);

        TVector<std::pair<ui64, TString>> data;
        TString s{32, 'c'};
        ui32 pp = 8 + 4 + 2 + 9;
        for (ui32 i = 0; i < 10; ++i) {
            data.push_back({i + 1, s.substr(pp)});
        }
        PQTabletPrepare({}, {}, tc);

        IActor* actor = CreateTabletCountersAggregator(false);
        auto aggregatorId = tc.Runtime->Register(actor);
        tc.Runtime->EnableScheduleForActor(aggregatorId);

        CmdWrite(0, "sourceid0", data, tc, false, {}, true);
        CmdWrite(0, "sourceid1", data, tc, false);
        CmdWrite(0, "sourceid2", data, tc, false);
        PQGetPartInfo(0, 30, tc);

        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTabletCounters::EvTabletAddLabeledCounters);
            tc.Runtime->DispatchEvents(options);
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
        const TString countersStr = ((NMon::TEvHttpInfoRes*) resp)->Answer;

        const TString referenceCounters = NResource::Find(TStringBuf("counters_datastreams.html"));
        UNIT_ASSERT_VALUES_EQUAL(countersStr, referenceCounters);
    }, 1);
}

Y_UNIT_TEST(PartitionFirstClass) {
    TTestContext tc;
    RunTestWithReboots({}, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& activeZone) {
        TFinalizer finalizer(tc);
        activeZone = false;

        tc.Prepare(dispatchName, setup, activeZone, true, true);
        tc.Runtime->SetScheduledLimit(100);

        TVector<std::pair<ui64, TString>> data;
        TString s{32, 'c'};
        ui32 pp = 8 + 4 + 2 + 9;
        for (ui32 i = 0; i < 10; ++i) {
            data.push_back({i + 1, s.substr(pp)});
        }
        PQTabletPrepare({}, {}, tc);

        IActor* actor = CreateTabletCountersAggregator(false);
        auto aggregatorId = tc.Runtime->Register(actor);
        tc.Runtime->EnableScheduleForActor(aggregatorId);

        CmdWrite(0, "sourceid0", data, tc, false, {}, true);
        CmdWrite(0, "sourceid1", data, tc, false);
        CmdWrite(0, "sourceid2", data, tc, false);
        PQGetPartInfo(0, 30, tc);

        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTabletCounters::EvTabletAddLabeledCounters);
            auto processedCountersEvent = tc.Runtime->DispatchEvents(options, TDuration::MilliSeconds(500));
            UNIT_ASSERT_VALUES_EQUAL(processedCountersEvent, false);
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

    for (ui32 i = 0; i < result->Record.LabeledCountersByGroupSize(); ++i) {
        auto& c = result->Record.GetLabeledCountersByGroup(i);
        groups.insert(c.GetGroup());
    }
    UNIT_ASSERT_VALUES_EQUAL(groups.size(), count);
    for (auto& g : mustHave) {
        UNIT_ASSERT(groups.contains(g));
    }
}

Y_UNIT_TEST(ImportantFlagSwitching) {
    const static TString TOPIC_NAME = "rt3.dc1--asdfgs--topic";
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        activeZone = false;
        tc.Runtime->SetScheduledLimit(600);

        auto MakeTopics = [&] (const TVector<TString>& users) {
            TVector<TString> res;
            for (const auto& u : users) {
                res.emplace_back(NKikimr::JoinPath({u, TOPIC_NAME}));
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
        CheckLabeledCountersResponse(tc, 8, {NKikimr::JoinPath({"user/1", TOPIC_NAME})});

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
        CheckLabeledCountersResponse(tc, 12, MakeTopics({"user/1", "user2/0"}));

        PQTabletPrepare({}, {{"user", true}}, tc);
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
        CheckLabeledCountersResponse(tc, 8, MakeTopics({"user/1"}));
    });
}
} // Y_UNIT_TEST_SUITE(PQCountersLabeled)

} // namespace NKikimr
