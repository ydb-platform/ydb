#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/actors/interconnect/interconnect.h>
#include <library/cpp/actors/helpers/selfping_actor.h>
#include <library/cpp/json/json_reader.h>
#include <util/stream/null.h>
#include <ydb/core/viewer/protos/viewer.pb.h>
#include "json_handlers.h"
#include "json_tabletinfo.h"
#include "json_vdiskinfo.h"
#include "json_pdiskinfo.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <library/cpp/actors/core/interconnect.h>

using namespace NKikimr;
using namespace NViewer;
using namespace NKikimrWhiteboard;

using namespace NSchemeShard;
using namespace Tests;
using namespace NMonitoring;

#ifdef NDEBUG
#define Ctest Cnull
#else
#define Ctest Cerr
#endif

#ifdef address_sanitizer_enabled
#define SANITIZER_TYPE address
#endif
#ifdef memory_sanitizer_enabled
#define SANITIZER_TYPE memory
#endif
#ifdef thread_sanitizer_enabled
#define SANITIZER_TYPE thread
#endif

using duration_nano_t = std::chrono::duration<ui64, std::nano>;
using duration_t = std::chrono::duration<double>;

duration_t GetBasePerformance() {
    duration_nano_t accm{};
    for (int i = 0; i < 1000000; ++i) {
        accm += duration_nano_t(NActors::MeasureTaskDurationNs());
    }
    return std::chrono::duration_cast<duration_t>(accm);
}

double BASE_PERF = GetBasePerformance().count();

Y_UNIT_TEST_SUITE(Viewer) {
    Y_UNIT_TEST(TabletMerging) {
        THPTimer timer;
        Cerr << "BASE_PERF = " << BASE_PERF << Endl;
        {
            TMap<ui32, TString> nodesBlob;
            timer.Reset();
            for (ui32 nodeId = 1; nodeId <= 1000; ++nodeId) {
                NKikimrWhiteboard::TEvTabletStateResponse nodeData;
                nodeData.MutableTabletStateInfo()->Reserve(10000);
                for (ui32 tabletId = 1; tabletId <= 10000; ++tabletId) {
                    NKikimrWhiteboard::TTabletStateInfo* tabletData = nodeData.AddTabletStateInfo();
                    tabletData->SetTabletId(tabletId);
                    tabletData->SetLeader(true);
                    tabletData->SetGeneration(13);
                    tabletData->SetChangeTime(TInstant::Now().MilliSeconds());
                    tabletData->MutableTenantId()->SetSchemeShard(8);
                    tabletData->MutableTenantId()->SetPathId(14);
                    tabletData->MutableChannelGroupIDs()->Add(9);
                    tabletData->MutableChannelGroupIDs()->Add(10);
                    tabletData->MutableChannelGroupIDs()->Add(11);
                }
                nodesBlob[nodeId] = nodeData.SerializeAsString();
            }
            Ctest << "Build = " << timer.Passed() << Endl;
            timer.Reset();
            TMap<ui32, NKikimrWhiteboard::TEvTabletStateResponse> nodesData;
            for (const auto& [nodeId, nodeBlob] : nodesBlob) {
                NKikimrWhiteboard::TEvTabletStateResponse nodeData;
                bool res = nodeData.ParseFromString(nodesBlob[nodeId]);
                Y_UNUSED(res);
                nodesData[nodeId] = std::move(nodeData);
            }
            NKikimrWhiteboard::TEvTabletStateResponse result;
            MergeWhiteboardResponses(result, nodesData);
            Ctest << "Merge = " << timer.Passed() << Endl;
            UNIT_ASSERT_LT(timer.Passed(), 8 * BASE_PERF);
            UNIT_ASSERT_VALUES_EQUAL(result.TabletStateInfoSize(), 10000);
            timer.Reset();
        }
        Ctest << "Destroy = " << timer.Passed() << Endl;
    }

    Y_UNIT_TEST(TabletMergingPacked) {
        THPTimer timer;
        {
            TMap<ui32, TString> nodesBlob;
            timer.Reset();
            for (ui32 nodeId = 1; nodeId <= 1000; ++nodeId) {
                THolder<TEvWhiteboard::TEvTabletStateResponse> nodeData = MakeHolder<TEvWhiteboard::TEvTabletStateResponse>();
                auto* tabletData = nodeData->AllocatePackedResponse(10000);
                for (ui32 tabletId = 1; tabletId <= 10000; ++tabletId) {
                    tabletData->TabletId = tabletId;
                    tabletData->FollowerId = 0;
                    tabletData->Generation = 13;
                    tabletData->Type = NKikimrTabletBase::TTabletTypes::TxProxy;
                    tabletData->State = NKikimrWhiteboard::TTabletStateInfo::Restored;
                    //tabletData->SetChangeTime(TInstant::Now().MilliSeconds());
                    ++tabletData;
                }
                nodesBlob[nodeId] = nodeData->Record.SerializeAsString();
            }
            Ctest << "Build = " << timer.Passed() << Endl;
            TMap<ui32, NKikimrWhiteboard::TEvTabletStateResponse> nodesData;
            for (const auto& [nodeId, nodeBlob] : nodesBlob) {
                NKikimrWhiteboard::TEvTabletStateResponse nodeData;
                bool res = nodeData.ParseFromString(nodesBlob[nodeId]);
                Y_UNUSED(res);
                nodesData[nodeId] = std::move(nodeData);
            }
            NKikimrWhiteboard::TEvTabletStateResponse result;
            MergeWhiteboardResponses(result, nodesData);
            Ctest << "Merge = " << timer.Passed() << Endl;
            UNIT_ASSERT_LT(timer.Passed(), 2 * BASE_PERF);
            UNIT_ASSERT_VALUES_EQUAL(result.TabletStateInfoSize(), 10000);
            timer.Reset();
        }
        Ctest << "Destroy = " << timer.Passed() << Endl;
    }

    Y_UNIT_TEST(VDiskMerging) {
        TMap<ui32, NKikimrWhiteboard::TEvVDiskStateResponse> nodesData;
        for (ui32 nodeId = 1; nodeId <= 1000; ++nodeId) {
            NKikimrWhiteboard::TEvVDiskStateResponse& nodeData = nodesData[nodeId];
            nodeData.MutableVDiskStateInfo()->Reserve(10);
            for (ui32 vDiskId = 1; vDiskId <= 1000; ++vDiskId) {
                NKikimrWhiteboard::TVDiskStateInfo* vDiskData = nodeData.AddVDiskStateInfo();
                vDiskData->MutableVDiskId()->SetDomain(vDiskId);
                vDiskData->MutableVDiskId()->SetGroupGeneration(vDiskId);
                vDiskData->MutableVDiskId()->SetGroupID(vDiskId);
                vDiskData->MutableVDiskId()->SetRing(vDiskId);
                vDiskData->MutableVDiskId()->SetVDisk(vDiskId);
                vDiskData->SetAllocatedSize(10);
                vDiskData->SetChangeTime(TInstant::Now().MilliSeconds());
            }
        }
        Ctest << "Data has built" << Endl;
        THPTimer timer;
        NKikimrWhiteboard::TEvVDiskStateResponse result;
        MergeWhiteboardResponses(result, nodesData);
        Ctest << "Merge = " << timer.Passed() << Endl;
        UNIT_ASSERT_LT(timer.Passed(), 10 * BASE_PERF);
        UNIT_ASSERT_VALUES_EQUAL(result.VDiskStateInfoSize(), 1000);
        Ctest << "Data has merged" << Endl;
    }

    Y_UNIT_TEST(PDiskMerging) {
        TMap<ui32, NKikimrWhiteboard::TEvPDiskStateResponse> nodesData;
        for (ui32 nodeId = 1; nodeId <= 1000; ++nodeId) {
            NKikimrWhiteboard::TEvPDiskStateResponse& nodeData = nodesData[nodeId];
            nodeData.MutablePDiskStateInfo()->Reserve(10);
            for (ui32 pDiskId = 1; pDiskId <= 100; ++pDiskId) {
                NKikimrWhiteboard::TPDiskStateInfo* pDiskData = nodeData.AddPDiskStateInfo();
                pDiskData->SetPDiskId(pDiskId);
                pDiskData->SetAvailableSize(100);
                pDiskData->SetChangeTime(TInstant::Now().MilliSeconds());
            }
        }
        Ctest << "Data has built" << Endl;
        THPTimer timer;
        NKikimrWhiteboard::TEvPDiskStateResponse result;
        MergeWhiteboardResponses(result, nodesData);
        Ctest << "Merge = " << timer.Passed() << Endl;
        UNIT_ASSERT_LT(timer.Passed(), 10 * BASE_PERF);
        UNIT_ASSERT_VALUES_EQUAL(result.PDiskStateInfoSize(), 100000);
        Ctest << "Data has merged" << Endl;
    }

    template <typename T>
    void TestSwagger() {
        T h;
        h.Init();

        TStringStream json;
        json << "{";
        h.PrintForSwagger(json);
        json << "}";

        NJson::TJsonReaderConfig jsonCfg;
        jsonCfg.DontValidateUtf8 = true;
        jsonCfg.AllowComments = false;

        ValidateJsonThrow(json.Str(), jsonCfg);
    }

    Y_UNIT_TEST(Swagger) {
        TestSwagger<TViewerJsonHandlers>();
        TestSwagger<TVDiskJsonHandlers>();
    }

    struct THttpRequest : NMonitoring::IHttpRequest {
        HTTP_METHOD Method;
        TCgiParameters CgiParameters;
        THttpHeaders HttpHeaders;

        THttpRequest(HTTP_METHOD method)
            : Method(method)
        {}

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

    class TMonPage: public IMonPage {
    public:
        TMonPage(const TString &path, const TString &title)
            : IMonPage(path, title)
        {
        }

        void Output(IMonHttpRequest&) override {
        }
    };

    void ChangeListNodes(TEvInterconnect::TEvNodesInfo::TPtr* ev, int nodesTotal) {
        auto& nodes = (*ev)->Get()->Nodes;
        
        auto sample = nodes[0];
        nodes.clear();

        for (int nodeId = 0; nodeId < nodesTotal; nodeId++) {
            nodes.emplace_back(sample);
        }
    }

    void ChangeTabletStateResponse(TEvWhiteboard::TEvTabletStateResponse::TPtr* ev, int tabletsTotal, int& tabletId, int& nodeId) {
        ui64* cookie = const_cast<ui64*>(&(ev->Get()->Cookie));
        *cookie = nodeId;

        auto& record = (*ev)->Get()->Record;
        record.clear_tabletstateinfo();

        for (int i = 0; i < tabletsTotal; i++) {
            auto tablet = record.add_tabletstateinfo();
            tablet->set_tabletid(tabletId++);
            tablet->set_type(NKikimrTabletBase::TTabletTypes::Mediator);
            tablet->set_nodeid(nodeId);
            tablet->set_generation(2);
        }

        nodeId++;
    }

    void ChangeDescribeSchemeResult(TEvSchemeShard::TEvDescribeSchemeResult::TPtr* ev, int tabletsTotal) {
        auto record = (*ev)->Get()->MutableRecord();
        auto params = record->mutable_pathdescription()->mutable_domaindescription()->mutable_processingparams();

        params->clear_mediators();
        for (int tabletId = 0; tabletId < tabletsTotal; tabletId++) {
            params->add_mediators(tabletId);
        }
    }

    Y_UNIT_TEST(Cluster10000Tablets)
    {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port)
                .SetNodeCount(2)
                .SetUseRealThreads(false)
                .SetDomainName("Root")
                .InitKikimrRunConfig();
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        TClient client(settings);
        TTestActorRuntime& runtime = *server.GetRuntime();

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        THttpRequest httpReq(HTTP_METHOD_GET);
        httpReq.CgiParameters.emplace("tablets", "true");
        auto page = MakeHolder<TMonPage>("viewer", "title");
        TMonService2HttpRequest monReq(nullptr, &httpReq, nullptr, page.Get(), "/json/cluster", nullptr);
        auto request = MakeHolder<NMon::TEvHttpInfo>(monReq);

        int tabletIdCount = 1;
        int nodeIdCount = 1;
        const int nodesTotal = 100;
        const int tabletsTotal = 100;
        auto observerFunc = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            Y_UNUSED(ev);
            switch (ev->GetTypeRewrite()) {
                case TEvInterconnect::EvNodesInfo: {
                    auto *x = reinterpret_cast<TEvInterconnect::TEvNodesInfo::TPtr*>(&ev);
                    ChangeListNodes(x, nodesTotal);
                    break;
                }
                case TEvWhiteboard::EvTabletStateResponse: {
                    auto *x = reinterpret_cast<TEvWhiteboard::TEvTabletStateResponse::TPtr*>(&ev);
                    ChangeTabletStateResponse(x, tabletsTotal, tabletIdCount, nodeIdCount);
                    break;
                }
                case NSchemeShard::TEvSchemeShard::EvDescribeSchemeResult: {
                    auto *x = reinterpret_cast<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr*>(&ev);
                    ChangeDescribeSchemeResult(x, nodesTotal * tabletsTotal);
                    break;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        THPTimer timer;

        runtime.Send(new IEventHandle(NKikimr::NViewer::MakeViewerID(0), sender, request.Release(), 0));
        runtime.GrabEdgeEvent<NMon::TEvHttpInfoRes>(handle);

        Ctest << "Request timer = " << timer.Passed() << Endl;
        Ctest << "BASE_PERF = " << BASE_PERF << Endl;
        UNIT_ASSERT_LT(timer.Passed(), BASE_PERF);

    }

    Y_UNIT_TEST(TenantInfo5kkTablets)
    {
        const int nodesTotal = 5;
        const int tabletsTotal = 1000000;

        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port)
                .SetNodeCount(nodesTotal)
                .SetUseRealThreads(false)
                .SetDomainName("Root")
                .InitKikimrRunConfig();
        TServer server(settings);
        server.EnableGRpc(grpcPort);

        TClient client(settings);
        TTestActorRuntime& runtime = *server.GetRuntime();

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        THttpRequest httpReq(HTTP_METHOD_GET);
        httpReq.CgiParameters.emplace("path", "/Root");
        httpReq.CgiParameters.emplace("tablets", "true");
        httpReq.CgiParameters.emplace("storage", "true");
        auto page = MakeHolder<TMonPage>("viewer", "title");
        TMonService2HttpRequest monReq(nullptr, &httpReq, nullptr, page.Get(), "/json/tenantinfo", nullptr);
        auto request = MakeHolder<NMon::TEvHttpInfo>(monReq);

        int tabletIdCount = 1;
        int nodeIdCount = 1;
        auto observerFunc = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            Y_UNUSED(ev);
            switch (ev->GetTypeRewrite()) {
                case NConsole::TEvConsole::EvListTenantsResponse: {
                    auto *x = reinterpret_cast<NConsole::TEvConsole::TEvListTenantsResponse::TPtr*>(&ev);
                    Ydb::Cms::ListDatabasesResult listTenantsResult;
                    (*x)->Get()->Record.GetResponse().operation().result().UnpackTo(&listTenantsResult);
                    listTenantsResult.Addpaths("/Root");
                    (*x)->Get()->Record.MutableResponse()->mutable_operation()->mutable_result()->PackFrom(listTenantsResult);
                    break;
                }
                case TEvTxProxySchemeCache::EvNavigateKeySetResult: {
                    auto *x = reinterpret_cast<TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr*>(&ev);
                    auto &domain = (*x)->Get()->Request->ResultSet.begin()->DomainInfo;
                    domain->Params.SetHive(1);
                    break;
                }
                case TEvHive::EvResponseHiveDomainStats: {
                    auto *x = reinterpret_cast<TEvHive::TEvResponseHiveDomainStats::TPtr*>(&ev);
                    auto &record = (*x)->Get()->Record;
                    auto *domainStats = record.AddDomainStats();
                    for (int i = 1; i <= nodesTotal; i++) {
                        domainStats->AddNodeIds(i);
                    }
                    domainStats->SetShardId(NKikimr::Tests::SchemeRoot);
                    domainStats->SetPathId(1);
                    break;
                }
                case TEvWhiteboard::EvTabletStateResponse: {
                    auto *x = reinterpret_cast<TEvWhiteboard::TEvTabletStateResponse::TPtr*>(&ev);
                    ChangeTabletStateResponse(x, tabletsTotal, tabletIdCount, nodeIdCount);
                    break;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        THPTimer timer;

        runtime.Send(new IEventHandle(NKikimr::NViewer::MakeViewerID(0), sender, request.Release(), 0));
        runtime.GrabEdgeEvent<NMon::TEvHttpInfoRes>(handle);

        Ctest << "Request timer = " << timer.Passed() << Endl;
        Ctest << "BASE_PERF = " << BASE_PERF << Endl;

#ifndef SANITIZER_TYPE
#if !defined(NDEBUG) || defined(_hardening_enabled_) 
        UNIT_ASSERT_VALUES_EQUAL_C(timer.Passed() < 30 * BASE_PERF, true, "timer = " << timer.Passed() << ", limit = " << 30 * BASE_PERF);
#else
        UNIT_ASSERT_VALUES_EQUAL_C(timer.Passed() < 10 * BASE_PERF, true, "timer = " << timer.Passed() << ", limit = " << 10 * BASE_PERF);
#endif
#endif
    }
}
