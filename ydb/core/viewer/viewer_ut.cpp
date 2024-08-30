#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/helpers/selfping_actor.h>
#include <library/cpp/http/misc/httpcodes.h>
#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_reader.h>
#include <util/stream/null.h>
#include <util/string/join.h>
#include <ydb/core/viewer/protos/viewer.pb.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include "viewer_tabletinfo.h"
#include "viewer_vdiskinfo.h"
#include "viewer_pdiskinfo.h"
#include "query_autocomplete_helper.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/library/actors/core/interconnect.h>

#include <util/string/builder.h>

using namespace NKikimr;
using namespace NViewer;
using namespace NKikimrWhiteboard;

using namespace NSchemeShard;
using namespace Tests;
using namespace NMonitoring;
using TNavigate = NSchemeCache::TSchemeCacheNavigate;

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

    struct THttpRequest : NMonitoring::IHttpRequest {
        HTTP_METHOD Method;
        TCgiParameters CgiParameters;
        THttpHeaders HttpHeaders;
        TString PostContent;

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
            return PostContent;
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

    void ChangeVDiskStateResponse(TEvWhiteboard::TEvVDiskStateResponse::TPtr* ev, NKikimrWhiteboard::EFlag diskSpace, ui64 used, ui64 limit) {
        auto& pbRecord = (*ev)->Get()->Record;
        auto state = pbRecord.add_vdiskstateinfo();
        state->mutable_vdiskid()->set_vdisk(0);
        state->mutable_vdiskid()->set_groupid(0);
        state->mutable_vdiskid()->set_groupgeneration(1);
        state->set_diskspace(diskSpace);
        state->set_vdiskstate(NKikimrWhiteboard::EVDiskState::OK);
        state->set_nodeid(0);
        state->set_allocatedsize(used);
        state->set_availablesize(limit - used);
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
                .SetUseSectorMap(true)
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
        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
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
                .SetUseSectorMap(true)
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
        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
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

                    // Event can be generated by workload manager, it is ok for not found response without domain
                    if (domain) {
                        domain->Params.SetHive(1);
                    }
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

    NJson::TJsonValue SendQuery(const TString& query, const TString& schema, const bool base64) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port);
        settings.InitKikimrRunConfig()
                .SetNodeCount(1)
                .SetUseRealThreads(false)
                .SetDomainName("Root")
                .SetUseSectorMap(true);
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        TClient client(settings);
        TTestActorRuntime& runtime = *server.GetRuntime();

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        THttpRequest httpReq(HTTP_METHOD_GET);
        httpReq.CgiParameters.emplace("schema", schema);
        httpReq.CgiParameters.emplace("base64", base64 ? "true" : "false");
        httpReq.CgiParameters.emplace("query", query);
        auto page = MakeHolder<TMonPage>("viewer", "title");
        TMonService2HttpRequest monReq(nullptr, &httpReq, nullptr, page.Get(), "/json/query", nullptr);
        auto request = MakeHolder<NMon::TEvHttpInfo>(monReq);

        runtime.Send(new IEventHandle(NKikimr::NViewer::MakeViewerID(0), sender, request.Release(), 0));
        NMon::TEvHttpInfoRes* result = runtime.GrabEdgeEvent<NMon::TEvHttpInfoRes>(handle);

        size_t pos = result->Answer.find('{');
        TString jsonResult = result->Answer.substr(pos);
        Ctest << "json result: " << jsonResult << Endl;
        NJson::TJsonValue json;
        try {
            NJson::ReadJsonTree(jsonResult, &json, true);
        }
        catch (yexception ex) {
            Ctest << ex.what() << Endl;
        }
        return json;
    }

    void QueryTest(const TString& query, const bool base64, const TString& reply) {
        NJson::TJsonValue result = SendQuery(query, "classic", base64);
        UNIT_ASSERT_VALUES_EQUAL(result.GetMap().at("column0").GetString(), reply);

        result = SendQuery(query, "ydb", base64);
        UNIT_ASSERT_VALUES_EQUAL(result.GetMap().at("result").GetArray()[0].GetMap().at("column0").GetString(), reply);

        result = SendQuery(query, "modern", base64);
        UNIT_ASSERT_VALUES_EQUAL(result.GetMap().at("result").GetArray()[0].GetArray()[0].GetString(), reply);

        result = SendQuery(query, "multi", base64);
        UNIT_ASSERT_VALUES_EQUAL(result.GetMap().at("result").GetArray()[0].GetMap().at("rows").GetArray()[0].GetArray()[0].GetString(), reply);
    }

    Y_UNIT_TEST(SelectStringWithBase64Encoding)
    {
        QueryTest("select \"Hello\"", true, "SGVsbG8=");
    }

    Y_UNIT_TEST(SelectStringWithNoBase64Encoding)
    {
        QueryTest("select \"Hello\"", false, "Hello");
    }

    void StorageSpaceTest(const TString& withValue, const NKikimrWhiteboard::EFlag diskSpace, const ui64 used, const ui64 limit, const bool isExpectingGroup) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port);
        settings.InitKikimrRunConfig()
                .SetNodeCount(1)
                .SetUseRealThreads(false)
                .SetDomainName("Root")
                .SetUseSectorMap(true);
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        TClient client(settings);
        TTestActorRuntime& runtime = *server.GetRuntime();

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        THttpRequest httpReq(HTTP_METHOD_GET);
        httpReq.CgiParameters.emplace("with", withValue);
        httpReq.CgiParameters.emplace("version", "v2");
        auto page = MakeHolder<TMonPage>("viewer", "title");
        TMonService2HttpRequest monReq(nullptr, &httpReq, nullptr, page.Get(), "/json/storage", nullptr);
        auto request = MakeHolder<NMon::TEvHttpInfo>(monReq);

        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            Y_UNUSED(ev);
            if (ev->GetTypeRewrite() == TEvWhiteboard::EvVDiskStateResponse) {
                auto *x = reinterpret_cast<TEvWhiteboard::TEvVDiskStateResponse::TPtr*>(&ev);
                ChangeVDiskStateResponse(x, diskSpace, used, limit);
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        runtime.Send(new IEventHandle(NKikimr::NViewer::MakeViewerID(0), sender, request.Release(), 0));
        NMon::TEvHttpInfoRes* result = runtime.GrabEdgeEvent<NMon::TEvHttpInfoRes>(handle);

        size_t pos = result->Answer.find('{');
        TString jsonResult = result->Answer.substr(pos);
        Ctest << "json result: " << jsonResult << Endl;
        NJson::TJsonValue json;
        try {
            NJson::ReadJsonTree(jsonResult, &json, true);
        }
        catch (yexception ex) {
            Ctest << ex.what() << Endl;
        }
        UNIT_ASSERT_VALUES_EQUAL(json.GetMap().contains("StorageGroups"), isExpectingGroup);
    }

    Y_UNIT_TEST(StorageGroupOutputWithoutFilterNoDepends)
    {
        StorageSpaceTest("all", NKikimrWhiteboard::EFlag::Green, 10, 100, true);
        StorageSpaceTest("all", NKikimrWhiteboard::EFlag::Red, 90, 100, true);
    }

    Y_UNIT_TEST(StorageGroupOutputWithSpaceCheckDependsOnVDiskSpaceStatus)
    {
        StorageSpaceTest("space", NKikimrWhiteboard::EFlag::Green, 10, 100, false);
        StorageSpaceTest("space", NKikimrWhiteboard::EFlag::Red, 10, 100, true);
    }

    Y_UNIT_TEST(StorageGroupOutputWithSpaceCheckDependsOnUsage)
    {
        StorageSpaceTest("space", NKikimrWhiteboard::EFlag::Green, 70, 100, false);
        StorageSpaceTest("space", NKikimrWhiteboard::EFlag::Green, 80, 100, true);
        StorageSpaceTest("space", NKikimrWhiteboard::EFlag::Green, 90, 100, true);
    }

    const TPathId SHARED_DOMAIN_KEY = {7000000000, 1};
    const TPathId SERVERLESS_DOMAIN_KEY = {7000000000, 2};
    const TPathId SERVERLESS_TABLE = {7000000001, 2};

    void ChangeNavigateKeySetResultServerless(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr* ev,
                                              TTestActorRuntime& runtime) {
        TSchemeCacheNavigate::TEntry& entry((*ev)->Get()->Request->ResultSet.front());
        TString path = CanonizePath(entry.Path);
        if (path == "/Root/serverless" || entry.TableId.PathId == SERVERLESS_DOMAIN_KEY) {
            entry.Status = TSchemeCacheNavigate::EStatus::Ok;
            entry.Kind = TSchemeCacheNavigate::EKind::KindExtSubdomain;
            entry.DomainInfo = MakeIntrusive<TDomainInfo>(SERVERLESS_DOMAIN_KEY, SHARED_DOMAIN_KEY);
            entry.Path = {"Root", "serverless"};
        } else if (path == "/Root/shared" || entry.TableId.PathId == SHARED_DOMAIN_KEY) {
            entry.Status = TSchemeCacheNavigate::EStatus::Ok;
            entry.Kind = TSchemeCacheNavigate::EKind::KindExtSubdomain;
            entry.DomainInfo = MakeIntrusive<TDomainInfo>(SHARED_DOMAIN_KEY, SHARED_DOMAIN_KEY);
            entry.Path = {"Root", "shared"};
            auto domains = runtime.GetAppData().DomainsInfo;
            entry.DomainInfo->Params.SetHive(domains->GetHive());
        } else if (path == "/Root/serverless/users" || entry.TableId.PathId == SERVERLESS_TABLE) {
            entry.Status = TSchemeCacheNavigate::EStatus::Ok;
            entry.Kind = TSchemeCacheNavigate::EKind::KindTable;
            entry.DomainInfo = MakeIntrusive<TDomainInfo>(SERVERLESS_DOMAIN_KEY, SHARED_DOMAIN_KEY);
            entry.Path = {"Root", "serverless", "users"};
            auto dirEntryInfo = MakeIntrusive<TSchemeCacheNavigate::TDirEntryInfo>();
            dirEntryInfo->Info.SetSchemeshardId(SERVERLESS_TABLE.OwnerId);
            dirEntryInfo->Info.SetPathId(SERVERLESS_TABLE.LocalPathId);
            entry.Self = dirEntryInfo;
        }
    }

    void ChangeBoardInfoServerless(TEvStateStorage::TEvBoardInfo::TPtr* ev,
                                   const std::vector<size_t>& sharedDynNodes = {},
                                   const std::vector<size_t>& exclusiveDynNodes = {}) {
        auto *record = (*ev)->Get();
        using EStatus = TEvStateStorage::TEvBoardInfo::EStatus;
        if (record->Path == "gpc+/Root/serverless" && !exclusiveDynNodes.empty()) {
            const_cast<EStatus&>(record->Status) = EStatus::Ok;
            for (auto exclusiveDynNodeId : exclusiveDynNodes) {
                TActorId actorOnExclusiveDynNode = TActorId(exclusiveDynNodeId, 0, 0, 0);
                record->InfoEntries[actorOnExclusiveDynNode] = {};
            }
        } else if (record->Path == "gpc+/Root/shared" && !sharedDynNodes.empty()) {
            const_cast<EStatus&>(record->Status) = EStatus::Ok;
            for (auto sharedDynNodeId : sharedDynNodes) {
                TActorId actorOnSharedDynNode = TActorId(sharedDynNodeId, 0, 0, 0);
                record->InfoEntries[actorOnSharedDynNode] = {};
            }
        }
    }

    void ChangeResponseHiveNodeStatsServerless(TEvHive::TEvResponseHiveNodeStats::TPtr* ev,
                                               size_t sharedDynNode = 0,
                                               size_t exclusiveDynNode = 0,
                                               size_t exclusiveDynNodeWithTablet = 0) {
        auto &record = (*ev)->Get()->Record;
        if (sharedDynNode) {
            auto *sharedNodeStats = record.MutableNodeStats()->Add();
            sharedNodeStats->SetNodeId(sharedDynNode);
            sharedNodeStats->MutableNodeDomain()->SetSchemeShard(SHARED_DOMAIN_KEY.OwnerId);
            sharedNodeStats->MutableNodeDomain()->SetPathId(SHARED_DOMAIN_KEY.LocalPathId);
        }

        if (exclusiveDynNode) {
            auto *exclusiveNodeStats = record.MutableNodeStats()->Add();
            exclusiveNodeStats->SetNodeId(exclusiveDynNode);
            exclusiveNodeStats->MutableNodeDomain()->SetSchemeShard(SERVERLESS_DOMAIN_KEY.OwnerId);
            exclusiveNodeStats->MutableNodeDomain()->SetPathId(SERVERLESS_DOMAIN_KEY.LocalPathId);
        }

        if (exclusiveDynNodeWithTablet) {
            auto *exclusiveDynNodeWithTabletStats = record.MutableNodeStats()->Add();
            exclusiveDynNodeWithTabletStats->SetNodeId(exclusiveDynNodeWithTablet);
            exclusiveDynNodeWithTabletStats->MutableNodeDomain()->SetSchemeShard(SERVERLESS_DOMAIN_KEY.OwnerId);
            exclusiveDynNodeWithTabletStats->MutableNodeDomain()->SetPathId(SERVERLESS_DOMAIN_KEY.LocalPathId);

            auto *stateStats = exclusiveDynNodeWithTabletStats->MutableStateStats()->Add();
            stateStats->SetTabletType(NKikimrTabletBase::TTabletTypes::DataShard);
            stateStats->SetVolatileState(NKikimrHive::TABLET_VOLATILE_STATE_RUNNING);
            stateStats->SetCount(1);
        }
    }

    Y_UNIT_TEST(ServerlessNodesPage)
    {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port)
                .SetNodeCount(1)
                .SetDynamicNodeCount(1)
                .SetUseRealThreads(false)
                .SetDomainName("Root")
                .SetUseSectorMap(true)
                .InitKikimrRunConfig();
        TServer server(settings);
        server.EnableGRpc(grpcPort);

        TClient client(settings);
        TTestActorRuntime& runtime = *server.GetRuntime();

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        THttpRequest httpReq(HTTP_METHOD_GET);
        httpReq.CgiParameters.emplace("database", "/Root/serverless");
        httpReq.CgiParameters.emplace("tablets", "true");
        httpReq.CgiParameters.emplace("enums", "true");
        httpReq.CgiParameters.emplace("sort", "");
        httpReq.CgiParameters.emplace("direct", "1");
        auto page = MakeHolder<TMonPage>("viewer", "title");
        TMonService2HttpRequest monReq(nullptr, &httpReq, nullptr, page.Get(), "/json/nodes", nullptr);
        auto request = MakeHolder<NMon::TEvHttpInfo>(monReq);

        size_t staticNodeId = 0;
        size_t sharedDynNodeId = 0;
        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvTxProxySchemeCache::EvNavigateKeySetResult: {
                    auto *x = reinterpret_cast<TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr*>(&ev);
                    ChangeNavigateKeySetResultServerless(x, runtime);
                    break;
                }
                case TEvInterconnect::EvNodesInfo: {
                    auto *x = reinterpret_cast<TEvInterconnect::TEvNodesInfo::TPtr*>(&ev);
                    TVector<TEvInterconnect::TNodeInfo> &nodes = (*x)->Get()->Nodes;
                    UNIT_ASSERT_EQUAL(nodes.size(), 2);
                    staticNodeId = nodes[0];
                    sharedDynNodeId = nodes[1];
                    break;
                }
                case TEvStateStorage::EvBoardInfo: {
                    auto *x = reinterpret_cast<TEvStateStorage::TEvBoardInfo::TPtr*>(&ev);
                    ChangeBoardInfoServerless(x, { sharedDynNodeId });
                    break;
                }
                case TEvHive::EvResponseHiveNodeStats: {
                    auto *x = reinterpret_cast<TEvHive::TEvResponseHiveNodeStats::TPtr*>(&ev);
                    ChangeResponseHiveNodeStatsServerless(x, sharedDynNodeId);
                    break;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        runtime.Send(new IEventHandle(NKikimr::NViewer::MakeViewerID(0), sender, request.Release(), 0));
        NMon::TEvHttpInfoRes* result = runtime.GrabEdgeEvent<NMon::TEvHttpInfoRes>(handle);

        size_t pos = result->Answer.find('{');
        TString jsonResult = result->Answer.substr(pos);
        Ctest << "json result: " << jsonResult << Endl;
        NJson::TJsonValue json;
        try {
            NJson::ReadJsonTree(jsonResult, &json, true);
        }
        catch (yexception ex) {
            Ctest << ex.what() << Endl;
        }
        UNIT_ASSERT_VALUES_EQUAL(json.GetMap().at("TotalNodes"), "1");
        UNIT_ASSERT_VALUES_EQUAL(json.GetMap().at("FoundNodes"), "1");
    }

    Y_UNIT_TEST(ServerlessWithExclusiveNodes)
    {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port)
                .SetNodeCount(1)
                .SetDynamicNodeCount(2)
                .SetUseRealThreads(false)
                .SetDomainName("Root")
                .SetUseSectorMap(true)
                .InitKikimrRunConfig();
        TServer server(settings);
        server.EnableGRpc(grpcPort);

        TClient client(settings);
        TTestActorRuntime& runtime = *server.GetRuntime();

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        THttpRequest httpReq(HTTP_METHOD_GET);
        httpReq.CgiParameters.emplace("database", "/Root/serverless");
        httpReq.CgiParameters.emplace("direct", "1");
        auto page = MakeHolder<TMonPage>("viewer", "title");
        TMonService2HttpRequest monReq(nullptr, &httpReq, nullptr, page.Get(), "/json/nodes", nullptr);
        auto request = MakeHolder<NMon::TEvHttpInfo>(monReq);

        size_t staticNodeId = 0;
        size_t sharedDynNodeId = 0;
        size_t exclusiveDynNodeId = 0;
        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvTxProxySchemeCache::EvNavigateKeySetResult: {
                    auto *x = reinterpret_cast<TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr*>(&ev);
                    ChangeNavigateKeySetResultServerless(x, runtime);
                    break;
                }
                case TEvInterconnect::EvNodesInfo: {
                    auto *x = reinterpret_cast<TEvInterconnect::TEvNodesInfo::TPtr*>(&ev);
                    TVector<TEvInterconnect::TNodeInfo> &nodes = (*x)->Get()->Nodes;
                    UNIT_ASSERT_EQUAL(nodes.size(), 3);
                    staticNodeId = nodes[0];
                    sharedDynNodeId = nodes[1];
                    exclusiveDynNodeId = nodes[2];
                    break;
                }
                case TEvStateStorage::EvBoardInfo: {
                    auto *x = reinterpret_cast<TEvStateStorage::TEvBoardInfo::TPtr*>(&ev);
                    ChangeBoardInfoServerless(x, { sharedDynNodeId }, { exclusiveDynNodeId });
                    break;
                }
                case TEvHive::EvResponseHiveNodeStats: {
                    auto *x = reinterpret_cast<TEvHive::TEvResponseHiveNodeStats::TPtr*>(&ev);
                    ChangeResponseHiveNodeStatsServerless(x, sharedDynNodeId, exclusiveDynNodeId);
                    break;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        runtime.Send(new IEventHandle(NKikimr::NViewer::MakeViewerID(0), sender, request.Release(), 0));
        NMon::TEvHttpInfoRes* result = runtime.GrabEdgeEvent<NMon::TEvHttpInfoRes>(handle);

        size_t pos = result->Answer.find('{');
        TString jsonResult = result->Answer.substr(pos);
        Ctest << "json result: " << jsonResult << Endl;
        NJson::TJsonValue json;
        try {
            NJson::ReadJsonTree(jsonResult, &json, true);
        }
        catch (yexception ex) {
            Ctest << ex.what() << Endl;
        }
        UNIT_ASSERT_VALUES_EQUAL(json.GetMap().at("TotalNodes"), "1");
        UNIT_ASSERT_VALUES_EQUAL(json.GetMap().at("FoundNodes"), "1");
        UNIT_ASSERT_VALUES_EQUAL(json.GetMap().at("Nodes").GetArray().size(), 1);
        auto node = json.GetMap().at("Nodes").GetArray()[0].GetMap();
        UNIT_ASSERT_VALUES_EQUAL(node.at("NodeId"), exclusiveDynNodeId);
    }

    Y_UNIT_TEST(SharedDoesntShowExclusiveNodes)
    {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port)
                .SetNodeCount(1)
                .SetDynamicNodeCount(2)
                .SetUseRealThreads(false)
                .SetDomainName("Root")
                .SetUseSectorMap(true)
                .InitKikimrRunConfig();
        TServer server(settings);
        server.EnableGRpc(grpcPort);

        TClient client(settings);
        TTestActorRuntime& runtime = *server.GetRuntime();

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        THttpRequest httpReq(HTTP_METHOD_GET);
        httpReq.CgiParameters.emplace("database", "/Root/shared");
        httpReq.CgiParameters.emplace("direct", "1");
        auto page = MakeHolder<TMonPage>("viewer", "title");
        TMonService2HttpRequest monReq(nullptr, &httpReq, nullptr, page.Get(), "/json/nodes", nullptr);
        auto request = MakeHolder<NMon::TEvHttpInfo>(monReq);

        size_t staticNodeId = 0;
        size_t sharedDynNodeId = 0;
        size_t exclusiveDynNodeId = 0;
        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvTxProxySchemeCache::EvNavigateKeySetResult: {
                    auto *x = reinterpret_cast<TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr*>(&ev);
                    ChangeNavigateKeySetResultServerless(x, runtime);
                    break;
                }
                case TEvInterconnect::EvNodesInfo: {
                    auto *x = reinterpret_cast<TEvInterconnect::TEvNodesInfo::TPtr*>(&ev);
                    TVector<TEvInterconnect::TNodeInfo> &nodes = (*x)->Get()->Nodes;
                    UNIT_ASSERT_EQUAL(nodes.size(), 3);
                    staticNodeId = nodes[0];
                    sharedDynNodeId = nodes[1];
                    exclusiveDynNodeId = nodes[2];
                    break;
                }
                case TEvStateStorage::EvBoardInfo: {
                    auto *x = reinterpret_cast<TEvStateStorage::TEvBoardInfo::TPtr*>(&ev);
                    ChangeBoardInfoServerless(x, { sharedDynNodeId }, { exclusiveDynNodeId });
                    break;
                }
                case TEvHive::EvResponseHiveNodeStats: {
                    auto *x = reinterpret_cast<TEvHive::TEvResponseHiveNodeStats::TPtr*>(&ev);
                    ChangeResponseHiveNodeStatsServerless(x, sharedDynNodeId, exclusiveDynNodeId);
                    break;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        runtime.Send(new IEventHandle(NKikimr::NViewer::MakeViewerID(0), sender, request.Release(), 0));
        NMon::TEvHttpInfoRes* result = runtime.GrabEdgeEvent<NMon::TEvHttpInfoRes>(handle);

        size_t pos = result->Answer.find('{');
        TString jsonResult = result->Answer.substr(pos);
        Ctest << "json result: " << jsonResult << Endl;
        NJson::TJsonValue json;
        try {
            NJson::ReadJsonTree(jsonResult, &json, true);
        }
        catch (yexception ex) {
            Ctest << ex.what() << Endl;
        }
        UNIT_ASSERT_VALUES_EQUAL(json.GetMap().at("TotalNodes"), "1");
        UNIT_ASSERT_VALUES_EQUAL(json.GetMap().at("FoundNodes"), "1");
        UNIT_ASSERT_VALUES_EQUAL(json.GetMap().at("Nodes").GetArray().size(), 1);
        auto node = json.GetMap().at("Nodes").GetArray()[0].GetMap();
        UNIT_ASSERT_VALUES_EQUAL(node.at("NodeId"), sharedDynNodeId);
    }

    Y_UNIT_TEST(ServerlessWithExclusiveNodesCheckTable)
    {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port)
                .SetNodeCount(1)
                .SetDynamicNodeCount(3)
                .SetUseRealThreads(false)
                .SetDomainName("Root")
                .SetUseSectorMap(true)
                .InitKikimrRunConfig();
        TServer server(settings);
        server.EnableGRpc(grpcPort);

        TClient client(settings);
        TTestActorRuntime& runtime = *server.GetRuntime();

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        THttpRequest httpReq(HTTP_METHOD_GET);
        httpReq.CgiParameters.emplace("database", "/Root/serverless");
        httpReq.CgiParameters.emplace("path", "/Root/serverless/users");
        httpReq.CgiParameters.emplace("direct", "1");
        httpReq.CgiParameters.emplace("tablets", "true");
        auto page = MakeHolder<TMonPage>("viewer", "title");
        TMonService2HttpRequest monReq(nullptr, &httpReq, nullptr, page.Get(), "/json/nodes", nullptr);
        auto request = MakeHolder<NMon::TEvHttpInfo>(monReq);

        size_t staticNodeId = 0;
        size_t sharedDynNodeId = 0;
        size_t exclusiveDynNodeId = 0;
        size_t secondExclusiveDynNodeId = 0;
        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvTxProxySchemeCache::EvNavigateKeySetResult: {
                    auto *x = reinterpret_cast<TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr*>(&ev);
                    ChangeNavigateKeySetResultServerless(x, runtime);
                    break;
                }
                case TEvInterconnect::EvNodesInfo: {
                    auto *x = reinterpret_cast<TEvInterconnect::TEvNodesInfo::TPtr*>(&ev);
                    TVector<TEvInterconnect::TNodeInfo> &nodes = (*x)->Get()->Nodes;
                    UNIT_ASSERT_EQUAL(nodes.size(), 4);
                    staticNodeId = nodes[0];
                    sharedDynNodeId = nodes[1];
                    exclusiveDynNodeId = nodes[2];
                    secondExclusiveDynNodeId = nodes[3];
                    break;
                }
                case TEvStateStorage::EvBoardInfo: {
                    auto *x = reinterpret_cast<TEvStateStorage::TEvBoardInfo::TPtr*>(&ev);
                    ChangeBoardInfoServerless(x, { sharedDynNodeId }, { exclusiveDynNodeId, secondExclusiveDynNodeId });
                    break;
                }
                case TEvHive::EvResponseHiveNodeStats: {
                    auto *x = reinterpret_cast<TEvHive::TEvResponseHiveNodeStats::TPtr*>(&ev);
                    ChangeResponseHiveNodeStatsServerless(x, sharedDynNodeId, exclusiveDynNodeId, secondExclusiveDynNodeId);
                    break;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        runtime.Send(new IEventHandle(NKikimr::NViewer::MakeViewerID(0), sender, request.Release(), 0));
        NMon::TEvHttpInfoRes* result = runtime.GrabEdgeEvent<NMon::TEvHttpInfoRes>(handle);

        size_t pos = result->Answer.find('{');
        TString jsonResult = result->Answer.substr(pos);
        Ctest << "json result: " << jsonResult << Endl;
        NJson::TJsonValue json;
        try {
            NJson::ReadJsonTree(jsonResult, &json, true);
        }
        catch (yexception ex) {
            Ctest << ex.what() << Endl;
        }
        UNIT_ASSERT_VALUES_EQUAL(json.GetMap().at("TotalNodes"), "2");
        UNIT_ASSERT_VALUES_EQUAL(json.GetMap().at("FoundNodes"), "2");
        UNIT_ASSERT_VALUES_EQUAL(json.GetMap().at("Nodes").GetArray().size(), 2);
        auto firstNode = json.GetMap().at("Nodes").GetArray()[0].GetMap();
        UNIT_ASSERT_VALUES_EQUAL(firstNode.at("NodeId"), exclusiveDynNodeId);
        UNIT_ASSERT(!firstNode.contains("Tablets"));
        auto secondNode = json.GetMap().at("Nodes").GetArray()[1].GetMap();
        UNIT_ASSERT_VALUES_EQUAL(secondNode.at("NodeId"), secondExclusiveDynNodeId);
        UNIT_ASSERT_VALUES_EQUAL(secondNode.at("Tablets").GetArray().size(), 1);
        auto tablet = secondNode.at("Tablets").GetArray()[0].GetMap();
        UNIT_ASSERT_VALUES_EQUAL(tablet.at("Type"), "DataShard");
        UNIT_ASSERT_VALUES_EQUAL(tablet.at("State"), "Green");
        UNIT_ASSERT_VALUES_EQUAL(tablet.at("Count"), 1);
    }

    Y_UNIT_TEST(LevenshteinDistance)
    {
        UNIT_ASSERT_VALUES_EQUAL(LevenshteinDistance("", ""), 0);
        UNIT_ASSERT_VALUES_EQUAL(LevenshteinDistance("kitten", "sitting"), 3);
        UNIT_ASSERT_VALUES_EQUAL(LevenshteinDistance("book", "back"), 2);
        UNIT_ASSERT_VALUES_EQUAL(LevenshteinDistance("", "abc"), 3);
        UNIT_ASSERT_VALUES_EQUAL(LevenshteinDistance("abc", ""), 3);
        UNIT_ASSERT_VALUES_EQUAL(LevenshteinDistance("apple", "apple"), 0);
        UNIT_ASSERT_VALUES_EQUAL(LevenshteinDistance("apple", "aple"), 1);
        UNIT_ASSERT_VALUES_EQUAL(LevenshteinDistance("UPPER", "upper"), 0);
        UNIT_ASSERT_VALUES_EQUAL(LevenshteinDistance("horse", "ros"), 3);
        UNIT_ASSERT_VALUES_EQUAL(LevenshteinDistance("intention", "execution"), 5);
        UNIT_ASSERT_VALUES_EQUAL(LevenshteinDistance("/slice/db", "/slice"), 3);
        UNIT_ASSERT_VALUES_EQUAL(LevenshteinDistance("/slice", "/slice/db"), 3);
        UNIT_ASSERT_VALUES_EQUAL(LevenshteinDistance("/slice/db26000", "/slice/db"), 5);
        UNIT_ASSERT_VALUES_EQUAL(LevenshteinDistance("/slice/db", "/slice/db26000"), 5);
    }

    TVector<TString> SimilarWordsDictionary = { "/slice", "/slice/db", "/slice/db26000" };
    TVector<TString> DifferentWordsDictionary = { "/orders", "/peoples", "/OrdinaryScheduleTables" };

    void FuzzySearcherTest(TVector<TString>& dictionary, TString search, ui32 limit, TVector<TString> expectations) {
        auto fuzzy = FuzzySearcher<TString>(dictionary);
        auto result = fuzzy.Search(search, limit);

        UNIT_ASSERT_VALUES_EQUAL(expectations.size(), result.size());
        for (ui32 i = 0; i < expectations.size(); i++) {
            UNIT_ASSERT_VALUES_EQUAL(expectations[i], result[i]);
        }
    }

    Y_UNIT_TEST(FuzzySearcherLimit1OutOf4)
    {
        FuzzySearcherTest(SimilarWordsDictionary, "/slice/db", 1, { "/slice/db" });
    }

    Y_UNIT_TEST(FuzzySearcherLimit2OutOf4)
    {
        FuzzySearcherTest(SimilarWordsDictionary, "/slice/db", 2, { "/slice/db", "/slice/db26000" });
    }

    Y_UNIT_TEST(FuzzySearcherLimit3OutOf4)
    {
        FuzzySearcherTest(SimilarWordsDictionary, "/slice/db", 3, { "/slice/db", "/slice/db26000", "/slice"});
    }

    Y_UNIT_TEST(FuzzySearcherLimit4OutOf4)
    {
        FuzzySearcherTest(SimilarWordsDictionary, "/slice/db", 4, { "/slice/db", "/slice/db26000", "/slice"});
    }

    Y_UNIT_TEST(FuzzySearcherLongWord)
    {
        FuzzySearcherTest(SimilarWordsDictionary, "/slice/db26001", 10, { "/slice/db26000", "/slice/db", "/slice"});
    }

    Y_UNIT_TEST(FuzzySearcherPriority)
    {
        FuzzySearcherTest(DifferentWordsDictionary, "/ord", 10, { "/orders", "/OrdinaryScheduleTables", "/peoples"});
        FuzzySearcherTest(DifferentWordsDictionary, "Tables", 10, { "/OrdinaryScheduleTables", "/orders", "/peoples"});
    }

    void JsonAutocompleteTest(HTTP_METHOD method, NJson::TJsonValue& value, TString prefix = "", TString database = "", TVector<TString> tables = {}, ui32 limit = 10, bool lowerCaseContentType = false) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port);
        settings.InitKikimrRunConfig()
                .SetNodeCount(1)
                .SetUseRealThreads(false)
                .SetDomainName("Root")
                .SetUseSectorMap(true);
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        TClient client(settings);
        TTestActorRuntime& runtime = *server.GetRuntime();

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        THttpRequest httpReq(method);
        if (method == HTTP_METHOD_GET) {
            if (database) {
                httpReq.CgiParameters.emplace("database", database);
            }
            if (tables.size() > 0) {
                httpReq.CgiParameters.emplace("table", JoinSeq(",", tables));
            }
            if (prefix) {
                httpReq.CgiParameters.emplace("prefix", prefix);
            }
            httpReq.CgiParameters.emplace("limit", ToString(limit));
        } else if (method == HTTP_METHOD_POST) {
            NJson::TJsonArray tableArray;
            for (const TString& table : tables) {
                tableArray.AppendValue(table);
            }

            NJson::TJsonValue root = NJson::TJsonMap{
                {"database", database},
                {"table", tableArray},
                {"prefix", prefix},
                {"limit", limit}
            };
            httpReq.PostContent = NJson::WriteJson(root);
            auto contentType = lowerCaseContentType ? "content-type" : "Content-Type";
            httpReq.HttpHeaders.AddHeader(contentType, "application/json");
        }
        httpReq.CgiParameters.emplace("direct", "1");
        auto page = MakeHolder<TMonPage>("viewer", "title");
        TMonService2HttpRequest monReq(nullptr, &httpReq, nullptr, page.Get(), "/json/autocomplete", nullptr);
        THolder<NMon::TEvHttpInfo> request = MakeHolder<NMon::TEvHttpInfo>(monReq);

        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            Y_UNUSED(ev);
            switch (ev->GetTypeRewrite()) {
                case NConsole::TEvConsole::EvListTenantsResponse: {
                    auto *x = reinterpret_cast<NConsole::TEvConsole::TEvListTenantsResponse::TPtr*>(&ev);
                    Ydb::Cms::ListDatabasesResult listTenantsResult;
                    (*x)->Get()->Record.GetResponse().operation().result().UnpackTo(&listTenantsResult);
                    listTenantsResult.Addpaths("/Root/slice");
                    listTenantsResult.Addpaths("/Root/qwerty");
                    listTenantsResult.Addpaths("/Root/MyDatabase");
                    listTenantsResult.Addpaths("/Root/TestDatabase");
                    listTenantsResult.Addpaths("/Root/test");
                    (*x)->Get()->Record.MutableResponse()->mutable_operation()->mutable_result()->PackFrom(listTenantsResult);
                    break;
                }
                case TEvTxProxySchemeCache::EvNavigateKeySetResult: {
                    auto *x = reinterpret_cast<TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr*>(&ev);
                    (*x)->Get()->Request->ErrorCount = 0;
                    for (auto& entry: (*x)->Get()->Request->ResultSet) {
                        if (entry.Path.size() <= 2) {
                            const TPathId pathId(1, 1);
                            auto listNodeEntry = MakeIntrusive<TNavigate::TListNodeEntry>();
                            listNodeEntry->Children.reserve(3);
                            listNodeEntry->Children.emplace_back("orders", pathId, TNavigate::KindTable);
                            listNodeEntry->Children.emplace_back("clients", pathId, TNavigate::KindTable);
                            listNodeEntry->Children.emplace_back("products", pathId, TNavigate::KindTable);
                            entry.ListNodeEntry = listNodeEntry;
                            entry.Kind = TSchemeCacheNavigate::EKind::KindExtSubdomain;
                        } else {
                            entry.Columns[1].Name = "id";
                            entry.Columns[2].Name = "name";
                            entry.Columns[3].Name = "description";
                            entry.Kind = TSchemeCacheNavigate::EKind::KindTable;
                        }
                        entry.Status = TSchemeCacheNavigate::EStatus::Ok;
                    }
                    break;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        runtime.Send(new IEventHandle(NKikimr::NViewer::MakeViewerID(0), sender, request.Release(), 0));
        NMon::TEvHttpInfoRes* result = runtime.GrabEdgeEvent<NMon::TEvHttpInfoRes>(handle);

        size_t pos = result->Answer.find('{');
        TString jsonResult = result->Answer.substr(pos);
        Ctest << "json result: " << jsonResult << Endl;
        try {
            NJson::ReadJsonTree(jsonResult, &value, true);
        }
        catch (yexception ex) {
            Ctest << ex.what() << Endl;
        }
    }

    void VerifyJsonAutocompleteSuccess(NJson::TJsonValue& value, TVector<TString> names) {
        UNIT_ASSERT_VALUES_EQUAL(value.GetMap().at("Success").GetBoolean(), true);
        UNIT_ASSERT_VALUES_EQUAL(value.GetMap().at("Result").GetMap().at("Total").GetInteger(), names.size());
        auto& entities = value.GetMap().at("Result").GetMap().at("Entities").GetArray();
        for (ui32 k = 0; k < names.size(); k++) {
            UNIT_ASSERT_VALUES_EQUAL(entities[k].GetMap().at("Name").GetString(), names[k]);
        }
    }

    Y_UNIT_TEST(JsonAutocompleteEmpty) {
        NJson::TJsonValue value;
        JsonAutocompleteTest(HTTP_METHOD_GET, value);
        VerifyJsonAutocompleteSuccess(value, {
            "/Root/test",
            "/Root/slice",
            "/Root/qwerty",
            "/Root/MyDatabase",
            "/Root/TestDatabase"
        });
    }

    Y_UNIT_TEST(JsonAutocompleteStartOfDatabaseName) {
        NJson::TJsonValue value;
        JsonAutocompleteTest(HTTP_METHOD_GET, value, "/Root");
        VerifyJsonAutocompleteSuccess(value, {
            "/Root/test",
            "/Root/slice",
            "/Root/qwerty",
            "/Root/MyDatabase",
            "/Root/TestDatabase"
        });
    }

    Y_UNIT_TEST(JsonAutocompleteEndOfDatabaseName) {
        NJson::TJsonValue value;
        JsonAutocompleteTest(HTTP_METHOD_GET, value, "Database");
        VerifyJsonAutocompleteSuccess(value, {
            "/Root/MyDatabase",
            "/Root/TestDatabase",
            "/Root/test",
            "/Root/slice",
            "/Root/qwerty"
        });
    }

    Y_UNIT_TEST(JsonAutocompleteSimilarDatabaseName) {
        NJson::TJsonValue value;
        JsonAutocompleteTest(HTTP_METHOD_GET, value, "/Root/Database");
        VerifyJsonAutocompleteSuccess(value, {
            "/Root/MyDatabase",
            "/Root/TestDatabase",
            "/Root/test",
            "/Root/slice",
            "/Root/qwerty"
        });
    }

    Y_UNIT_TEST(JsonAutocompleteSimilarDatabaseNameWithLimit) {
        NJson::TJsonValue value;
        JsonAutocompleteTest(HTTP_METHOD_GET, value, "/Root/Database", "", {}, 2);
        VerifyJsonAutocompleteSuccess(value, {
            "/Root/MyDatabase",
            "/Root/TestDatabase"
        });
    }

    Y_UNIT_TEST(JsonAutocompleteSimilarDatabaseNamePOST) {
        NJson::TJsonValue value;
        JsonAutocompleteTest(HTTP_METHOD_POST, value, "/Root/Database", "", {}, 2);
        VerifyJsonAutocompleteSuccess(value, {
            "/Root/MyDatabase",
            "/Root/TestDatabase"
        });
    }

    Y_UNIT_TEST(JsonAutocompleteSimilarDatabaseNameLowerCase) {
        NJson::TJsonValue value;
        JsonAutocompleteTest(HTTP_METHOD_POST, value, "/Root/Database", "", {}, 2, true);
        VerifyJsonAutocompleteSuccess(value, {
            "/Root/MyDatabase",
            "/Root/TestDatabase"
        });

    }

    Y_UNIT_TEST(JsonAutocompleteScheme) {
        NJson::TJsonValue value;
        JsonAutocompleteTest(HTTP_METHOD_GET, value, "clien", "/Root/Database");
        VerifyJsonAutocompleteSuccess(value, {
            "clients",
            "orders",
            "products"
        });
    }

    Y_UNIT_TEST(JsonAutocompleteSchemePOST) {
        NJson::TJsonValue value;
        JsonAutocompleteTest(HTTP_METHOD_POST, value, "clien", "/Root/Database");
        VerifyJsonAutocompleteSuccess(value, {
            "clients",
            "orders",
            "products"
        });
    }

    Y_UNIT_TEST(JsonAutocompleteEmptyColumns) {
        NJson::TJsonValue value;
        JsonAutocompleteTest(HTTP_METHOD_GET, value, "", "/Root/Database", {"orders"});
        VerifyJsonAutocompleteSuccess(value, {
            "id",
            "name",
            "description"
        });
    }

    Y_UNIT_TEST(JsonAutocompleteColumns) {
        NJson::TJsonValue value;
        JsonAutocompleteTest(HTTP_METHOD_GET, value, "nam", "/Root/Database", {"orders", "products"});
        VerifyJsonAutocompleteSuccess(value, {
            "name",
            "id",
            "description",
        });
    }

    Y_UNIT_TEST(JsonAutocompleteColumnsPOST) {
        NJson::TJsonValue value;
        JsonAutocompleteTest(HTTP_METHOD_POST, value, "nam", "/Root/Database", {"orders", "products"});
        VerifyJsonAutocompleteSuccess(value, {
            "name",
            "id",
            "description",
        });
    }

    void ChangeBSGroupStateResponse(TEvWhiteboard::TEvBSGroupStateResponse::TPtr* ev) {
        ui64 nodeId = (*ev)->Cookie;
        auto& pbRecord = (*ev)->Get()->Record;

        pbRecord.clear_bsgroupstateinfo();

        for (ui64 groupId = 1; groupId <= 9; groupId++) {
            if (groupId % 9 == nodeId % 9) {
                continue;
            }
            auto state = pbRecord.add_bsgroupstateinfo();
            state->set_groupid(groupId);
            state->set_storagepoolname("/Root:test");
            state->set_nodeid(nodeId);
            for (int k = 1; k <= 8; k++) {
                auto vdisk = groupId * 8 + k;
                auto vdiskId = state->add_vdiskids();
                vdiskId->set_groupid(groupId);
                vdiskId->set_groupgeneration(1);
                vdiskId->set_vdisk(vdisk);
            }
        }
    }

    void ChangePDiskStateResponse(TEvWhiteboard::TEvPDiskStateResponse::TPtr* ev) {
        auto& pbRecord = (*ev)->Get()->Record;
        pbRecord.clear_pdiskstateinfo();
        for (int k = 0; k < 2; k++) {
            auto state = pbRecord.add_pdiskstateinfo();
            state->set_pdiskid(k);
        }
    }

    void ChangeVDiskStateOn9NodeResponse(NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateResponse::TPtr* ev) {
        ui64 nodeId = (*ev)->Cookie;
        auto& pbRecord = (*ev)->Get()->Record;

        pbRecord.clear_vdiskstateinfo();

        for (int k = 0; k < 8; k++) {
            auto groupId = (nodeId + k) % 9 + 1;
            auto vdisk = groupId * 8 + k + 1;
            ui32 pdisk = k / 4;
            ui32 slotid = k % 4;
            auto state = pbRecord.add_vdiskstateinfo();
            state->set_pdiskid(pdisk);
            state->set_vdiskslotid(slotid);
            state->mutable_vdiskid()->set_groupid(groupId);
            state->mutable_vdiskid()->set_groupgeneration(1);
            state->mutable_vdiskid()->set_vdisk(vdisk++);
            state->set_vdiskstate(NKikimrWhiteboard::EVDiskState::OK);
            state->set_nodeid(nodeId);
        }
    }

    void AddGroupsInControllerSelectGroupsResult(TEvBlobStorage::TEvControllerSelectGroupsResult::TPtr* ev,  int groupCount) {
        auto& pbRecord = (*ev)->Get()->Record;
        auto pbMatchGroups = pbRecord.mutable_matchinggroups(0);

        auto sample = pbMatchGroups->groups(0);
        pbMatchGroups->ClearGroups();

        for (int groupId = 1; groupId <= groupCount; groupId++) {
            auto group = pbMatchGroups->add_groups();
            group->CopyFrom(sample);
            group->set_groupid(groupId++);
            group->set_storagepoolname("/Root:test");
        }
    };

    void JsonStorage9Nodes9GroupsListingTest(TString version, bool groupFilter, bool nodeFilter, bool pdiskFilter, ui32 expectedFoundGroups, ui32 expectedTotalGroups) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port);
        settings.InitKikimrRunConfig()
                .SetNodeCount(9)
                .SetUseRealThreads(false)
                .SetDomainName("Root")
                .SetUseSectorMap(true);
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        TClient client(settings);
        TTestActorRuntime& runtime = *server.GetRuntime();

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        THttpRequest httpReq(HTTP_METHOD_GET);
        httpReq.CgiParameters.emplace("with", "all");
        httpReq.CgiParameters.emplace("version", version);
        if (groupFilter) {
            httpReq.CgiParameters.emplace("group_id", "1");
        }
        if (nodeFilter) {
            httpReq.CgiParameters.emplace("node_id", ToString(runtime.GetFirstNodeId()));
        }
        if (pdiskFilter) {
            httpReq.CgiParameters.emplace("pdisk_id", "0");
        }
        auto page = MakeHolder<TMonPage>("viewer", "title");
        TMonService2HttpRequest monReq(nullptr, &httpReq, nullptr, page.Get(), "/json/storage", nullptr);
        auto request = MakeHolder<NMon::TEvHttpInfo>(monReq);

        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
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
                case TEvWhiteboard::EvBSGroupStateResponse: {
                    auto *x = reinterpret_cast<TEvWhiteboard::TEvBSGroupStateResponse::TPtr*>(&ev);
                    ChangeBSGroupStateResponse(x);
                    break;
                }
                case TEvWhiteboard::EvVDiskStateResponse: {
                    auto *x = reinterpret_cast<TEvWhiteboard::TEvVDiskStateResponse::TPtr*>(&ev);
                    ChangeVDiskStateOn9NodeResponse(x);
                    break;
                }
                case TEvWhiteboard::EvPDiskStateResponse: {
                    auto *x = reinterpret_cast<TEvWhiteboard::TEvPDiskStateResponse::TPtr*>(&ev);
                    ChangePDiskStateResponse(x);
                    break;
                }
                case TEvBlobStorage::EvControllerSelectGroupsResult: {
                    auto *x = reinterpret_cast<TEvBlobStorage::TEvControllerSelectGroupsResult::TPtr*>(&ev);
                    AddGroupsInControllerSelectGroupsResult(x, 9);
                    break;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        runtime.Send(new IEventHandle(NKikimr::NViewer::MakeViewerID(0), sender, request.Release(), 0));
        NMon::TEvHttpInfoRes* result = runtime.GrabEdgeEvent<NMon::TEvHttpInfoRes>(handle);

        size_t pos = result->Answer.find('{');
        TString jsonResult = result->Answer.substr(pos);
        NJson::TJsonValue json;
        try {
            NJson::ReadJsonTree(jsonResult, &json, true);
        }
        catch (yexception ex) {
            Ctest << ex.what() << Endl;
        }

        UNIT_ASSERT_VALUES_EQUAL(json.GetMap().at("FoundGroups"), ToString(expectedFoundGroups));
        UNIT_ASSERT_VALUES_EQUAL(json.GetMap().at("TotalGroups"), ToString(expectedTotalGroups));
    }

    Y_UNIT_TEST(JsonStorageListingV1) {
        JsonStorage9Nodes9GroupsListingTest("v1", false, false, false, 9, 9);
    }

    Y_UNIT_TEST(JsonStorageListingV2) {
        JsonStorage9Nodes9GroupsListingTest("v2", false, false, false, 9, 9);
    }

    Y_UNIT_TEST(JsonStorageListingV1GroupIdFilter) {
        JsonStorage9Nodes9GroupsListingTest("v1", true, false, false, 1, 9);
    }

    Y_UNIT_TEST(JsonStorageListingV2GroupIdFilter) {
        JsonStorage9Nodes9GroupsListingTest("v2", true, false, false, 1, 9);
    }

    Y_UNIT_TEST(JsonStorageListingV1NodeIdFilter) {
        JsonStorage9Nodes9GroupsListingTest("v1", false, true, false, 8, 8);
    }

    Y_UNIT_TEST(JsonStorageListingV2NodeIdFilter) {
        JsonStorage9Nodes9GroupsListingTest("v2", false, true, false, 8, 8);
    }

    Y_UNIT_TEST(JsonStorageListingV1PDiskIdFilter) {
        JsonStorage9Nodes9GroupsListingTest("v1", false, true, true, 4, 8);
        JsonStorage9Nodes9GroupsListingTest("v1", false, true, true, 4, 8);
    }

    Y_UNIT_TEST(JsonStorageListingV2PDiskIdFilter) {
        JsonStorage9Nodes9GroupsListingTest("v2", false, true, true, 4, 8);
    }

    struct TFakeTicketParserActor : public TActor<TFakeTicketParserActor> {
        TFakeTicketParserActor()
            : TActor<TFakeTicketParserActor>(&TFakeTicketParserActor::StFunc)
        {}

        STFUNC(StFunc) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTicketParser::TEvAuthorizeTicket, Handle);
                default:
                    break;
            }
        }

        void Handle(TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
            LOG_INFO_S(*TlsActivationContext, NKikimrServices::TICKET_PARSER, "Ticket parser: got TEvAuthorizeTicket event: " << ev->Get()->Ticket << " " << ev->Get()->Database << " " << ev->Get()->Entries.size());
            ++AuthorizeTicketRequests;

            if (ev->Get()->Database != "/Root") {
                Fail(ev, TStringBuilder() << "Incorrect database " << ev->Get()->Database);
                return;
            }

            if (ev->Get()->Ticket != "test_ydb_token") {
                Fail(ev, TStringBuilder() << "Incorrect token " << ev->Get()->Ticket);
                return;
            }

            bool databaseIdFound = false;
            bool folderIdFound = false;
            for (const TEvTicketParser::TEvAuthorizeTicket::TEntry& entry : ev->Get()->Entries) {
                for (const std::pair<TString, TString>& attr : entry.Attributes) {
                    if (attr.first == "database_id") {
                        databaseIdFound = true;
                        if (attr.second != "test_database_id") {
                            Fail(ev, TStringBuilder() << "Incorrect database_id " << attr.second);
                            return;
                        }
                    } else if (attr.first == "folder_id") {
                        folderIdFound = true;
                        if (attr.second != "test_folder_id") {
                            Fail(ev, TStringBuilder() << "Incorrect folder_id " << attr.second);
                            return;
                        }
                    }
                }
            }
            if (!databaseIdFound) {
                Fail(ev, "database_id not found");
                return;
            }
            if (!folderIdFound) {
                Fail(ev, "folder_id not found");
                return;
            }

            Success(ev);
        }

        void Fail(TEvTicketParser::TEvAuthorizeTicket::TPtr& ev, const TString& message) {
            ++AuthorizeTicketFails;
            TEvTicketParser::TError err;
            err.Retryable = false;
            err.Message = message ? message : "Test error";
            LOG_INFO_S(*TlsActivationContext, NKikimrServices::TICKET_PARSER, "Send TEvAuthorizeTicketResult: " << err.Message);
            Send(ev->Sender, new TEvTicketParser::TEvAuthorizeTicketResult(ev->Get()->Ticket, err));
        }

        void Success(TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
            ++AuthorizeTicketSuccesses;
            NACLib::TUserToken::TUserTokenInitFields args;
            args.UserSID = "user_name";
            args.GroupSIDs.push_back("group_name");
            TIntrusivePtr<NACLib::TUserToken> userToken = MakeIntrusive<NACLib::TUserToken>(args);
            LOG_INFO_S(*TlsActivationContext, NKikimrServices::TICKET_PARSER, "Send TEvAuthorizeTicketResult success");
            Send(ev->Sender, new TEvTicketParser::TEvAuthorizeTicketResult(ev->Get()->Ticket, userToken));
        }

        size_t AuthorizeTicketRequests = 0;
        size_t AuthorizeTicketSuccesses = 0;
        size_t AuthorizeTicketFails = 0;
    };

    TString PostQuery(TKeepAliveHttpClient& httpClient, TString query, TString action = "", TString transactionMode = "") {
        TStringStream requestBody;
        requestBody
            << "{ \"query\": \"" << query << "\","
            << " \"database\": \"/Root\","
            << " \"action\": \"" << action << "\","
            << " \"syntax\": \"yql_v1\","
            << " \"transaction_mode\": \"" << transactionMode << "\","
            << " \"stats\": \"none\" }";
        TStringStream responseStream;
        TKeepAliveHttpClient::THeaders headers;
        headers["Content-Type"] = "application/json";
        headers["Authorization"] = "test_ydb_token";
        const TKeepAliveHttpClient::THttpCode statusCode = httpClient.DoPost("/viewer/query?timeout=600000&base64=false&schema=modern", requestBody.Str(), &responseStream, headers);
        const TString response = responseStream.ReadAll();
        UNIT_ASSERT_EQUAL_C(statusCode, HTTP_OK, statusCode << ": " << response);
        return response;
    }

    Y_UNIT_TEST(ExecuteQueryDoesntExecuteSchemeOperationsInsideTransation) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 monPort = tp.GetPort(8765);
        auto settings = TServerSettings(port);
        settings.InitKikimrRunConfig()
                .SetNodeCount(1)
                .SetUseRealThreads(true)
                .SetDomainName("Root")
                .SetUseSectorMap(true)
                .SetMonitoringPortOffset(monPort, true);

        TServer server(settings);
        server.EnableGRpc(grpcPort);
        TClient client(settings);
        client.InitRootScheme();

        TTestActorRuntime& runtime = *server.GetRuntime();
        runtime.SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);

        TKeepAliveHttpClient httpClient("localhost", monPort);

        //Scheme operations cannot be executed inside transaction
        TString response = PostQuery(httpClient, "CREATE TABLE `/Root/Test` (Key Uint64, Value String, PRIMARY KEY (Key));", "execute-query", "serializable-read-write");
        {
            NJson::TJsonReaderConfig jsonCfg;
            NJson::TJsonValue json;
            NJson::ReadJsonTree(response, &jsonCfg, &json, /* throwOnError = */ true);
            UNIT_ASSERT_EQUAL_C(json["status"].GetString(), "PRECONDITION_FAILED", response);
        }
    }

    Y_UNIT_TEST(UseTransactionWhenExecuteDataActionQuery) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 monPort = tp.GetPort(8765);
        auto settings = TServerSettings(port);
        settings.InitKikimrRunConfig()
                .SetNodeCount(1)
                .SetUseRealThreads(true)
                .SetDomainName("Root")
                .SetUseSectorMap(true)
                .SetMonitoringPortOffset(monPort, true);

        TServer server(settings);
        server.EnableGRpc(grpcPort);
        TClient client(settings);
        client.InitRootScheme();

        TTestActorRuntime& runtime = *server.GetRuntime();
        runtime.SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);

        TKeepAliveHttpClient httpClient("localhost", monPort);

        PostQuery(httpClient, "CREATE TABLE `/Root/Test` (Key Uint64, Value String, PRIMARY KEY (Key));", "execute-query");
        PostQuery(httpClient, "INSERT INTO `/Root/Test` (Key, Value) VALUES (1, 'testvalue');", "execute-query");
        TString response = PostQuery(httpClient, "SELECT * FROM `/Root/Test`;", "execute-data");
        {
            NJson::TJsonReaderConfig jsonCfg;
            NJson::TJsonValue json;
            NJson::ReadJsonTree(response, &jsonCfg, &json, /* throwOnError = */ true);
            auto resultSets = json["result"].GetArray();
            UNIT_ASSERT_EQUAL_C(1, resultSets.size(), response);
        }
    }

    Y_UNIT_TEST(FloatPointJsonQuery) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 monPort = tp.GetPort(8765);
        auto settings = TServerSettings(port);
        settings.InitKikimrRunConfig()
                .SetNodeCount(1)
                .SetUseRealThreads(true)
                .SetDomainName("Root")
                .SetUseSectorMap(true)
                .SetMonitoringPortOffset(monPort, true);

        TServer server(settings);
        server.EnableGRpc(grpcPort);
        TClient client(settings);

        TTestActorRuntime& runtime = *server.GetRuntime();
        runtime.SetLogPriority(NKikimrServices::GRPC_SERVER, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);

        TKeepAliveHttpClient httpClient("localhost", monPort);
        TStringStream responseStream;
        TKeepAliveHttpClient::THeaders headers;
        headers["Content-Type"] = "application/json";
        headers["Authorization"] = "test_ydb_token";
        TString requestBody = R"json({
            "query": "SELECT cast('311111111113.222222223' as Double);",
            "database": "/Root",
            "action": "execute-script",
            "syntax": "yql_v1",
            "stats": "none"
        })json";
        const TKeepAliveHttpClient::THttpCode statusCode = httpClient.DoPost("/viewer/query?timeout=600000&base64=false&schema=modern", requestBody, &responseStream, headers);
        const TString response = responseStream.ReadAll();
        UNIT_ASSERT_EQUAL_C(statusCode, HTTP_OK, statusCode << ": " << response);
        {
            NJson::TJsonReaderConfig jsonCfg;

            NJson::TJsonValue json;
            NJson::ReadJsonTree(response, &jsonCfg, &json, /* throwOnError = */ true);

            auto resultSets = json["result"].GetArray();
            UNIT_ASSERT_EQUAL_C(1, resultSets.size(), response);

            double parsed = resultSets.begin()->GetArray().begin()->GetDouble();
            double expected = 311111111113.22222;
            UNIT_ASSERT_DOUBLES_EQUAL(parsed, expected, 0.00001);
        }
    }

    Y_UNIT_TEST(AuthorizeYdbTokenWithDatabaseAttributes) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 monPort = tp.GetPort(8765);
        auto settings = TServerSettings(port);
        settings.InitKikimrRunConfig()
                .SetNodeCount(1)
                .SetUseRealThreads(true)
                .SetDomainName("Root")
                .SetUseSectorMap(true)
                .SetMonitoringPortOffset(monPort, true); // authorization is implemented only in async mon

        auto& securityConfig = *settings.AppConfig->MutableDomainsConfig()->MutableSecurityConfig();
        securityConfig.SetEnforceUserTokenCheckRequirement(true);

        TFakeTicketParserActor* ticketParser = nullptr;
        settings.CreateTicketParser = [&](const TTicketParserSettings&) -> IActor* {
            ticketParser = new TFakeTicketParserActor();
            return ticketParser;
        };

        TServer server(settings);
        server.EnableGRpc(grpcPort);
        TClient client(settings);

        const auto alterAttrsStatus = client.AlterUserAttributes("/", "Root", {
            { "folder_id", "test_folder_id" },
            { "database_id", "test_database_id" },
        });
        UNIT_ASSERT_EQUAL(alterAttrsStatus, NMsgBusProxy::MSTATUS_OK);

        TTestActorRuntime& runtime = *server.GetRuntime();
        runtime.SetLogPriority(NKikimrServices::GRPC_SERVER, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);

        TKeepAliveHttpClient httpClient("localhost", monPort);
        TStringStream responseStream;
        TKeepAliveHttpClient::THeaders headers;
        headers["Content-Type"] = "application/json";
        headers["Authorization"] = "test_ydb_token";
        TString requestBody = R"json({
            "query": "SELECT 42;",
            "database": "/Root",
            "action": "execute-script",
            "syntax": "yql_v1",
            "stats": "profile"
        })json";
        const TKeepAliveHttpClient::THttpCode statusCode = httpClient.DoPost("/viewer/query?timeout=600000&base64=false&schema=modern", requestBody, &responseStream, headers);
        const TString response = responseStream.ReadAll();
        UNIT_ASSERT_EQUAL_C(statusCode, HTTP_OK, statusCode << ": " << response);

        UNIT_ASSERT(ticketParser);
        UNIT_ASSERT_VALUES_EQUAL_C(ticketParser->AuthorizeTicketRequests, 1, response);
        UNIT_ASSERT_VALUES_EQUAL_C(ticketParser->AuthorizeTicketSuccesses, 1, response);
    }

    Y_UNIT_TEST(SimpleFeatureFlags) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 monPort = tp.GetPort(8765);
        auto settings = TServerSettings(port);

        settings.InitKikimrRunConfig()
                .SetNodeCount(1)
                .SetUseRealThreads(true)
                .SetDomainName("Root")
                .SetUseSectorMap(true)
                .SetMonitoringPortOffset(monPort, true);

        TServer server(settings);
        server.EnableGRpc(grpcPort);
        TClient client(settings);

        TKeepAliveHttpClient httpClient("localhost", monPort);
        TStringStream responseStream;
        TKeepAliveHttpClient::THeaders headers;
        headers["Content-Type"] = "application/json";
        const TKeepAliveHttpClient::THttpCode statusCode = httpClient.DoGet("/viewer/feature_flags?timeout=600000&base64=false", &responseStream, headers);
        const TString response = responseStream.ReadAll();
        UNIT_ASSERT_EQUAL_C(statusCode, HTTP_OK, statusCode << ": " << response);
        NJson::TJsonReaderConfig jsonCfg;
        NJson::TJsonValue json;
        NJson::ReadJsonTree(response, &jsonCfg, &json, /* throwOnError = */ true);
        auto resultSets = json["Databases"].GetArray();
        UNIT_ASSERT_EQUAL_C(1, resultSets.size(), response);
    }
}
