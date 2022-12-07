#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/actors/interconnect/interconnect.h>
#include <library/cpp/json/json_reader.h>
#include <util/stream/null.h>
#include <ydb/core/viewer/protos/viewer.pb.h>
#include "json_handlers.h"
#include "json_tabletinfo.h"
#include "json_vdiskinfo.h"
#include "json_pdiskinfo.h"

using namespace NKikimr;
using namespace NViewer;
using namespace NKikimrWhiteboard;

#ifdef NDEBUG
#define Ctest Cnull
#else
#define Ctest Cerr
#endif

Y_UNIT_TEST_SUITE(Viewer) {
    Y_UNIT_TEST(TabletMerging) {
        THPTimer timer;
        {
            TMap<ui32, TString> nodesBlob;
            timer.Reset();
            for (ui32 nodeId = 1; nodeId <= 10000; ++nodeId) {
                THolder<TEvWhiteboard::TEvTabletStateResponse> nodeData = MakeHolder<TEvWhiteboard::TEvTabletStateResponse>();
                nodeData->Record.MutableTabletStateInfo()->Reserve(10000);
                for (ui32 tabletId = 1; tabletId <= 10000; ++tabletId) {
                    NKikimrWhiteboard::TTabletStateInfo* tabletData = nodeData->Record.AddTabletStateInfo();
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
                nodesBlob[nodeId] = nodeData->Record.SerializeAsString();
            }
            Ctest << "Build = " << timer.Passed() << Endl;
            timer.Reset();
            TMap<ui32, THolder<TEvWhiteboard::TEvTabletStateResponse>> nodesData;
            for (const auto& [nodeId, nodeBlob] : nodesBlob) {
                THolder<TEvWhiteboard::TEvTabletStateResponse> nodeData = MakeHolder<TEvWhiteboard::TEvTabletStateResponse>();
                bool res = nodeData->Record.ParseFromString(nodesBlob[nodeId]);
                Y_UNUSED(res);
                nodesData[nodeId] = std::move(nodeData);
            }
            THolder<TEvWhiteboard::TEvTabletStateResponse> result = MergeWhiteboardResponses(nodesData);
            Ctest << "Merge = " << timer.Passed() << Endl;
            UNIT_ASSERT_LT(timer.Passed(), 30);
            UNIT_ASSERT_VALUES_EQUAL(result->Record.TabletStateInfoSize(), 10000);
            timer.Reset();
        }
        Ctest << "Destroy = " << timer.Passed() << Endl;
    }

    Y_UNIT_TEST(TabletMergingPacked) {
        THPTimer timer;
        {
            TMap<ui32, TString> nodesBlob;
            timer.Reset();
            for (ui32 nodeId = 1; nodeId <= 10000; ++nodeId) {
                THolder<TEvWhiteboard::TEvTabletStateResponse> nodeData = MakeHolder<TEvWhiteboard::TEvTabletStateResponse>();
                auto* tabletData = nodeData->AllocatePackedResponse(10000);
                for (ui32 tabletId = 1; tabletId <= 10000; ++tabletId) {
                    tabletData->TabletId = tabletId;
                    tabletData->FollowerId = 0;
                    tabletData->Generation = 13;
                    //tabletData->SetChangeTime(TInstant::Now().MilliSeconds());
                    ++tabletData;
                }
                nodesBlob[nodeId] = nodeData->Record.SerializeAsString();
            }
            Ctest << "Build = " << timer.Passed() << Endl;
            TMap<ui32, THolder<TEvWhiteboard::TEvTabletStateResponse>> nodesData;
            for (const auto& [nodeId, nodeBlob] : nodesBlob) {
                THolder<TEvWhiteboard::TEvTabletStateResponse> nodeData = MakeHolder<TEvWhiteboard::TEvTabletStateResponse>();
                bool res = nodeData->Record.ParseFromString(nodesBlob[nodeId]);
                Y_UNUSED(res);
                nodesData[nodeId] = std::move(nodeData);
            }
            THolder<TEvWhiteboard::TEvTabletStateResponse> result = MergeWhiteboardResponses(nodesData);
            Ctest << "Merge = " << timer.Passed() << Endl;
            UNIT_ASSERT_LT(timer.Passed(), 10);
            UNIT_ASSERT_VALUES_EQUAL(result->Record.TabletStateInfoSize(), 10000);
            timer.Reset();
        }
        Ctest << "Destroy = " << timer.Passed() << Endl;
    }

    Y_UNIT_TEST(VDiskMerging) {
        TMap<ui32, THolder<TEvWhiteboard::TEvVDiskStateResponse>> nodesData;
        for (ui32 nodeId = 1; nodeId <= 1000; ++nodeId) {
            THolder<TEvWhiteboard::TEvVDiskStateResponse>& nodeData = nodesData[nodeId] = MakeHolder<TEvWhiteboard::TEvVDiskStateResponse>();
            nodeData->Record.MutableVDiskStateInfo()->Reserve(10);
            for (ui32 vDiskId = 1; vDiskId <= 1000; ++vDiskId) {
                NKikimrWhiteboard::TVDiskStateInfo* vDiskData = nodeData->Record.AddVDiskStateInfo();
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
        THolder<TEvWhiteboard::TEvVDiskStateResponse> result = MergeWhiteboardResponses(nodesData);
        Ctest << "Merge = " << timer.Passed() << Endl;
        UNIT_ASSERT_LT(timer.Passed(), 10);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.VDiskStateInfoSize(), 1000);
        Ctest << "Data has merged" << Endl;
    }

    Y_UNIT_TEST(PDiskMerging) {
        TMap<ui32, THolder<TEvWhiteboard::TEvPDiskStateResponse>> nodesData;
        for (ui32 nodeId = 1; nodeId <= 1000; ++nodeId) {
            THolder<TEvWhiteboard::TEvPDiskStateResponse>& nodeData = nodesData[nodeId] = MakeHolder<TEvWhiteboard::TEvPDiskStateResponse>();
            nodeData->Record.MutablePDiskStateInfo()->Reserve(10);
            for (ui32 pDiskId = 1; pDiskId <= 100; ++pDiskId) {
                NKikimrWhiteboard::TPDiskStateInfo* pDiskData = nodeData->Record.AddPDiskStateInfo();
                pDiskData->SetPDiskId(pDiskId);
                pDiskData->SetAvailableSize(100);
                pDiskData->SetChangeTime(TInstant::Now().MilliSeconds());
            }
        }
        Ctest << "Data has built" << Endl;
        THPTimer timer;
        THolder<TEvWhiteboard::TEvPDiskStateResponse> result = MergeWhiteboardResponses(nodesData);
        Ctest << "Merge = " << timer.Passed() << Endl;
        UNIT_ASSERT_LT(timer.Passed(), 10);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.PDiskStateInfoSize(), 100000);
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
}
