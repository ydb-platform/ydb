#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include "health_check.h"

#include <unordered_map>

namespace NKikimr {

using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(THealthCheckTest) {
    void BasicTest(IEventBase* ev) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);

        auto settings = TServerSettings(port);
        settings.SetDomainName("Root");
        TServer server(settings);
        server.EnableGRpc(grpcPort);

        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();

        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        runtime->Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, ev, 0));
        NHealthCheck::TEvSelfCheckResult* result = runtime->GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle);

        UNIT_ASSERT(result != nullptr);
    }

    Y_UNIT_TEST(Basic) {
        BasicTest(new NHealthCheck::TEvSelfCheckRequest());
    }

    Y_UNIT_TEST(BasicNodeCheckRequest) {
        BasicTest(new NHealthCheck::TEvNodeCheckRequest());
    }

    int const GROUP_START_ID = 1200;
    int const VCARD_START_ID = 5500;

    void ChangeDescribeSchemeResult(TEvSchemeShard::TEvDescribeSchemeResult::TPtr* ev) {
        auto pool = (*ev)->Get()->MutableRecord()->mutable_pathdescription()->mutable_domaindescription()->add_storagepools();
        pool->set_name("/Root:test");
        pool->set_kind("kind");
    };

    void AddGroupsInControllerSelectGroupsResult(TEvBlobStorage::TEvControllerSelectGroupsResult::TPtr* ev,  int groupCount) {
        auto &pbRecord = (*ev)->Get()->Record;
        auto pbMatchGroups = pbRecord.mutable_matchinggroups(0);

        auto sample = pbMatchGroups->GetGroups(0);
        pbMatchGroups->ClearGroups();

        auto groupId = GROUP_START_ID;
        for (int i = 0; i < groupCount; i++) {
            auto group = pbMatchGroups->add_groups();
            group->CopyFrom(sample);
            group->SetGroupID(groupId++);
        }
    };

    void AddGroupVSlotInControllerConfigResponse(TEvBlobStorage::TEvControllerConfigResponse::TPtr* ev, int groupCount, int vslotCount) {
        auto &pbRecord = (*ev)->Get()->Record;
        auto pbConfig = pbRecord.mutable_response()->mutable_status(0)->mutable_baseconfig();

        auto groupSample = pbConfig->GetGroup(0);
        auto vslotSample = pbConfig->GetVSlot(0);
        auto vslotIdSample = pbConfig->GetGroup(0).GetVSlotId(0);
        pbConfig->clear_group();
        pbConfig->clear_vslot();

        auto groupId = GROUP_START_ID;
        auto vslotId = VCARD_START_ID;
        for (int i = 0; i < groupCount; i++) {

            auto group = pbConfig->add_group();
            group->CopyFrom(groupSample);
            group->set_groupid(groupId);

            group->clear_vslotid();
            for (int j = 0; j < vslotCount; j++) {
                auto vslot = pbConfig->add_vslot();
                vslot->CopyFrom(vslotSample);
                vslot->SetVDiskIdx(vslotId);
                vslot->set_groupid(groupId);
                vslot->mutable_vslotid()->set_vslotid(vslotId);

                auto slotId = group->add_vslotid();
                slotId->CopyFrom(vslotIdSample);
                slotId->set_vslotid(vslotId);

                vslotId++;
            }
            groupId++;
        }
    };

    void AddVSlotInVDiskStateResponse(NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateResponse::TPtr* ev, int groupCount, int vslotCount) {
        auto &pbRecord = (*ev)->Get()->Record;

        auto sample = pbRecord.GetVDiskStateInfo(0);
        pbRecord.clear_vdiskstateinfo();

        auto groupId = GROUP_START_ID;
        auto slotId = VCARD_START_ID;
        for (int i = 0; i < groupCount; i++) {
            for (int j = 0; j < vslotCount; j++) {
                auto state = pbRecord.add_vdiskstateinfo();
                state->CopyFrom(sample);
                state->mutable_vdiskid()->set_vdisk(slotId++);
                state->mutable_vdiskid()->set_groupid(groupId);
            }
            groupId++;
        }
    }

    void ListingTest(int const groupNumber, int const groupVdiscNumber) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port)
                .SetNodeCount(2)
                .SetUseRealThreads(false)
                .SetDomainName("Root");
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        TClient client(settings);
        TTestActorRuntime &runtime = *server.GetRuntime();

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        auto observerFunc = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvSchemeShard::EvDescribeSchemeResult: {
                    auto *x = reinterpret_cast<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr*>(&ev);
                    ChangeDescribeSchemeResult(x);
                    break;
                }
                case TEvBlobStorage::EvControllerSelectGroupsResult: {
                    auto *x = reinterpret_cast<TEvBlobStorage::TEvControllerSelectGroupsResult::TPtr*>(&ev);
                    AddGroupsInControllerSelectGroupsResult(x, groupNumber);
                    break;
                }
                case TEvBlobStorage::EvControllerConfigResponse: {
                    auto *x = reinterpret_cast<TEvBlobStorage::TEvControllerConfigResponse::TPtr*>(&ev);
                    AddGroupVSlotInControllerConfigResponse(x, groupNumber, groupVdiscNumber);
                    break;
                }
                case NNodeWhiteboard::TEvWhiteboard::EvVDiskStateResponse: {
                    auto *x = reinterpret_cast<NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateResponse::TPtr*>(&ev);
                    AddVSlotInVDiskStateResponse(x, groupNumber, groupVdiscNumber);
                    break;
                }
            }
            
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        auto *request = new NHealthCheck::TEvSelfCheckRequest;
        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, request, 0));
        NHealthCheck::TEvSelfCheckResult* result = runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle);

        int groupCount = 0;
        for (const auto& issue_log : result->Result.Getissue_log()) {
            if (issue_log.Gettype() == "STORAGE_GROUP" && issue_log.Getlocation().Getstorage().Getpool().Getname() == "/Root:test") {
                groupCount++;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(groupCount, groupNumber);

        int vdiscCount = 0;
        for (const auto& issue_log : result->Result.Getissue_log()) {
            if (issue_log.Gettype() == "VDISK" && issue_log.Getlocation().Getstorage().Getpool().Getname() == "/Root:test") {
                vdiscCount++;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(vdiscCount, groupNumber * groupVdiscNumber);
    }

    Y_UNIT_TEST(GroupsListing) {
        ListingTest(15, 1);
    }

    Y_UNIT_TEST(VCardListing) {
        ListingTest(1, 20);
    }

    Y_UNIT_TEST(GroupsVCardListing) {
        ListingTest(15, 20);
    }
}

}
