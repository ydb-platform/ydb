#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include "health_check.cpp"

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
    int const VCARD_START_ID = 124;

    void ChangeDescribeSchemeResult(TEvSchemeShard::TEvDescribeSchemeResult::TPtr* ev, ui64 size = 20000000, ui64 quota = 90000000) {
        auto record = (*ev)->Get()->MutableRecord();
        auto pool = record->mutable_pathdescription()->mutable_domaindescription()->add_storagepools();
        pool->set_name("/Root:test");
        pool->set_kind("kind");

        auto domain = record->mutable_pathdescription()->mutable_domaindescription();
        domain->mutable_diskspaceusage()->mutable_tables()->set_totalsize(size);
        if (quota == 0) {
            domain->clear_databasequotas();
        } else {
            domain->mutable_databasequotas()->set_data_size_hard_quota(quota);
        }
    }

    void AddGroupsInControllerSelectGroupsResult(TEvBlobStorage::TEvControllerSelectGroupsResult::TPtr* ev,  int groupCount) {
        auto& pbRecord = (*ev)->Get()->Record;
        auto pbMatchGroups = pbRecord.mutable_matchinggroups(0);

        auto sample = pbMatchGroups->groups(0);
        pbMatchGroups->ClearGroups();

        auto groupId = GROUP_START_ID;
        for (int i = 0; i < groupCount; i++) {
            auto group = pbMatchGroups->add_groups();
            group->CopyFrom(sample);
            group->set_groupid(groupId++);
        }
    };

    void AddGroupVSlotInControllerConfigResponse(TEvBlobStorage::TEvControllerConfigResponse::TPtr* ev, int groupCount, int vslotCount, TString erasurespecies = NHealthCheck::TSelfCheckRequest::BLOCK_4_2) {
        auto& pbRecord = (*ev)->Get()->Record;
        auto pbConfig = pbRecord.mutable_response()->mutable_status(0)->mutable_baseconfig();

        auto groupSample = pbConfig->group(0);
        auto vslotSample = pbConfig->vslot(0);
        auto vslotIdSample = pbConfig->group(0).vslotid(0);
        pbConfig->clear_group();
        pbConfig->clear_vslot();
        pbConfig->clear_pdisk();

        auto groupId = GROUP_START_ID;
        auto vslotId = VCARD_START_ID;
        for (int i = 0; i < groupCount; i++) {

            auto group = pbConfig->add_group();
            group->CopyFrom(groupSample);
            group->set_groupid(groupId);
            group->set_erasurespecies(erasurespecies);

            group->clear_vslotid();
            for (int j = 0; j < vslotCount; j++) {
                auto vslot = pbConfig->add_vslot();
                vslot->CopyFrom(vslotSample);
                vslot->set_vdiskidx(vslotId);
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
        auto& pbRecord = (*ev)->Get()->Record;

        auto sample = pbRecord.vdiskstateinfo(0);
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

    void AddVSlotInVDiskStateResponse(NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateResponse::TPtr* ev, const TVector<Ydb::Monitoring::StatusFlag::Status>& vdiskStatuses) {
        auto& pbRecord = (*ev)->Get()->Record;

        auto sample = pbRecord.vdiskstateinfo(0);
        pbRecord.clear_vdiskstateinfo();

        auto groupId = GROUP_START_ID;
        auto vslotId = VCARD_START_ID;

        for (auto status: vdiskStatuses) {
            switch (status) {
            case Ydb::Monitoring::StatusFlag::RED: {
                auto state = pbRecord.add_vdiskstateinfo();
                state->CopyFrom(sample);
                state->mutable_vdiskid()->set_vdisk(vslotId++);
                state->mutable_vdiskid()->set_groupid(groupId); 
                state->set_pdiskid(100);
                state->set_vdiskstate(NKikimrWhiteboard::EVDiskState::PDiskError);
                break;
            }
            case Ydb::Monitoring::StatusFlag::BLUE: {
                auto state = pbRecord.add_vdiskstateinfo();
                state->CopyFrom(sample);
                state->mutable_vdiskid()->set_vdisk(vslotId++);
                state->mutable_vdiskid()->set_groupid(groupId); 
                state->set_pdiskid(100);
                state->set_vdiskstate(NKikimrWhiteboard::EVDiskState::OK);
                state->set_replicated(false);
                break;
            }
            case Ydb::Monitoring::StatusFlag::YELLOW: {
                auto state = pbRecord.add_vdiskstateinfo();
                state->CopyFrom(sample);
                state->mutable_vdiskid()->set_vdisk(vslotId++);
                state->mutable_vdiskid()->set_groupid(groupId); 
                state->set_pdiskid(100);
                state->set_vdiskstate(NKikimrWhiteboard::EVDiskState::SyncGuidRecovery);
                break;
            }
            default:
                break;
            }
        }
    }

    void ListingTest(int const groupNumber, int const vdiscPerGroupNumber) {
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
        TTestActorRuntime& runtime = *server.GetRuntime();

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
                    AddGroupVSlotInControllerConfigResponse(x, groupNumber, vdiscPerGroupNumber);
                    break;
                }
                case NNodeWhiteboard::TEvWhiteboard::EvVDiskStateResponse: {
                    auto *x = reinterpret_cast<NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateResponse::TPtr*>(&ev);
                    AddVSlotInVDiskStateResponse(x, groupNumber, vdiscPerGroupNumber);
                    break;
                }
            }
            
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        auto *request = new NHealthCheck::TEvSelfCheckRequest;
        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, request, 0));
        NHealthCheck::TEvSelfCheckResult* result = runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle);

        int groupIssuesCount = 0;
        int groupIssuesNumber = groupNumber <= NHealthCheck::TSelfCheckRequest::MERGING_IGNORE_SIZE ? groupNumber : 1;
        for (const auto& issue_log : result->Result.Getissue_log()) {
            if (issue_log.type() == "STORAGE_GROUP" && issue_log.location().storage().pool().name() == "/Root:test") {
                if (groupNumber <= NHealthCheck::TSelfCheckRequest::MERGING_IGNORE_SIZE) {
                    UNIT_ASSERT_VALUES_EQUAL(issue_log.location().storage().pool().group().id_size(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(issue_log.listed(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(issue_log.count(), 0);
                } else {
                    int groupListed = std::min<int>(groupNumber, (int)NHealthCheck::TSelfCheckRequest::MERGER_ISSUE_LIMIT);
                    UNIT_ASSERT_VALUES_EQUAL(issue_log.location().storage().pool().group().id_size(), groupListed);
                    UNIT_ASSERT_VALUES_EQUAL(issue_log.listed(), groupListed);
                    UNIT_ASSERT_VALUES_EQUAL(issue_log.count(), groupNumber);
                }
                groupIssuesCount++;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(groupIssuesCount, groupIssuesNumber);

        int vdiscCountField = groupNumber <= NHealthCheck::TSelfCheckRequest::MERGING_IGNORE_SIZE ? vdiscPerGroupNumber : groupNumber * vdiscPerGroupNumber;
        int issueVdiscPerGroupNumber = vdiscCountField <= NHealthCheck::TSelfCheckRequest::MERGING_IGNORE_SIZE ? vdiscCountField : 1;
        int issueVdiscNumber = groupIssuesNumber * issueVdiscPerGroupNumber;

        int issueVdiscCount = 0;
        for (const auto& issue_log : result->Result.issue_log()) {
            if (issue_log.type() == "VDISK" && issue_log.location().storage().pool().name() == "/Root:test") {
                issueVdiscCount++;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(issueVdiscCount, issueVdiscNumber);
    }

    void CheckGroupStatusDependsOnVdisks(TString erasurespecies, const Ydb::Monitoring::StatusFlag::Status expectiongGroupStatus, const TVector<Ydb::Monitoring::StatusFlag::Status>& vdiskStatuses) {
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
        TTestActorRuntime& runtime = *server.GetRuntime();

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
                    AddGroupsInControllerSelectGroupsResult(x, 1);
                    break;
                }
                case TEvBlobStorage::EvControllerConfigResponse: {
                    auto *x = reinterpret_cast<TEvBlobStorage::TEvControllerConfigResponse::TPtr*>(&ev);
                    AddGroupVSlotInControllerConfigResponse(x, 1, vdiskStatuses.size(), erasurespecies);
                    break;
                }
                case NNodeWhiteboard::TEvWhiteboard::EvVDiskStateResponse: {
                    auto *x = reinterpret_cast<NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateResponse::TPtr*>(&ev);
                    AddVSlotInVDiskStateResponse(x, vdiskStatuses);
                    break;
                }
                case NNodeWhiteboard::TEvWhiteboard::EvPDiskStateResponse: {
                    auto *x = reinterpret_cast<NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateResponse::TPtr*>(&ev);
                    (*x)->Get()->Record.clear_pdiskstateinfo();
                    break;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        auto *request = new NHealthCheck::TEvSelfCheckRequest;
        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, request, 0));
        NHealthCheck::TEvSelfCheckResult* result = runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle);
        int groupIssuesCount = 0;
        for (const auto& issue_log : result->Result.Getissue_log()) {
            if (issue_log.type() == "STORAGE_GROUP" && issue_log.location().storage().pool().name() == "/Root:test") {
                UNIT_ASSERT_VALUES_EQUAL((int)issue_log.status(), (int)expectiongGroupStatus);
                groupIssuesCount++;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(groupIssuesCount, 1);
    }

    void ChangeUsageDescribeSchemeResult(TEvSchemeShard::TEvDescribeSchemeResult::TPtr* ev, ui64 size, ui64 quota) {
        auto record = (*ev)->Get()->MutableRecord();
        auto pool = record->mutable_pathdescription()->mutable_domaindescription()->add_storagepools();
        pool->set_name("/Root:test");
        pool->set_kind("kind");

        auto domain = record->mutable_pathdescription()->mutable_domaindescription();
        domain->mutable_diskspaceusage()->mutable_tables()->set_totalsize(size);
        domain->mutable_databasequotas()->set_data_stream_reserved_storage_quota(quota);
    }

     void StorageTest(ui64 usage, ui64 quota, ui64 storageIssuesNumber, Ydb::Monitoring::StatusFlag::Status status = Ydb::Monitoring::StatusFlag::GREEN) {
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
        TTestActorRuntime& runtime = *server.GetRuntime();

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        auto observerFunc = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvSchemeShard::EvDescribeSchemeResult: {
                    auto *x = reinterpret_cast<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr*>(&ev);
                    ChangeDescribeSchemeResult(x, usage, quota);
                    break;
                }
            }
            
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        auto *request = new NHealthCheck::TEvSelfCheckRequest;
        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, request, 0));
        NHealthCheck::TEvSelfCheckResult* result = runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle);
        
        int storageIssuesCount = 0;
        for (const auto& issue_log : result->Result.Getissue_log()) {
            if (issue_log.type() == "STORAGE" && issue_log.reason_size() == 0 && issue_log.status() == status) {
                storageIssuesCount++;
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(storageIssuesCount, storageIssuesNumber);
    }

    Y_UNIT_TEST(IssuesGroupsListing) {
        int groupNumber = NHealthCheck::TSelfCheckRequest::MERGING_IGNORE_SIZE;
        ListingTest(groupNumber, 1);
    }

    Y_UNIT_TEST(IssuesVCardListing) {
        int vcardNumber = NHealthCheck::TSelfCheckRequest::MERGING_IGNORE_SIZE;
        ListingTest(1, vcardNumber);
    }

    Y_UNIT_TEST(IssuesGroupsVCardListing) {
        int groupNumber = NHealthCheck::TSelfCheckRequest::MERGING_IGNORE_SIZE;
        int vcardNumber = NHealthCheck::TSelfCheckRequest::MERGING_IGNORE_SIZE;
        ListingTest(groupNumber, vcardNumber);
    }

    Y_UNIT_TEST(IssuesGroupsMerging) {
        int groupNumber = NHealthCheck::TSelfCheckRequest::MERGER_ISSUE_LIMIT;
        ListingTest(groupNumber, 1);
    }

    Y_UNIT_TEST(IssuesVCardMerging) {
        int vcardNumber = NHealthCheck::TSelfCheckRequest::MERGER_ISSUE_LIMIT;
        ListingTest(1, vcardNumber);
    }

    Y_UNIT_TEST(IssuesGroupsVCardMerging) {
        int groupNumber = NHealthCheck::TSelfCheckRequest::MERGER_ISSUE_LIMIT;
        int vcardNumber = NHealthCheck::TSelfCheckRequest::MERGER_ISSUE_LIMIT;
        ListingTest(groupNumber, vcardNumber);
    }

    Y_UNIT_TEST(IssuesGroupsDeleting) {
        ListingTest(100, 1);
    }

    Y_UNIT_TEST(IssuesVCardDeleting) {
        ListingTest(1, 100);
    }

    Y_UNIT_TEST(IssuesGroupsVCardDeleting) {
        ListingTest(100, 100);
    }
    
    Y_UNIT_TEST(NoneRedGroupWhenRedVdisk) {
        CheckGroupStatusDependsOnVdisks(NHealthCheck::TSelfCheckRequest::NONE, Ydb::Monitoring::StatusFlag::RED, {Ydb::Monitoring::StatusFlag::RED});
    }
    
    Y_UNIT_TEST(NoneRedGroupWhenBlueVdisk) {
        CheckGroupStatusDependsOnVdisks(NHealthCheck::TSelfCheckRequest::NONE, Ydb::Monitoring::StatusFlag::RED, {Ydb::Monitoring::StatusFlag::BLUE});
    }
    
    Y_UNIT_TEST(NoneYellowGroupWhenYellowVdisk) {
        CheckGroupStatusDependsOnVdisks(NHealthCheck::TSelfCheckRequest::NONE, Ydb::Monitoring::StatusFlag::YELLOW, {Ydb::Monitoring::StatusFlag::YELLOW});
    }
    
    Y_UNIT_TEST(Block42RedGroupWhen3RedVdisks) {
        CheckGroupStatusDependsOnVdisks(NHealthCheck::TSelfCheckRequest::BLOCK_4_2, Ydb::Monitoring::StatusFlag::RED, {Ydb::Monitoring::StatusFlag::RED, Ydb::Monitoring::StatusFlag::RED, Ydb::Monitoring::StatusFlag::RED});
    }
    
    Y_UNIT_TEST(Block42RedGroupWhen2RedBlueVdisks) {
        CheckGroupStatusDependsOnVdisks(NHealthCheck::TSelfCheckRequest::BLOCK_4_2, Ydb::Monitoring::StatusFlag::RED, {Ydb::Monitoring::StatusFlag::RED, Ydb::Monitoring::StatusFlag::RED, Ydb::Monitoring::StatusFlag::BLUE});
    }
    
    Y_UNIT_TEST(Block42OrangeGroupWhen2RedVdisks) {
        CheckGroupStatusDependsOnVdisks(NHealthCheck::TSelfCheckRequest::BLOCK_4_2, Ydb::Monitoring::StatusFlag::ORANGE, {Ydb::Monitoring::StatusFlag::RED, Ydb::Monitoring::StatusFlag::RED});
    }
    
    Y_UNIT_TEST(Block42OrangeGroupWhenRedBlueVdisks) {
        CheckGroupStatusDependsOnVdisks(NHealthCheck::TSelfCheckRequest::BLOCK_4_2, Ydb::Monitoring::StatusFlag::ORANGE, {Ydb::Monitoring::StatusFlag::RED, Ydb::Monitoring::StatusFlag::BLUE});
    }
    
    Y_UNIT_TEST(Block42YellowGroupWhenRedVdisk) {
        CheckGroupStatusDependsOnVdisks(NHealthCheck::TSelfCheckRequest::BLOCK_4_2, Ydb::Monitoring::StatusFlag::YELLOW, {Ydb::Monitoring::StatusFlag::RED});
    }
    
    Y_UNIT_TEST(Block42BlueGroupWhenBlueVdisk) {
        CheckGroupStatusDependsOnVdisks(NHealthCheck::TSelfCheckRequest::BLOCK_4_2, Ydb::Monitoring::StatusFlag::BLUE, {Ydb::Monitoring::StatusFlag::BLUE});
    }
    
    Y_UNIT_TEST(Block42YellowGroupWhenYellowVdisk) {
        CheckGroupStatusDependsOnVdisks(NHealthCheck::TSelfCheckRequest::BLOCK_4_2, Ydb::Monitoring::StatusFlag::YELLOW, {Ydb::Monitoring::StatusFlag::YELLOW});
    }
    
    Y_UNIT_TEST(Mirrot3dcYellowGroupWhen3RedVdisks) {
        CheckGroupStatusDependsOnVdisks(NHealthCheck::TSelfCheckRequest::MIRROR_3_DC, Ydb::Monitoring::StatusFlag::YELLOW, {Ydb::Monitoring::StatusFlag::RED, Ydb::Monitoring::StatusFlag::RED, Ydb::Monitoring::StatusFlag::RED});
    }
    
    Y_UNIT_TEST(Mirrot3dcYellowGroupWhen2RedBlueVdisks) {
        CheckGroupStatusDependsOnVdisks(NHealthCheck::TSelfCheckRequest::MIRROR_3_DC, Ydb::Monitoring::StatusFlag::YELLOW, {Ydb::Monitoring::StatusFlag::RED, Ydb::Monitoring::StatusFlag::RED, Ydb::Monitoring::StatusFlag::BLUE});
    }
    
    Y_UNIT_TEST(Mirrot3dcYellowGroupWhen2RedVdisks) {
        CheckGroupStatusDependsOnVdisks(NHealthCheck::TSelfCheckRequest::MIRROR_3_DC, Ydb::Monitoring::StatusFlag::YELLOW, {Ydb::Monitoring::StatusFlag::RED, Ydb::Monitoring::StatusFlag::RED});
    }
    
    Y_UNIT_TEST(Mirrot3dcYellowGroupWhenRedBlueVdisks) {
        CheckGroupStatusDependsOnVdisks(NHealthCheck::TSelfCheckRequest::MIRROR_3_DC, Ydb::Monitoring::StatusFlag::YELLOW, {Ydb::Monitoring::StatusFlag::RED, Ydb::Monitoring::StatusFlag::BLUE});
    }
    
    Y_UNIT_TEST(Mirrot3dcYellowGroupWhenRedVdisk) {
        CheckGroupStatusDependsOnVdisks(NHealthCheck::TSelfCheckRequest::MIRROR_3_DC, Ydb::Monitoring::StatusFlag::YELLOW, {Ydb::Monitoring::StatusFlag::RED});
    }
    
    Y_UNIT_TEST(Mirrot3dcBlueGroupWhenBlueVdisk) {
        CheckGroupStatusDependsOnVdisks(NHealthCheck::TSelfCheckRequest::MIRROR_3_DC, Ydb::Monitoring::StatusFlag::BLUE, {Ydb::Monitoring::StatusFlag::BLUE});
    }
    
    Y_UNIT_TEST(Mirrot3dcYellowGroupWhenYellowVdisk) {
        CheckGroupStatusDependsOnVdisks(NHealthCheck::TSelfCheckRequest::MIRROR_3_DC, Ydb::Monitoring::StatusFlag::YELLOW, {Ydb::Monitoring::StatusFlag::YELLOW});
    }

    Y_UNIT_TEST(StorageLimit95) {
        StorageTest(95, 100, 1, Ydb::Monitoring::StatusFlag::RED);
    }

    Y_UNIT_TEST(StorageLimit87) {
        StorageTest(87, 100, 1, Ydb::Monitoring::StatusFlag::ORANGE);
    }

    Y_UNIT_TEST(StorageLimit80) {
        StorageTest(80, 100, 1, Ydb::Monitoring::StatusFlag::YELLOW);
    }

    Y_UNIT_TEST(StorageLimit50) {
        StorageTest(50, 100, 0, Ydb::Monitoring::StatusFlag::GREEN);
    }

    Y_UNIT_TEST(StorageNoQuota) {
        StorageTest(100, 0, 0, Ydb::Monitoring::StatusFlag::GREEN);
    }
}

}
