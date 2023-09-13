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
    int const VCARD_START_ID = 55;

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
        for (int i = 0; i < groupCount; i++) {

            auto group = pbConfig->add_group();
            group->CopyFrom(groupSample);
            group->set_groupid(groupId);
            group->set_erasurespecies(erasurespecies);

            group->clear_vslotid();
            auto vslotId = VCARD_START_ID;
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
        for (int i = 0; i < groupCount; i++) {
            auto slotId = VCARD_START_ID;
            for (int j = 0; j < vslotCount; j++) {
                auto state = pbRecord.add_vdiskstateinfo();
                state->CopyFrom(sample);
                state->mutable_vdiskid()->set_vdisk(slotId++);
                state->mutable_vdiskid()->set_groupid(groupId);
                state->set_vdiskstate(NKikimrWhiteboard::EVDiskState::SyncGuidRecovery);
            }
            groupId++;
        }
    }

    void AddVSlotInVDiskStateResponse(NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateResponse::TPtr* ev, const TVector<Ydb::Monitoring::StatusFlag::Status>& vdiskStatuses) {
        auto& pbRecord = (*ev)->Get()->Record;

        auto sample = pbRecord.vdiskstateinfo(0);
        pbRecord.clear_vdiskstateinfo();

        auto groupId = GROUP_START_ID;
        auto vslotId = VCARD_START_ID; // vslotId have to be less than 256

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

    Ydb::Monitoring::SelfCheckResult RequestHc(int const groupNumber, int const vdiscPerGroupNumber, bool const isMergeRecords = false, int const recordsLimit = 0) {
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
        request->Request.set_merge_records(isMergeRecords);
        request->Request.set_records_limit(recordsLimit);
        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, request, 0));
        return runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;
    }

    void CheckHcResult(Ydb::Monitoring::SelfCheckResult& result, int const groupNumber, int const vdiscPerGroupNumber, bool const isMergeRecords = false, int const recordsLimit = 0) {
        int groupIssuesCount = 0;
        int groupIssuesNumber = !isMergeRecords ? groupNumber : 1;
        for (const auto& issue_log : result.Getissue_log()) {
            if (issue_log.type() == "STORAGE_GROUP" && issue_log.location().storage().pool().name() == "/Root:test") {
                if (!isMergeRecords || groupNumber == 1) {
                    UNIT_ASSERT_VALUES_EQUAL(issue_log.location().storage().pool().group().id_size(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(issue_log.listed(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(issue_log.count(), 0);
                } else {
                    int groupListed = recordsLimit == 0 ? groupNumber : std::min<int>(groupNumber, recordsLimit);
                    UNIT_ASSERT_VALUES_EQUAL(issue_log.location().storage().pool().group().id_size(), groupListed);
                    UNIT_ASSERT_VALUES_EQUAL(issue_log.listed(), groupListed);
                    UNIT_ASSERT_VALUES_EQUAL(issue_log.count(), groupNumber);
                }
                groupIssuesCount++;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(groupIssuesCount, groupIssuesNumber);

        int issueVdiscNumber = !isMergeRecords ? vdiscPerGroupNumber : 1;

        int issueVdiscCount = 0;
        for (const auto& issue_log : result.issue_log()) {
            if (issue_log.type() == "VDISK" && issue_log.location().storage().pool().name() == "/Root:test") {
                issueVdiscCount++;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(issueVdiscCount, issueVdiscNumber);
    }

    void ListingTest(int const groupNumber, int const vdiscPerGroupNumber, bool const isMergeRecords = false, int const recordsLimit = 0) {
        auto result = RequestHc(groupNumber, vdiscPerGroupNumber, isMergeRecords, recordsLimit);
        CheckHcResult(result, groupNumber, vdiscPerGroupNumber, isMergeRecords, recordsLimit);
    }

    Ydb::Monitoring::SelfCheckResult RequestHcWithVdisks(TString erasurespecies, const TVector<Ydb::Monitoring::StatusFlag::Status>& vdiskStatuses) {
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
        request->Request.set_merge_records(true);
        request->Request.set_records_limit(10);

        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, request, 0));
        return runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;
    }

    void CheckHcResultHasIssuesWithStatus(Ydb::Monitoring::SelfCheckResult& result, const TString& type, const Ydb::Monitoring::StatusFlag::Status expectingStatus, ui32 total) {
        int issuesCount = 0;
        for (const auto& issue_log : result.Getissue_log()) {
            if (issue_log.type() == type && issue_log.location().storage().pool().name() == "/Root:test" && issue_log.status() == expectingStatus) {
                issuesCount++;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(issuesCount, total);
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

    Y_UNIT_TEST(OneIssueListing) {
        ListingTest(1, 1);
    }

    Y_UNIT_TEST(Issues100GroupsListing) {
        ListingTest(100, 1);
    }

    Y_UNIT_TEST(Issues100VCardListing) {
        ListingTest(1, 100);
    }

    Y_UNIT_TEST(Issues100Groups100VCardListing) {
        ListingTest(5, 5);
    }

    Y_UNIT_TEST(Issues100GroupsMerging) {
        ListingTest(100, 1, true);
    }

    Y_UNIT_TEST(Issues100VCardMerging) {
        ListingTest(1, 100, true);
    }

    Y_UNIT_TEST(Issues100Groups100VCardMerging) {
        ListingTest(100, 100, true);
    }

    Y_UNIT_TEST(Issues100GroupsMergingAndLimiting) {
        ListingTest(100, 1, true, 10);
    }

    Y_UNIT_TEST(Issues100VCardMergingAndLimiting) {
        ListingTest(1, 100, true, 10);
    }

    Y_UNIT_TEST(Issues100Groups100VCardMergingAndLimiting) {
        ListingTest(100, 100, true, 10);
    }

    Y_UNIT_TEST(NoneRedGroupWhenRedVdisk) {
        auto result = RequestHcWithVdisks(NHealthCheck::TSelfCheckRequest::NONE, {Ydb::Monitoring::StatusFlag::RED});
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 1);
    }

    Y_UNIT_TEST(NoneRedGroupWhenBlueVdisk) {
        auto result = RequestHcWithVdisks(NHealthCheck::TSelfCheckRequest::NONE, {Ydb::Monitoring::StatusFlag::BLUE});
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 1);
    }

    Y_UNIT_TEST(NoneYellowGroupWhenYellowVdisk) {
        auto result = RequestHcWithVdisks(NHealthCheck::TSelfCheckRequest::NONE, {Ydb::Monitoring::StatusFlag::YELLOW});
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::YELLOW, 1);
    }

    Y_UNIT_TEST(Block42OrangeGroupWhen100YellowAnd2RedVdisks) {
        TVector<Ydb::Monitoring::StatusFlag::Status> vdiskStatuses(100, Ydb::Monitoring::StatusFlag::YELLOW);
        vdiskStatuses.emplace_back(Ydb::Monitoring::StatusFlag::RED);
        vdiskStatuses.emplace_back(Ydb::Monitoring::StatusFlag::RED);
        auto result = RequestHcWithVdisks(NHealthCheck::TSelfCheckRequest::BLOCK_4_2, vdiskStatuses);
        
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::ORANGE, 1);
        CheckHcResultHasIssuesWithStatus(result, "VDISK", Ydb::Monitoring::StatusFlag::RED, 1);
    }

    Y_UNIT_TEST(Block42RedGroupWhen3RedVdisks) {
        auto result = RequestHcWithVdisks(NHealthCheck::TSelfCheckRequest::BLOCK_4_2, {Ydb::Monitoring::StatusFlag::RED, Ydb::Monitoring::StatusFlag::RED, Ydb::Monitoring::StatusFlag::RED});
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 1);
    }

    Y_UNIT_TEST(Block42RedGroupWhen2RedBlueVdisks) {
        auto result = RequestHcWithVdisks(NHealthCheck::TSelfCheckRequest::BLOCK_4_2, {Ydb::Monitoring::StatusFlag::RED, Ydb::Monitoring::StatusFlag::RED, Ydb::Monitoring::StatusFlag::BLUE});
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 1);
    }

    Y_UNIT_TEST(Block42OrangeGroupWhen2RedVdisks) {
        auto result = RequestHcWithVdisks(NHealthCheck::TSelfCheckRequest::BLOCK_4_2, {Ydb::Monitoring::StatusFlag::RED, Ydb::Monitoring::StatusFlag::RED});
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::ORANGE, 1);
    }

    Y_UNIT_TEST(Block42OrangeGroupWhenRedBlueVdisks) {
        auto result = RequestHcWithVdisks(NHealthCheck::TSelfCheckRequest::BLOCK_4_2, {Ydb::Monitoring::StatusFlag::RED, Ydb::Monitoring::StatusFlag::BLUE});
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::ORANGE, 1);
    }

    Y_UNIT_TEST(Block42YellowGroupWhenRedVdisk) {
        auto result = RequestHcWithVdisks(NHealthCheck::TSelfCheckRequest::BLOCK_4_2, {Ydb::Monitoring::StatusFlag::RED});
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::YELLOW, 1);
    }

    Y_UNIT_TEST(Block42BlueGroupWhenBlueVdisk) {
        auto result = RequestHcWithVdisks(NHealthCheck::TSelfCheckRequest::BLOCK_4_2, {Ydb::Monitoring::StatusFlag::BLUE});
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::BLUE, 1);
    }

    Y_UNIT_TEST(Block42YellowGroupWhenYellowVdisk) {
        auto result = RequestHcWithVdisks(NHealthCheck::TSelfCheckRequest::BLOCK_4_2, {Ydb::Monitoring::StatusFlag::YELLOW});
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::YELLOW, 1);
    }

    Y_UNIT_TEST(Mirrot3dcYellowGroupWhen3RedVdisks) {
        auto result = RequestHcWithVdisks(NHealthCheck::TSelfCheckRequest::MIRROR_3_DC, {Ydb::Monitoring::StatusFlag::RED, Ydb::Monitoring::StatusFlag::RED, Ydb::Monitoring::StatusFlag::RED});
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::YELLOW, 1);
    }

    Y_UNIT_TEST(Mirrot3dcYellowGroupWhen2RedBlueVdisks) {
        auto result = RequestHcWithVdisks(NHealthCheck::TSelfCheckRequest::MIRROR_3_DC, {Ydb::Monitoring::StatusFlag::RED, Ydb::Monitoring::StatusFlag::RED, Ydb::Monitoring::StatusFlag::BLUE});
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::YELLOW, 1);
    }

    Y_UNIT_TEST(Mirrot3dcYellowGroupWhen2RedVdisks) {
        auto result = RequestHcWithVdisks(NHealthCheck::TSelfCheckRequest::MIRROR_3_DC, {Ydb::Monitoring::StatusFlag::RED, Ydb::Monitoring::StatusFlag::RED});
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::YELLOW, 1);
    }

    Y_UNIT_TEST(Mirrot3dcYellowGroupWhenRedBlueVdisks) {
        auto result = RequestHcWithVdisks(NHealthCheck::TSelfCheckRequest::MIRROR_3_DC, {Ydb::Monitoring::StatusFlag::RED, Ydb::Monitoring::StatusFlag::BLUE});
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::YELLOW, 1);
    }

    Y_UNIT_TEST(Mirrot3dcYellowGroupWhenRedVdisk) {
        auto result = RequestHcWithVdisks(NHealthCheck::TSelfCheckRequest::MIRROR_3_DC, {Ydb::Monitoring::StatusFlag::RED});
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::YELLOW, 1);
    }

    Y_UNIT_TEST(Mirrot3dcBlueGroupWhenBlueVdisk) {
        auto result = RequestHcWithVdisks(NHealthCheck::TSelfCheckRequest::MIRROR_3_DC, {Ydb::Monitoring::StatusFlag::BLUE});
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::BLUE, 1);
    }

    Y_UNIT_TEST(Mirrot3dcYellowGroupWhenYellowVdisk) {
        auto result = RequestHcWithVdisks(NHealthCheck::TSelfCheckRequest::MIRROR_3_DC, {Ydb::Monitoring::StatusFlag::YELLOW});
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::YELLOW, 1);
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

    void CheckHcProtobufSize(Ydb::Monitoring::SelfCheckResult& result, const Ydb::Monitoring::StatusFlag::Status expectingStatus, ui32 total) {
        int issuesCount = 0;
        for (const auto& issue_log : result.Getissue_log()) {
            if (issue_log.type() == "HEALTHCHECK_STATUS" && issue_log.status() == expectingStatus) {
                issuesCount++;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(issuesCount, total);
    }

    Y_UNIT_TEST(ProtobufUnderLimit50Mb) {
        auto result = RequestHc(3000, 1);
        CheckHcProtobufSize(result, Ydb::Monitoring::StatusFlag::RED, 0);
        CheckHcProtobufSize(result, Ydb::Monitoring::StatusFlag::ORANGE, 0);
        CheckHcProtobufSize(result, Ydb::Monitoring::StatusFlag::YELLOW, 0);
    }

    Y_UNIT_TEST(ProtobufBeyondLimit50Mb) {
        auto result = RequestHc(350000, 1);
        CheckHcProtobufSize(result, Ydb::Monitoring::StatusFlag::RED, 1);
    }
}

}
