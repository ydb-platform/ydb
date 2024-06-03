#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include "health_check.cpp"

#include <util/stream/null.h>

namespace NKikimr {

using namespace NSchemeShard;
using namespace Tests;
using namespace NSchemeCache;
using namespace NNodeWhiteboard;

#ifdef NDEBUG
#define Ctest Cnull
#else
#define Ctest Cerr
#endif

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

    const int GROUP_START_ID = 0x80000000;
    const int VCARD_START_ID = 55;

    const TPathId SUBDOMAIN_KEY = {7000000000, 1};
    const TPathId SERVERLESS_DOMAIN_KEY = {7000000000, 2};
    const TPathId SHARED_DOMAIN_KEY = {7000000000, 3};
    const TString STORAGE_POOL_NAME = "/Root:test";

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

    void AddGroupVSlotInControllerConfigResponse(TEvBlobStorage::TEvControllerConfigResponse::TPtr* ev, const int groupCount, const int vslotCount) {
        auto& pbRecord = (*ev)->Get()->Record;
        auto pbConfig = pbRecord.mutable_response()->mutable_status(0)->mutable_baseconfig();

        auto groupSample = pbConfig->group(0);
        auto vslotSample = pbConfig->vslot(0);
        auto vslotIdSample = pbConfig->group(0).vslotid(0);
        pbConfig->clear_group();
        pbConfig->clear_vslot();
        for (auto& pdisk: *pbConfig->mutable_pdisk()) {
            pdisk.mutable_pdiskmetrics()->set_state(NKikimrBlobStorage::TPDiskState::Normal);
        }

        auto groupId = GROUP_START_ID;
        for (int i = 0; i < groupCount; i++) {

            auto group = pbConfig->add_group();
            group->CopyFrom(groupSample);
            group->set_groupid(groupId);
            group->set_erasurespecies(NHealthCheck::TSelfCheckRequest::BLOCK_4_2);
            group->set_operatingstatus(NKikimrBlobStorage::TGroupStatus::DEGRADED);

            group->clear_vslotid();
            auto vslotId = VCARD_START_ID;
            for (int j = 0; j < vslotCount; j++) {
                auto vslot = pbConfig->add_vslot();
                vslot->CopyFrom(vslotSample);
                vslot->set_vdiskidx(vslotId);
                vslot->set_groupid(groupId);
                vslot->set_failrealmidx(vslotId);
                vslot->set_status("ERROR");
                vslot->mutable_vslotid()->set_vslotid(vslotId);

                auto slotId = group->add_vslotid();
                slotId->CopyFrom(vslotIdSample);
                slotId->set_vslotid(vslotId);

                vslotId++;
            }
            groupId++;
        }
    };

    void AddGroupVSlotInControllerConfigResponseWithStaticGroup(TEvBlobStorage::TEvControllerConfigResponse::TPtr* ev,
        const NKikimrBlobStorage::TGroupStatus::E groupStatus, const TVector<NKikimrBlobStorage::EVDiskStatus>& vdiskStatuses)
    {
        auto& pbRecord = (*ev)->Get()->Record;
        auto pbConfig = pbRecord.mutable_response()->mutable_status(0)->mutable_baseconfig();

        auto groupSample = pbConfig->group(0);
        auto vslotSample = pbConfig->vslot(0);
        auto vslotIdSample = pbConfig->group(0).vslotid(0);

        for (auto& pdisk: *pbConfig->mutable_pdisk()) {
            pdisk.mutable_pdiskmetrics()->set_state(NKikimrBlobStorage::TPDiskState::Normal);
        }

        pbConfig->clear_group();

        auto staticGroup = pbConfig->add_group();
        staticGroup->CopyFrom(groupSample);
        staticGroup->set_groupid(0);
        staticGroup->set_storagepoolid(0);
        staticGroup->set_operatingstatus(groupStatus);
        staticGroup->set_erasurespecies(NHealthCheck::TSelfCheckRequest::BLOCK_4_2);

        auto group = pbConfig->add_group();
        group->CopyFrom(groupSample);
        group->set_groupid(GROUP_START_ID);
        group->set_storagepoolid(1);
        group->set_operatingstatus(groupStatus);
        group->set_erasurespecies(NHealthCheck::TSelfCheckRequest::BLOCK_4_2);

        group->clear_vslotid();
        auto vslotId = VCARD_START_ID;

        for (auto status: vdiskStatuses) {
            auto vslot = pbConfig->add_vslot();
            vslot->CopyFrom(vslotSample);
            vslot->set_vdiskidx(vslotId);
            vslot->set_groupid(GROUP_START_ID);
            vslot->set_failrealmidx(vslotId);
            vslot->mutable_vslotid()->set_vslotid(vslotId);

            auto slotId = group->add_vslotid();
            slotId->CopyFrom(vslotIdSample);
            slotId->set_vslotid(vslotId);

            const auto *descriptor = NKikimrBlobStorage::EVDiskStatus_descriptor();
            vslot->set_status(descriptor->FindValueByNumber(status)->name());

            vslotId++;
        }

        auto spStatus = pbRecord.mutable_response()->mutable_status(1);
        spStatus->clear_storagepool();
        auto sPool = spStatus->add_storagepool();
        sPool->set_storagepoolid(1);
        sPool->set_name(STORAGE_POOL_NAME);
    };

    void AddVSlotInVDiskStateResponse(TEvWhiteboard::TEvVDiskStateResponse::TPtr* ev, int groupCount, int vslotCount) {
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
                state->set_nodeid(1);
            }
            groupId++;
        }
    }

    void SetLongHostValue(TEvInterconnect::TEvNodesInfo::TPtr* ev) {
        TString host(1000000, 'a');
        auto& pbRecord = (*ev)->Get()->Nodes;
        for (auto itIssue = pbRecord.begin(); itIssue != pbRecord.end(); ++itIssue) {
            itIssue->Host = host;
        }
    }

    Ydb::Monitoring::SelfCheckResult RequestHc(int const groupNumber, int const vdiscPerGroupNumber, bool const isMergeRecords = false, bool const largeSizeVdisksIssues = false) {
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

        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
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
                case TEvInterconnect::EvNodesInfo: {
                    if (largeSizeVdisksIssues) {
                        auto *x = reinterpret_cast<TEvInterconnect::TEvNodesInfo::TPtr*>(&ev);
                        SetLongHostValue(x);
                    }
                    break;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        auto *request = new NHealthCheck::TEvSelfCheckRequest;
        request->Request.set_merge_records(isMergeRecords);
        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, request, 0));
        return runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;
    }

    void CheckHcResult(Ydb::Monitoring::SelfCheckResult& result, int const groupNumber, int const vdiscPerGroupNumber, bool const isMergeRecords = false) {
        int groupIssuesCount = 0;
        int groupIssuesNumber = !isMergeRecords ? groupNumber : 1;
        for (const auto& issue_log : result.Getissue_log()) {
            if (issue_log.type() == "STORAGE_GROUP" && issue_log.location().storage().pool().name() == "/Root:test") {
                if (!isMergeRecords || groupNumber == 1) {
                    UNIT_ASSERT_VALUES_EQUAL(issue_log.location().storage().pool().group().id_size(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(issue_log.listed(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(issue_log.count(), 0);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(issue_log.location().storage().pool().group().id_size(), groupNumber);
                    UNIT_ASSERT_VALUES_EQUAL(issue_log.listed(), groupNumber);
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

    void ListingTest(int const groupNumber, int const vdiscPerGroupNumber, bool const isMergeRecords = false) {
        auto result = RequestHc(groupNumber, vdiscPerGroupNumber, isMergeRecords);
        CheckHcResult(result, groupNumber, vdiscPerGroupNumber, isMergeRecords);
    }

    Ydb::Monitoring::SelfCheckResult RequestHcWithVdisks(const NKikimrBlobStorage::TGroupStatus::E groupStatus, const TVector<NKikimrBlobStorage::EVDiskStatus>& vdiskStatuses) {
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

        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
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
                    AddGroupVSlotInControllerConfigResponseWithStaticGroup(x, groupStatus, vdiskStatuses);
                    break;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        auto *request = new NHealthCheck::TEvSelfCheckRequest;
        request->Request.set_merge_records(true);

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

        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
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

    Y_UNIT_TEST(YellowroupIssueWhenPartialGroupStatus) {
        auto result = RequestHcWithVdisks(NKikimrBlobStorage::TGroupStatus::PARTIAL, {NKikimrBlobStorage::ERROR});
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::YELLOW, 1);
    }

    Y_UNIT_TEST(BlueGroupIssueWhenPartialGroupStatusAndReplicationDisks) {
        auto result = RequestHcWithVdisks(NKikimrBlobStorage::TGroupStatus::PARTIAL, {NKikimrBlobStorage::REPLICATING});
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::BLUE, 1);
    }

    Y_UNIT_TEST(OrangeGroupIssueWhenDegradedGroupStatus) {
        auto result = RequestHcWithVdisks(NKikimrBlobStorage::TGroupStatus::DEGRADED, {});
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::ORANGE, 1);
    }

    Y_UNIT_TEST(RedGroupIssueWhenDisintegratedGroupStatus) {
        auto result = RequestHcWithVdisks(NKikimrBlobStorage::TGroupStatus::DISINTEGRATED, {});
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 1);
    }

    Y_UNIT_TEST(RedGroupIssueWhenUnknownGroupStatus) {
        auto result = RequestHcWithVdisks(NKikimrBlobStorage::TGroupStatus::UNKNOWN, {});
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 1);
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

    void CheckHcProtobufSizeIssue(Ydb::Monitoring::SelfCheckResult& result, const Ydb::Monitoring::StatusFlag::Status expectingStatus, ui32 total) {
        int issuesCount = 0;
        for (const auto& issue_log : result.Getissue_log()) {
            if (issue_log.type() == "HEALTHCHECK_STATUS" && issue_log.status() == expectingStatus) {
                issuesCount++;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(issuesCount, total);
        UNIT_ASSERT_LT(result.ByteSizeLong(), 50_MB);
    }

    Y_UNIT_TEST(ProtobufBelowLimitFor10VdisksIssues) {
        auto result = RequestHc(1, 100);
        CheckHcProtobufSizeIssue(result, Ydb::Monitoring::StatusFlag::YELLOW, 0);
        CheckHcProtobufSizeIssue(result, Ydb::Monitoring::StatusFlag::ORANGE, 0);
        CheckHcProtobufSizeIssue(result, Ydb::Monitoring::StatusFlag::RED, 0);
    }

    Y_UNIT_TEST(ProtobufUnderLimitFor70LargeVdisksIssues) {
        auto result = RequestHc(1, 70, false, true);
        CheckHcProtobufSizeIssue(result, Ydb::Monitoring::StatusFlag::RED, 1);
    }

    Y_UNIT_TEST(ProtobufUnderLimitFor100LargeVdisksIssues) {
        auto result = RequestHc(1, 100, false, true);
        CheckHcProtobufSizeIssue(result, Ydb::Monitoring::StatusFlag::RED, 1);
    }

    void ClearLoadAverage(TEvWhiteboard::TEvSystemStateResponse::TPtr* ev) {
        auto *systemStateInfo = (*ev)->Get()->Record.MutableSystemStateInfo();
        for (NKikimrWhiteboard::TSystemStateInfo &state : *systemStateInfo) {
            auto &loadAverage = *state.MutableLoadAverage();
            for (int i = 0; i < loadAverage.size(); ++i) {
                loadAverage[i] = 0;
            }
        }
    }

    void ChangeNavigateKeyResultServerless(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr* ev,
        NKikimrSubDomains::EServerlessComputeResourcesMode serverlessComputeResourcesMode,
        TTestActorRuntime& runtime)
    {
        TSchemeCacheNavigate::TEntry& entry((*ev)->Get()->Request->ResultSet.front());
        TString path = CanonizePath(entry.Path);
        if (path == "/Root/serverless" || entry.TableId.PathId == SERVERLESS_DOMAIN_KEY) {
            entry.Status = TSchemeCacheNavigate::EStatus::Ok;
            entry.Kind = TSchemeCacheNavigate::EKind::KindExtSubdomain;
            entry.Path = {"Root", "serverless"};
            entry.DomainInfo = MakeIntrusive<TDomainInfo>(SERVERLESS_DOMAIN_KEY, SHARED_DOMAIN_KEY);
            entry.DomainInfo->ServerlessComputeResourcesMode = serverlessComputeResourcesMode;
        } else if (path == "/Root/shared" || entry.TableId.PathId == SHARED_DOMAIN_KEY) {
            entry.Status = TSchemeCacheNavigate::EStatus::Ok;
            entry.Kind = TSchemeCacheNavigate::EKind::KindExtSubdomain;
            entry.Path = {"Root", "shared"};
            entry.DomainInfo = MakeIntrusive<TDomainInfo>(SHARED_DOMAIN_KEY, SHARED_DOMAIN_KEY);
            auto domains = runtime.GetAppData().DomainsInfo;
            ui64 hiveId = domains->GetHive();
            entry.DomainInfo->Params.SetHive(hiveId);
        }
    }

    void ChangeDescribeSchemeResultServerless(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr* ev) {
        auto record = (*ev)->Get()->MutableRecord();
        if (record->path() == "/Root/serverless" || record->path() == "/Root/shared") {
            record->set_status(NKikimrScheme::StatusSuccess);
            auto pool = record->mutable_pathdescription()->mutable_domaindescription()->add_storagepools();
            pool->set_name(STORAGE_POOL_NAME);
            pool->set_kind("kind");
        }
    }

    void ChangeGetTenantStatusResponse(NConsole::TEvConsole::TEvGetTenantStatusResponse::TPtr* ev, const TString &path) {
        auto *operation = (*ev)->Get()->Record.MutableResponse()->mutable_operation();
        Ydb::Cms::GetDatabaseStatusResult getTenantStatusResult;
        getTenantStatusResult.set_path(path);
        operation->mutable_result()->PackFrom(getTenantStatusResult);
        operation->set_status(Ydb::StatusIds::SUCCESS);
    }

    void AddPathsToListTenantsResponse(NConsole::TEvConsole::TEvListTenantsResponse::TPtr* ev, const TVector<TString> &paths) {
        Ydb::Cms::ListDatabasesResult listTenantsResult;
        (*ev)->Get()->Record.GetResponse().operation().result().UnpackTo(&listTenantsResult);
        for (const auto &path : paths) {
            listTenantsResult.Addpaths(path);
        }
        (*ev)->Get()->Record.MutableResponse()->mutable_operation()->mutable_result()->PackFrom(listTenantsResult);
    }

    void ChangeResponseHiveNodeStats(TEvHive::TEvResponseHiveNodeStats::TPtr* ev, ui32 sharedDynNodeId = 0,
        ui32 exclusiveDynNodeId = 0)
    {
        auto &record = (*ev)->Get()->Record;
        if (sharedDynNodeId) {
            auto *sharedNodeStats = record.MutableNodeStats()->Add();
            sharedNodeStats->SetNodeId(sharedDynNodeId);
            sharedNodeStats->MutableNodeDomain()->SetSchemeShard(SHARED_DOMAIN_KEY.OwnerId);
            sharedNodeStats->MutableNodeDomain()->SetPathId(SHARED_DOMAIN_KEY.LocalPathId);
        }

        if (exclusiveDynNodeId) {
            auto *exclusiveNodeStats = record.MutableNodeStats()->Add();
            exclusiveNodeStats->SetNodeId(exclusiveDynNodeId);
            exclusiveNodeStats->MutableNodeDomain()->SetSchemeShard(SERVERLESS_DOMAIN_KEY.OwnerId);
            exclusiveNodeStats->MutableNodeDomain()->SetPathId(SERVERLESS_DOMAIN_KEY.LocalPathId);
        }
    }

    Y_UNIT_TEST(SpecificServerless) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port)
                .SetNodeCount(1)
                .SetDynamicNodeCount(1)
                .SetUseRealThreads(false)
                .SetDomainName("Root");
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        TClient client(settings);
        TTestActorRuntime& runtime = *server.GetRuntime();

        auto &dynamicNameserviceConfig = runtime.GetAppData().DynamicNameserviceConfig;
        dynamicNameserviceConfig->MaxStaticNodeId = runtime.GetNodeId(server.StaticNodes() - 1);
        dynamicNameserviceConfig->MinDynamicNodeId = runtime.GetNodeId(server.StaticNodes());
        dynamicNameserviceConfig->MaxDynamicNodeId = runtime.GetNodeId(server.StaticNodes() + server.DynamicNodes() - 1);

        ui32 sharedDynNodeId = runtime.GetNodeId(1);

        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case NConsole::TEvConsole::EvGetTenantStatusResponse: {
                    auto *x = reinterpret_cast<NConsole::TEvConsole::TEvGetTenantStatusResponse::TPtr*>(&ev);
                    ChangeGetTenantStatusResponse(x, "/Root/serverless");
                    break;
                }
                case TEvTxProxySchemeCache::EvNavigateKeySetResult: {
                    auto *x = reinterpret_cast<TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr*>(&ev);
                    ChangeNavigateKeyResultServerless(x, NKikimrSubDomains::EServerlessComputeResourcesModeShared, runtime);
                    break;
                }
                case TEvHive::EvResponseHiveNodeStats: {
                    auto *x = reinterpret_cast<TEvHive::TEvResponseHiveNodeStats::TPtr*>(&ev);
                    ChangeResponseHiveNodeStats(x, sharedDynNodeId);
                    break;
                }
                case TEvSchemeShard::EvDescribeSchemeResult: {
                    auto *x = reinterpret_cast<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr*>(&ev);
                    ChangeDescribeSchemeResultServerless(x);
                    break;
                }
                case TEvBlobStorage::EvControllerConfigResponse: {
                    auto *x = reinterpret_cast<TEvBlobStorage::TEvControllerConfigResponse::TPtr*>(&ev);
                    TVector<NKikimrBlobStorage::EVDiskStatus> vdiskStatuses = { NKikimrBlobStorage::EVDiskStatus::READY };
                    AddGroupVSlotInControllerConfigResponseWithStaticGroup(x, NKikimrBlobStorage::TGroupStatus::FULL, vdiskStatuses);
                    break;
                }
                case TEvWhiteboard::EvSystemStateResponse: {
                    auto *x = reinterpret_cast<TEvWhiteboard::TEvSystemStateResponse::TPtr*>(&ev);
                    ClearLoadAverage(x);
                    break;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        auto *request = new NHealthCheck::TEvSelfCheckRequest;
        request->Request.set_return_verbose_status(true);
        request->Database = "/Root/serverless";
        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, request, 0));
        const auto result = runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;
        Ctest << result.ShortDebugString();
        UNIT_ASSERT_VALUES_EQUAL(result.self_check_result(), Ydb::Monitoring::SelfCheck::GOOD);

        UNIT_ASSERT_VALUES_EQUAL(result.database_status_size(), 1);
        const auto &database_status = result.database_status(0);
        UNIT_ASSERT_VALUES_EQUAL(database_status.name(), "/Root/serverless");
        UNIT_ASSERT_VALUES_EQUAL(database_status.overall(), Ydb::Monitoring::StatusFlag::GREEN);

        UNIT_ASSERT_VALUES_EQUAL(database_status.compute().overall(), Ydb::Monitoring::StatusFlag::GREEN);
        UNIT_ASSERT_VALUES_EQUAL(database_status.compute().nodes().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(database_status.compute().nodes()[0].id(), ToString(sharedDynNodeId));

        UNIT_ASSERT_VALUES_EQUAL(database_status.storage().overall(), Ydb::Monitoring::StatusFlag::GREEN);
        UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools()[0].id(), STORAGE_POOL_NAME);
    }

    Y_UNIT_TEST(SpecificServerlessWithExclusiveNodes) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port)
                .SetNodeCount(1)
                .SetDynamicNodeCount(2)
                .SetUseRealThreads(false)
                .SetDomainName("Root");
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        TClient client(settings);
        TTestActorRuntime& runtime = *server.GetRuntime();

        auto &dynamicNameserviceConfig = runtime.GetAppData().DynamicNameserviceConfig;
        dynamicNameserviceConfig->MaxStaticNodeId = runtime.GetNodeId(server.StaticNodes() - 1);
        dynamicNameserviceConfig->MinDynamicNodeId = runtime.GetNodeId(server.StaticNodes());
        dynamicNameserviceConfig->MaxDynamicNodeId = runtime.GetNodeId(server.StaticNodes() + server.DynamicNodes() - 1);

        ui32 sharedDynNodeId = runtime.GetNodeId(1);
        ui32 exclusiveDynNodeId = runtime.GetNodeId(2);

        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case NConsole::TEvConsole::EvGetTenantStatusResponse: {
                    auto *x = reinterpret_cast<NConsole::TEvConsole::TEvGetTenantStatusResponse::TPtr*>(&ev);
                    ChangeGetTenantStatusResponse(x, "/Root/serverless");
                    break;
                }
                case TEvTxProxySchemeCache::EvNavigateKeySetResult: {
                    auto *x = reinterpret_cast<TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr*>(&ev);
                    ChangeNavigateKeyResultServerless(x, NKikimrSubDomains::EServerlessComputeResourcesModeExclusive, runtime);
                    break;
                }
                case TEvHive::EvResponseHiveNodeStats: {
                    auto *x = reinterpret_cast<TEvHive::TEvResponseHiveNodeStats::TPtr*>(&ev);
                    ChangeResponseHiveNodeStats(x, sharedDynNodeId, exclusiveDynNodeId);
                    break;
                }
                case TEvSchemeShard::EvDescribeSchemeResult: {
                    auto *x = reinterpret_cast<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr*>(&ev);
                    ChangeDescribeSchemeResultServerless(x);
                    break;
                }
                case TEvBlobStorage::EvControllerConfigResponse: {
                    auto *x = reinterpret_cast<TEvBlobStorage::TEvControllerConfigResponse::TPtr*>(&ev);
                    TVector<NKikimrBlobStorage::EVDiskStatus> vdiskStatuses = { NKikimrBlobStorage::EVDiskStatus::READY };
                    AddGroupVSlotInControllerConfigResponseWithStaticGroup(x, NKikimrBlobStorage::TGroupStatus::FULL, vdiskStatuses);
                    break;
                }
                case TEvWhiteboard::EvSystemStateResponse: {
                    auto *x = reinterpret_cast<TEvWhiteboard::TEvSystemStateResponse::TPtr*>(&ev);
                    ClearLoadAverage(x);
                    break;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        auto *request = new NHealthCheck::TEvSelfCheckRequest;
        request->Request.set_return_verbose_status(true);
        request->Database = "/Root/serverless";
        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, request, 0));
        const auto result = runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;

        Ctest << result.ShortDebugString();
        UNIT_ASSERT_VALUES_EQUAL(result.self_check_result(), Ydb::Monitoring::SelfCheck::GOOD);

        UNIT_ASSERT_VALUES_EQUAL(result.database_status_size(), 1);
        const auto &database_status = result.database_status(0);
        UNIT_ASSERT_VALUES_EQUAL(database_status.name(), "/Root/serverless");
        UNIT_ASSERT_VALUES_EQUAL(database_status.overall(), Ydb::Monitoring::StatusFlag::GREEN);

        UNIT_ASSERT_VALUES_EQUAL(database_status.compute().overall(), Ydb::Monitoring::StatusFlag::GREEN);
        UNIT_ASSERT_VALUES_EQUAL(database_status.compute().nodes().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(database_status.compute().nodes()[0].id(), ToString(exclusiveDynNodeId));

        UNIT_ASSERT_VALUES_EQUAL(database_status.storage().overall(), Ydb::Monitoring::StatusFlag::GREEN);
        UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools()[0].id(), STORAGE_POOL_NAME);
    }

    Y_UNIT_TEST(IgnoreServerlessWhenNotSpecific) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port)
                .SetNodeCount(1)
                .SetDynamicNodeCount(1)
                .SetUseRealThreads(false)
                .SetDomainName("Root");
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        TClient client(settings);
        TTestActorRuntime& runtime = *server.GetRuntime();

        auto &dynamicNameserviceConfig = runtime.GetAppData().DynamicNameserviceConfig;
        dynamicNameserviceConfig->MaxStaticNodeId = runtime.GetNodeId(server.StaticNodes() - 1);
        dynamicNameserviceConfig->MinDynamicNodeId = runtime.GetNodeId(server.StaticNodes());
        dynamicNameserviceConfig->MaxDynamicNodeId = runtime.GetNodeId(server.StaticNodes() + server.DynamicNodes() - 1);

        ui32 sharedDynNodeId = runtime.GetNodeId(1);

        bool firstConsoleResponse = true;
        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                 case NConsole::TEvConsole::EvListTenantsResponse: {
                    auto *x = reinterpret_cast<NConsole::TEvConsole::TEvListTenantsResponse::TPtr*>(&ev);
                    AddPathsToListTenantsResponse(x, { "/Root/serverless", "/Root/shared" });
                    break;
                }
                case NConsole::TEvConsole::EvGetTenantStatusResponse: {
                    auto *x = reinterpret_cast<NConsole::TEvConsole::TEvGetTenantStatusResponse::TPtr*>(&ev);
                    if (firstConsoleResponse) {
                        firstConsoleResponse = false;
                        ChangeGetTenantStatusResponse(x, "/Root/serverless");
                    } else {
                        ChangeGetTenantStatusResponse(x, "/Root/shared");
                    }
                    break;
                }
                case TEvTxProxySchemeCache::EvNavigateKeySetResult: {
                    auto *x = reinterpret_cast<TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr*>(&ev);
                    ChangeNavigateKeyResultServerless(x, NKikimrSubDomains::EServerlessComputeResourcesModeShared, runtime);
                    break;
                }
                case TEvHive::EvResponseHiveNodeStats: {
                    auto *x = reinterpret_cast<TEvHive::TEvResponseHiveNodeStats::TPtr*>(&ev);
                    ChangeResponseHiveNodeStats(x, sharedDynNodeId);
                    break;
                }
                case TEvSchemeShard::EvDescribeSchemeResult: {
                    auto *x = reinterpret_cast<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr*>(&ev);
                    ChangeDescribeSchemeResultServerless(x);
                    break;
                }
                case TEvBlobStorage::EvControllerConfigResponse: {
                    auto *x = reinterpret_cast<TEvBlobStorage::TEvControllerConfigResponse::TPtr*>(&ev);
                    TVector<NKikimrBlobStorage::EVDiskStatus> vdiskStatuses = { NKikimrBlobStorage::EVDiskStatus::READY };
                    AddGroupVSlotInControllerConfigResponseWithStaticGroup(x, NKikimrBlobStorage::TGroupStatus::FULL, vdiskStatuses);
                    break;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        auto *request = new NHealthCheck::TEvSelfCheckRequest;
        request->Request.set_return_verbose_status(true);
        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, request, 0));
        const auto result = runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;
        Ctest << result.ShortDebugString();
        UNIT_ASSERT_VALUES_EQUAL(result.self_check_result(), Ydb::Monitoring::SelfCheck::GOOD);

        bool databaseFoundInResult = false;
        for (const auto &database_status : result.database_status()) {
            if (database_status.name() == "/Root/serverless") {
                databaseFoundInResult = true;
            }
        }
        UNIT_ASSERT(!databaseFoundInResult);
    }

    Y_UNIT_TEST(DontIgnoreServerlessWithExclusiveNodesWhenNotSpecific) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port)
                .SetNodeCount(1)
                .SetDynamicNodeCount(2)
                .SetUseRealThreads(false)
                .SetDomainName("Root");
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        TClient client(settings);
        TTestActorRuntime& runtime = *server.GetRuntime();

        auto &dynamicNameserviceConfig = runtime.GetAppData().DynamicNameserviceConfig;
        dynamicNameserviceConfig->MaxStaticNodeId = runtime.GetNodeId(server.StaticNodes() - 1);
        dynamicNameserviceConfig->MinDynamicNodeId = runtime.GetNodeId(server.StaticNodes());
        dynamicNameserviceConfig->MaxDynamicNodeId = runtime.GetNodeId(server.StaticNodes() + server.DynamicNodes() - 1);

        ui32 sharedDynNodeId = runtime.GetNodeId(1);
        ui32 exclusiveDynNodeId = runtime.GetNodeId(2);

        bool firstConsoleResponse = true;
        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case NConsole::TEvConsole::EvListTenantsResponse: {
                    auto *x = reinterpret_cast<NConsole::TEvConsole::TEvListTenantsResponse::TPtr*>(&ev);
                    AddPathsToListTenantsResponse(x, { "/Root/serverless", "/Root/shared" });
                    break;
                }
                case NConsole::TEvConsole::EvGetTenantStatusResponse: {
                    auto *x = reinterpret_cast<NConsole::TEvConsole::TEvGetTenantStatusResponse::TPtr*>(&ev);
                    if (firstConsoleResponse) {
                        firstConsoleResponse = false;
                        ChangeGetTenantStatusResponse(x, "/Root/serverless");
                    } else {
                        ChangeGetTenantStatusResponse(x, "/Root/shared");
                    }
                    break;
                }
                case TEvTxProxySchemeCache::EvNavigateKeySetResult: {
                    auto *x = reinterpret_cast<TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr*>(&ev);
                    ChangeNavigateKeyResultServerless(x, NKikimrSubDomains::EServerlessComputeResourcesModeExclusive, runtime);
                    break;
                }
                case TEvHive::EvResponseHiveNodeStats: {
                    auto *x = reinterpret_cast<TEvHive::TEvResponseHiveNodeStats::TPtr*>(&ev);
                    ChangeResponseHiveNodeStats(x, sharedDynNodeId, exclusiveDynNodeId);
                    break;
                }
                case TEvSchemeShard::EvDescribeSchemeResult: {
                    auto *x = reinterpret_cast<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr*>(&ev);
                    ChangeDescribeSchemeResultServerless(x);
                    break;
                }
                case TEvBlobStorage::EvControllerConfigResponse: {
                    auto *x = reinterpret_cast<TEvBlobStorage::TEvControllerConfigResponse::TPtr*>(&ev);
                    TVector<NKikimrBlobStorage::EVDiskStatus> vdiskStatuses = { NKikimrBlobStorage::EVDiskStatus::READY };
                    AddGroupVSlotInControllerConfigResponseWithStaticGroup(x, NKikimrBlobStorage::TGroupStatus::FULL, vdiskStatuses);
                    break;
                }
                case TEvWhiteboard::EvSystemStateResponse: {
                    auto *x = reinterpret_cast<TEvWhiteboard::TEvSystemStateResponse::TPtr*>(&ev);
                    ClearLoadAverage(x);
                    break;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        auto *request = new NHealthCheck::TEvSelfCheckRequest;
        request->Request.set_return_verbose_status(true);
        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, request, 0));
        const auto result = runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;

        Ctest << result.ShortDebugString();
        UNIT_ASSERT_VALUES_EQUAL(result.self_check_result(), Ydb::Monitoring::SelfCheck::GOOD);

        bool databaseFoundInResult = false;
        for (const auto &database_status : result.database_status()) {
            if (database_status.name() == "/Root/serverless") {
                databaseFoundInResult = true;
                UNIT_ASSERT_VALUES_EQUAL(database_status.overall(), Ydb::Monitoring::StatusFlag::GREEN);

                UNIT_ASSERT_VALUES_EQUAL(database_status.compute().overall(), Ydb::Monitoring::StatusFlag::GREEN);
                UNIT_ASSERT_VALUES_EQUAL(database_status.compute().nodes().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(database_status.compute().nodes()[0].id(), ToString(exclusiveDynNodeId));

                UNIT_ASSERT_VALUES_EQUAL(database_status.storage().overall(), Ydb::Monitoring::StatusFlag::GREEN);
                UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools()[0].id(), STORAGE_POOL_NAME);
            }
        }
        UNIT_ASSERT(databaseFoundInResult);
    }

    Y_UNIT_TEST(ServerlessWhenTroublesWithSharedNodes) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port)
                .SetNodeCount(1)
                .SetUseRealThreads(false)
                .SetDomainName("Root");
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        TClient client(settings);
        TTestActorRuntime& runtime = *server.GetRuntime();

        auto &dynamicNameserviceConfig = runtime.GetAppData().DynamicNameserviceConfig;
        dynamicNameserviceConfig->MaxStaticNodeId = runtime.GetNodeId(server.StaticNodes() - 1);

        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case NConsole::TEvConsole::EvGetTenantStatusResponse: {
                    auto *x = reinterpret_cast<NConsole::TEvConsole::TEvGetTenantStatusResponse::TPtr*>(&ev);
                    ChangeGetTenantStatusResponse(x, "/Root/serverless");
                    break;
                }
                case TEvTxProxySchemeCache::EvNavigateKeySetResult: {
                    auto *x = reinterpret_cast<TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr*>(&ev);
                    ChangeNavigateKeyResultServerless(x, NKikimrSubDomains::EServerlessComputeResourcesModeShared, runtime);
                    break;
                }
                case TEvSchemeShard::EvDescribeSchemeResult: {
                    auto *x = reinterpret_cast<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr*>(&ev);
                    ChangeDescribeSchemeResultServerless(x);
                    break;
                }
                case TEvBlobStorage::EvControllerConfigResponse: {
                    auto *x = reinterpret_cast<TEvBlobStorage::TEvControllerConfigResponse::TPtr*>(&ev);
                    TVector<NKikimrBlobStorage::EVDiskStatus> vdiskStatuses = { NKikimrBlobStorage::EVDiskStatus::READY };
                    AddGroupVSlotInControllerConfigResponseWithStaticGroup(x, NKikimrBlobStorage::TGroupStatus::FULL, vdiskStatuses);
                    break;
                }
                case TEvWhiteboard::EvSystemStateResponse: {
                    auto *x = reinterpret_cast<TEvWhiteboard::TEvSystemStateResponse::TPtr*>(&ev);
                    ClearLoadAverage(x);
                    break;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        auto *request = new NHealthCheck::TEvSelfCheckRequest;
        request->Request.set_return_verbose_status(true);
        request->Database = "/Root/serverless";
        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, request, 0));
        const auto result = runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;

        Ctest << result.ShortDebugString();
        UNIT_ASSERT_VALUES_EQUAL(result.self_check_result(), Ydb::Monitoring::SelfCheck::EMERGENCY);

        UNIT_ASSERT_VALUES_EQUAL(result.database_status_size(), 1);
        const auto &database_status = result.database_status(0);
        UNIT_ASSERT_VALUES_EQUAL(database_status.name(), "/Root/serverless");
        UNIT_ASSERT_VALUES_EQUAL(database_status.overall(), Ydb::Monitoring::StatusFlag::RED);

        UNIT_ASSERT_VALUES_EQUAL(database_status.compute().overall(), Ydb::Monitoring::StatusFlag::RED);
        UNIT_ASSERT_VALUES_EQUAL(database_status.compute().nodes().size(), 0);

        UNIT_ASSERT_VALUES_EQUAL(database_status.storage().overall(), Ydb::Monitoring::StatusFlag::GREEN);
        UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools()[0].id(), STORAGE_POOL_NAME);
    }

    Y_UNIT_TEST(ServerlessWithExclusiveNodesWhenTroublesWithSharedNodes) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port)
                .SetNodeCount(1)
                .SetDynamicNodeCount(1)
                .SetUseRealThreads(false)
                .SetDomainName("Root");
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        TClient client(settings);
        TTestActorRuntime& runtime = *server.GetRuntime();

        auto &dynamicNameserviceConfig = runtime.GetAppData().DynamicNameserviceConfig;
        dynamicNameserviceConfig->MaxStaticNodeId = runtime.GetNodeId(server.StaticNodes() - 1);
        dynamicNameserviceConfig->MinDynamicNodeId = runtime.GetNodeId(server.StaticNodes());
        dynamicNameserviceConfig->MaxDynamicNodeId = runtime.GetNodeId(server.StaticNodes() + server.DynamicNodes() - 1);

        ui32 staticNode = runtime.GetNodeId(0);
        ui32 exclusiveDynNodeId = runtime.GetNodeId(1);

        bool firstConsoleResponse = true;
        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case NConsole::TEvConsole::EvListTenantsResponse: {
                    auto *x = reinterpret_cast<NConsole::TEvConsole::TEvListTenantsResponse::TPtr*>(&ev);
                    AddPathsToListTenantsResponse(x, { "/Root/serverless", "/Root/shared" });
                    break;
                }
                case NConsole::TEvConsole::EvGetTenantStatusResponse: {
                    auto *x = reinterpret_cast<NConsole::TEvConsole::TEvGetTenantStatusResponse::TPtr*>(&ev);
                    if (firstConsoleResponse) {
                        firstConsoleResponse = false;
                        ChangeGetTenantStatusResponse(x, "/Root/serverless");
                    } else {
                        ChangeGetTenantStatusResponse(x, "/Root/shared");
                    }
                    break;
                }
                case TEvTxProxySchemeCache::EvNavigateKeySetResult: {
                    auto *x = reinterpret_cast<TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr*>(&ev);
                    ChangeNavigateKeyResultServerless(x, NKikimrSubDomains::EServerlessComputeResourcesModeExclusive, runtime);
                    break;
                }
                case TEvHive::EvResponseHiveNodeStats: {
                    auto *x = reinterpret_cast<TEvHive::TEvResponseHiveNodeStats::TPtr*>(&ev);
                    ChangeResponseHiveNodeStats(x, 0, exclusiveDynNodeId);
                    break;
                }
                case TEvSchemeShard::EvDescribeSchemeResult: {
                    auto *x = reinterpret_cast<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr*>(&ev);
                    ChangeDescribeSchemeResultServerless(x);
                    break;
                }
                case TEvBlobStorage::EvControllerConfigResponse: {
                    auto *x = reinterpret_cast<TEvBlobStorage::TEvControllerConfigResponse::TPtr*>(&ev);
                    TVector<NKikimrBlobStorage::EVDiskStatus> vdiskStatuses = { NKikimrBlobStorage::EVDiskStatus::READY };
                    AddGroupVSlotInControllerConfigResponseWithStaticGroup(x, NKikimrBlobStorage::TGroupStatus::FULL, vdiskStatuses);
                    break;
                }
                case TEvWhiteboard::EvSystemStateResponse: {
                    auto *x = reinterpret_cast<TEvWhiteboard::TEvSystemStateResponse::TPtr*>(&ev);
                    ClearLoadAverage(x);
                    break;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        auto *request = new NHealthCheck::TEvSelfCheckRequest;
        request->Request.set_return_verbose_status(true);
        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, request, 0));
        const auto result = runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;

        UNIT_ASSERT_VALUES_EQUAL(result.self_check_result(), Ydb::Monitoring::SelfCheck::EMERGENCY);

        bool serverlessDatabaseFoundInResult = false;
        bool sharedDatabaseFoundInResult = false;
        bool rootDatabaseFoundInResult = false;

        UNIT_ASSERT_VALUES_EQUAL(result.database_status_size(), 3);
        for (const auto &database_status : result.database_status()) {
            if (database_status.name() == "/Root/serverless") {
                serverlessDatabaseFoundInResult = true;

                UNIT_ASSERT_VALUES_EQUAL(database_status.overall(), Ydb::Monitoring::StatusFlag::GREEN);

                UNIT_ASSERT_VALUES_EQUAL(database_status.compute().overall(), Ydb::Monitoring::StatusFlag::GREEN);
                UNIT_ASSERT_VALUES_EQUAL(database_status.compute().nodes().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(database_status.compute().nodes()[0].id(), ToString(exclusiveDynNodeId));

                UNIT_ASSERT_VALUES_EQUAL(database_status.storage().overall(), Ydb::Monitoring::StatusFlag::GREEN);
                UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools()[0].id(), STORAGE_POOL_NAME);
            } else if (database_status.name() == "/Root/shared") {
                sharedDatabaseFoundInResult = true;

                UNIT_ASSERT_VALUES_EQUAL(database_status.overall(), Ydb::Monitoring::StatusFlag::RED);

                UNIT_ASSERT_VALUES_EQUAL(database_status.compute().overall(), Ydb::Monitoring::StatusFlag::RED);
                UNIT_ASSERT_VALUES_EQUAL(database_status.compute().nodes().size(), 0);

                UNIT_ASSERT_VALUES_EQUAL(database_status.storage().overall(), Ydb::Monitoring::StatusFlag::GREEN);
                UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools()[0].id(), STORAGE_POOL_NAME);
            } else if (database_status.name() == "/Root") {
                rootDatabaseFoundInResult = true;

                UNIT_ASSERT_VALUES_EQUAL(database_status.overall(), Ydb::Monitoring::StatusFlag::GREEN);

                UNIT_ASSERT_VALUES_EQUAL(database_status.compute().overall(), Ydb::Monitoring::StatusFlag::GREEN);
                UNIT_ASSERT_VALUES_EQUAL(database_status.compute().nodes().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(database_status.compute().nodes()[0].id(), ToString(staticNode));

                UNIT_ASSERT_VALUES_EQUAL(database_status.storage().overall(), Ydb::Monitoring::StatusFlag::GREEN);
                UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools()[0].id(), "static");
            }
        }
        UNIT_ASSERT(serverlessDatabaseFoundInResult);
        UNIT_ASSERT(sharedDatabaseFoundInResult);
        UNIT_ASSERT(rootDatabaseFoundInResult);
    }

    Y_UNIT_TEST(SharedWhenTroublesWithExclusiveNodes) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port)
                .SetNodeCount(1)
                .SetDynamicNodeCount(1)
                .SetUseRealThreads(false)
                .SetDomainName("Root");
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        TClient client(settings);
        TTestActorRuntime& runtime = *server.GetRuntime();

        auto &dynamicNameserviceConfig = runtime.GetAppData().DynamicNameserviceConfig;
        dynamicNameserviceConfig->MaxStaticNodeId = runtime.GetNodeId(server.StaticNodes() - 1);
        dynamicNameserviceConfig->MinDynamicNodeId = runtime.GetNodeId(server.StaticNodes());
        dynamicNameserviceConfig->MaxDynamicNodeId = runtime.GetNodeId(server.StaticNodes() + server.DynamicNodes() - 1);

        ui32 staticNode = runtime.GetNodeId(0);
        ui32 sharedDynNodeId = runtime.GetNodeId(1);

        bool firstConsoleResponse = true;
        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case NConsole::TEvConsole::EvListTenantsResponse: {
                    auto *x = reinterpret_cast<NConsole::TEvConsole::TEvListTenantsResponse::TPtr*>(&ev);
                    AddPathsToListTenantsResponse(x, { "/Root/serverless", "/Root/shared" });
                    break;
                }
                case NConsole::TEvConsole::EvGetTenantStatusResponse: {
                    auto *x = reinterpret_cast<NConsole::TEvConsole::TEvGetTenantStatusResponse::TPtr*>(&ev);
                    if (firstConsoleResponse) {
                        firstConsoleResponse = false;
                        ChangeGetTenantStatusResponse(x, "/Root/serverless");
                    } else {
                        ChangeGetTenantStatusResponse(x, "/Root/shared");
                    }
                    break;
                }
                case TEvTxProxySchemeCache::EvNavigateKeySetResult: {
                    auto *x = reinterpret_cast<TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr*>(&ev);
                    ChangeNavigateKeyResultServerless(x, NKikimrSubDomains::EServerlessComputeResourcesModeExclusive, runtime);
                    break;
                }
                case TEvHive::EvResponseHiveNodeStats: {
                    auto *x = reinterpret_cast<TEvHive::TEvResponseHiveNodeStats::TPtr*>(&ev);
                    ChangeResponseHiveNodeStats(x, sharedDynNodeId);
                    break;
                }
                case TEvSchemeShard::EvDescribeSchemeResult: {
                    auto *x = reinterpret_cast<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr*>(&ev);
                    ChangeDescribeSchemeResultServerless(x);
                    break;
                }
                case TEvBlobStorage::EvControllerConfigResponse: {
                    auto *x = reinterpret_cast<TEvBlobStorage::TEvControllerConfigResponse::TPtr*>(&ev);
                    TVector<NKikimrBlobStorage::EVDiskStatus> vdiskStatuses = { NKikimrBlobStorage::EVDiskStatus::READY };
                    AddGroupVSlotInControllerConfigResponseWithStaticGroup(x, NKikimrBlobStorage::TGroupStatus::FULL, vdiskStatuses);
                    break;
                }
                case TEvWhiteboard::EvSystemStateResponse: {
                    auto *x = reinterpret_cast<TEvWhiteboard::TEvSystemStateResponse::TPtr*>(&ev);
                    ClearLoadAverage(x);
                    break;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        auto *request = new NHealthCheck::TEvSelfCheckRequest;
        request->Request.set_return_verbose_status(true);
        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, request, 0));
        const auto result = runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;

        Ctest << result.ShortDebugString();
        UNIT_ASSERT_VALUES_EQUAL(result.self_check_result(), Ydb::Monitoring::SelfCheck::EMERGENCY);

        bool serverlessDatabaseFoundInResult = false;
        bool sharedDatabaseFoundInResult = false;
        bool rootDatabaseFoundInResult = false;

        UNIT_ASSERT_VALUES_EQUAL(result.database_status_size(), 3);
        for (const auto &database_status : result.database_status()) {
            if (database_status.name() == "/Root/serverless") {
                serverlessDatabaseFoundInResult = true;

                UNIT_ASSERT_VALUES_EQUAL(database_status.overall(), Ydb::Monitoring::StatusFlag::RED);

                UNIT_ASSERT_VALUES_EQUAL(database_status.compute().overall(), Ydb::Monitoring::StatusFlag::RED);
                UNIT_ASSERT_VALUES_EQUAL(database_status.compute().nodes().size(), 0);

                UNIT_ASSERT_VALUES_EQUAL(database_status.storage().overall(), Ydb::Monitoring::StatusFlag::GREEN);
                UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools()[0].id(), STORAGE_POOL_NAME);
            } else if (database_status.name() == "/Root/shared") {
                sharedDatabaseFoundInResult = true;

                UNIT_ASSERT_VALUES_EQUAL(database_status.overall(), Ydb::Monitoring::StatusFlag::GREEN);

                UNIT_ASSERT_VALUES_EQUAL(database_status.compute().overall(), Ydb::Monitoring::StatusFlag::GREEN);
                UNIT_ASSERT_VALUES_EQUAL(database_status.compute().nodes().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(database_status.compute().nodes()[0].id(), ToString(sharedDynNodeId));

                UNIT_ASSERT_VALUES_EQUAL(database_status.storage().overall(), Ydb::Monitoring::StatusFlag::GREEN);
                UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools()[0].id(), STORAGE_POOL_NAME);
            } else if (database_status.name() == "/Root") {
                rootDatabaseFoundInResult = true;

                UNIT_ASSERT_VALUES_EQUAL(database_status.overall(), Ydb::Monitoring::StatusFlag::GREEN);

                UNIT_ASSERT_VALUES_EQUAL(database_status.compute().overall(), Ydb::Monitoring::StatusFlag::GREEN);
                UNIT_ASSERT_VALUES_EQUAL(database_status.compute().nodes().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(database_status.compute().nodes()[0].id(), ToString(staticNode));

                UNIT_ASSERT_VALUES_EQUAL(database_status.storage().overall(), Ydb::Monitoring::StatusFlag::GREEN);
                UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools()[0].id(), "static");
            }
        }
        UNIT_ASSERT(serverlessDatabaseFoundInResult);
        UNIT_ASSERT(sharedDatabaseFoundInResult);
        UNIT_ASSERT(rootDatabaseFoundInResult);
    }

    Y_UNIT_TEST(NoStoragePools) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port)
                .SetNodeCount(1)
                .SetDynamicNodeCount(1)
                .SetUseRealThreads(false)
                .SetDomainName("Root");
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        TClient client(settings);
        TTestActorRuntime& runtime = *server.GetRuntime();

        auto &dynamicNameserviceConfig = runtime.GetAppData().DynamicNameserviceConfig;
        dynamicNameserviceConfig->MaxStaticNodeId = runtime.GetNodeId(server.StaticNodes() - 1);
        dynamicNameserviceConfig->MinDynamicNodeId = runtime.GetNodeId(server.StaticNodes());
        dynamicNameserviceConfig->MaxDynamicNodeId = runtime.GetNodeId(server.StaticNodes() + server.DynamicNodes() - 1);

        ui32 dynNodeId = runtime.GetNodeId(1);

        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case NConsole::TEvConsole::EvGetTenantStatusResponse: {
                    auto *x = reinterpret_cast<NConsole::TEvConsole::TEvGetTenantStatusResponse::TPtr*>(&ev);
                    ChangeGetTenantStatusResponse(x, "/Root/database");
                    break;
                }
                case TEvTxProxySchemeCache::EvNavigateKeySetResult: {
                    auto *x = reinterpret_cast<TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr*>(&ev);
                    TSchemeCacheNavigate::TEntry& entry((*x)->Get()->Request->ResultSet.front());
                    TString path = CanonizePath(entry.Path);
                    if (path == "/Root/database" || entry.TableId.PathId == SUBDOMAIN_KEY) {
                        entry.Status = TSchemeCacheNavigate::EStatus::Ok;
                        entry.Kind = TSchemeCacheNavigate::EKind::KindExtSubdomain;
                        entry.Path = {"Root", "database"};
                        entry.DomainInfo = MakeIntrusive<TDomainInfo>(SUBDOMAIN_KEY, SUBDOMAIN_KEY);
                        auto domains = runtime.GetAppData().DomainsInfo;
                        ui64 hiveId = domains->GetHive();
                        entry.DomainInfo->Params.SetHive(hiveId);
                    }
                    break;
                }
                case TEvHive::EvResponseHiveNodeStats: {
                    auto *x = reinterpret_cast<TEvHive::TEvResponseHiveNodeStats::TPtr*>(&ev);
                    auto &record = (*x)->Get()->Record;
                    auto *nodeStats = record.MutableNodeStats()->Add();
                    nodeStats->SetNodeId(dynNodeId);
                    nodeStats->MutableNodeDomain()->SetSchemeShard(SUBDOMAIN_KEY.OwnerId);
                    nodeStats->MutableNodeDomain()->SetPathId(SUBDOMAIN_KEY.LocalPathId);
                    break;
                }
                case TEvSchemeShard::EvDescribeSchemeResult: {
                    auto *x = reinterpret_cast<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr*>(&ev);
                    auto record = (*x)->Get()->MutableRecord();
                    if (record->path() == "/Root/database") {
                        record->set_status(NKikimrScheme::StatusSuccess);
                        // no pools
                    }
                    break;
                }
                case TEvBlobStorage::EvControllerConfigResponse: {
                    auto *x = reinterpret_cast<TEvBlobStorage::TEvControllerConfigResponse::TPtr*>(&ev);
                    TVector<NKikimrBlobStorage::EVDiskStatus> vdiskStatuses = { NKikimrBlobStorage::EVDiskStatus::READY };
                    AddGroupVSlotInControllerConfigResponseWithStaticGroup(x, NKikimrBlobStorage::TGroupStatus::FULL, vdiskStatuses);
                    break;
                }
                case TEvWhiteboard::EvSystemStateResponse: {
                    auto *x = reinterpret_cast<TEvWhiteboard::TEvSystemStateResponse::TPtr*>(&ev);
                    ClearLoadAverage(x);
                    break;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        auto *request = new NHealthCheck::TEvSelfCheckRequest;
        request->Request.set_return_verbose_status(true);
        request->Database = "/Root/database";
        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, request, 0));
        const auto result = runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;

        Ctest << result.ShortDebugString();

        UNIT_ASSERT_VALUES_EQUAL(result.self_check_result(), Ydb::Monitoring::SelfCheck::EMERGENCY);
        UNIT_ASSERT_VALUES_EQUAL(result.database_status_size(), 1);
        const auto &database_status = result.database_status(0);
        UNIT_ASSERT_VALUES_EQUAL(database_status.name(),  "/Root/database");
        UNIT_ASSERT_VALUES_EQUAL(database_status.overall(), Ydb::Monitoring::StatusFlag::RED);

        UNIT_ASSERT_VALUES_EQUAL(database_status.compute().overall(), Ydb::Monitoring::StatusFlag::GREEN);
        UNIT_ASSERT_VALUES_EQUAL(database_status.compute().nodes().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(database_status.compute().nodes()[0].id(), ToString(dynNodeId));

        UNIT_ASSERT_VALUES_EQUAL(database_status.storage().overall(), Ydb::Monitoring::StatusFlag::RED);
        UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools().size(), 0);
    }

    Y_UNIT_TEST(NoBscConfigResponse) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port)
                .SetNodeCount(1)
                .SetDynamicNodeCount(1)
                .SetUseRealThreads(false)
                .SetDomainName("Root");
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        TClient client(settings);
        TTestActorRuntime& runtime = *server.GetRuntime();

        auto &dynamicNameserviceConfig = runtime.GetAppData().DynamicNameserviceConfig;
        dynamicNameserviceConfig->MaxStaticNodeId = runtime.GetNodeId(server.StaticNodes() - 1);
        dynamicNameserviceConfig->MinDynamicNodeId = runtime.GetNodeId(server.StaticNodes());
        dynamicNameserviceConfig->MaxDynamicNodeId = runtime.GetNodeId(server.StaticNodes() + server.DynamicNodes() - 1);

        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvControllerConfigResponse) {
                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        auto *request = new NHealthCheck::TEvSelfCheckRequest;
        request->Request.set_return_verbose_status(true);
        request->Database = "/Root";
        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, request, 0));
        const auto result = runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;

        Ctest << result.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL(result.self_check_result(), Ydb::Monitoring::SelfCheck::EMERGENCY);

        bool bscTabletIssueFoundInResult = false;
        for (const auto &issue_log : result.issue_log()) {
            if (issue_log.level() == 3 && issue_log.type() == "SYSTEM_TABLET") {
                UNIT_ASSERT_VALUES_EQUAL(issue_log.location().compute().tablet().id().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(issue_log.location().compute().tablet().id()[0], ToString(MakeBSControllerID()));
                UNIT_ASSERT_VALUES_EQUAL(issue_log.location().compute().tablet().type(), "BSController");
                bscTabletIssueFoundInResult = true;
            }
        }
        UNIT_ASSERT(bscTabletIssueFoundInResult);

        UNIT_ASSERT_VALUES_EQUAL(result.database_status_size(), 1);
        const auto &database_status = result.database_status(0);

        UNIT_ASSERT_VALUES_EQUAL(database_status.name(), "/Root");
        UNIT_ASSERT_VALUES_EQUAL(database_status.overall(), Ydb::Monitoring::StatusFlag::RED);

        UNIT_ASSERT_VALUES_EQUAL(database_status.compute().overall(), Ydb::Monitoring::StatusFlag::RED);
        UNIT_ASSERT_VALUES_EQUAL(database_status.storage().overall(), Ydb::Monitoring::StatusFlag::RED);
        UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools()[0].id(), "static");
    }

    void HiveSyncTest(bool syncPeriod) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port)
                .SetNodeCount(1)
                .SetDynamicNodeCount(1)
                .SetUseRealThreads(false)
                .SetDomainName("Root");
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        TClient client(settings);
        TTestActorRuntime& runtime = *server.GetRuntime();

        ui32 dynNodeId = runtime.GetNodeId(1);

        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvHive::EvResponseHiveInfo: {
                    auto *x = reinterpret_cast<TEvHive::TEvResponseHiveInfo::TPtr*>(&ev);
                    auto& record = (*x)->Get()->Record;
                    record.SetStartTimeTimestamp(0);
                    if (syncPeriod) {
                        record.SetResponseTimestamp(NHealthCheck::TSelfCheckRequest::HIVE_SYNCHRONIZATION_PERIOD_MS / 2);
                    } else {
                        record.SetResponseTimestamp(NHealthCheck::TSelfCheckRequest::HIVE_SYNCHRONIZATION_PERIOD_MS * 2);
                    }
                    auto *tablet = record.MutableTablets()->Add();
                    tablet->SetTabletID(1);
                    tablet->SetNodeID(dynNodeId);
                    tablet->SetTabletType(NKikimrTabletBase::TTabletTypes::DataShard);
                    tablet->SetVolatileState(NKikimrHive::TABLET_VOLATILE_STATE_BOOTING);
                    tablet->MutableObjectDomain()->SetSchemeShard(SUBDOMAIN_KEY.OwnerId);
                    tablet->MutableObjectDomain()->SetPathId(SUBDOMAIN_KEY.LocalPathId);
                    break;
                }
                case TEvHive::EvResponseHiveNodeStats: {
                    auto *x = reinterpret_cast<TEvHive::TEvResponseHiveNodeStats::TPtr*>(&ev);
                    auto &record = (*x)->Get()->Record;
                    auto *nodeStats = record.MutableNodeStats()->Add();
                    nodeStats->SetNodeId(dynNodeId);
                    nodeStats->MutableNodeDomain()->SetSchemeShard(SUBDOMAIN_KEY.OwnerId);
                    nodeStats->MutableNodeDomain()->SetPathId(SUBDOMAIN_KEY.LocalPathId);
                    break;
                }
                case NConsole::TEvConsole::EvGetTenantStatusResponse: {
                    auto *x = reinterpret_cast<NConsole::TEvConsole::TEvGetTenantStatusResponse::TPtr*>(&ev);
                    ChangeGetTenantStatusResponse(x, "/Root/database");
                    break;
                }
                case TEvTxProxySchemeCache::EvNavigateKeySetResult: {
                    auto *x = reinterpret_cast<TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr*>(&ev);
                    TSchemeCacheNavigate::TEntry& entry((*x)->Get()->Request->ResultSet.front());
                    entry.Status = TSchemeCacheNavigate::EStatus::Ok;
                    entry.Kind = TSchemeCacheNavigate::EKind::KindExtSubdomain;
                    entry.Path = {"Root", "database"};
                    entry.DomainInfo = MakeIntrusive<TDomainInfo>(SUBDOMAIN_KEY, SUBDOMAIN_KEY);

                    break;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        auto *request = new NHealthCheck::TEvSelfCheckRequest;
        request->Request.set_return_verbose_status(true);
        request->Database = "/Root/database";
        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, request, 0));
        const auto result = runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;

        Cerr << result.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL(result.database_status_size(), 1);

        bool deadTabletIssueFoundInResult = false;
        for (const auto &issue_log : result.issue_log()) {
            if (issue_log.level() == 4 && issue_log.type() == "TABLET") {
                UNIT_ASSERT_VALUES_EQUAL(issue_log.location().compute().tablet().id().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(issue_log.location().compute().tablet().type(), "DataShard");
                deadTabletIssueFoundInResult = true;
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(syncPeriod, !deadTabletIssueFoundInResult);
    }

    Y_UNIT_TEST(HiveSyncPeriodIgnoresTabletsState) {
        HiveSyncTest(true);
    }

    Y_UNIT_TEST(AfterHiveSyncPeriodReportsTabletsState) {
        HiveSyncTest(false);
    }
}
}
