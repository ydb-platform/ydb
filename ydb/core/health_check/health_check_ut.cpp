#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

#include <ydb/core/mind/hive/hive_events.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/util/proto_duration.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/public/api/protos/ydb_cms.pb.h>
#include "health_check.h"

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
    Ydb::Monitoring::SelfCheckResult BasicTest(IEventBase* ev) {
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

        return result->Result;
    }

    Y_UNIT_TEST(Basic) {
        BasicTest(new NHealthCheck::TEvSelfCheckRequest());
    }

    Y_UNIT_TEST(BasicNodeCheckRequest) {
        BasicTest(new NHealthCheck::TEvNodeCheckRequest());
    }

    Y_UNIT_TEST(DatabaseDoesNotExist) {
        auto ev = std::make_unique<NHealthCheck::TEvSelfCheckRequest>();
        ev->Database = "/Root/this/db/does/not/exist";
        auto result = BasicTest(ev.release());
        Cerr << result.ShortDebugString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(result.issue_log().size(), 1);
    }

    const int GROUP_START_ID = 0x80000000;
    const int VCARD_START_ID = 55;
    const int PDISK_START_ID = 42;
    const int DEFAULT_GROUP_GENERATION = 3;

    const TPathId SUBDOMAIN_KEY = {7000000000, 1};
    const TPathId SERVERLESS_DOMAIN_KEY = {7000000000, 2};
    const TPathId SHARED_DOMAIN_KEY = {7000000000, 3};
    const TString STORAGE_POOL_NAME = "/Root:test";

    struct TTestVSlotInfo {
        std::optional<NKikimrBlobStorage::EVDiskStatus> Status;
        ui32 Generation = DEFAULT_GROUP_GENERATION;
        std::optional<NKikimrBlobStorage::TPDiskState::E> PDiskState;
        NKikimrBlobStorage::EDriveStatus PDiskStatus = NKikimrBlobStorage::ACTIVE;
        bool PhantomOnly = false;

        TTestVSlotInfo(std::optional<NKikimrBlobStorage::EVDiskStatus> status = NKikimrBlobStorage::READY,
                       ui32 generation = DEFAULT_GROUP_GENERATION)
            : Status(status)
            , Generation(generation)
        {
        }

        TTestVSlotInfo(NKikimrBlobStorage::EVDiskStatus status, NKikimrBlobStorage::TPDiskState::E pDiskState)
            : Status(status)
            , PDiskState(pDiskState)
        {
        }

        TTestVSlotInfo(NKikimrBlobStorage::EVDiskStatus status, NKikimrBlobStorage::EDriveStatus pDiskStatus = NKikimrBlobStorage::ACTIVE)
            : Status(status)
            , PDiskStatus(pDiskStatus)
        {
        }
    };

    using TVDisks = TVector<TTestVSlotInfo>;

    TTestVSlotInfo PhantomOnlyVDisk() {
        TTestVSlotInfo vdisk{std::optional<NKikimrBlobStorage::EVDiskStatus>(NKikimrBlobStorage::REPLICATING)};
        vdisk.PhantomOnly = true;
        return vdisk;
    }

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
        domain->SetShardsLimit(quota);
        domain->SetShardsInside(size);
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
            group->set_erasurespecies(NHealthCheck::BLOCK_4_2);
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

    void AddGroupsToSysViewResponse(NSysView::TEvSysView::TEvGetGroupsResponse::TPtr* ev, size_t groupCount = 1, bool addStatic = true) {
        auto& record = (*ev)->Get()->Record;
        auto entrySample = record.entries(0);
        record.clear_entries();

        auto addGroup = [&](size_t groupId, size_t poolId) {
            auto* entry = record.add_entries();
            entry->CopyFrom(entrySample);
            entry->mutable_key()->set_groupid(groupId);
            entry->mutable_info()->set_erasurespeciesv2(NHealthCheck::BLOCK_4_2);
            entry->mutable_info()->set_storagepoolid(poolId);
            entry->mutable_info()->set_generation(DEFAULT_GROUP_GENERATION);
        };

        if (addStatic) {
            addGroup(0, 0);
        }

        auto groupId = GROUP_START_ID;
        for (size_t i = 0; i < groupCount; ++i) {
            addGroup(groupId++, 1);
        }
    }

    void AddBridgeGroupsToSysViewResponse(NSysView::TEvSysView::TEvGetGroupsResponse::TPtr* ev, size_t groupCount = 1, size_t pileCount = 2,
                                          bool inlinePile = false) {
        auto& record = (*ev)->Get()->Record;
        auto entrySample = record.entries(0);
        record.clear_entries();

        auto addGroup = [&](size_t groupId, size_t poolId, std::optional<size_t> proxyGroup, ui32 pileId = 0) {
            auto* entry = record.add_entries();
            entry->CopyFrom(entrySample);
            entry->mutable_key()->set_groupid(groupId);
            if (proxyGroup) {
                entry->mutable_info()->set_erasurespeciesv2(NHealthCheck::BLOCK_4_2);
            } else {
                entry->mutable_info()->set_erasurespeciesv2(NHealthCheck::NONE);
            }
            entry->mutable_info()->set_storagepoolid(poolId);
            entry->mutable_info()->set_generation(DEFAULT_GROUP_GENERATION);
            if (proxyGroup) {
                entry->mutable_info()->set_proxygroupid(*proxyGroup);
                entry->mutable_info()->set_bridgepileid(pileId + 1);
            }
            return entry;
        };

        auto groupId = GROUP_START_ID;
        for (size_t i = 0; i < groupCount; ++i) {
            size_t proxyGroup = groupId;
            auto* proxyEntry = addGroup(groupId++, 1, {});
            if (inlinePile) { // group converted from non-bridged one is its own part in pile 0, listed without ProxyGroupId
                proxyEntry->mutable_info()->set_erasurespeciesv2(NHealthCheck::BLOCK_4_2);
                proxyEntry->mutable_info()->set_bridgepileid(1);
            }
            for (size_t j = inlinePile; j < pileCount; ++j) {
                addGroup(groupId++, 1, proxyGroup, j);
            }
        }
    }

    void AddVSlotsToSysViewResponse(NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr* ev, size_t groupCount,
                                    const TVDisks& vslots, ui32 groupStartId = GROUP_START_ID,
                                    bool rewrite = true, bool withPdisk = false, bool withPhantomOnly = true) {
        auto& record = (*ev)->Get()->Record;
        auto entrySample = record.entries(0);
        if (rewrite) {
            record.clear_entries();
        }

        auto groupId = groupStartId;
        const auto *descriptor = NKikimrBlobStorage::EVDiskStatus_descriptor();
        for (size_t i = 0; i < groupCount; ++i) {
            static int vslotId;
            if (rewrite) {
                vslotId = VCARD_START_ID;
            }
            auto pdiskId = PDISK_START_ID;
            for (const auto& vslot : vslots) {
                auto* entry = record.add_entries();
                entry->CopyFrom(entrySample);
                entry->mutable_key()->set_vslotid(vslotId);
                if (withPdisk) {
                    entry->mutable_key()->set_pdiskid(pdiskId);
                }
                entry->mutable_info()->set_groupid(groupId);
                entry->mutable_info()->set_failrealm(vslotId);
                if (vslot.Status) {
                    entry->mutable_info()->set_statusv2(descriptor->FindValueByNumber(*vslot.Status)->name());
                }
                if (withPhantomOnly) {
                    entry->mutable_info()->set_phantomonly(vslot.PhantomOnly);
                } else {
                    entry->mutable_info()->clear_phantomonly();
                }
                entry->mutable_info()->set_groupgeneration(vslot.Generation);
                entry->mutable_info()->set_vdisk(vslotId);
                ++vslotId;
                ++pdiskId;
            }
            ++groupId;
        }
    }

    void AddStoragePoolsToSysViewResponse(NSysView::TEvSysView::TEvGetStoragePoolsResponse::TPtr* ev) {
        auto& record = (*ev)->Get()->Record;
        record.clear_entries();
        auto* entry = record.add_entries();
        entry->mutable_key()->set_storagepoolid(1);
        entry->mutable_info()->set_name(STORAGE_POOL_NAME);
    }

    void AddPDisksToSysViewResponse(NSysView::TEvSysView::TEvGetPDisksResponse::TPtr* ev, const TTestActorRuntime& runtime, const TVDisks& vslots, double occupancy) {
        auto& record = (*ev)->Get()->Record;
        auto entrySample = record.entries(0);
        record.clear_entries();
        const size_t totalSize = 3'200'000'000'000ull;
        const auto *descriptorState = NKikimrBlobStorage::TPDiskState::E_descriptor();
        const auto *descriptorStatusV2 = NKikimrBlobStorage::EDriveStatus_descriptor();
        for (ui32 nodeIdx = 0; nodeIdx < runtime.GetNodeCount(); ++nodeIdx) {
            auto pdiskId = PDISK_START_ID;
            for (const auto& vslot : vslots) {
                auto* entry = record.add_entries();
                entry->CopyFrom(entrySample);
                entry->mutable_key()->set_nodeid(runtime.GetNodeId(nodeIdx));
                entry->mutable_key()->set_pdiskid(pdiskId);
                entry->mutable_info()->set_totalsize(totalSize);
                entry->mutable_info()->set_availablesize((1 - occupancy) * totalSize);
                if (vslot.PDiskState) {
                    entry->mutable_info()->set_state(descriptorState->FindValueByNumber(*vslot.PDiskState)->name());
                }
                entry->mutable_info()->set_statusv2(descriptorStatusV2->FindValueByNumber(vslot.PDiskStatus)->name());
                ++pdiskId;
            }
        }
    }

    void SetPDisksStateNormalToSysViewResponse(NSysView::TEvSysView::TEvGetPDisksResponse::TPtr* ev) {
        auto& record = (*ev)->Get()->Record;
        const auto *descriptor = NKikimrBlobStorage::TPDiskState::E_descriptor();
        const TString state = descriptor->FindValueByNumber(NKikimrBlobStorage::TPDiskState::Normal)->name();
        for (auto& entry: *record.mutable_entries()) {
            entry.mutable_info()->set_state(state);
        }
    }

    void AddGroupVSlotInControllerConfigResponseWithStaticGroup(TEvBlobStorage::TEvControllerConfigResponse::TPtr* ev,
        const NKikimrBlobStorage::TGroupStatus::E groupStatus, const TVDisks& vslots)
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
        staticGroup->set_erasurespecies(NHealthCheck::BLOCK_4_2);
        staticGroup->set_groupgeneration(DEFAULT_GROUP_GENERATION);

        auto group = pbConfig->add_group();
        group->CopyFrom(groupSample);
        group->set_groupid(GROUP_START_ID);
        group->set_storagepoolid(1);
        group->set_operatingstatus(groupStatus);
        group->set_erasurespecies(NHealthCheck::BLOCK_4_2);
        group->set_groupgeneration(DEFAULT_GROUP_GENERATION);

        group->clear_vslotid();
        auto vslotId = VCARD_START_ID;

        for (const auto& vslotInfo : vslots) {
            auto vslot = pbConfig->add_vslot();
            vslot->CopyFrom(vslotSample);
            vslot->set_vdiskidx(vslotId);
            vslot->set_groupid(GROUP_START_ID);
            vslot->set_failrealmidx(vslotId);
            vslot->mutable_vslotid()->set_vslotid(vslotId);

            auto slotId = group->add_vslotid();
            slotId->CopyFrom(vslotIdSample);
            slotId->set_vslotid(vslotId);

            if (vslotInfo.Status) {
                const auto *descriptor = NKikimrBlobStorage::EVDiskStatus_descriptor();
                vslot->set_status(descriptor->FindValueByNumber(*vslotInfo.Status)->name());
            }
            vslot->set_groupgeneration(vslotInfo.Generation);

            vslotId++;
        }

        auto spStatus = pbRecord.mutable_response()->mutable_status(1);
        spStatus->clear_storagepool();
        auto sPool = spStatus->add_storagepool();
        sPool->set_storagepoolid(1);
        sPool->set_name(STORAGE_POOL_NAME);
    };

    void AddVSlotInVDiskStateResponse(TEvWhiteboard::TEvVDiskStateResponse::TPtr* ev, int groupCount, const TVDisks& vslots,
                                      ui32 groupStartId = GROUP_START_ID) {
        auto& pbRecord = (*ev)->Get()->Record;

        auto sample = pbRecord.vdiskstateinfo(0);
        pbRecord.clear_vdiskstateinfo();

        auto groupId = groupStartId;
        for (int i = 0; i < groupCount; i++) {
            auto slotId = VCARD_START_ID;
            for (const auto& vslot : vslots) {
                auto state = pbRecord.add_vdiskstateinfo();
                state->CopyFrom(sample);
                state->mutable_vdiskid()->set_groupid(groupId);
                state->mutable_vdiskid()->set_groupgeneration(DEFAULT_GROUP_GENERATION);
                state->mutable_vdiskid()->set_ring(slotId);
                state->mutable_vdiskid()->set_domain(0);
                state->mutable_vdiskid()->set_vdisk(slotId++);
                state->set_vdiskstate(NKikimrWhiteboard::EVDiskState::SyncGuidRecovery);
                if (vslot.PhantomOnly) {
                    state->set_detailedreplicationstatus(NKikimrWhiteboard::TVDiskDetailedReplicationStatus::PhantomsOnly);
                } else {
                    state->clear_detailedreplicationstatus();
                }
                state->set_nodeid(1);
            }
            groupId++;
        }
    }

    void ChangeGroupStateResponse(NNodeWhiteboard::TEvWhiteboard::TEvBSGroupStateResponse::TPtr* ev) {
        for (auto& groupInfo : *(*ev)->Get()->Record.mutable_bsgroupstateinfo()) {
            groupInfo.set_erasurespecies(NHealthCheck::BLOCK_4_2);
        }
    }

    void SetLongHostValue(TEvInterconnect::TEvNodesInfo::TPtr* ev) {
        auto nodes = MakeIntrusive<TIntrusiveVector<TEvInterconnect::TNodeInfo>>((*ev)->Get()->Nodes);
        TString host(1000000, 'a');
        for (auto it = nodes->begin(); it != nodes->end(); ++it) {
            it->Host = host;
        }
        auto newEv = IEventHandle::Downcast<TEvInterconnect::TEvNodesInfo>(
            new IEventHandle((*ev)->Recipient, (*ev)->Sender, new TEvInterconnect::TEvNodesInfo(nodes))
        );
        ev->Swap(newEv);
    }

    Ydb::Monitoring::SelfCheckResult RequestHc(size_t const groupNumber, size_t const vdiscPerGroupNumber, bool const isMergeRecords = false, bool const largeSizeVdisksIssues = false) {
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
                case NSysView::TEvSysView::EvGetVSlotsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr*>(&ev);
                    AddVSlotsToSysViewResponse(x, groupNumber, TVDisks{vdiscPerGroupNumber, NKikimrBlobStorage::EVDiskStatus::ERROR});
                    break;
                }
                case NSysView::TEvSysView::EvGetGroupsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetGroupsResponse::TPtr*>(&ev);
                    AddGroupsToSysViewResponse(x, groupNumber, false);
                    break;
                }
                case NSysView::TEvSysView::EvGetStoragePoolsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetStoragePoolsResponse::TPtr*>(&ev);
                    AddStoragePoolsToSysViewResponse(x);
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

    bool HasTabletIssue(const Ydb::Monitoring::SelfCheckResult& result) {
       for (const auto& issue_log : result.issue_log()) {
            if (issue_log.level() == 4 && issue_log.type() == "TABLET") {
                return true;
            }
        }
        return false;
    }

    void ListingTest(int const groupNumber, int const vdiscPerGroupNumber, bool const isMergeRecords = false) {
        auto result = RequestHc(groupNumber, vdiscPerGroupNumber, isMergeRecords);
        CheckHcResult(result, groupNumber, vdiscPerGroupNumber, isMergeRecords);
    }

    Ydb::Monitoring::SelfCheckResult RequestHcWithVdisks(const NKikimrBlobStorage::TGroupStatus::E groupStatus, const TVDisks& vdisks,
                                                         bool forStaticGroup = false, double occupancy = 0, bool withPhantomOnly = true,
                                                         bool returnHints = false) {
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
                    AddGroupVSlotInControllerConfigResponseWithStaticGroup(x, groupStatus, vdisks);
                    break;
                }
                case NSysView::TEvSysView::EvGetVSlotsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr*>(&ev);
                    if (forStaticGroup) {
                        AddVSlotsToSysViewResponse(x, 1, vdisks, 0, true, true, withPhantomOnly);
                    } else {
                        AddVSlotsToSysViewResponse(x, 1, vdisks, GROUP_START_ID, true, true, withPhantomOnly);
                    }
                    break;
                }
                case NSysView::TEvSysView::EvGetPDisksResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetPDisksResponse::TPtr*>(&ev);
                    AddPDisksToSysViewResponse(x, runtime, vdisks, occupancy);
                    break;
                }
                case NSysView::TEvSysView::EvGetGroupsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetGroupsResponse::TPtr*>(&ev);
                    AddGroupsToSysViewResponse(x);
                    break;
                }
                case NSysView::TEvSysView::EvGetStoragePoolsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetStoragePoolsResponse::TPtr*>(&ev);
                    AddStoragePoolsToSysViewResponse(x);
                    break;
                }
                case NNodeWhiteboard::TEvWhiteboard::EvVDiskStateResponse: {
                    auto *x = reinterpret_cast<NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateResponse::TPtr*>(&ev);
                    if (forStaticGroup) {
                        AddVSlotInVDiskStateResponse(x, 1, vdisks, 0);
                    } else {
                        AddVSlotInVDiskStateResponse(x, 1, vdisks);
                    }
                    break;
                }
                case NNodeWhiteboard::TEvWhiteboard::EvBSGroupStateResponse: {
                    auto* x = reinterpret_cast<NNodeWhiteboard::TEvWhiteboard::TEvBSGroupStateResponse::TPtr*>(&ev);
                    ChangeGroupStateResponse(x);
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        auto *request = new NHealthCheck::TEvSelfCheckRequest;
        request->Request.set_merge_records(true);
        request->Request.set_return_hints(returnHints);

        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, request, 0));
        return runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;
    }

    Ydb::Monitoring::SelfCheckResult RequestHcWithBridgeVdisks(const TVector<TVDisks>& groupVdisks,
            size_t bridgeGroupCount = 1, bool ordinary = false,
            std::array<NKikimrBridge::TClusterState::EPileState, 2> pileStates = {NKikimrBridge::TClusterState::SYNCHRONIZED, NKikimrBridge::TClusterState::SYNCHRONIZED},
            bool inlinePile = false, bool bridgeModeEnabled = false)
    {
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
        if (ordinary || bridgeModeEnabled) {
            runtime.GetAppData().BridgeModeEnabled = true;
        }

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        auto bridgeInfo = std::make_shared<TBridgeInfo>();
        bridgeInfo->Piles.push_back(TBridgeInfo::TPile{.BridgePileId = TBridgePileId::FromPileIndex(0), .Name = "pile0", .State = pileStates[0]});
        bridgeInfo->Piles.push_back(TBridgeInfo::TPile{.BridgePileId = TBridgePileId::FromPileIndex(1), .Name = "pile1", .State = pileStates[1]});
        bridgeInfo->SelfNodePile = bridgeInfo->Piles.data();
        bridgeInfo->StaticNodeIdToPile[runtime.GetNodeId(0)] = &bridgeInfo->Piles[0];
        bridgeInfo->StaticNodeIdToPile[runtime.GetNodeId(1)] = &bridgeInfo->Piles[1];

        auto* nodeWardenStorageConfig = new TEvNodeWardenStorageConfig(std::make_shared<NKikimrBlobStorage::TStorageConfig>(), false, nullptr);
        nodeWardenStorageConfig->BridgeInfo = bridgeInfo;
        runtime.Send(new IEventHandle(GetNameserviceActorId(), runtime.AllocateEdgeActor(), nodeWardenStorageConfig));

        if (bridgeModeEnabled) {
            // NodeWarden defers bridge-mode queries until it has bridge information.
            runtime.Send(new IEventHandle(MakeBlobStorageNodeWardenID(runtime.GetNodeId(0)), sender,
                new TEvNodeWardenStorageConfig(std::make_shared<NKikimrBlobStorage::TStorageConfig>(), false, bridgeInfo)));
        }

        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvSchemeShard::EvDescribeSchemeResult: {
                    auto *x = reinterpret_cast<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr*>(&ev);
                    ChangeDescribeSchemeResult(x);
                    break;
                }
                case NSysView::TEvSysView::EvGetVSlotsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr*>(&ev);
                    bool first = true;
                    // first ++ hits the group itself if ordinary or inline, else its pile 0 part
                    size_t groupId = ordinary || inlinePile ? GROUP_START_ID - 1 : GROUP_START_ID;
                    for (const auto& vdisks : groupVdisks) {
                        AddVSlotsToSysViewResponse(x, 1, vdisks, ++groupId, std::exchange(first, false));
                    }
                    break;
                }
                case NSysView::TEvSysView::EvGetGroupsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetGroupsResponse::TPtr*>(&ev);
                    if (ordinary) {
                        AddGroupsToSysViewResponse(x, groupVdisks.size(), false);
                    } else {
                        AddBridgeGroupsToSysViewResponse(x, bridgeGroupCount, 2, inlinePile);
                    }
                    break;
                }
                case NSysView::TEvSysView::EvGetStoragePoolsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetStoragePoolsResponse::TPtr*>(&ev);
                    AddStoragePoolsToSysViewResponse(x);
                    break;
                }
                case NNodeWhiteboard::TEvWhiteboard::EvBSGroupStateResponse: {
                    auto* x = reinterpret_cast<NNodeWhiteboard::TEvWhiteboard::TEvBSGroupStateResponse::TPtr*>(&ev);
                    ChangeGroupStateResponse(x);
                    break;
                }
                case TEvBlobStorage::EvNodeWardenStorageConfig: {
                    auto* x = reinterpret_cast<TEvNodeWardenStorageConfig::TPtr*>(&ev);
                    (*x)->Get()->BridgeInfo = bridgeInfo;
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

    Ydb::Monitoring::SelfCheckResult RequestHcInBridgeMode(const TVector<TVDisks>& groupVdisks,
            std::array<NKikimrBridge::TClusterState::EPileState, 2> pileStates, bool inlinePile = false)
    {
        return RequestHcWithBridgeVdisks(groupVdisks, 1, false, pileStates, inlinePile, true);
    }

    struct TLocationFilter {
        std::vector<std::function<bool(const Ydb::Monitoring::Location&)>> Filters;

        bool operator()(const Ydb::Monitoring::Location& location) {
            auto reverseApply = [&](auto&& func) { return func(location); };
            return std::ranges::all_of(Filters, reverseApply);
        }

        TLocationFilter& Pool(const TString& name) {
            Filters.emplace_back([=](auto&& location) { return location.storage().pool().name() == name; });
            return *this;
        }

        TLocationFilter& Pile(const TString& name) {
            Filters.emplace_back([=](auto&& location) { return location.storage().pool().group().pile().name() == name || location.compute().pile().name() == name; });
            return *this;
        }

        TLocationFilter& TabletType(const TString& type) {
            Filters.emplace_back([=](auto&& location) { return location.compute().tablet().type() == type; });
            return *this;
        }
    };

    void CheckHcResultHasIssuesWithStatus(const Ydb::Monitoring::SelfCheckResult& result, const TString& type,
                                          const Ydb::Monitoring::StatusFlag::Status expectingStatus, ui32 total,
                                          TLocationFilter locationFilter = {}) {
        int issuesCount = 0;
        for (const auto& issue_log : result.Getissue_log()) {
            if (issue_log.type() == type && locationFilter(issue_log.location()) && issue_log.status() == expectingStatus) {
                issuesCount++;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL_C(issuesCount, total, "Wrong issues count for " << type << " with expecting status " << expectingStatus);
    }

    void CheckHcResultHasIssueWithMessage(const Ydb::Monitoring::SelfCheckResult& result, const TString& type,
            Ydb::Monitoring::StatusFlag::Status status, const TString& message)
    {
        const bool found = std::ranges::any_of(result.issue_log(), [&](const auto& issue) {
            return issue.type() == type && issue.status() == status && issue.message() == message;
        });
        UNIT_ASSERT_C(found, "No " << type << " issue with status " << status << " and message '" << message << "'");
    }

    // checks that issues of the given types are linked to each other by reason, from the first type down to the last one
    void CheckHcResultHasReasonChain(const Ydb::Monitoring::SelfCheckResult& result, const std::vector<TString>& types) {
        std::unordered_map<TString, const Ydb::Monitoring::IssueLog*> issueById;
        for (const auto& issueLog : result.Getissue_log()) {
            issueById[issueLog.id()] = &issueLog;
        }
        const Ydb::Monitoring::IssueLog* issue = nullptr;
        for (const auto& issueLog : result.Getissue_log()) {
            if (issueLog.type() == types.front()) {
                UNIT_ASSERT_C(!issue, "Found more than one issue of type " << types.front());
                issue = &issueLog;
            }
        }
        UNIT_ASSERT_C(issue, "Issue of type " << types.front() << " not found");
        for (auto it = std::next(types.begin()); it != types.end(); ++it) {
            UNIT_ASSERT_C(issue->reason_size() > 0, "Issue " << issue->type() << " has no reasons");
            const auto& reasonId = issue->reason(0);
            const auto reasonIt = issueById.find(reasonId);
            UNIT_ASSERT_C(reasonIt != issueById.end(), "Reason " << reasonId << " of " << issue->type() << " issue not found");
            issue = reasonIt->second;
            UNIT_ASSERT_VALUES_EQUAL(issue->type(), *it);
        }
    }

    struct TGroupLayout {
        TString Erasure;
        ui32 Realms;
        ui32 Domains;

        ui32 VDisks() const {
            return Realms * Domains;
        }
    };

    // a mirror-3-dc group has 9 vdisks in 3 fail realms (data centers) of 3 fail domains (nodes) each
    TGroupLayout Mirror3DcLayout() {
        return {NHealthCheck::MIRROR_3_DC, 3, 3};
    }
    // a block-4-2 group has 8 vdisks in a single fail realm of 8 fail domains (nodes)
    TGroupLayout Block42Layout() { 
        return {NHealthCheck::BLOCK_4_2, 1, 8};
    }
    // the nodes are spread over the data centers in the same way for any layout
    const ui32 NODES_PER_DATA_CENTER = 3;

    void SetNodeDataCenters(TEvInterconnect::TEvNodesInfo::TPtr* ev, ui32 firstNodeId, ui32 nodesPerDataCenter) {
        auto nodes = MakeIntrusive<TIntrusiveVector<TEvInterconnect::TNodeInfo>>((*ev)->Get()->Nodes);
        for (auto& node : *nodes) {
            TString dataCenter = TStringBuilder() << "dc-" << (node.NodeId - firstNodeId) / nodesPerDataCenter;
            node.Location = TNodeLocation(dataCenter, {}, TStringBuilder() << "rack-" << node.NodeId, "unit-1");
        }
        auto newEv = IEventHandle::Downcast<TEvInterconnect::TEvNodesInfo>(
            new IEventHandle((*ev)->Recipient, (*ev)->Sender, new TEvInterconnect::TEvNodesInfo(nodes))
        );
        ev->Swap(newEv);
    }

    void AddGroupsToSysViewResponse(NSysView::TEvSysView::TEvGetGroupsResponse::TPtr* ev, const TString& erasure, size_t groupCount) {
        auto& record = (*ev)->Get()->Record;
        auto entrySample = record.entries(0);
        record.clear_entries();

        auto groupId = GROUP_START_ID;
        for (size_t i = 0; i < groupCount; ++i) {
            auto* entry = record.add_entries();
            entry->CopyFrom(entrySample);
            entry->mutable_key()->set_groupid(groupId++);
            entry->mutable_info()->set_erasurespeciesv2(erasure);
            entry->mutable_info()->set_storagepoolid(1);
            entry->mutable_info()->set_generation(DEFAULT_GROUP_GENERATION);
        }
    }

    // every group takes the same set of nodes: the vdisk of the fail domain d of the fail realm r lives on
    // the node with the index r * layout.Domains + d
    void AddVSlotsToSysViewResponse(NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr* ev, const TGroupLayout& layout,
                                             const TVector<THashSet<ui32>>& failedVDisksPerGroup, ui32 firstNodeId) {
        auto& record = (*ev)->Get()->Record;
        auto entrySample = record.entries(0);
        record.clear_entries();

        const auto* descriptor = NKikimrBlobStorage::EVDiskStatus_descriptor();
        auto groupId = GROUP_START_ID;
        auto vslotId = VCARD_START_ID;
        for (const auto& failedVDisks : failedVDisksPerGroup) {
            for (ui32 vDiskIdx = 0; vDiskIdx < layout.VDisks(); ++vDiskIdx) {
                auto* entry = record.add_entries();
                entry->CopyFrom(entrySample);
                entry->mutable_key()->set_nodeid(firstNodeId + vDiskIdx);
                entry->mutable_key()->set_pdiskid(failedVDisks.contains(vDiskIdx) ? PDISK_START_ID + 1 : PDISK_START_ID);
                entry->mutable_key()->set_vslotid(vslotId++);
                entry->mutable_info()->set_groupid(groupId);
                entry->mutable_info()->set_groupgeneration(DEFAULT_GROUP_GENERATION);
                entry->mutable_info()->set_failrealm(vDiskIdx / layout.Domains);
                entry->mutable_info()->set_faildomain(vDiskIdx % layout.Domains);
                entry->mutable_info()->set_vdisk(vDiskIdx);
                auto status = failedVDisks.contains(vDiskIdx) ? NKikimrBlobStorage::EVDiskStatus::ERROR
                                                              : NKikimrBlobStorage::EVDiskStatus::READY;
                entry->mutable_info()->set_statusv2(descriptor->FindValueByNumber(status)->name());
            }
            ++groupId;
        }
    }

    Ydb::Monitoring::SelfCheckResult RequestHcWithGroups(const TGroupLayout& layout, const TVector<THashSet<ui32>>& failedVDisksPerGroup) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port)
                .SetNodeCount(layout.VDisks())
                .SetUseRealThreads(false)
                .SetDomainName("Root");
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        TClient client(settings);
        TTestActorRuntime& runtime = *server.GetRuntime();

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;
        const ui32 firstNodeId = runtime.GetNodeId(0);

        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvSchemeShard::EvDescribeSchemeResult: {
                    auto *x = reinterpret_cast<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr*>(&ev);
                    ChangeDescribeSchemeResult(x);
                    break;
                }
                case TEvInterconnect::EvNodesInfo: {
                    auto *x = reinterpret_cast<TEvInterconnect::TEvNodesInfo::TPtr*>(&ev);
                    SetNodeDataCenters(x, firstNodeId, NODES_PER_DATA_CENTER);
                    break;
                }
                case NSysView::TEvSysView::EvGetVSlotsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr*>(&ev);
                    AddVSlotsToSysViewResponse(x, layout, failedVDisksPerGroup, firstNodeId);
                    break;
                }
                case NSysView::TEvSysView::EvGetPDisksResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetPDisksResponse::TPtr*>(&ev);
                    AddPDisksToSysViewResponse(x, runtime, TVDisks{{}, {NKikimrBlobStorage::ERROR, NKikimrBlobStorage::TPDiskState::DeviceIoError}}, .5);
                    break;
                }
                case NSysView::TEvSysView::EvGetGroupsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetGroupsResponse::TPtr*>(&ev);
                    AddGroupsToSysViewResponse(x, layout.Erasure, failedVDisksPerGroup.size());
                    break;
                }
                case NSysView::TEvSysView::EvGetStoragePoolsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetStoragePoolsResponse::TPtr*>(&ev);
                    AddStoragePoolsToSysViewResponse(x);
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

    const Ydb::Monitoring::IssueLog* FindIssue(const Ydb::Monitoring::SelfCheckResult& result, const TString& type,
                                               TLocationFilter locationFilter = {}) {
        for (const auto& issueLog : result.issue_log()) {
            if (issueLog.type() == type && locationFilter(issueLog.location())) {
                return &issueLog;
            }
        }
        return nullptr;
    }

    Y_UNIT_TEST(Mirror3DcDataCenterOutageMergesVDisksOfAllGroups) {
        // the whole first data center is down, so every group is missing all the disks of its first fail realm
        TVector<THashSet<ui32>> failedVDisksPerGroup(3, THashSet<ui32>{0, 1, 2});
        auto result = RequestHcWithGroups(Mirror3DcLayout(), failedVDisksPerGroup);
        Ctest << result.ShortDebugString() << Endl;

        auto poolFilter = TLocationFilter().Pool(STORAGE_POOL_NAME);
        // all the groups are degraded in the same way, and so are all the disks of the failed data center
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::YELLOW, 1, poolFilter);
        CheckHcResultHasIssuesWithStatus(result, "VDISK", Ydb::Monitoring::StatusFlag::RED, 1, poolFilter);
        CheckHcResultHasReasonChain(result, {"STORAGE_POOL", "STORAGE_GROUP", "VDISK"});

        const auto* vDiskIssue = FindIssue(result, "VDISK", poolFilter);
        UNIT_ASSERT(vDiskIssue);
        UNIT_ASSERT_VALUES_EQUAL(vDiskIssue->message(), "VDisks are not available");
        const ui32 failedVDiskCount = failedVDisksPerGroup.size() * Mirror3DcLayout().Domains;
        UNIT_ASSERT_VALUES_EQUAL(vDiskIssue->count(), failedVDiskCount);
        UNIT_ASSERT_VALUES_EQUAL(vDiskIssue->location().storage().pool().group().vdisk().id_size(), failedVDiskCount);
        // the merged issue covers several nodes, so it is not about a single one anymore
        UNIT_ASSERT_VALUES_EQUAL(vDiskIssue->location().storage().node().id(), 0);
    }

    Y_UNIT_TEST(Mirror3DcSingleDiskFailuresAreMergedByNode) {
        // every group is missing a single disk of its first fail realm, and these disks are on different nodes
        TVector<THashSet<ui32>> failedVDisksPerGroup = {{0}, {1}, {2}};
        auto result = RequestHcWithGroups(Mirror3DcLayout(), failedVDisksPerGroup);
        Ctest << result.ShortDebugString() << Endl;

        auto poolFilter = TLocationFilter().Pool(STORAGE_POOL_NAME);
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::YELLOW, 1, poolFilter);
        // no group lost more than one disk in the same fail realm, so the disk issues are still kept per node
        CheckHcResultHasIssuesWithStatus(result, "VDISK", Ydb::Monitoring::StatusFlag::RED, 3, poolFilter);

        const auto* vDiskIssue = FindIssue(result, "VDISK", poolFilter);
        UNIT_ASSERT(vDiskIssue);
        UNIT_ASSERT_VALUES_EQUAL(vDiskIssue->message(), "VDisk is not available");
        UNIT_ASSERT(vDiskIssue->location().storage().node().id() != 0);
    }

    Y_UNIT_TEST(Block42DisksOfSameDataCenterAreMergedByNode) {
        // every group is missing two disks of its only fail realm, and both are on the nodes of the same data center
        TVector<THashSet<ui32>> failedVDisksPerGroup(3, THashSet<ui32>{0, 1});
        auto result = RequestHcWithGroups(Block42Layout(), failedVDisksPerGroup);
        Ctest << result.ShortDebugString() << Endl;

        auto poolFilter = TLocationFilter().Pool(STORAGE_POOL_NAME);
        // fail realms are not data centers in block-4-2, so the disk issues are kept per node
        CheckHcResultHasIssuesWithStatus(result, "VDISK", Ydb::Monitoring::StatusFlag::RED, 2, poolFilter);

        const auto* vDiskIssue = FindIssue(result, "VDISK", poolFilter);
        UNIT_ASSERT(vDiskIssue);
        UNIT_ASSERT_VALUES_EQUAL(vDiskIssue->count(), failedVDisksPerGroup.size());
        UNIT_ASSERT(vDiskIssue->location().storage().node().id() != 0);
    }

    Y_UNIT_TEST(Mirror3DcSharedPDiskKeepsAllReasonLinks) {
        // the first group lost a single disk, the second one lost two disks of the same fail realm, and the lost disks
        // of the first fail domain share the same broken pdisk: the disks of the second group are merged across the nodes
        TVector<THashSet<ui32>> failedVDisksPerGroup = {{0}, {0, 1}};
        auto result = RequestHcWithGroups(Mirror3DcLayout(), failedVDisksPerGroup);
        Ctest << result.ShortDebugString() << Endl;

        auto poolFilter = TLocationFilter().Pool(STORAGE_POOL_NAME);
        CheckHcResultHasIssuesWithStatus(result, "VDISK", Ydb::Monitoring::StatusFlag::RED, 2, poolFilter);

        THashMap<TString, const Ydb::Monitoring::IssueLog*> issueById;
        for (const auto& issueLog : result.issue_log()) {
            issueById[issueLog.id()] = &issueLog;
        }
        for (const auto& issueLog : result.issue_log()) {
            for (const auto& reason : issueLog.reason()) {
                UNIT_ASSERT_C(issueById.contains(reason), "issue " << issueLog.id() << " has a missing reason " << reason);
            }
            if (issueLog.type() == "VDISK" && poolFilter(issueLog.location())) {
                // every failed vdisk is on a broken pdisk, so none of the vdisk issues can lose its cause
                UNIT_ASSERT_C(issueLog.reason_size() > 0, "vdisk issue " << issueLog.id() << " has no reasons");
                for (const auto& reason : issueLog.reason()) {
                    UNIT_ASSERT_VALUES_EQUAL(issueById[reason]->type(), "PDISK");
                }
            }
        }
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

    void ShardsQuotaTest(ui64 usage, ui64 quota, ui64 storageIssuesNumber, Ydb::Monitoring::StatusFlag::Status status = Ydb::Monitoring::StatusFlag::GREEN) {
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
            Ctest << issue_log.ShortDebugString() << Endl;
            if (issue_log.type() == "COMPUTE_QUOTA" && issue_log.reason_size() == 0 && issue_log.status() == status) {
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

    Y_UNIT_TEST(YellowGroupIssueWhenPartialGroupStatus) {
        auto result = RequestHcWithVdisks(NKikimrBlobStorage::TGroupStatus::PARTIAL, TVDisks{NKikimrBlobStorage::ERROR});
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::YELLOW, 1, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasReasonChain(result, {"STORAGE_POOL", "STORAGE_GROUP", "VDISK"});
    }

    Y_UNIT_TEST(BlueGroupIssueWhenPartialGroupStatusAndReplicationDisks) {
        auto result = RequestHcWithVdisks(NKikimrBlobStorage::TGroupStatus::PARTIAL, TVDisks{NKikimrBlobStorage::REPLICATING});
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::BLUE, 1, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasReasonChain(result, {"STORAGE_POOL", "STORAGE_GROUP", "VDISK"});
    }

    Y_UNIT_TEST(NonStaticGroupKeepsBlueIssueAndGetsPhantomOnlyHintFromSysView) {
        auto result = RequestHcWithVdisks(NKikimrBlobStorage::TGroupStatus::PARTIAL, TVDisks{PhantomOnlyVDisk()},
            false, 0, true, true);
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::BLUE, 1, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasIssuesWithStatus(result, "VDISK", Ydb::Monitoring::StatusFlag::BLUE, 1, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasIssuesWithStatus(result, "HINT-PHANTOM-ONLY-VDISK", Ydb::Monitoring::StatusFlag::UNSPECIFIED, 1, TLocationFilter().Pool("/Root:test"));
    }

    Y_UNIT_TEST(NonStaticGroupDoesNotUseWhiteboardFallbackForPhantomOnlyHint) {
        auto result = RequestHcWithVdisks(NKikimrBlobStorage::TGroupStatus::PARTIAL, TVDisks{PhantomOnlyVDisk()},
            false, 0, false, true);
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::BLUE, 1, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasIssuesWithStatus(result, "VDISK", Ydb::Monitoring::StatusFlag::BLUE, 1, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasIssuesWithStatus(result, "HINT-PHANTOM-ONLY-VDISK", Ydb::Monitoring::StatusFlag::UNSPECIFIED, 0, TLocationFilter().Pool("/Root:test"));
    }

    Y_UNIT_TEST(PhantomOnlyHintRequiresReturnHints) {
        auto result = RequestHcWithVdisks(NKikimrBlobStorage::TGroupStatus::PARTIAL, TVDisks{PhantomOnlyVDisk()},
            false, 0, true);
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::BLUE, 1, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasIssuesWithStatus(result, "VDISK", Ydb::Monitoring::StatusFlag::BLUE, 1, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasIssuesWithStatus(result, "HINT-PHANTOM-ONLY-VDISK", Ydb::Monitoring::StatusFlag::UNSPECIFIED, 0, TLocationFilter().Pool("/Root:test"));
    }

    Y_UNIT_TEST(OrangeGroupIssueWhenDegradedGroupStatus) {
        auto result = RequestHcWithVdisks(NKikimrBlobStorage::TGroupStatus::DEGRADED, TVDisks{2, NKikimrBlobStorage::ERROR});
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::ORANGE, 1, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasReasonChain(result, {"STORAGE_POOL", "STORAGE_GROUP", "VDISK"});
    }

    Y_UNIT_TEST(RedGroupIssueWhenDisintegratedGroupStatus) {
        auto result = RequestHcWithVdisks(NKikimrBlobStorage::TGroupStatus::DISINTEGRATED, TVDisks{3, NKikimrBlobStorage::ERROR});
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 1, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasReasonChain(result, {"STORAGE_POOL", "STORAGE_GROUP", "VDISK"});
    }

    Y_UNIT_TEST(StaticGroupIssue) {
        auto result = RequestHcWithVdisks(NKikimrBlobStorage::TGroupStatus::PARTIAL, TVDisks{NKikimrBlobStorage::ERROR}, /*forStatic*/ true);
        Cerr << result.ShortDebugString() << Endl;
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::YELLOW, 1, TLocationFilter().Pool("static"));
        CheckHcResultHasReasonChain(result, {"STORAGE_POOL", "STORAGE_GROUP", "VDISK"});
    }

    Y_UNIT_TEST(GreenStatusWhenCreatingGroup) {
        std::optional<NKikimrBlobStorage::EVDiskStatus> emptyStatus;
        auto result = RequestHcWithVdisks(NKikimrBlobStorage::TGroupStatus::PARTIAL, TVDisks{8, emptyStatus});
        Cerr << result.ShortDebugString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(result.self_check_result(), Ydb::Monitoring::SelfCheck::GOOD);
    }

    Y_UNIT_TEST(GreenStatusWhenInitPending) {
        auto result = RequestHcWithVdisks(NKikimrBlobStorage::TGroupStatus::PARTIAL, TVDisks{8, NKikimrBlobStorage::INIT_PENDING});
        Cerr << result.ShortDebugString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(result.self_check_result(), Ydb::Monitoring::SelfCheck::GOOD);
    }

    Y_UNIT_TEST(IgnoreOtherGenerations) {
        TVDisks vdisks;
        vdisks.emplace_back(NKikimrBlobStorage::ERROR, DEFAULT_GROUP_GENERATION - 1);
        vdisks.emplace_back(NKikimrBlobStorage::READY, DEFAULT_GROUP_GENERATION);
        vdisks.emplace_back(NKikimrBlobStorage::ERROR, DEFAULT_GROUP_GENERATION + 1);
        auto result = RequestHcWithVdisks(NKikimrBlobStorage::TGroupStatus::PARTIAL, vdisks);
        Cerr << result.ShortDebugString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(result.self_check_result(), Ydb::Monitoring::SelfCheck::GOOD);
    }

    Y_UNIT_TEST(OnlyDiskIssueOnSpaceIssues) {
        auto result = RequestHcWithVdisks(NKikimrBlobStorage::TGroupStatus::PARTIAL, TVDisks{3, NKikimrBlobStorage::READY}, false, 0.9);
        Cerr << result.ShortDebugString() << Endl;
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::YELLOW, 0, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 0, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasIssuesWithStatus(result, "PDISK", Ydb::Monitoring::StatusFlag::YELLOW, 3);
    }

    Y_UNIT_TEST(OnlyDiskIssueOnInitialPDisks) {
        auto result = RequestHcWithVdisks(NKikimrBlobStorage::TGroupStatus::PARTIAL, TVDisks{3, {NKikimrBlobStorage::READY, NKikimrBlobStorage::TPDiskState::DeviceIoError}});
        Cerr << result.ShortDebugString() << Endl;
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::YELLOW, 0, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::ORANGE, 0, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 0, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasIssuesWithStatus(result, "PDISK", Ydb::Monitoring::StatusFlag::RED, 3);
    }

    Y_UNIT_TEST(OnlyDiskIssueOnFaultyPDisks) {
        auto result = RequestHcWithVdisks(NKikimrBlobStorage::TGroupStatus::PARTIAL, TVDisks{3, {NKikimrBlobStorage::READY, NKikimrBlobStorage::FAULTY}});
        Cerr << result.ShortDebugString() << Endl;
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::YELLOW, 0, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::ORANGE, 0, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 0, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasIssuesWithStatus(result, "PDISK", Ydb::Monitoring::StatusFlag::RED, 3);
    }

    Y_UNIT_TEST(BridgeGroupNoIssues) {
        auto result = RequestHcWithBridgeVdisks({2, TVDisks{8, NKikimrBlobStorage::READY}});
        Cerr << result.ShortDebugString() << Endl;
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::YELLOW, 0, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::ORANGE, 0, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 0, TLocationFilter().Pool("/Root:test"));
    }

    Y_UNIT_TEST(BridgeOrdinaryGroupHasNoPileAttribution) {
        const auto result = RequestHcWithBridgeVdisks({TVDisks{3, NKikimrBlobStorage::ERROR}}, 0, true);
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 1,
            TLocationFilter().Pool("/Root:test"));
        for (const auto& issue : result.issue_log()) {
            UNIT_ASSERT_VALUES_EQUAL(issue.message().find("in pile"), TString::npos);
            UNIT_ASSERT(issue.location().storage().pool().group().pile().name().empty());
        }
    }

    Y_UNIT_TEST(BridgeGroupDegradedInBothPiles) {
        auto result = RequestHcWithBridgeVdisks({2, TVDisks{2, NKikimrBlobStorage::ERROR}});
        Cerr << result.ShortDebugString() << Endl;
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::ORANGE, 1, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasIssuesWithStatus(result, "BRIDGE_GROUP", Ydb::Monitoring::StatusFlag::ORANGE, 1, TLocationFilter().Pool("/Root:test").Pile("pile0"));
        CheckHcResultHasIssuesWithStatus(result, "BRIDGE_GROUP", Ydb::Monitoring::StatusFlag::ORANGE, 1, TLocationFilter().Pool("/Root:test").Pile("pile1"));
        CheckHcResultHasReasonChain(result, {"STORAGE_POOL", "STORAGE_GROUP", "BRIDGE_GROUP", "VDISK"});
    }

    Y_UNIT_TEST(BridgeGroupDegradedInOnePile) {
        auto result = RequestHcWithBridgeVdisks({TVDisks{2, NKikimrBlobStorage::ERROR}, TVDisks{NKikimrBlobStorage::REPLICATING}});
        Cerr << result.ShortDebugString() << Endl;
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::ORANGE, 1);
        CheckHcResultHasIssuesWithStatus(result, "BRIDGE_GROUP", Ydb::Monitoring::StatusFlag::ORANGE, 1, TLocationFilter().Pool("/Root:test").Pile("pile0"));
        CheckHcResultHasIssuesWithStatus(result, "BRIDGE_GROUP", Ydb::Monitoring::StatusFlag::ORANGE, 0, TLocationFilter().Pool("/Root:test").Pile("pile1"));
        CheckHcResultHasReasonChain(result, {"STORAGE_POOL", "STORAGE_GROUP", "BRIDGE_GROUP", "VDISK"});
    }

    Y_UNIT_TEST(BridgeGroupDeadInOnePile) {
        auto result = RequestHcWithBridgeVdisks({TVDisks{3, NKikimrBlobStorage::ERROR}, TVDisks{8, NKikimrBlobStorage::READY}});
        Cerr << result.ShortDebugString() << Endl;
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::ORANGE, 1);
        CheckHcResultHasIssuesWithStatus(result, "BRIDGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 1, TLocationFilter().Pool("/Root:test").Pile("pile0"));
        CheckHcResultHasIssuesWithStatus(result, "BRIDGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 0, TLocationFilter().Pool("/Root:test").Pile("pile1"));
        CheckHcResultHasReasonChain(result, {"STORAGE_POOL", "STORAGE_GROUP", "BRIDGE_GROUP", "VDISK"});
    }

    Y_UNIT_TEST(BridgeGroupDeadInBothPiles) {
        auto result = RequestHcWithBridgeVdisks({2, TVDisks{3, NKikimrBlobStorage::ERROR}});
        Cerr << result.ShortDebugString() << Endl;
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 1, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasIssuesWithStatus(result, "BRIDGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 1, TLocationFilter().Pool("/Root:test").Pile("pile0"));
        CheckHcResultHasReasonChain(result, {"STORAGE_POOL", "STORAGE_GROUP", "BRIDGE_GROUP", "VDISK"});
        CheckHcResultHasIssuesWithStatus(result, "BRIDGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 1, TLocationFilter().Pool("/Root:test").Pile("pile1"));;
    }

    Y_UNIT_TEST(BridgeTwoGroups) {
        TVector<TVDisks> disks;
        disks.emplace_back(3, NKikimrBlobStorage::ERROR);
        disks.emplace_back();
        disks.emplace_back();
        disks.emplace_back(3, NKikimrBlobStorage::ERROR);
        disks.emplace_back();
        auto result = RequestHcWithBridgeVdisks(disks, 2);
        Cerr << result.ShortDebugString() << Endl;
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::ORANGE, 1, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasReasonChain(result, {"STORAGE_POOL", "STORAGE_GROUP", "BRIDGE_GROUP", "VDISK"});
    }

    Y_UNIT_TEST(BridgeConvertedGroupWithDisconnectedPile) {
        // pile0 part is the converted group itself (degraded), pile1 part is dead but not synchronized, so it doesn't count
        auto result = RequestHcWithBridgeVdisks({TVDisks{2, NKikimrBlobStorage::ERROR}, TVDisks{3, NKikimrBlobStorage::ERROR}}, 1, false,
            {NKikimrBridge::TClusterState::SYNCHRONIZED, NKikimrBridge::TClusterState::DISCONNECTED}, true);
        Cerr << result.ShortDebugString() << Endl;
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::ORANGE, 1, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasIssuesWithStatus(result, "BRIDGE_GROUP", Ydb::Monitoring::StatusFlag::ORANGE, 1, TLocationFilter().Pool("/Root:test").Pile("pile0"));
        CheckHcResultHasIssuesWithStatus(result, "BRIDGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 0);
        CheckHcResultHasReasonChain(result, {"STORAGE_POOL", "STORAGE_GROUP", "BRIDGE_GROUP", "VDISK"});
    }

    Y_UNIT_TEST(BridgeGroupNoSynchronizedPiles) {
        auto result = RequestHcWithBridgeVdisks({2, TVDisks{8, NKikimrBlobStorage::READY}}, 1, false,
            {NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1, NKikimrBridge::TClusterState::DISCONNECTED});
        Cerr << result.ShortDebugString() << Endl;
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 1, TLocationFilter().Pool("/Root:test"));
    }

    const auto checkDeadStage2Pile = [](bool inlinePile) {
        const auto result = RequestHcInBridgeMode({TVDisks{8, NKikimrBlobStorage::READY}, TVDisks{3, NKikimrBlobStorage::ERROR}},
            {NKikimrBridge::TClusterState::SYNCHRONIZED, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2}, inlinePile);
        Cerr << result.ShortDebugString() << Endl;
        CheckHcResultHasIssuesWithStatus(result, "BRIDGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 1, TLocationFilter().Pool("/Root:test").Pile("pile1"));
        CheckHcResultHasIssuesWithStatus(result, "BRIDGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 0, TLocationFilter().Pool("/Root:test").Pile("pile0"));
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::ORANGE, 1, TLocationFilter().Pool("/Root:test").Pile("pile1"));
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 0, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasIssueWithMessage(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::ORANGE, "Group failed in pile pile1");
        CheckHcResultHasReasonChain(result, {"STORAGE_POOL", "STORAGE_GROUP", "BRIDGE_GROUP", "VDISK"});
    };

    Y_UNIT_TEST(BridgeGroupDeadInStage2Pile) {
        checkDeadStage2Pile(false);
    }

    Y_UNIT_TEST(BridgeConvertedGroupDeadInStage2Pile) {
        checkDeadStage2Pile(true);
    }

    Y_UNIT_TEST(BridgeGroupDeadInSynchronizedAndStage2Piles) {
        const auto result = RequestHcInBridgeMode({TVDisks{3, NKikimrBlobStorage::ERROR}, TVDisks{3, NKikimrBlobStorage::ERROR}},
            {NKikimrBridge::TClusterState::SYNCHRONIZED, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2});
        Cerr << result.ShortDebugString() << Endl;
        CheckHcResultHasIssuesWithStatus(result, "BRIDGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 1, TLocationFilter().Pool("/Root:test").Pile("pile1"));
        CheckHcResultHasIssuesWithStatus(result, "BRIDGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 1, TLocationFilter().Pool("/Root:test").Pile("pile0"));
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 1, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 0, TLocationFilter().Pool("/Root:test").Pile("pile0"));
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 0, TLocationFilter().Pool("/Root:test").Pile("pile1"));
        CheckHcResultHasIssueWithMessage(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, "Group dead in all piles");
    }

    const auto checkDeadSynchronizedPile = [](NKikimrBridge::TClusterState::EPileState otherState, bool inlinePile) {
        const auto result = RequestHcInBridgeMode({TVDisks{3, NKikimrBlobStorage::ERROR}, TVDisks{8, NKikimrBlobStorage::READY}},
            {NKikimrBridge::TClusterState::SYNCHRONIZED, otherState}, inlinePile);
        Cerr << result.ShortDebugString() << Endl;
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 1, TLocationFilter().Pool("/Root:test").Pile("pile0"));
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::ORANGE, 0, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasIssuesWithStatus(result, "BRIDGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 1, TLocationFilter().Pool("/Root:test").Pile("pile0"));
        CheckHcResultHasIssuesWithStatus(result, "BRIDGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 0, TLocationFilter().Pool("/Root:test").Pile("pile1"));
        CheckHcResultHasIssueWithMessage(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, "Group failed in pile pile0");
        CheckHcResultHasReasonChain(result, {"STORAGE_POOL", "STORAGE_GROUP", "BRIDGE_GROUP", "VDISK"});
    };

    Y_UNIT_TEST(BridgeGroupDeadInSynchronizedPileWithHealthyStage2Pile) {
        checkDeadSynchronizedPile(NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2, false);
    }

    Y_UNIT_TEST(BridgeConvertedGroupDeadInSynchronizedPileWithHealthyStage2Pile) {
        checkDeadSynchronizedPile(NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2, true);
    }

    Y_UNIT_TEST(BridgeGroupDeadInSynchronizedPileWithHealthySuspendedPile) {
        checkDeadSynchronizedPile(NKikimrBridge::TClusterState::SUSPENDED, false);
    }

    Y_UNIT_TEST(BridgeGroupHealthyStage2PileHasNoIssues) {
        const auto result = RequestHcInBridgeMode({2, TVDisks{8, NKikimrBlobStorage::READY}},
            {NKikimrBridge::TClusterState::SYNCHRONIZED, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2});
        Cerr << result.ShortDebugString() << Endl;
        for (auto status : {Ydb::Monitoring::StatusFlag::YELLOW, Ydb::Monitoring::StatusFlag::ORANGE, Ydb::Monitoring::StatusFlag::RED}) {
            CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", status, 0, TLocationFilter().Pool("/Root:test"));
            CheckHcResultHasIssuesWithStatus(result, "BRIDGE_GROUP", status, 0, TLocationFilter().Pool("/Root:test"));
        }
    }

    const auto checkIgnoredDeadPile = [](NKikimrBridge::TClusterState::EPileState otherState) {
        const auto result = RequestHcInBridgeMode({TVDisks{8, NKikimrBlobStorage::READY}, TVDisks{3, NKikimrBlobStorage::ERROR}},
            {NKikimrBridge::TClusterState::SYNCHRONIZED, otherState});
        Cerr << result.ShortDebugString() << Endl;
        CheckHcResultHasIssuesWithStatus(result, "BRIDGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 0, TLocationFilter().Pool("/Root:test").Pile("pile1"));
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::ORANGE, 0, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 0, TLocationFilter().Pool("/Root:test"));
    };

    Y_UNIT_TEST(BridgeGroupDeadInStage1PileIsIgnored) {
        checkIgnoredDeadPile(NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
    }

    Y_UNIT_TEST(BridgeGroupDeadInSuspendedPileIsIgnored) {
        checkIgnoredDeadPile(NKikimrBridge::TClusterState::SUSPENDED);
    }

    Y_UNIT_TEST(BridgeGroupDeadInDisconnectedPileIsIgnored) {
        checkIgnoredDeadPile(NKikimrBridge::TClusterState::DISCONNECTED);
    }

    Y_UNIT_TEST(BridgeGroupStage2PartWithoutSynchronizedParts) {
        const auto result = RequestHcInBridgeMode({2, TVDisks{8, NKikimrBlobStorage::READY}},
            {NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2, NKikimrBridge::TClusterState::DISCONNECTED});
        Cerr << result.ShortDebugString() << Endl;
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 1, TLocationFilter().Pool("/Root:test"));
        CheckHcResultHasIssueWithMessage(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, "Group has no parts in synchronized piles");
        for (const auto& issue : result.issue_log()) {
            UNIT_ASSERT_UNEQUAL(issue.type(), "BRIDGE_GROUP");
        }
    }

    /* HC currently infers group status on its own, so it's never unknown
    Y_UNIT_TEST(RedGroupIssueWhenUnknownGroupStatus) {
        auto result = RequestHcWithVdisks(NKikimrBlobStorage::TGroupStatus::UNKNOWN, {});
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 1);
    }
    */

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

    void AddBadServerlessTablet(TEvHive::TEvResponseHiveInfo::TPtr* ev) {
        auto &record = (*ev)->Get()->Record;
        auto* tablet = record.MutableTablets()->Add();
        tablet->SetTabletID(1);
        tablet->MutableObjectDomain()->SetSchemeShard(SERVERLESS_DOMAIN_KEY.OwnerId);
        tablet->MutableObjectDomain()->SetPathId(SERVERLESS_DOMAIN_KEY.LocalPathId);
        tablet->SetRestartsPerPeriod(500);
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
                    AddGroupVSlotInControllerConfigResponseWithStaticGroup(x, NKikimrBlobStorage::TGroupStatus::FULL, TVDisks(1));
                    break;
                }
                case NSysView::TEvSysView::EvGetVSlotsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr*>(&ev);
                    AddVSlotsToSysViewResponse(x, 1, TVDisks(1));
                    break;
                }
                case NSysView::TEvSysView::EvGetGroupsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetGroupsResponse::TPtr*>(&ev);
                    AddGroupsToSysViewResponse(x);
                    break;
                }
                case NSysView::TEvSysView::EvGetPDisksResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetPDisksResponse::TPtr*>(&ev);
                    SetPDisksStateNormalToSysViewResponse(x);
                    break;
                }
                case NSysView::TEvSysView::EvGetStoragePoolsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetStoragePoolsResponse::TPtr*>(&ev);
                    AddStoragePoolsToSysViewResponse(x);
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
                    AddGroupVSlotInControllerConfigResponseWithStaticGroup(x, NKikimrBlobStorage::TGroupStatus::FULL, TVDisks(1));
                    break;
                }
                case NSysView::TEvSysView::EvGetVSlotsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr*>(&ev);
                    AddVSlotsToSysViewResponse(x, 1, TVDisks(1));
                    break;
                }
                case NSysView::TEvSysView::EvGetGroupsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetGroupsResponse::TPtr*>(&ev);
                    AddGroupsToSysViewResponse(x);
                    break;
                }
                case NSysView::TEvSysView::EvGetStoragePoolsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetStoragePoolsResponse::TPtr*>(&ev);
                    AddStoragePoolsToSysViewResponse(x);
                    break;
                }
                case NSysView::TEvSysView::EvGetPDisksResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetPDisksResponse::TPtr*>(&ev);
                    SetPDisksStateNormalToSysViewResponse(x);
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
                    if (!firstConsoleResponse) {
                        ChangeGetTenantStatusResponse(x, "/Root/serverless");
                    } else {
                        firstConsoleResponse = false;
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
                    AddGroupVSlotInControllerConfigResponseWithStaticGroup(x, NKikimrBlobStorage::TGroupStatus::FULL, TVDisks(1));
                    break;
                }
                case NSysView::TEvSysView::EvGetVSlotsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr*>(&ev);
                    AddVSlotsToSysViewResponse(x, 1, TVDisks(1));
                    break;
                }
                case NSysView::TEvSysView::EvGetGroupsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetGroupsResponse::TPtr*>(&ev);
                    AddGroupsToSysViewResponse(x);
                    break;
                }
                case NSysView::TEvSysView::EvGetPDisksResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetPDisksResponse::TPtr*>(&ev);
                    SetPDisksStateNormalToSysViewResponse(x);
                    break;
                }
                case NSysView::TEvSysView::EvGetStoragePoolsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetStoragePoolsResponse::TPtr*>(&ev);
                    AddStoragePoolsToSysViewResponse(x);
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

    Y_UNIT_TEST(ServerlessBadTablets) {
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
                    if (!firstConsoleResponse) {
                        ChangeGetTenantStatusResponse(x, "/Root/serverless");
                    } else {
                        firstConsoleResponse = false;
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
                case TEvHive::EvResponseHiveInfo: {
                    auto *x = reinterpret_cast<TEvHive::TEvResponseHiveInfo::TPtr*>(&ev);
                    AddBadServerlessTablet(x);
                    break;
                }
                case TEvSchemeShard::EvDescribeSchemeResult: {
                    auto *x = reinterpret_cast<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr*>(&ev);
                    ChangeDescribeSchemeResultServerless(x);
                    break;
                }
                case TEvBlobStorage::EvControllerConfigResponse: {
                    auto *x = reinterpret_cast<TEvBlobStorage::TEvControllerConfigResponse::TPtr*>(&ev);
                    AddGroupVSlotInControllerConfigResponseWithStaticGroup(x, NKikimrBlobStorage::TGroupStatus::FULL, TVDisks(1));
                    break;
                }
                case NSysView::TEvSysView::EvGetVSlotsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr*>(&ev);
                    AddVSlotsToSysViewResponse(x, 1, TVDisks(1));
                    break;
                }
                case NSysView::TEvSysView::EvGetGroupsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetGroupsResponse::TPtr*>(&ev);
                    AddGroupsToSysViewResponse(x);
                    break;
                }
                case NSysView::TEvSysView::EvGetStoragePoolsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetStoragePoolsResponse::TPtr*>(&ev);
                    AddStoragePoolsToSysViewResponse(x);
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
        UNIT_ASSERT(HasTabletIssue(result));
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
                    AddGroupVSlotInControllerConfigResponseWithStaticGroup(x, NKikimrBlobStorage::TGroupStatus::FULL, TVDisks(1));
                    break;
                }
                case NSysView::TEvSysView::EvGetVSlotsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr*>(&ev);
                    AddVSlotsToSysViewResponse(x, 1, TVDisks(1));
                    break;
                }
                case NSysView::TEvSysView::EvGetGroupsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetGroupsResponse::TPtr*>(&ev);
                    AddGroupsToSysViewResponse(x);
                    break;
                }
                case NSysView::TEvSysView::EvGetStoragePoolsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetStoragePoolsResponse::TPtr*>(&ev);
                    AddStoragePoolsToSysViewResponse(x);
                    break;
                }
                case NSysView::TEvSysView::EvGetPDisksResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetPDisksResponse::TPtr*>(&ev);
                    SetPDisksStateNormalToSysViewResponse(x);
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
                    AddGroupVSlotInControllerConfigResponseWithStaticGroup(x, NKikimrBlobStorage::TGroupStatus::FULL, TVDisks(1));
                    break;
                }
                case NSysView::TEvSysView::EvGetVSlotsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr*>(&ev);
                    AddVSlotsToSysViewResponse(x, 1, TVDisks(1));
                    break;
                }
                case NSysView::TEvSysView::EvGetGroupsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetGroupsResponse::TPtr*>(&ev);
                    AddGroupsToSysViewResponse(x);
                    break;
                }
                case NSysView::TEvSysView::EvGetStoragePoolsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetStoragePoolsResponse::TPtr*>(&ev);
                    AddStoragePoolsToSysViewResponse(x);
                    break;
                }
                case NSysView::TEvSysView::EvGetPDisksResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetPDisksResponse::TPtr*>(&ev);
                    SetPDisksStateNormalToSysViewResponse(x);
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
                    AddGroupVSlotInControllerConfigResponseWithStaticGroup(x, NKikimrBlobStorage::TGroupStatus::FULL, TVDisks(1));
                    break;
                }
                case NSysView::TEvSysView::EvGetVSlotsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr*>(&ev);
                    AddVSlotsToSysViewResponse(x, 1, TVDisks(1));
                    break;
                }
                case NSysView::TEvSysView::EvGetGroupsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetGroupsResponse::TPtr*>(&ev);
                    AddGroupsToSysViewResponse(x);
                    break;
                }
                case NSysView::TEvSysView::EvGetStoragePoolsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetStoragePoolsResponse::TPtr*>(&ev);
                    AddStoragePoolsToSysViewResponse(x);
                    break;
                }
                case NSysView::TEvSysView::EvGetPDisksResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetPDisksResponse::TPtr*>(&ev);
                    SetPDisksStateNormalToSysViewResponse(x);
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
                    AddGroupVSlotInControllerConfigResponseWithStaticGroup(x, NKikimrBlobStorage::TGroupStatus::FULL, TVDisks(1));
                    break;
                }
                case NSysView::TEvSysView::EvGetVSlotsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr*>(&ev);
                    AddVSlotsToSysViewResponse(x, 1, TVDisks(1));
                    break;
                }
                case NSysView::TEvSysView::EvGetGroupsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetGroupsResponse::TPtr*>(&ev);
                    AddGroupsToSysViewResponse(x);
                    break;
                }
                case NSysView::TEvSysView::EvGetStoragePoolsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetStoragePoolsResponse::TPtr*>(&ev);
                    AddStoragePoolsToSysViewResponse(x);
                    break;
                }
                case NSysView::TEvSysView::EvGetPDisksResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetPDisksResponse::TPtr*>(&ev);
                    SetPDisksStateNormalToSysViewResponse(x);
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
                    AddGroupVSlotInControllerConfigResponseWithStaticGroup(x, NKikimrBlobStorage::TGroupStatus::FULL, TVDisks(1));
                    break;
                }
                case NSysView::TEvSysView::EvGetVSlotsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr*>(&ev);
                    AddVSlotsToSysViewResponse(x, 1, TVDisks(1));
                    break;
                }
                case NSysView::TEvSysView::EvGetGroupsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetGroupsResponse::TPtr*>(&ev);
                    AddGroupsToSysViewResponse(x);
                    break;
                }
                case NSysView::TEvSysView::EvGetStoragePoolsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetStoragePoolsResponse::TPtr*>(&ev);
                    AddStoragePoolsToSysViewResponse(x);
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

    Y_UNIT_TEST(NoBscResponse) {
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
            auto type = ev->GetTypeRewrite();
            if (EventSpaceBegin(TKikimrEvents::ES_SYSTEM_VIEW) <= type && type <= EventSpaceEnd(TKikimrEvents::ES_SYSTEM_VIEW)) {
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

    Y_UNIT_TEST(NoBscResponseThenPipeFailure) {
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

        const ui64 bscId = MakeBSControllerID();
        std::unordered_set<TActorId> bscClients; // We can't exactly pinpoint HC actor, so we'll settle for this
        bool bscTimeoutFired = false;
        bool wasResult = false;

        // BSC never answers
        auto sysViewObserver = runtime.AddObserver([&](TAutoPtr<IEventHandle>& ev) {
            auto type = ev->GetTypeRewrite();
            if (EventSpaceBegin(TKikimrEvents::ES_SYSTEM_VIEW) <= type && type <= EventSpaceEnd(TKikimrEvents::ES_SYSTEM_VIEW)) {
                ev.Reset();
            }
        });
        // hold the pipe connection result to inject failure after bsc timeout
        auto pipeObserver = runtime.AddObserver<TEvTabletPipe::TEvClientConnected>([&](auto&& ev) {
            if (ev->Get()->TabletId == bscId) {
                bscClients.insert(ev->Recipient);
                ev.Reset();
            }
        });
        // first wakeup of the request actor is the bsc timeout (50% of the timeout)
        auto wakeupObserver = runtime.AddObserver<TEvents::TEvWakeup>([&](auto&& ev) {
            if (bscClients.contains(ev->Recipient)) {
                bscTimeoutFired = true;
            }
        });
        auto resultObserver = runtime.AddObserver<NHealthCheck::TEvSelfCheckResult>([&](auto&&) {
            wasResult = true;
        });
        // keep at least one request in flight
        TBlockEvents<TEvHive::TEvResponseHiveInfo> blockHiveInfo(runtime);

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        auto *request = new NHealthCheck::TEvSelfCheckRequest;
        request->Request.mutable_operation_params()->mutable_operation_timeout()->set_seconds(20);
        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, request, 0));
        const TInstant start = runtime.GetCurrentTime();

        TDispatchOptions opts;
        opts.CustomFinalCondition = [&]() { return bscTimeoutFired && !blockHiveInfo.empty(); };
        runtime.DispatchEvents(opts);
        UNIT_ASSERT(!wasResult);

        // pipe to bsc fails after bsc requests were already timed out
        for (const auto& actor : bscClients) {
            runtime.Send(new IEventHandle(actor, actor,
                new TEvTabletPipe::TEvClientConnected(bscId, NKikimrProto::ERROR, TActorId(), TActorId(), false, false, 0)));
        }
        runtime.SimulateSleep(TDuration::Seconds(1));
        // bsc requests must not be accounted twice, so we still wait for hive
        UNIT_ASSERT(!wasResult);

        blockHiveInfo.Stop().Unblock();
        const auto result = runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;
        Ctest << result.ShortDebugString() << Endl;
        UNIT_ASSERT(wasResult);
        // reply comes after hive response, not by the final timeout
        UNIT_ASSERT_LT(runtime.GetCurrentTime() - start, TDuration::Seconds(20));
    }

    Y_UNIT_TEST(BridgeNoBscResponse) {
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

        auto bridgeInfo = std::make_shared<TBridgeInfo>();
        bridgeInfo->Piles.push_back(TBridgeInfo::TPile{.BridgePileId = TBridgePileId::FromPileIndex(0), .Name = "pile0", .State = NKikimrBridge::TClusterState::SYNCHRONIZED});
        bridgeInfo->Piles.push_back(TBridgeInfo::TPile{.BridgePileId = TBridgePileId::FromPileIndex(1), .Name = "pile1", .State = NKikimrBridge::TClusterState::SYNCHRONIZED});
        bridgeInfo->SelfNodePile = bridgeInfo->Piles.data();
        bridgeInfo->StaticNodeIdToPile[runtime.GetNodeId(0)] = &bridgeInfo->Piles[0];

        auto* nodeWardenStorageConfig = new TEvNodeWardenStorageConfig(std::make_shared<NKikimrBlobStorage::TStorageConfig>(), false, nullptr);
        nodeWardenStorageConfig->BridgeInfo = bridgeInfo;
        runtime.Send(new IEventHandle(GetNameserviceActorId(), runtime.AllocateEdgeActor(), nodeWardenStorageConfig));

        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            auto type = ev->GetTypeRewrite();
            if (EventSpaceBegin(TKikimrEvents::ES_SYSTEM_VIEW) <= type && type <= EventSpaceEnd(TKikimrEvents::ES_SYSTEM_VIEW)) {
                return TTestActorRuntime::EEventAction::DROP;
            }
            if (type == TEvBlobStorage::EvNodeWardenStorageConfig) {
                auto* x = reinterpret_cast<TEvNodeWardenStorageConfig::TPtr*>(&ev);
                (*x)->Get()->BridgeInfo = bridgeInfo;
            }
            if (type == NNodeWhiteboard::TEvWhiteboard::EvBSGroupStateResponse) {
                auto* x = reinterpret_cast<NNodeWhiteboard::TEvWhiteboard::TEvBSGroupStateResponse::TPtr*>(&ev);
                for (auto& group : *(*x)->Get()->Record.mutable_bsgroupstateinfo()) {
                    group.set_bridgepileid(1);
                }
            }
            if (type == NNodeWhiteboard::TEvWhiteboard::EvVDiskStateResponse) {
                auto *x = reinterpret_cast<NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateResponse::TPtr*>(&ev);
                (*x)->Get()->Record.mutable_vdiskstateinfo(0)->set_vdiskstate(NKikimrWhiteboard::EVDiskState::SyncGuidRecovery);
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

        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 1, TLocationFilter().Pool("static").Pile("pile0"));
    }

    // How a static node answers the whiteboard requests of one kind.
    enum class EWhiteboardReply {
        Received, // the reply arrives
        Missing, // the reply never arrives before the final timeout
        Unreachable, // every request is undelivered, so the node is reported unavailable
        UndeliveredOnce, // the first request is undelivered, its retry gets the reply
    };

    struct TStaticWhiteboardDisk {
        ui32 Node = 0; // index of the static node
        ui32 Realm = 0;
        ui32 Domain = 0;
        // state in a received VDisk reply; nullopt means the disk is absent from the reply
        std::optional<NKikimrWhiteboard::EVDiskState> VDiskState = NKikimrWhiteboard::EVDiskState::OK;
        // state in a received PDisk reply
        NKikimrBlobStorage::TPDiskState::E PDiskState = NKikimrBlobStorage::TPDiskState::Normal;
    };

    struct TStaticWhiteboardScenario {
        TString Erasure = NHealthCheck::BLOCK_4_2;
        ui32 StaticNodes = 1;
        std::vector<TStaticWhiteboardDisk> Disks;
        std::vector<EWhiteboardReply> VDiskReplies; // per static node
        std::vector<EWhiteboardReply> PDiskReplies; // per static node
        bool Bridge = false;
        bool MergeRecords = false;
        TDuration OperationTimeout; // zero means the server default, 20 s
    };

    std::vector<TStaticWhiteboardDisk> Block42Disks(ui32 node = 0) {
        std::vector<TStaticWhiteboardDisk> disks;
        for (ui32 domain = 0; domain < 8; ++domain) {
            disks.push_back({.Node = node, .Realm = 0, .Domain = domain});
        }
        return disks;
    }

    std::vector<TStaticWhiteboardDisk> Mirror3DcDisks(ui32 node = 0) {
        std::vector<TStaticWhiteboardDisk> disks;
        for (ui32 realm = 0; realm < 3; ++realm) {
            for (ui32 domain = 0; domain < 3; ++domain) {
                disks.push_back({.Node = node, .Realm = realm, .Domain = domain});
            }
        }
        return disks;
    }

    // a disk the node reports with a failed VDisk state
    void MakeFailed(TStaticWhiteboardDisk& disk) {
        disk.VDiskState = NKikimrWhiteboard::EVDiskState::LocalRecoveryError;
    }

    // a disk that lives on node 1, whose replies are missing
    void MakeUnknown(TStaticWhiteboardDisk& disk) {
        disk.Node = 1;
    }

    Ydb::Monitoring::SelfCheckResult RequestHcWithStaticWhiteboard(const TStaticWhiteboardScenario& scenario) {
        UNIT_ASSERT_VALUES_EQUAL(scenario.VDiskReplies.size(), scenario.StaticNodes);
        UNIT_ASSERT_VALUES_EQUAL(scenario.PDiskReplies.size(), scenario.StaticNodes);
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port)
                .SetNodeCount(scenario.StaticNodes)
                .SetDynamicNodeCount(1)
                .SetUseRealThreads(false)
                .SetDomainName("Root");
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        TClient client(settings);
        TTestActorRuntime& runtime = *server.GetRuntime();

        auto& dynamicNameserviceConfig = runtime.GetAppData().DynamicNameserviceConfig;
        dynamicNameserviceConfig->MaxStaticNodeId = runtime.GetNodeId(server.StaticNodes() - 1);
        dynamicNameserviceConfig->MinDynamicNodeId = runtime.GetNodeId(server.StaticNodes());
        dynamicNameserviceConfig->MaxDynamicNodeId = runtime.GetNodeId(server.StaticNodes() + server.DynamicNodes() - 1);

        auto bridgeInfo = std::make_shared<TBridgeInfo>();
        if (scenario.Bridge) {
            bridgeInfo->Piles.push_back(TBridgeInfo::TPile{.BridgePileId = TBridgePileId::FromPileIndex(0), .Name = "pile0", .State = NKikimrBridge::TClusterState::SYNCHRONIZED});
            bridgeInfo->SelfNodePile = bridgeInfo->Piles.data();
            for (ui32 node = 0; node < scenario.StaticNodes; ++node) {
                bridgeInfo->StaticNodeIdToPile[runtime.GetNodeId(node)] = &bridgeInfo->Piles[0];
            }
            auto* config = new TEvNodeWardenStorageConfig(std::make_shared<NKikimrBlobStorage::TStorageConfig>(), false, nullptr);
            config->BridgeInfo = bridgeInfo;
            runtime.Send(new IEventHandle(GetNameserviceActorId(), runtime.AllocateEdgeActor(), config));
        }

        ui32 groupId = 0;
        ui32 generation = 0;
        auto indexForNode = [&](TNodeId nodeId) -> std::optional<ui32> {
            for (ui32 index = 0; index < scenario.StaticNodes; ++index) {
                if (runtime.GetNodeId(index) == nodeId) {
                    return index;
                }
            }
            return std::nullopt;
        };
        // whiteboard requests seen so far, by static node index and request type
        std::map<std::pair<ui32, ui32>, ui32> requestCounts;
        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            const auto type = ev->GetTypeRewrite();
            // force the whiteboard fallback
            if (EventSpaceBegin(TKikimrEvents::ES_SYSTEM_VIEW) <= type && type <= EventSpaceEnd(TKikimrEvents::ES_SYSTEM_VIEW)) {
                return TTestActorRuntime::EEventAction::DROP;
            }
            if (type == TEvBlobStorage::EvNodeWardenStorageConfig) {
                auto* x = reinterpret_cast<TEvNodeWardenStorageConfig::TPtr*>(&ev);
                auto config = *(*x)->Get()->Config;
                auto* serviceSet = config.MutableBlobStorageConfig()->MutableServiceSet();
                if (scenario.Bridge && (serviceSet->vdisks_size() == 0 || serviceSet->pdisks_size() == 0)) {
                    // the initial empty config sent to the nameservice
                    (*x)->Get()->BridgeInfo = bridgeInfo;
                    return TTestActorRuntime::EEventAction::PROCESS;
                }
                UNIT_ASSERT(serviceSet->vdisks_size() > 0);
                UNIT_ASSERT(serviceSet->pdisks_size() > 0);
                const auto sampleVDisk = serviceSet->vdisks(0);
                const auto samplePDisk = serviceSet->pdisks(0);
                groupId = sampleVDisk.GetVDiskID().GetGroupID();
                generation = sampleVDisk.GetVDiskID().GetGroupGeneration();
                serviceSet->clear_vdisks();
                serviceSet->clear_pdisks();
                for (ui32 i = 0; i < scenario.Disks.size(); ++i) {
                    const auto& disk = scenario.Disks[i];
                    const auto nodeId = runtime.GetNodeId(disk.Node);
                    auto* pDisk = serviceSet->add_pdisks();
                    pDisk->CopyFrom(samplePDisk);
                    pDisk->SetNodeID(nodeId);
                    pDisk->SetPDiskID(PDISK_START_ID + i);
                    auto* vDisk = serviceSet->add_vdisks();
                    vDisk->CopyFrom(sampleVDisk);
                    auto* id = vDisk->MutableVDiskID();
                    id->SetRing(disk.Realm);
                    id->SetDomain(disk.Domain);
                    id->SetVDisk(0);
                    vDisk->MutableVDiskLocation()->SetNodeID(nodeId);
                    vDisk->MutableVDiskLocation()->SetPDiskID(PDISK_START_ID + i);
                }
                (*x)->Get()->Config = std::make_shared<NKikimrBlobStorage::TStorageConfig>(std::move(config));
                if (scenario.Bridge) {
                    (*x)->Get()->BridgeInfo = bridgeInfo;
                }
            }
            if (type == TEvWhiteboard::EvBSGroupStateResponse) {
                // the group must always be known, otherwise it is not evaluated
                auto* x = reinterpret_cast<TEvWhiteboard::TEvBSGroupStateResponse::TPtr*>(&ev);
                for (auto& group : *(*x)->Get()->Record.mutable_bsgroupstateinfo()) {
                    if (group.groupid() != groupId) {
                        continue;
                    }
                    group.set_erasurespecies(scenario.Erasure);
                    group.clear_vdiskids();
                    for (const auto& disk : scenario.Disks) {
                        auto* id = group.add_vdiskids();
                        id->set_groupid(groupId);
                        id->set_groupgeneration(generation);
                        id->set_ring(disk.Realm);
                        id->set_domain(disk.Domain);
                        id->set_vdisk(0);
                    }
                    if (scenario.Bridge) {
                        group.set_bridgepileid(1);
                    }
                }
            }
            if (type == TEvWhiteboard::EvVDiskStateRequest || type == TEvWhiteboard::EvPDiskStateRequest) {
                const auto node = indexForNode(ev->GetRecipientRewrite().NodeId());
                if (node) {
                    const auto reply = type == TEvWhiteboard::EvVDiskStateRequest ? scenario.VDiskReplies[*node] : scenario.PDiskReplies[*node];
                    const bool first = requestCounts[{*node, type}]++ == 0;
                    if (reply == EWhiteboardReply::Unreachable || (reply == EWhiteboardReply::UndeliveredOnce && first)) {
                        runtime.Send(new IEventHandle(ev->Sender, ev->Recipient,
                            new TEvents::TEvUndelivered(type, TEvents::TEvUndelivered::ReasonActorUnknown), 0, ev->Cookie));
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
            }
            if (type == TEvWhiteboard::EvVDiskStateResponse) {
                const auto node = indexForNode(ev->Sender.NodeId());
                if (node) {
                    if (scenario.VDiskReplies[*node] == EWhiteboardReply::Missing) {
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    auto* x = reinterpret_cast<TEvWhiteboard::TEvVDiskStateResponse::TPtr*>(&ev);
                    auto& record = (*x)->Get()->Record;
                    NKikimrWhiteboard::TVDiskStateInfo sample;
                    if (record.vdiskstateinfo_size()) {
                        sample = record.vdiskstateinfo(0);
                    }
                    record.clear_vdiskstateinfo();
                    for (ui32 i = 0; i < scenario.Disks.size(); ++i) {
                        const auto& disk = scenario.Disks[i];
                        if (disk.Node != *node || !disk.VDiskState) {
                            continue;
                        }
                        auto* state = record.add_vdiskstateinfo();
                        state->CopyFrom(sample);
                        auto* id = state->mutable_vdiskid();
                        id->set_groupid(groupId);
                        id->set_groupgeneration(generation);
                        id->set_ring(disk.Realm);
                        id->set_domain(disk.Domain);
                        id->set_vdisk(0);
                        state->set_vdiskstate(*disk.VDiskState);
                        state->set_replicated(true);
                        // real replies carry the disk space flag
                        state->set_diskspace(NKikimrWhiteboard::EFlag::Green);
                        state->set_pdiskid(PDISK_START_ID + i);
                    }
                }
            }
            if (type == TEvWhiteboard::EvPDiskStateResponse) {
                const auto node = indexForNode(ev->Sender.NodeId());
                if (node) {
                    if (scenario.PDiskReplies[*node] == EWhiteboardReply::Missing) {
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    auto* x = reinterpret_cast<TEvWhiteboard::TEvPDiskStateResponse::TPtr*>(&ev);
                    auto& record = (*x)->Get()->Record;
                    record.clear_pdiskstateinfo();
                    for (ui32 i = 0; i < scenario.Disks.size(); ++i) {
                        const auto& disk = scenario.Disks[i];
                        if (disk.Node == *node) {
                            auto* state = record.add_pdiskstateinfo();
                            state->set_pdiskid(PDISK_START_ID + i);
                            state->set_state(disk.PDiskState);
                        }
                    }
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;
        auto* request = new NHealthCheck::TEvSelfCheckRequest;
        request->Request.set_return_verbose_status(true);
        request->Request.set_merge_records(scenario.MergeRecords);
        if (scenario.OperationTimeout) {
            SetDuration(scenario.OperationTimeout, *request->Request.mutable_operation_params()->mutable_operation_timeout());
        }
        request->Database = "/Root";
        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, request, 0));
        const auto result = runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;
        Ctest << result.ShortDebugString() << Endl;
        return result;
    }

    const Ydb::Monitoring::StoragePoolStatus& GetStaticPool(const Ydb::Monitoring::SelfCheckResult& result) {
        for (const auto& db : result.database_status()) {
            for (const auto& pool : db.storage().pools()) {
                if (pool.id() == "static") {
                    return pool;
                }
            }
        }
        UNIT_FAIL("Static pool is missing");
        return result.database_status(0).storage().pools(0);
    }

    const Ydb::Monitoring::StorageGroupStatus& GetStaticWhiteboardGroup(const Ydb::Monitoring::SelfCheckResult& result) {
        const auto& pool = GetStaticPool(result);
        UNIT_ASSERT_VALUES_EQUAL(pool.groups_size(), 1);
        return pool.groups(0);
    }

    // verbose overall of the static group, of all of its VDisks and of all of their PDisks
    void CheckStaticWhiteboardOverall(const Ydb::Monitoring::SelfCheckResult& result,
            Ydb::Monitoring::StatusFlag::Status group, Ydb::Monitoring::StatusFlag::Status vdisks,
            Ydb::Monitoring::StatusFlag::Status pdisks)
    {
        const auto& status = GetStaticWhiteboardGroup(result);
        UNIT_ASSERT_VALUES_EQUAL(status.overall(), group);
        for (const auto& vdisk : status.vdisks()) {
            UNIT_ASSERT_VALUES_EQUAL_C(vdisk.overall(), vdisks, vdisk.id());
            UNIT_ASSERT_VALUES_EQUAL_C(vdisk.pdisk().overall(), pdisks, vdisk.id());
        }
    }

    std::vector<const Ydb::Monitoring::IssueLog*> FindIssues(const Ydb::Monitoring::SelfCheckResult& result,
            const TString& type, Ydb::Monitoring::StatusFlag::Status status, TLocationFilter filter = {})
    {
        std::vector<const Ydb::Monitoring::IssueLog*> issues;
        for (const auto& issue : result.issue_log()) {
            if (issue.type() == type && issue.status() == status && filter(issue.location())) {
                issues.push_back(&issue);
            }
        }
        return issues;
    }

    const Ydb::Monitoring::IssueLog& GetSingleIssue(const Ydb::Monitoring::SelfCheckResult& result,
            const TString& type, Ydb::Monitoring::StatusFlag::Status status, TLocationFilter filter = {})
    {
        const auto issues = FindIssues(result, type, status, filter);
        UNIT_ASSERT_VALUES_EQUAL_C(issues.size(), 1, "Wrong issues count for " << type << " with status " << status);
        return *issues.front();
    }

    // issues of the given type and status that the issue names as its reasons
    std::vector<const Ydb::Monitoring::IssueLog*> GetReasonIssues(const Ydb::Monitoring::SelfCheckResult& result,
            const Ydb::Monitoring::IssueLog& issue, const TString& type, Ydb::Monitoring::StatusFlag::Status status)
    {
        std::vector<const Ydb::Monitoring::IssueLog*> reasons;
        for (const auto& reasonId : issue.reason()) {
            for (const auto& candidate : result.issue_log()) {
                if (candidate.id() == reasonId && candidate.type() == type && candidate.status() == status) {
                    reasons.push_back(&candidate);
                }
            }
        }
        return reasons;
    }

    void CheckAllReasonsResolve(const Ydb::Monitoring::SelfCheckResult& result) {
        THashSet<TString> ids;
        for (const auto& issue : result.issue_log()) {
            ids.insert(issue.id());
        }
        for (const auto& issue : result.issue_log()) {
            for (const auto& reasonId : issue.reason()) {
                UNIT_ASSERT_C(ids.contains(reasonId), "Reason " << reasonId << " of " << issue.id() << " is not in the issue log");
            }
        }
    }

    // there is no issue of the type at all, whatever its status is
    void CheckNoIssues(const Ydb::Monitoring::SelfCheckResult& result, const TString& type, TLocationFilter filter = {}) {
        for (auto status : {Ydb::Monitoring::StatusFlag::GREY, Ydb::Monitoring::StatusFlag::BLUE, Ydb::Monitoring::StatusFlag::YELLOW,
                Ydb::Monitoring::StatusFlag::ORANGE, Ydb::Monitoring::StatusFlag::RED})
        {
            CheckHcResultHasIssuesWithStatus(result, type, status, 0, filter);
        }
    }

    // verbose overall of the static pool and its only issue, which must be caused by the issues of its groups
    void CheckStaticPool(const Ydb::Monitoring::SelfCheckResult& result, Ydb::Monitoring::StatusFlag::Status status,
            const TString& message = {})
    {
        UNIT_ASSERT_VALUES_EQUAL(GetStaticPool(result).overall(), status);
        if (status == Ydb::Monitoring::StatusFlag::GREEN) {
            CheckNoIssues(result, "STORAGE_POOL", TLocationFilter().Pool("static"));
            return;
        }
        auto isStaticPool = [](const Ydb::Monitoring::IssueLog& issue) {
            return issue.location().storage().pool().name() == "static";
        };
        std::vector<const Ydb::Monitoring::IssueLog*> poolIssues;
        for (const auto& issue : result.issue_log()) {
            if (issue.type() == "STORAGE_POOL" && isStaticPool(issue)) {
                poolIssues.push_back(&issue);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(poolIssues.size(), 1);
        const auto& poolIssue = *poolIssues.front();
        UNIT_ASSERT_VALUES_EQUAL(poolIssue.status(), status);
        UNIT_ASSERT_VALUES_EQUAL(poolIssue.message(), message);
        UNIT_ASSERT(poolIssue.reason_size() > 0);
        for (const auto& reasonId : poolIssue.reason()) {
            const auto reason = std::ranges::find_if(result.issue_log(), [&](const auto& issue) {
                return issue.id() == reasonId;
            });
            UNIT_ASSERT_C(reason != result.issue_log().end(), "Reason " << reasonId << " of the pool issue is not in the issue log");
            UNIT_ASSERT_VALUES_EQUAL_C(reason->type(), "STORAGE_GROUP", reason->ShortDebugString());
            UNIT_ASSERT_C(isStaticPool(*reason), reason->ShortDebugString());
        }
    }

    TStaticWhiteboardScenario TwoNodeScenario(const TString& erasure, std::vector<TStaticWhiteboardDisk> disks) {
        TStaticWhiteboardScenario scenario;
        scenario.Erasure = erasure;
        scenario.StaticNodes = 2;
        scenario.Disks = std::move(disks);
        scenario.VDiskReplies = {EWhiteboardReply::Received, EWhiteboardReply::Missing};
        scenario.PDiskReplies = {EWhiteboardReply::Received, EWhiteboardReply::Missing};
        return scenario;
    }

    // the single static node never answers
    TStaticWhiteboardScenario AllMissingScenario() {
        TStaticWhiteboardScenario scenario;
        scenario.Disks = Block42Disks();
        scenario.VDiskReplies = {EWhiteboardReply::Missing};
        scenario.PDiskReplies = {EWhiteboardReply::Missing};
        return scenario;
    }

    Y_UNIT_TEST(WhiteboardMissingRepliesAreNotGroupFailure) {
        const auto result = RequestHcWithStaticWhiteboard(AllMissingScenario());
        const auto pool = TLocationFilter().Pool("static");
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 0, pool);
        const auto& groupIssue = GetSingleIssue(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::GREY, pool);
        UNIT_ASSERT_VALUES_EQUAL(groupIssue.message(), "Group has disks with no data");
        CheckHcResultHasIssuesWithStatus(result, "VDISK", Ydb::Monitoring::StatusFlag::RED, 0, pool);
        CheckHcResultHasIssuesWithStatus(result, "VDISK", Ydb::Monitoring::StatusFlag::GREY, 8, pool);
        CheckHcResultHasIssuesWithStatus(result, "PDISK", Ydb::Monitoring::StatusFlag::RED, 0);
        CheckHcResultHasIssuesWithStatus(result, "PDISK", Ydb::Monitoring::StatusFlag::GREY, 8);
        CheckStaticWhiteboardOverall(result, Ydb::Monitoring::StatusFlag::GREY, Ydb::Monitoring::StatusFlag::GREY, Ydb::Monitoring::StatusFlag::GREY);

        UNIT_ASSERT_VALUES_EQUAL(groupIssue.reason_size(), 8);
        const auto vdiskReasons = GetReasonIssues(result, groupIssue, "VDISK", Ydb::Monitoring::StatusFlag::GREY);
        UNIT_ASSERT_VALUES_EQUAL(vdiskReasons.size(), 8);
        for (const auto* vdisk : vdiskReasons) {
            UNIT_ASSERT_VALUES_EQUAL(vdisk->message(), "VDisk has no data");
            UNIT_ASSERT_VALUES_EQUAL(GetReasonIssues(result, *vdisk, "PDISK", Ydb::Monitoring::StatusFlag::GREY).size(), 1);
        }
        CheckAllReasonsResolve(result);

        // the pool counts disks with no data as failed, while the group stays unknown
        CheckStaticPool(result, Ydb::Monitoring::StatusFlag::RED, "Pool failed");
        const auto& poolIssue = GetSingleIssue(result, "STORAGE_POOL", Ydb::Monitoring::StatusFlag::RED, pool);
        const auto poolReasons = GetReasonIssues(result, poolIssue, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::GREY);
        UNIT_ASSERT_VALUES_EQUAL(poolReasons.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(poolReasons.front()->id(), groupIssue.id());
        UNIT_ASSERT_VALUES_EQUAL(result.database_status_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result.database_status(0).storage().overall(), Ydb::Monitoring::StatusFlag::RED);

        // the legacy fallback signal stays
        CheckHcResultHasIssueWithMessage(result, "STORAGE", Ydb::Monitoring::StatusFlag::RED, "System tablet BSC didn't provide information");
        UNIT_ASSERT_VALUES_EQUAL(result.self_check_result(), Ydb::Monitoring::SelfCheck::EMERGENCY);
    }

    Y_UNIT_TEST(WhiteboardMissingVDiskReplyWithHealthyPDisk) {
        auto scenario = AllMissingScenario();
        scenario.PDiskReplies = {EWhiteboardReply::Received};
        const auto result = RequestHcWithStaticWhiteboard(scenario);
        const auto pool = TLocationFilter().Pool("static");
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 0, pool);
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::GREY, 1, pool);
        CheckHcResultHasIssuesWithStatus(result, "VDISK", Ydb::Monitoring::StatusFlag::RED, 0, pool);
        CheckHcResultHasIssuesWithStatus(result, "VDISK", Ydb::Monitoring::StatusFlag::GREY, 8, pool);
        CheckNoIssues(result, "PDISK");
        // a healthy PDisk says nothing about the VDisk, so it must not be GREEN
        CheckStaticWhiteboardOverall(result, Ydb::Monitoring::StatusFlag::GREY, Ydb::Monitoring::StatusFlag::GREY, Ydb::Monitoring::StatusFlag::GREEN);
        for (const auto* vdisk : FindIssues(result, "VDISK", Ydb::Monitoring::StatusFlag::GREY, pool)) {
            UNIT_ASSERT_VALUES_EQUAL(vdisk->reason_size(), 0);
        }
    }

    Y_UNIT_TEST(WhiteboardMissingPDiskReplyWithReceivedVDisk) {
        auto scenario = AllMissingScenario();
        scenario.VDiskReplies = {EWhiteboardReply::Received};
        const auto result = RequestHcWithStaticWhiteboard(scenario);
        const auto pool = TLocationFilter().Pool("static");
        CheckNoIssues(result, "STORAGE_GROUP", pool);
        // received VDisks have the disk space flag, but must not become "degraded" because of an unknown PDisk
        CheckNoIssues(result, "VDISK", pool);
        CheckHcResultHasIssuesWithStatus(result, "PDISK", Ydb::Monitoring::StatusFlag::GREY, 8);
        CheckHcResultHasIssuesWithStatus(result, "PDISK", Ydb::Monitoring::StatusFlag::RED, 0);
        CheckNoIssues(result, "STORAGE_NODE");
        CheckStaticWhiteboardOverall(result, Ydb::Monitoring::StatusFlag::GREEN, Ydb::Monitoring::StatusFlag::GREEN, Ydb::Monitoring::StatusFlag::GREY);
        // the received VDisks are healthy, so nothing is counted as failed
        CheckStaticPool(result, Ydb::Monitoring::StatusFlag::GREEN);
    }

    Y_UNIT_TEST(WhiteboardTwoFailedAndOneUnknownDisk) {
        auto scenario = TwoNodeScenario(NHealthCheck::BLOCK_4_2, Block42Disks());
        MakeFailed(scenario.Disks[0]);
        MakeFailed(scenario.Disks[1]);
        MakeUnknown(scenario.Disks[7]);
        const auto result = RequestHcWithStaticWhiteboard(scenario);
        const auto pool = TLocationFilter().Pool("static");
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 0, pool);
        const auto& orange = GetSingleIssue(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::ORANGE, pool);
        UNIT_ASSERT_VALUES_EQUAL(orange.message(), "Group has no redundancy");
        const auto& grey = GetSingleIssue(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::GREY, pool);
        UNIT_ASSERT_VALUES_EQUAL(grey.message(), "Group has disks with no data");
        CheckHcResultHasIssuesWithStatus(result, "VDISK", Ydb::Monitoring::StatusFlag::RED, 2, pool);
        CheckHcResultHasIssuesWithStatus(result, "VDISK", Ydb::Monitoring::StatusFlag::GREY, 1, pool);
        CheckHcResultHasIssuesWithStatus(result, "PDISK", Ydb::Monitoring::StatusFlag::GREY, 1);
        UNIT_ASSERT_VALUES_EQUAL(GetStaticWhiteboardGroup(result).overall(), Ydb::Monitoring::StatusFlag::ORANGE);
        // the known verdict cites only the failed disks, the unknown one cites only the disk with no data
        UNIT_ASSERT_VALUES_EQUAL(orange.reason_size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(GetReasonIssues(result, orange, "VDISK", Ydb::Monitoring::StatusFlag::RED).size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(grey.reason_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(GetReasonIssues(result, grey, "VDISK", Ydb::Monitoring::StatusFlag::GREY).size(), 1);
        CheckAllReasonsResolve(result);
        // the pool counts the disk with no data as failed
        CheckStaticPool(result, Ydb::Monitoring::StatusFlag::RED, "Pool failed");
    }

    Y_UNIT_TEST(WhiteboardThreeFailedAndOneUnknownDiskStaysRed) {
        auto scenario = TwoNodeScenario(NHealthCheck::BLOCK_4_2, Block42Disks());
        MakeFailed(scenario.Disks[0]);
        MakeFailed(scenario.Disks[1]);
        MakeFailed(scenario.Disks[2]);
        MakeUnknown(scenario.Disks[7]);
        const auto result = RequestHcWithStaticWhiteboard(scenario);
        const auto pool = TLocationFilter().Pool("static");
        CheckHcResultHasIssuesWithStatus(result, "VDISK", Ydb::Monitoring::StatusFlag::GREY, 1, pool);
        CheckHcResultHasIssuesWithStatus(result, "VDISK", Ydb::Monitoring::StatusFlag::RED, 3, pool);
        const auto& red = GetSingleIssue(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, pool);
        UNIT_ASSERT_VALUES_EQUAL(red.message(), "Group failed");
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::GREY, 0, pool);
    }

    Y_UNIT_TEST(WhiteboardMissingVDiskReplyWithFailedPDisk) {
        auto scenario = TwoNodeScenario(NHealthCheck::BLOCK_4_2, Block42Disks());
        scenario.PDiskReplies = {EWhiteboardReply::Received, EWhiteboardReply::Received};
        for (ui32 i = 5; i < 8; ++i) {
            MakeUnknown(scenario.Disks[i]);
        }
        scenario.Disks[5].PDiskState = NKikimrBlobStorage::TPDiskState::DeviceIoError;
        const auto result = RequestHcWithStaticWhiteboard(scenario);
        const auto pool = TLocationFilter().Pool("static");
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 0, pool);
        // the known PDisk failure is evidence, the other two disks are unknown
        const auto& red = GetSingleIssue(result, "VDISK", Ydb::Monitoring::StatusFlag::RED, pool);
        UNIT_ASSERT_VALUES_EQUAL(red.message(), "VDisk is not available");
        const auto pdiskReasons = GetReasonIssues(result, red, "PDISK", Ydb::Monitoring::StatusFlag::RED);
        UNIT_ASSERT_VALUES_EQUAL(pdiskReasons.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(pdiskReasons[0]->message(), "PDisk state is DeviceIoError");
        CheckHcResultHasIssuesWithStatus(result, "VDISK", Ydb::Monitoring::StatusFlag::GREY, 2, pool);
        const auto& status = GetStaticWhiteboardGroup(result);
        for (int i = 0; i < status.vdisks_size(); ++i) {
            const auto expected = i == 5 ? Ydb::Monitoring::StatusFlag::RED : i > 5 ? Ydb::Monitoring::StatusFlag::GREY : Ydb::Monitoring::StatusFlag::GREEN;
            UNIT_ASSERT_VALUES_EQUAL_C(status.vdisks(i).overall(), expected, "VDisk " << i);
        }
        const auto& yellow = GetSingleIssue(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::YELLOW, pool);
        UNIT_ASSERT_VALUES_EQUAL(yellow.reason_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(GetReasonIssues(result, yellow, "VDISK", Ydb::Monitoring::StatusFlag::RED).size(), 1);
        const auto& grey = GetSingleIssue(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::GREY, pool);
        UNIT_ASSERT_VALUES_EQUAL(grey.message(), "Group has disks with no data");
        UNIT_ASSERT_VALUES_EQUAL(grey.reason_size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(GetReasonIssues(result, grey, "VDISK", Ydb::Monitoring::StatusFlag::GREY).size(), 2);
    }

    Y_UNIT_TEST(WhiteboardMixedRepliesMergedRecords) {
        auto scenario = TwoNodeScenario(NHealthCheck::BLOCK_4_2, Block42Disks());
        scenario.MergeRecords = true;
        MakeFailed(scenario.Disks[0]);
        MakeFailed(scenario.Disks[1]);
        MakeUnknown(scenario.Disks[6]);
        MakeUnknown(scenario.Disks[7]);
        const auto result = RequestHcWithStaticWhiteboard(scenario);
        const auto pool = TLocationFilter().Pool("static");
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 0, pool);
        // a single group is not merged, so the messages stay singular
        const auto& orange = GetSingleIssue(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::ORANGE, pool);
        const auto& grey = GetSingleIssue(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::GREY, pool);
        UNIT_ASSERT_VALUES_EQUAL(grey.message(), "Group has disks with no data");
        const auto& greyVDisks = GetSingleIssue(result, "VDISK", Ydb::Monitoring::StatusFlag::GREY, pool);
        UNIT_ASSERT_VALUES_EQUAL(greyVDisks.message(), "VDisks have no data");
        UNIT_ASSERT_VALUES_EQUAL(greyVDisks.count(), 2);
        const auto& redVDisks = GetSingleIssue(result, "VDISK", Ydb::Monitoring::StatusFlag::RED, pool);
        UNIT_ASSERT_VALUES_EQUAL(redVDisks.count(), 2);
        const auto& greyPDisks = GetSingleIssue(result, "PDISK", Ydb::Monitoring::StatusFlag::GREY);
        UNIT_ASSERT_VALUES_EQUAL(greyPDisks.message(), "PDisks have no data");
        UNIT_ASSERT_VALUES_EQUAL(greyPDisks.count(), 2);
        UNIT_ASSERT_VALUES_EQUAL(grey.reason_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(GetReasonIssues(result, grey, "VDISK", Ydb::Monitoring::StatusFlag::GREY).size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(GetReasonIssues(result, greyVDisks, "PDISK", Ydb::Monitoring::StatusFlag::GREY).size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(orange.reason_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(GetReasonIssues(result, orange, "VDISK", Ydb::Monitoring::StatusFlag::RED).size(), 1);
        CheckAllReasonsResolve(result);
    }

    Y_UNIT_TEST(WhiteboardMirror3DcFailedAndUnknownDisksInOneRealm) {
        auto scenario = TwoNodeScenario(NHealthCheck::MIRROR_3_DC, Mirror3DcDisks());
        MakeFailed(scenario.Disks[0]); // 0.0
        MakeUnknown(scenario.Disks[1]); // 0.1
        const auto result = RequestHcWithStaticWhiteboard(scenario);
        const auto pool = TLocationFilter().Pool("static");
        CheckHcResultHasIssuesWithStatus(result, "VDISK", Ydb::Monitoring::StatusFlag::GREY, 1, pool);
        // the unknown disk shares the failed realm, so the pessimistic verdict is not worse
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::YELLOW, 1, pool);
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::GREY, 0, pool);
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::ORANGE, 0, pool);
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 0, pool);
        CheckStaticPool(result, Ydb::Monitoring::StatusFlag::YELLOW, "Pool degraded");
    }

    Y_UNIT_TEST(WhiteboardMirror3DcFailedAndUnknownDisksAcrossRealms) {
        auto scenario = TwoNodeScenario(NHealthCheck::MIRROR_3_DC, Mirror3DcDisks());
        MakeFailed(scenario.Disks[0]); // 0.0
        MakeUnknown(scenario.Disks[3]); // 1.0
        const auto result = RequestHcWithStaticWhiteboard(scenario);
        const auto pool = TLocationFilter().Pool("static");
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::ORANGE, 0, pool);
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::YELLOW, 1, pool);
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::GREY, 1, pool);
        // the pool counts the disk with no data as failed, which leaves two failed realms
        CheckStaticPool(result, Ydb::Monitoring::StatusFlag::ORANGE, "Pool has no redundancy");
    }

    Y_UNIT_TEST(WhiteboardMirror3DcUnknownRealmDegradesPool) {
        auto scenario = TwoNodeScenario(NHealthCheck::MIRROR_3_DC, Mirror3DcDisks());
        for (ui32 i = 6; i < 9; ++i) {
            MakeUnknown(scenario.Disks[i]); // 2.x
        }
        const auto result = RequestHcWithStaticWhiteboard(scenario);
        const auto pool = TLocationFilter().Pool("static");
        CheckHcResultHasIssuesWithStatus(result, "VDISK", Ydb::Monitoring::StatusFlag::RED, 0, pool);
        CheckHcResultHasIssuesWithStatus(result, "VDISK", Ydb::Monitoring::StatusFlag::GREY, 3, pool);
        for (auto status : {Ydb::Monitoring::StatusFlag::BLUE, Ydb::Monitoring::StatusFlag::YELLOW,
                Ydb::Monitoring::StatusFlag::ORANGE, Ydb::Monitoring::StatusFlag::RED})
        {
            CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", status, 0, pool);
        }
        const auto& grey = GetSingleIssue(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::GREY, pool);
        UNIT_ASSERT_VALUES_EQUAL(grey.message(), "Group has disks with no data");
        UNIT_ASSERT_VALUES_EQUAL(grey.reason_size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(GetStaticWhiteboardGroup(result).overall(), Ydb::Monitoring::StatusFlag::GREY);
        // nothing is known to be failed, but the pool counts the realm with no data as failed
        CheckStaticPool(result, Ydb::Monitoring::StatusFlag::YELLOW, "Pool degraded");
    }

    Y_UNIT_TEST(WhiteboardMirror3DcTwoFailedAndTwoUnknownInAnotherRealm) {
        auto scenario = TwoNodeScenario(NHealthCheck::MIRROR_3_DC, Mirror3DcDisks());
        MakeFailed(scenario.Disks[0]); // 0.0
        MakeFailed(scenario.Disks[1]); // 0.1
        MakeUnknown(scenario.Disks[3]); // 1.0
        MakeUnknown(scenario.Disks[4]); // 1.1
        const auto result = RequestHcWithStaticWhiteboard(scenario);
        const auto pool = TLocationFilter().Pool("static");
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 0, pool);
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::YELLOW, 1, pool);
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::GREY, 1, pool);
    }

    Y_UNIT_TEST(WhiteboardMirror3DcTwoFailedRealmsAndUnknownThird) {
        auto scenario = TwoNodeScenario(NHealthCheck::MIRROR_3_DC, Mirror3DcDisks());
        MakeFailed(scenario.Disks[0]); // 0.0
        MakeFailed(scenario.Disks[3]); // 1.0
        MakeUnknown(scenario.Disks[6]); // 2.0
        const auto result = RequestHcWithStaticWhiteboard(scenario);
        const auto pool = TLocationFilter().Pool("static");
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 0, pool);
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::ORANGE, 1, pool);
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::GREY, 1, pool);
    }

    Y_UNIT_TEST(WhiteboardMirror3DcKnownFailureStaysRed) {
        auto scenario = TwoNodeScenario(NHealthCheck::MIRROR_3_DC, Mirror3DcDisks());
        MakeFailed(scenario.Disks[0]); // 0.0
        MakeFailed(scenario.Disks[3]); // 1.0
        MakeFailed(scenario.Disks[6]); // 2.0
        MakeUnknown(scenario.Disks[1]); // 0.1
        const auto result = RequestHcWithStaticWhiteboard(scenario);
        const auto pool = TLocationFilter().Pool("static");
        CheckHcResultHasIssuesWithStatus(result, "VDISK", Ydb::Monitoring::StatusFlag::GREY, 1, pool);
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 1, pool);
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::GREY, 0, pool);
    }

    Y_UNIT_TEST(BridgeWhiteboardMissingRepliesDoNotBlamePile) {
        auto scenario = AllMissingScenario();
        scenario.Bridge = true;
        const auto result = RequestHcWithStaticWhiteboard(scenario);
        // this is what the skipper reads: a RED group issue that carries the pile name
        for (const auto& issue : result.issue_log()) {
            const bool blamesPile = issue.type().Contains("GROUP") && issue.status() == Ydb::Monitoring::StatusFlag::RED
                && issue.location().storage().pool().group().pile().name() == "pile0";
            UNIT_ASSERT_C(!blamesPile, "Pile is blamed by " << issue.ShortDebugString());
        }
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::GREY, 1, TLocationFilter().Pool("static").Pile("pile0"));
    }

    // guard: a node that answered and does not report the disk is evidence of failure
    Y_UNIT_TEST(WhiteboardDiskAbsentFromReplyIsGroupFailure) {
        TStaticWhiteboardScenario scenario;
        scenario.Disks = Block42Disks();
        scenario.VDiskReplies = {EWhiteboardReply::Received};
        scenario.PDiskReplies = {EWhiteboardReply::Received};
        for (ui32 i = 5; i < 8; ++i) {
            scenario.Disks[i].VDiskState = std::nullopt;
        }
        const auto result = RequestHcWithStaticWhiteboard(scenario);
        const auto pool = TLocationFilter().Pool("static");
        UNIT_ASSERT_VALUES_EQUAL(GetSingleIssue(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, pool).message(), "Group failed");
        CheckHcResultHasIssuesWithStatus(result, "VDISK", Ydb::Monitoring::StatusFlag::RED, 3, pool);
        CheckHcResultHasIssueWithMessage(result, "VDISK", Ydb::Monitoring::StatusFlag::RED, "VDisk is not available");
        CheckHcResultHasIssuesWithStatus(result, "VDISK", Ydb::Monitoring::StatusFlag::GREY, 0, pool);
        CheckNoIssues(result, "PDISK");
    }

    // guard: an unreachable node is a dead-node signal and stays RED
    Y_UNIT_TEST(WhiteboardUnreachableNodeIsGroupFailure) {
        TStaticWhiteboardScenario scenario;
        scenario.Disks = Block42Disks();
        scenario.VDiskReplies = {EWhiteboardReply::Unreachable};
        scenario.PDiskReplies = {EWhiteboardReply::Unreachable};
        const auto result = RequestHcWithStaticWhiteboard(scenario);
        const auto pool = TLocationFilter().Pool("static");
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, 1, pool);
        CheckHcResultHasIssuesWithStatus(result, "VDISK", Ydb::Monitoring::StatusFlag::RED, 8, pool);
        CheckHcResultHasIssuesWithStatus(result, "VDISK", Ydb::Monitoring::StatusFlag::GREY, 0, pool);
        CheckHcResultHasIssuesWithStatus(result, "PDISK", Ydb::Monitoring::StatusFlag::RED, 8);
        CheckHcResultHasIssuesWithStatus(result, "PDISK", Ydb::Monitoring::StatusFlag::GREY, 0);
        // identical node issues share an id, so do not count them
        CheckHcResultHasIssueWithMessage(result, "STORAGE_NODE", Ydb::Monitoring::StatusFlag::RED, "Storage node is not available");
    }

    // an undelivered request is a dead-node signal at once, even when its retries are not over at the deadline
    Y_UNIT_TEST(WhiteboardUndeliveredNodeIsGroupFailureBeforeRetriesEnd) {
        TStaticWhiteboardScenario scenario;
        scenario.Disks = Block42Disks();
        scenario.VDiskReplies = {EWhiteboardReply::Unreachable};
        scenario.PDiskReplies = {EWhiteboardReply::Unreachable};
        // The request covers the whole domain, so the static config is requested at start and the whiteboard
        // requests are sent at once, at about 0 ms. They are undelivered immediately and retried at 500 ms,
        // undelivered again. The next retry, whose failure would give the node up, is due at 1000 ms, but the
        // final timeout fires at 750 ms. Any timeout below 1 s leaves that retry pending; from 1 s on the node is
        // given up before the deadline, as in WhiteboardUnreachableNodeIsGroupFailure.
        scenario.OperationTimeout = TDuration::MilliSeconds(750);
        const auto result = RequestHcWithStaticWhiteboard(scenario);
        const auto pool = TLocationFilter().Pool("static");
        UNIT_ASSERT_VALUES_EQUAL(GetSingleIssue(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::RED, pool).message(), "Group failed");
        CheckHcResultHasIssuesWithStatus(result, "STORAGE_GROUP", Ydb::Monitoring::StatusFlag::GREY, 0, pool);
        CheckHcResultHasIssuesWithStatus(result, "VDISK", Ydb::Monitoring::StatusFlag::RED, 8, pool);
        CheckHcResultHasIssuesWithStatus(result, "VDISK", Ydb::Monitoring::StatusFlag::GREY, 0, pool);
        CheckHcResultHasIssuesWithStatus(result, "PDISK", Ydb::Monitoring::StatusFlag::RED, 8);
        CheckHcResultHasIssuesWithStatus(result, "PDISK", Ydb::Monitoring::StatusFlag::GREY, 0);
        // identical node issues share an id, so do not count them
        CheckHcResultHasIssueWithMessage(result, "STORAGE_NODE", Ydb::Monitoring::StatusFlag::RED, "Storage node is not available");
        CheckStaticPool(result, Ydb::Monitoring::StatusFlag::RED, "Pool failed");
    }

    // guard: a retry that gets the reply wins over the undelivered request before it
    Y_UNIT_TEST(WhiteboardRetryAfterUndeliveredIsReceived) {
        TStaticWhiteboardScenario scenario;
        scenario.Disks = Block42Disks();
        scenario.VDiskReplies = {EWhiteboardReply::UndeliveredOnce};
        scenario.PDiskReplies = {EWhiteboardReply::UndeliveredOnce};
        const auto result = RequestHcWithStaticWhiteboard(scenario);
        const auto pool = TLocationFilter().Pool("static");
        CheckNoIssues(result, "STORAGE_GROUP", pool);
        CheckNoIssues(result, "VDISK", pool);
        CheckNoIssues(result, "PDISK");
        CheckNoIssues(result, "STORAGE_NODE");
        CheckStaticWhiteboardOverall(result, Ydb::Monitoring::StatusFlag::GREEN, Ydb::Monitoring::StatusFlag::GREEN, Ydb::Monitoring::StatusFlag::GREEN);
        CheckStaticPool(result, Ydb::Monitoring::StatusFlag::GREEN);
    }

    Y_UNIT_TEST(TestNoSchemeShardResponse) {
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

        TBlockEvents<TEvSchemeShard::TEvDescribeScheme> blockSS(runtime);

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        auto *request = new NHealthCheck::TEvSelfCheckRequest;
        request->Request.set_return_verbose_status(true);
        request->Database = "/Root";
        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, request, 0));
        const auto result = runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;

        Ctest << result.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL(result.self_check_result(), Ydb::Monitoring::SelfCheck::EMERGENCY);
        CheckHcResultHasIssuesWithStatus(result, "SYSTEM_TABLET", Ydb::Monitoring::StatusFlag::RED, 1, TLocationFilter().TabletType("SchemeShard"));

        UNIT_ASSERT_VALUES_EQUAL(result.database_status_size(), 1);
        const auto &database_status = result.database_status(0);

        UNIT_ASSERT_VALUES_EQUAL(database_status.name(), "/Root");
        UNIT_ASSERT_VALUES_EQUAL(database_status.overall(), Ydb::Monitoring::StatusFlag::RED);

        UNIT_ASSERT_VALUES_EQUAL(database_status.compute().overall(), Ydb::Monitoring::StatusFlag::RED);
        UNIT_ASSERT_VALUES_EQUAL(database_status.storage().overall(), Ydb::Monitoring::StatusFlag::GREY);
    }

    Y_UNIT_TEST(ShardsLimit999) {
        ShardsQuotaTest(999, 1000, 1, Ydb::Monitoring::StatusFlag::RED);
    }

    Y_UNIT_TEST(ShardsLimit995) {
        ShardsQuotaTest(995, 1000, 1, Ydb::Monitoring::StatusFlag::ORANGE);
    }

    Y_UNIT_TEST(ShardsLimit905) {
        ShardsQuotaTest(905, 1000, 1, Ydb::Monitoring::StatusFlag::YELLOW);
    }

    Y_UNIT_TEST(ShardsLimit800) {
        ShardsQuotaTest(805, 1000, 0, Ydb::Monitoring::StatusFlag::GREEN);
    }

    Y_UNIT_TEST(ShardsNoLimit) {
        ShardsQuotaTest(105, 0, 0, Ydb::Monitoring::StatusFlag::GREEN);
    }

    Y_UNIT_TEST(TestTabletIsDead) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port)
                .SetNodeCount(2)
                .SetDynamicNodeCount(1)
                .SetUseRealThreads(false)
                .SetDomainName("Root");
        TServer server(settings);
        server.EnableGRpc(grpcPort);

        TClient client(settings);

        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();

        // only have local on dynamic nodes
        runtime->Send(new IEventHandle(MakeLocalID(runtime->GetNodeId(0)), sender, new TEvents::TEvPoisonPill()));
        runtime->Send(new IEventHandle(MakeLocalID(runtime->GetNodeId(1)), sender, new TEvents::TEvPoisonPill()));

        server.SetupDynamicLocalService(2, "Root");
        server.StartPQTablets(1);
        server.DestroyDynamicLocalService(2);
        runtime->AdvanceCurrentTime(TDuration::Minutes(5));

        TAutoPtr<IEventHandle> handle;
        runtime->Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, new NHealthCheck::TEvSelfCheckRequest(), 0));
        auto result = runtime->GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;
        Cerr << result.ShortDebugString();

        UNIT_ASSERT(HasTabletIssue(result));
    }

    Y_UNIT_TEST(TestBootingTabletIsNotDead) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port)
                .SetNodeCount(2)
                .SetDynamicNodeCount(1)
                .SetUseRealThreads(false)
                .SetDomainName("Root");
        TServer server(settings);
        server.EnableGRpc(grpcPort);

        TClient client(settings);

        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();

        auto blockBoot = runtime->AddObserver<NHive::TEvPrivate::TEvProcessBootQueue>([](auto&& ev) { ev.Reset(); });

        server.SetupDynamicLocalService(2, "Root");
        server.StartPQTablets(1, false);
        runtime->AdvanceCurrentTime(TDuration::Minutes(5));

        TAutoPtr<IEventHandle> handle;
        runtime->Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, new NHealthCheck::TEvSelfCheckRequest(), 0));
        auto result = runtime->GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;
        Cerr << result.ShortDebugString();

        UNIT_ASSERT(!HasTabletIssue(result));
    }

    Y_UNIT_TEST(TestReBootingTabletIsDead) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port)
                .SetNodeCount(2)
                .SetDynamicNodeCount(2)
                .SetUseRealThreads(false)
                .SetDomainName("Root");
        TServer server(settings);
        server.EnableGRpc(grpcPort);

        TClient client(settings);

        TTestActorRuntime* runtime = server.GetRuntime();
        runtime->SetLogPriority(NKikimrServices::HIVE, NActors::NLog::PRI_TRACE);
        TActorId sender = runtime->AllocateEdgeActor();

        // only have local on dynamic nodes
        runtime->Send(new IEventHandle(MakeLocalID(runtime->GetNodeId(0)), sender, new TEvents::TEvPoisonPill()));
        runtime->Send(new IEventHandle(MakeLocalID(runtime->GetNodeId(1)), sender, new TEvents::TEvPoisonPill()));


        server.SetupDynamicLocalService(2, "Root");
        server.StartPQTablets(1, true);
        server.SetupDynamicLocalService(3, "Root");
        auto blockBoot = runtime->AddObserver<NHive::TEvPrivate::TEvProcessBootQueue>([](auto&& ev) { ev.Reset(); });
        server.DestroyDynamicLocalService(2);
        runtime->AdvanceCurrentTime(TDuration::Minutes(5));

        TAutoPtr<IEventHandle> handle;
        runtime->Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, new NHealthCheck::TEvSelfCheckRequest(), 0));
        auto result = runtime->GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;
        Cerr << result.ShortDebugString();

        UNIT_ASSERT(HasTabletIssue(result));
    }

    Y_UNIT_TEST(TestStoppedTabletIsNotDead) {
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

        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        runtime->SetLogPriority(NKikimrServices::HIVE, NActors::NLog::PRI_TRACE);

        // kill local so that tablet cannot start
        runtime->Send(new IEventHandle(MakeLocalID(runtime->GetNodeId(0)), sender, new TEvents::TEvPoisonPill()));

        ui64 tabletId = server.StartPQTablets(1, false).front();
        runtime->DispatchEvents({}, TDuration::MilliSeconds(50));
        runtime->AdvanceCurrentTime(TDuration::Minutes(5));

        {
            TAutoPtr<IEventHandle> handle;
            runtime->Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, new NHealthCheck::TEvSelfCheckRequest(), 0));
            auto result = runtime->GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;
            Cerr << result.ShortDebugString() << Endl;

            UNIT_ASSERT(HasTabletIssue(result));
        }

        runtime->SendToPipe(72057594037968897, sender, new TEvHive::TEvStopTablet(tabletId, sender), 0, GetPipeConfigWithRetries());
        TAutoPtr<IEventHandle> handle;
        runtime->GrabEdgeEvent<TEvHive::TEvStopTabletResult>(handle);

        {
            TAutoPtr<IEventHandle> handle;
            runtime->Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, new NHealthCheck::TEvSelfCheckRequest(), 0));
            auto result = runtime->GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;
            Cerr << result.ShortDebugString() << Endl;

            UNIT_ASSERT(!HasTabletIssue(result));
        }
    }

    Y_UNIT_TEST(TestTabletsInUnresolvaleDatabase) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port)
                .SetNodeCount(2)
                .SetDynamicNodeCount(1)
                .SetUseRealThreads(false)
                .SetDomainName("Root");
        TServer server(settings);
        server.EnableGRpc(grpcPort);

        TClient client(settings);

        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();

        // only have local on dynamic nodes
        runtime->Send(new IEventHandle(MakeLocalID(runtime->GetNodeId(0)), sender, new TEvents::TEvPoisonPill()));
        runtime->Send(new IEventHandle(MakeLocalID(runtime->GetNodeId(1)), sender, new TEvents::TEvPoisonPill()));

        server.SetupDynamicLocalService(2, "Root");
        server.StartPQTablets(1);
        server.DestroyDynamicLocalService(2);
        auto observer = runtime->AddObserver<TEvTxProxySchemeCache::TEvNavigateKeySetResult>([](auto&& ev) {
            for (auto& entry : ev->Get()->Request->ResultSet) {
                entry.Status = TSchemeCacheNavigate::EStatus::RedirectLookupError;
            }
        });
        runtime->AdvanceCurrentTime(TDuration::Minutes(5));

        TAutoPtr<IEventHandle> handle;
        runtime->Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, new NHealthCheck::TEvSelfCheckRequest(), 0));
        auto result = runtime->GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;
        Cerr << result.ShortDebugString();

        UNIT_ASSERT(!HasTabletIssue(result));
    }

    Y_UNIT_TEST(TestOnlyRequestNeededTablets) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port)
                .SetNodeCount(2)
                .SetDynamicNodeCount(1)
                .SetUseRealThreads(false)
                .SetDomainName("Root");
        TServer server(settings);
        server.EnableGRpc(grpcPort);

        TClient client(settings);

        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();

        struct THiveInfoObserver {
            size_t TabletCount = 0;
            TTestActorRuntimeBase::TEventObserverHolder Observer;

            void Do(TEvHive::TEvResponseHiveInfo::TPtr& ev) {
                TabletCount += ev->Get()->Record.TabletsSize();
            }

            THiveInfoObserver(TServer& server) {
                Observer = server.GetRuntime()->AddObserver<TEvHive::TEvResponseHiveInfo>([this](auto&& ev) { Do(ev); });
            }
        };

        struct TCreateTabletObserver {
            size_t TabletCount = 0;
            TTestActorRuntimeBase::TEventObserverHolder Observer;
            ui64 SchemeShard;

            void Do(TEvHive::TEvCreateTablet::TPtr& ev) {
                auto& record = ev->Get()->Record;
                auto* allowedDomain = record.AddAllowedDomains();
                allowedDomain->SetSchemeShard(SchemeShard);
                allowedDomain->SetPathId(1);
                record.MutableObjectDomain()->SetSchemeShard(SchemeShard);
                record.MutableObjectDomain()->SetPathId(1 + (++TabletCount % 2));
            }

            TCreateTabletObserver(TServer& server) {
                Observer = server.GetRuntime()->AddObserver<TEvHive::TEvCreateTablet>([this](auto&& ev) { Do(ev); });
                SchemeShard = ChangeStateStorage(Tests::SchemeRoot, server.GetSettings().Domain);
            }
        };

        struct TNavigateObserver {
            ui64 Hive;
            ui64 SchemeShard;
            TTestActorRuntimeBase::TEventObserverHolder Observer;

            void Do(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
                TSchemeCacheNavigate::TEntry& entry(ev->Get()->Request->ResultSet.front());
                TString path = CanonizePath(entry.Path);
                if (path == "/Root/database") {
                    entry.Status = TSchemeCacheNavigate::EStatus::Ok;
                    entry.Kind = TSchemeCacheNavigate::EKind::KindExtSubdomain;
                    entry.Path = {"Root", "database"};
                    entry.DomainInfo = MakeIntrusive<TDomainInfo>(TPathId{SchemeShard, 2}, TPathId{SchemeShard, 1});
                    entry.DomainInfo->Params.SetHive(Hive);
                }
            }

            TNavigateObserver(TServer& server) {
                Observer = server.GetRuntime()->AddObserver<TEvTxProxySchemeCache::TEvNavigateKeySetResult>([this](auto&& ev) { Do(ev); });
                SchemeShard = ChangeStateStorage(Tests::SchemeRoot, server.GetSettings().Domain);
                Hive = server.GetRuntime()->GetAppData().DomainsInfo->GetHive();
            }

        };

        THiveInfoObserver infoObserver(server);
        TCreateTabletObserver tabletObserver(server);
        TNavigateObserver navigateObserver(server);
        auto tenantObserver = runtime->AddObserver<NConsole::TEvConsole::TEvGetTenantStatusResponse>([](auto&& ev) { ChangeGetTenantStatusResponse(&ev, "/Root/database"); });

        server.SetupDynamicLocalService(2, "Root");
        server.StartPQTablets(10);

        TAutoPtr<IEventHandle> handle;
        auto ev = std::make_unique<NHealthCheck::TEvSelfCheckRequest>();
        ev->Database = "/Root/database";
        runtime->Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, ev.release(), 0));
        runtime->GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle);

        UNIT_ASSERT_VALUES_EQUAL(infoObserver.TabletCount, tabletObserver.TabletCount / 2);
    }

    void SendHealthCheckConfigUpdate(TTestActorRuntime &runtime, const TActorId& sender, const NKikimrConfig::THealthCheckConfig &cfg) {
        auto *event = new NConsole::TEvConsole::TEvConfigureRequest;

        event->Record.AddActions()->MutableRemoveConfigItems()->MutableCookieFilter()->AddCookies("cookie");

        auto &item = *event->Record.AddActions()->MutableAddConfigItem()->MutableConfigItem();
        item.MutableConfig()->MutableHealthCheckConfig()->CopyFrom(cfg);
        item.SetCookie("cookie");

        runtime.SendToPipe(MakeConsoleID(), sender, event, 0, GetPipeConfigWithRetries());

        TAutoPtr<IEventHandle> handle;
        auto record = runtime.GrabEdgeEvent<NConsole::TEvConsole::TEvConfigureResponse>(handle)->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.MutableStatus()->GetCode(), Ydb::StatusIds::SUCCESS);
    }

    void ChangeNodeRestartsPerPeriod(TTestActorRuntime &runtime, const TActorId& sender, const ui32 restartsYellow, const ui32 restartsOrange) {
        NKikimrConfig::TAppConfig ext;
        auto &cfg = *ext.MutableHealthCheckConfig();
        cfg.MutableThresholds()->SetNodeRestartsYellow(restartsYellow);
        cfg.MutableThresholds()->SetNodeRestartsOrange(restartsOrange);
        SendHealthCheckConfigUpdate(runtime, sender, cfg);
    }

    void TestConfigUpdateNodeRestartsPerPeriod(TTestActorRuntime &runtime, const TActorId& sender, const ui32 restartsYellow, const ui32 restartsOrange, const ui32 nodeId, Ydb::Monitoring::StatusFlag::Status expectedStatus) {
        ChangeNodeRestartsPerPeriod(runtime, sender, restartsYellow, restartsOrange);

        TAutoPtr<IEventHandle> handle;
        auto *request = new NHealthCheck::TEvSelfCheckRequest;
        request->Request.set_return_verbose_status(true);
        request->Database = "/Root/database";

        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, request, 0));
        auto result = runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;
        Ctest << result.ShortDebugString() << Endl;

        const auto &database_status = result.database_status(0);
        UNIT_ASSERT_VALUES_EQUAL(database_status.name(), "/Root/database");
        UNIT_ASSERT_VALUES_EQUAL(database_status.compute().overall(), expectedStatus);
        UNIT_ASSERT_VALUES_EQUAL(database_status.compute().nodes()[0].id(), ToString(nodeId));
    }

    Y_UNIT_TEST(HealthCheckConfigUpdate) {
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
        TActorId sender = runtime.AllocateEdgeActor();

        const ui32 nodeRestarts = 10;
        const ui32 nodeId = runtime.GetNodeId(1);
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
                    const TString path = CanonizePath(entry.Path);
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
                    record.ClearNodeStats();
                    auto *nodeStats = record.MutableNodeStats()->Add();
                    nodeStats->SetNodeId(nodeId);
                    nodeStats->SetRestartsPerPeriod(nodeRestarts);
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
                    AddGroupVSlotInControllerConfigResponseWithStaticGroup(x, NKikimrBlobStorage::TGroupStatus::FULL, TVDisks(1));
                    break;
                }
                case NSysView::TEvSysView::EvGetVSlotsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr*>(&ev);
                    AddVSlotsToSysViewResponse(x, 1, TVDisks(1));
                    break;
                }
                case NSysView::TEvSysView::EvGetGroupsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetGroupsResponse::TPtr*>(&ev);
                    AddGroupsToSysViewResponse(x);
                    break;
                }
                case NSysView::TEvSysView::EvGetStoragePoolsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetStoragePoolsResponse::TPtr*>(&ev);
                    AddStoragePoolsToSysViewResponse(x);
                    break;
                }
                case TEvWhiteboard::EvSystemStateResponse: {
                    auto *x = reinterpret_cast<TEvWhiteboard::TEvSystemStateResponse::TPtr*>(&ev);
                    ClearLoadAverage(x);
                    break;
                }
                case TEvInterconnect::EvNodesInfo: {
                    auto *x = reinterpret_cast<TEvInterconnect::TEvNodesInfo::TPtr*>(&ev);
                    auto nodes = MakeIntrusive<TIntrusiveVector<TEvInterconnect::TNodeInfo>>((*x)->Get()->Nodes);
                    if (!nodes->empty()) {
                        nodes->erase(nodes->begin() + 1, nodes->end());
                        nodes->begin()->NodeId = nodeId;
                    }
                    auto newEv = IEventHandle::Downcast<TEvInterconnect::TEvNodesInfo>(
                        new IEventHandle((*x)->Recipient, (*x)->Sender, new TEvInterconnect::TEvNodesInfo(nodes))
                    );
                    x->Swap(newEv);
                    break;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        TestConfigUpdateNodeRestartsPerPeriod(runtime, sender, nodeRestarts + 5, nodeRestarts + 10, nodeId, Ydb::Monitoring::StatusFlag::GREEN);
        TestConfigUpdateNodeRestartsPerPeriod(runtime, sender, nodeRestarts / 2, nodeRestarts + 5, nodeId, Ydb::Monitoring::StatusFlag::YELLOW);
        TestConfigUpdateNodeRestartsPerPeriod(runtime, sender, nodeRestarts / 5, nodeRestarts / 2, nodeId, Ydb::Monitoring::StatusFlag::ORANGE);
    }

    void LayoutCorrectTest(bool layoutCorrect) {
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
        TActorId sender = runtime.AllocateEdgeActor();

        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case NSysView::TEvSysView::EvGetGroupsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetGroupsResponse::TPtr*>(&ev);
                    auto& record = (*x)->Get()->Record;
                    for (auto& entry : *record.mutable_entries()) {
                        entry.mutable_info()->set_layoutcorrect(layoutCorrect);
                    }
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        TAutoPtr<IEventHandle> handle;
        auto *request = new NHealthCheck::TEvSelfCheckRequest;
        request->Request.set_return_verbose_status(true);
        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, request, 0));
        auto result = runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;

        if (layoutCorrect) {
            UNIT_ASSERT_VALUES_EQUAL(result.self_check_result(), Ydb::Monitoring::SelfCheck::GOOD);
            UNIT_ASSERT_VALUES_EQUAL(result.database_status_size(), 1);
            const auto &database_status = result.database_status(0);

            UNIT_ASSERT_VALUES_EQUAL(database_status.storage().overall(), Ydb::Monitoring::StatusFlag::GREEN);
            UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools()[0].overall(), Ydb::Monitoring::StatusFlag::GREEN);
            UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools()[0].groups().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools()[0].groups()[0].overall(), Ydb::Monitoring::StatusFlag::GREEN);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(result.self_check_result(), Ydb::Monitoring::SelfCheck::MAINTENANCE_REQUIRED);
            UNIT_ASSERT_VALUES_EQUAL(result.database_status_size(), 1);
            const auto &database_status = result.database_status(0);

            UNIT_ASSERT_VALUES_EQUAL(database_status.overall(), Ydb::Monitoring::StatusFlag::ORANGE);
            UNIT_ASSERT_VALUES_EQUAL(database_status.storage().overall(), Ydb::Monitoring::StatusFlag::ORANGE);
            UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools()[0].overall(), Ydb::Monitoring::StatusFlag::ORANGE);
            UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools()[0].groups().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(database_status.storage().pools()[0].groups()[0].overall(), Ydb::Monitoring::StatusFlag::ORANGE);

            for (const auto &issue_log : result.issue_log()) {
                if (issue_log.level() == 1 && issue_log.type() == "DATABASE") {
                    UNIT_ASSERT_VALUES_EQUAL(issue_log.location().database().name(), "/Root");
                } else if (issue_log.level() == 2 && issue_log.type() == "STORAGE") {
                    UNIT_ASSERT_VALUES_EQUAL(issue_log.location().database().name(), "/Root");
                    UNIT_ASSERT_VALUES_EQUAL(issue_log.message(), "Storage has no redundancy");
                } else if (issue_log.level() == 3 && issue_log.type() == "STORAGE_POOL") {
                    UNIT_ASSERT_VALUES_EQUAL(issue_log.location().storage().pool().name(), "static");
                    UNIT_ASSERT_VALUES_EQUAL(issue_log.message(), "Pool has no redundancy");
                } else if (issue_log.level() == 4 && issue_log.type() == "STORAGE_GROUP") {
                    UNIT_ASSERT_VALUES_EQUAL(issue_log.location().storage().pool().name(), "static");
                    UNIT_ASSERT_VALUES_EQUAL(issue_log.message(), "Group layout is incorrect");
                }
            }
        }
    }

    Y_UNIT_TEST(LayoutIncorrect) {
        LayoutCorrectTest(false);
    }

    Y_UNIT_TEST(LayoutCorrect) {
        LayoutCorrectTest(true);
    }

    Y_UNIT_TEST(UnknowPDiskState) {
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
        TActorId sender = runtime.AllocateEdgeActor();

        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case NSysView::TEvSysView::EvGetVSlotsResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr*>(&ev);
                    auto& record = (*x)->Get()->Record;
                    for (auto& entry : *record.mutable_entries()) {
                        entry.mutable_info()->set_statusv2("ERROR");
                    }
                    break;
                }
                case NSysView::TEvSysView::EvGetPDisksResponse: {
                    auto* x = reinterpret_cast<NSysView::TEvSysView::TEvGetPDisksResponse::TPtr*>(&ev);
                    auto& record = (*x)->Get()->Record;
                    for (auto& entry : *record.mutable_entries()) {
                        entry.mutable_info()->set_state("UNKNOWN_STATE?");
                    }
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);

        TAutoPtr<IEventHandle> handle;
        auto *request = new NHealthCheck::TEvSelfCheckRequest;
        request->Request.set_return_verbose_status(true);
        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, request, 0));
        auto result = runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;

        bool pdiskIssueFoundInResult = false;
        for (const auto &issue_log : result.issue_log()) {
            if (issue_log.type() == "PDISK") {
                pdiskIssueFoundInResult = true;
            }
        }
        UNIT_ASSERT(!pdiskIssueFoundInResult);
    }

    Y_UNIT_TEST(TestSystemStateRetriesAfterReceivingResponse) {
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

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        std::optional<TActorId> targetActor;
        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvWhiteboard::EvSystemStateResponse: {
                    if (ev->Cookie == 1) {
                        if (!targetActor) {
                            targetActor = ev->Recipient;
                            runtime.Send(ev.Release());
                            runtime.Send(new IEventHandle(
                                *targetActor,
                                sender,
                                new NHealthCheck::TEvPrivate::TEvRetryNodeWhiteboard(1, TEvWhiteboard::TEvSystemStateRequest::EventType)
                            ));

                        }
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observerFunc);
        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, new NHealthCheck::TEvSelfCheckRequest(), 0));

        auto result = runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;
        UNIT_ASSERT_VALUES_EQUAL(result.self_check_result(), Ydb::Monitoring::SelfCheck::GOOD);
    }

    Y_UNIT_TEST(TestNodeDisconnected) {
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

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        auto observer = runtime.AddObserver<TEvWhiteboard::TEvSystemStateResponse>([&](auto&& ev) {
            auto actor = ev->Recipient;
            auto nodeId = ev->Sender.NodeId();
            Cerr << "Observing " << ev->ToString() << Endl;
            runtime.Send(ev.Release());
            runtime.Send(new IEventHandle(actor, sender, new TEvInterconnect::TEvNodeDisconnected(nodeId)));
        });

        TBlockEvents<TEvHive::TEvResponseHiveInfo> block(runtime); // just make it arrive later
        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, new NHealthCheck::TEvSelfCheckRequest(), 0));
        runtime.DispatchEvents({}, TDuration::MilliSeconds(500));
        block.Stop().Unblock();
        auto result = runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;
        Cerr << result.ShortDebugString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(result.self_check_result(), Ydb::Monitoring::SelfCheck::GOOD);
    }

    void TestStateStorage(ui32 deadNodes, std::optional<Ydb::Monitoring::StatusFlag::Status> expectedStatus) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        auto settings = TServerSettings(port)
                .SetNodeCount(9)
                .SetUseRealThreads(false)
                .SetDomainName("Root");
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        TClient client(settings);
        TTestActorRuntime& runtime = *server.GetRuntime();

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        TIntrusivePtr<TStateStorageInfo> info = new TStateStorageInfo();
        info->RingGroups.push_back({ERingGroupState::PRIMARY, false, 9, {}, {}});
        info->RingGroups.back().Rings.resize(9);
        info->RingGroups.back().NToSelect = 9;
        for (ui32 i = 0; i < 9; ++i) {
            info->RingGroups.back().Rings[i].Replicas.emplace_back(runtime.GetNodeId(i), "FAKE");
        }

        auto ssObserver = runtime.AddObserver<TEvStateStorage::TEvListStateStorageResult>([&](auto&& ev) { ev->Get()->Info = info; });
        auto sbObserver = runtime.AddObserver<TEvStateStorage::TEvListSchemeBoardResult>([&](auto&& ev) { ev->Get()->Info = info; });
        auto bObserver = runtime.AddObserver<TEvStateStorage::TEvListBoardResult>([&](auto&& ev) { ev->Get()->Info = info; });

        auto disconnectObserver = runtime.AddObserver<TEvWhiteboard::TEvSystemStateResponse>([&](auto&& ev) {
            auto actor = ev->Recipient;
            auto nodeId = ev->Sender.NodeId();
            auto nodeIdx = nodeId - runtime.GetNodeId(0);
            if (nodeIdx < deadNodes) {
                runtime.Send(new IEventHandle(actor, actor, new TEvInterconnect::TEvNodeDisconnected(nodeId)), actor.NodeId() - runtime.GetNodeId(0));
                ev.Reset();
            }
        });

        auto *request = new NHealthCheck::TEvSelfCheckRequest();
        request->Request.set_merge_records(true);
        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, request, 0));
        auto result = runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;
        Cerr << result.ShortDebugString() << Endl;
        if (expectedStatus) {
            CheckHcResultHasIssuesWithStatus(result, "STATE_STORAGE", *expectedStatus, 1);
            CheckHcResultHasIssuesWithStatus(result, "SCHEME_BOARD", *expectedStatus, 1);
            CheckHcResultHasIssuesWithStatus(result, "BOARD", *expectedStatus, 1);
            for (const TString& type : {"STATE_STORAGE", "SCHEME_BOARD", "BOARD"}) {
                CheckHcResultHasReasonChain(result, {type, type + "_RING", type + "_NODE"});
            }
        }

        auto statusToResult = [](std::optional<Ydb::Monitoring::StatusFlag::Status> status) {
            if (!status) {
                return Ydb::Monitoring::SelfCheck::GOOD;
            }
            switch (*status) {
            case Ydb::Monitoring::StatusFlag::GREEN:
            default:
                return Ydb::Monitoring::SelfCheck::GOOD;
            case Ydb::Monitoring::StatusFlag::YELLOW:
            case Ydb::Monitoring::StatusFlag::BLUE:
                return Ydb::Monitoring::SelfCheck::DEGRADED;
            case Ydb::Monitoring::StatusFlag::ORANGE:
                return Ydb::Monitoring::SelfCheck::MAINTENANCE_REQUIRED;
            case Ydb::Monitoring::StatusFlag::RED:
                return Ydb::Monitoring::SelfCheck::EMERGENCY;
            }
        };

        UNIT_ASSERT_VALUES_EQUAL(result.self_check_result(), statusToResult(expectedStatus));
    }

    Y_UNIT_TEST(TestStateStorageOk) {
        TestStateStorage(0, std::nullopt);
    }

    Y_UNIT_TEST(TestStateStorageBlue) {
        TestStateStorage(1, Ydb::Monitoring::StatusFlag::BLUE);
    }

    Y_UNIT_TEST(TestStateStorageYellow) {
        TestStateStorage(3, Ydb::Monitoring::StatusFlag::YELLOW);
    }

    Y_UNIT_TEST(TestStateStorageRed) {
        TestStateStorage(6, Ydb::Monitoring::StatusFlag::RED);
    }

    Y_UNIT_TEST(CLusterNotBootstrapped) {
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
        auto config = std::make_shared<NKikimrBlobStorage::TStorageConfig>();
        config->MutableSelfManagementConfig()->SetEnabled(true);

        auto observer = runtime.AddObserver<TEvNodeWardenStorageConfig>([&](TEvNodeWardenStorageConfig::TPtr& ev) {
            ev->Get()->Config = config;
        });

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        auto *request = new NHealthCheck::TEvSelfCheckRequest;
        request->Request.set_return_verbose_status(true);
        request->Database = "/Root";
        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, request, 0));
        const auto result = runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;

        Ctest << result.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL(result.self_check_result(), Ydb::Monitoring::SelfCheck::MAINTENANCE_REQUIRED);
        UNIT_ASSERT(std::ranges::any_of(result.issue_log(), [](auto&& issue) { return issue.message() == "Cluster is not bootstrapped"; }));
    }

    Y_UNIT_TEST(BridgeTimeDifference) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        NKikimrConfig::TBridgeConfig bridgeConfig;
        bridgeConfig.AddPiles()->SetName("pile0");
        bridgeConfig.AddPiles()->SetName("pile1");
        auto settings = TServerSettings(port, {}, {}, bridgeConfig)
                .SetNodeCount(4)
                .SetUseRealThreads(false)
                .SetEnableStorage(false)
                .SetDomainName("Root");
        TServer server(settings);
        server.EnableGRpc(grpcPort);
        TClient client(settings);
        TTestActorRuntime& runtime = *server.GetRuntime();

        auto bridgeInfo = std::make_shared<TBridgeInfo>();
        bridgeInfo->Piles.push_back(TBridgeInfo::TPile{.BridgePileId = TBridgePileId::FromPileIndex(0), .Name = "pile0", .State = NKikimrBridge::TClusterState::SYNCHRONIZED, .IsPrimary = true});
        bridgeInfo->Piles.push_back(TBridgeInfo::TPile{.BridgePileId = TBridgePileId::FromPileIndex(1), .Name = "pile1", .State = NKikimrBridge::TClusterState::SYNCHRONIZED});
        bridgeInfo->SelfNodePile = bridgeInfo->Piles.data();
        bridgeInfo->PrimaryPile = bridgeInfo->Piles.data();
        bridgeInfo->StaticNodeIdToPile[runtime.GetNodeId(0)] = &bridgeInfo->Piles[0];
        bridgeInfo->StaticNodeIdToPile[runtime.GetNodeId(1)] = &bridgeInfo->Piles[0];
        bridgeInfo->StaticNodeIdToPile[runtime.GetNodeId(2)] = &bridgeInfo->Piles[1];
        bridgeInfo->StaticNodeIdToPile[runtime.GetNodeId(3)] = &bridgeInfo->Piles[1];

        auto* nodeWardenStorageConfig = new TEvNodeWardenStorageConfig(std::make_shared<NKikimrBlobStorage::TStorageConfig>(), false, nullptr);
        nodeWardenStorageConfig->BridgeInfo = bridgeInfo;
        runtime.Send(new IEventHandle(GetNameserviceActorId(), runtime.AllocateEdgeActor(), nodeWardenStorageConfig));

        auto configObserver = runtime.AddObserver<TEvNodeWardenStorageConfig>([&](TEvNodeWardenStorageConfig::TPtr& ev) {
            ev->Get()->BridgeInfo = bridgeInfo;
        });

        auto wbObserver = runtime.AddObserver<TEvWhiteboard::TEvSystemStateResponse>([nodeBase = runtime.GetNodeId(0)](TEvWhiteboard::TEvSystemStateResponse::TPtr& ev) {
            auto nodeId = ev->Sender.NodeId();
            switch (nodeId - nodeBase) {
                default:
                    break;
                case 0:
                    ev->Get()->Record.MutableSystemStateInfo(0)->SetMaxClockSkewWithPeerUs(15'000);
                    ev->Get()->Record.MutableSystemStateInfo(0)->SetMaxClockSkewPeerId(nodeBase + 1);
                    ev->Get()->Record.MutableSystemStateInfo(0)->MutableLocation()->SetBridgePileName("pile0");
                    break;
                case 1:
                    ev->Get()->Record.MutableSystemStateInfo(0)->SetMaxClockSkewWithPeerUs(15'000);
                    ev->Get()->Record.MutableSystemStateInfo(0)->SetMaxClockSkewPeerId(nodeBase);
                    ev->Get()->Record.MutableSystemStateInfo(0)->MutableLocation()->SetBridgePileName("pile0");
                    break;
                case 2:
                    ev->Get()->Record.MutableSystemStateInfo(0)->SetMaxClockSkewWithPeerUs(30'000);
                    ev->Get()->Record.MutableSystemStateInfo(0)->SetMaxClockSkewPeerId(nodeBase + 3);
                    ev->Get()->Record.MutableSystemStateInfo(0)->MutableLocation()->SetBridgePileName("pile1");
                    break;
                case 3:
                    ev->Get()->Record.MutableSystemStateInfo(0)->SetMaxClockSkewWithPeerUs(30'000);
                    ev->Get()->Record.MutableSystemStateInfo(0)->SetMaxClockSkewPeerId(nodeBase + 2);
                    ev->Get()->Record.MutableSystemStateInfo(0)->MutableLocation()->SetBridgePileName("pile1");
                    break;
            }
        });

        TActorId sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        auto *request = new NHealthCheck::TEvSelfCheckRequest;
        request->Request.set_return_verbose_status(true);
        request->Database = "/Root";
        runtime.Send(new IEventHandle(NHealthCheck::MakeHealthCheckID(), sender, request, 0));
        auto result = runtime.GrabEdgeEvent<NHealthCheck::TEvSelfCheckResult>(handle)->Result;

        Ctest << result.ShortDebugString() << Endl;
        CheckHcResultHasIssuesWithStatus(result, "CLOCK_SKEW", Ydb::Monitoring::StatusFlag::YELLOW, 2, TLocationFilter().Pile("pile0"));
    }
}
}
