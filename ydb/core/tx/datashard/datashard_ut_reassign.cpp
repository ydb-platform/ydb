#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "datashard_active_transaction.h"

#include <ydb/core/base/hive.h>

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(DataShardReassign) {

    Y_UNIT_TEST(AutoReassignOnYellowFlag) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::TABLET_MAIN, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::LOCAL, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        auto shards = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        // Make sure shard has some data commits
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (0, 0);");

        bool addYellowFlag = false;
        bool captureReassign = true;
        bool captureCheckResult = true;
        TVector<THolder<IEventHandle>> capturedReassign;
        TVector<THolder<IEventHandle>> capturedCheckResult;
        auto captureEvents = [&](TAutoPtr<IEventHandle> &ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::TEvPutResult::EventType: {
                    auto* msg = ev->Get<TEvBlobStorage::TEvPutResult>();
                    if (addYellowFlag && msg->Id.TabletID() == shards[0] && msg->Id.Channel() == 0) {
                        Cerr << "--- Adding yellow flag to TEvPutResult for " << msg->Id << Endl;
                        const_cast<TStorageStatusFlags&>(msg->StatusFlags)
                            .Raw |= NKikimrBlobStorage::StatusDiskSpaceLightYellowMove;
                    }
                    break;
                }
                case TEvHive::TEvReassignTablet::EventType: {
                    if (captureReassign) {
                        Cerr << "--- Captured TEvReassignTablet event" << Endl;
                        capturedReassign.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
                case TEvTablet::TEvCheckBlobstorageStatusResult::EventType: {
                    if (captureCheckResult) {
                        Cerr << "--- Captured TEvCheckBlobstorageStatusResult event" << Endl;
                        capturedCheckResult.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(captureEvents);

        // Perform transaction to trigger yellow flag checks
        addYellowFlag = true;
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);");
        addYellowFlag = false;

        // Wait until we capture a reassign message
        if (capturedReassign.empty()) {
            Cerr << "--- Waiting for TEvReassignTablet event..." << Endl;
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                [&](IEventHandle&) -> bool {
                    return !capturedReassign.empty();
                });
            runtime.DispatchEvents(options);
        }
        UNIT_ASSERT_VALUES_EQUAL(capturedReassign.size(), 1u);

        // We expect tablet to ask for channel 0 reassignment
        {
            auto* msg = capturedReassign[0]->Get<TEvHive::TEvReassignTablet>();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetTabletID(), shards[0]);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetChannels().size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetChannels()[0], 0u);
        }
        capturedReassign.clear();

        if (capturedCheckResult.empty()) {
            Cerr << "--- Waiting for TEvCheckBlobstorageStatusResult..." << Endl;
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                [&](IEventHandle&) -> bool {
                    return !capturedCheckResult.empty();
                });
            runtime.DispatchEvents(options);
        }
        UNIT_ASSERT_VALUES_EQUAL(capturedCheckResult.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(capturedReassign.size(), 0u);

        captureCheckResult = false;
        {
            ui32 group = 2181038080;
            for (auto& ev : capturedCheckResult) {
                auto* msg = ev->Get<TEvTablet::TEvCheckBlobstorageStatusResult>();
                msg->LightYellowMoveGroups.push_back(group);
                runtime.Send(ev.Release(), 0, /* viaActorSystem */ true);
            }
            capturedCheckResult.clear();
        }

        // Wait until we capture a new reassign message
        if (capturedReassign.empty()) {
            Cerr << "--- Waiting for TEvReassignTablet event..." << Endl;
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                [&](IEventHandle&) -> bool {
                    return !capturedReassign.empty();
                });
            runtime.DispatchEvents(options);
        }
        UNIT_ASSERT_VALUES_EQUAL(capturedReassign.size(), 1u);

        // We expect tablet to ask for reassignment of all matching channels
        {
            auto* msg = capturedReassign[0]->Get<TEvHive::TEvReassignTablet>();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetTabletID(), shards[0]);
            UNIT_ASSERT_C(msg->Record.GetChannels().size() > 1,
                "Unexpected number of channels: " << msg->Record.GetChannels().size());
        }
    }

} // Y_UNIT_TEST_SUITE(DataShardReassign)

} // namespace NKikimr
