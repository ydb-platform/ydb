#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/rm_service/kqp_snapshot_manager.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/datashard_ut_common_kqp.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/base/blobstorage.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScripting;
using namespace Tests;
using namespace NKikimr::NDataShard::NKqpHelpers;

namespace {

    /* sum(value) == 596400 */
    TString FillTableQuery() {
        TStringBuilder sql;
        sql << "UPSERT INTO `/Root/table-1` (key, value) VALUES ";
        for (int i = 1; i < 100; ++i) {
            sql << " (" << i << ", " << i << i << "),";
        }
        sql << " (100500, 100500);";
        return sql;
    }

    void EnableLogging(TTestActorRuntime& runtime) {
        runtime.SetLogPriority(NKikimrServices::KQP_YQL, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::KQP_WORKER, NActors::NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::KQP_RESOURCE_MANAGER, NActors::NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::KQP_NODE, NActors::NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::KQP_COMPUTE, NActors::NLog::PRI_TRACE);
    }

}

Y_UNIT_TEST_SUITE(KqpScan) {


    /*
     * Start scan on shard with amount of rows bigger then ScanData capacity (~ 10 ScanData's to
     * complete scan). Event capture filter will pass only first ScanData from one table actor and will
     * kill tablet after that. So in order to complete scan ComputeActor need to handle scan restart after
     * each ScanData.
     */
    Y_UNIT_TEST(ScanRetryRead) {
        NKikimrConfig::TAppConfig appCfg;

        auto* rm = appCfg.MutableTableServiceConfig()->MutableResourceManager();
        rm->SetChannelBufferSize(100);
        rm->SetMinChannelBufferSize(100);

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetNodeCount(2)
            .SetAppConfig(appCfg)
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        // EnableLogging(runtime);

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        ExecSQL(server, sender, FillTableQuery());

        TSet<TActorId> scans;
        TSet<TActorId> killedTablets;

        ui64 result = 0;

        auto captureEvents = [&](TAutoPtr<IEventHandle> &ev) -> auto {

            switch (ev->GetTypeRewrite()) {
                /*
                 * Trick executor to think that all datashard are located on node 1.
                 */
                case NKqp::TKqpExecuterEvents::EvShardsResolveStatus: {
                    auto* msg = ev->Get<NKqp::NShardResolver::TEvShardsResolveStatus>();
                    for (auto& [shardId, nodeId]: msg->ShardNodes) {
                        Cerr << "-- nodeId: " << nodeId << Endl;
                        Cerr.Flush();
                        nodeId = runtime.GetNodeId(0);
                    }
                    break;
                }

                case TEvDataShard::EvKqpScan: {
                    Cerr << (TStringBuilder() << "-- EvScan " << ev->Sender << " -> " << ev->Recipient << Endl);
                    Cerr.Flush();
                    break;
                }

                /*
                 * Respond to streamData with acks. Without that execution pipeline will stop
                 * producing new tuples.
                 */
                case NKqp::TKqpExecuterEvents::EvStreamData: {
                    auto& record = ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>()->Record;

                    Cerr << (TStringBuilder() << "-- EvStreamData: " << record.AsJSON() << Endl);
                    Cerr.Flush();

                    Y_ASSERT(record.GetResultSet().rows().size() == 1);
                    Y_ASSERT(record.GetResultSet().rows().at(0).items().size() == 1);
                    result = record.GetResultSet().rows().at(0).items().at(0).uint64_value();

                    auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>(record.GetSeqNo(), record.GetChannelId());
                    resp->Record.SetEnough(false);
                    resp->Record.SetFreeSpace(100);
                    runtime.Send(new IEventHandle(ev->Sender, sender, resp.Release()));
                    return TTestActorRuntime::EEventAction::DROP;
                }

                /* Drop message and kill tablet if we already had seen this tablet */
                case NKqp::TKqpComputeEvents::EvScanData: {
                    if (scans.contains(ev->Sender)) {
                        if (killedTablets.empty()) { // do only 1 kill per test
                            runtime.Send(new IEventHandle(ev->Sender, ev->Sender, new NKqp::TEvKqpCompute::TEvKillScanTablet));
                            Cerr << (TStringBuilder() << "-- EvScanData from " << ev->Sender << ": hijack event, kill tablet " << ev->Sender << Endl);
                            Cerr.Flush();
                        }
                    } else {
                        scans.insert(ev->Sender);
                        runtime.EnableScheduleForActor(ev->Sender);
                        Cerr << (TStringBuilder() << "-- EvScanData from " << ev->Sender << ": pass" << Endl);
                        Cerr.Flush();
                    }

                    break;
                }

                default:
                    break;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(captureEvents);

        auto streamSender = runtime.AllocateEdgeActor();
        SendRequest(runtime, streamSender, MakeStreamRequest(streamSender, "SELECT sum(value) FROM `/Root/table-1`;", false));
        auto ev = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(streamSender);

        UNIT_ASSERT_VALUES_EQUAL(result, 596400);
    }

    /*
     * Force remote scans by meddling with EvShardsResolveStatus. Check that remote scan actually took place.
     */
    Y_UNIT_TEST(RemoteShardScan) {
        NKikimrConfig::TAppConfig appCfg;

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetNodeCount(2)
            .SetAppConfig(appCfg)
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        EnableLogging(runtime);

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", 7);
        ExecSQL(server, sender, FillTableQuery());

        ui64 firstNodeId = server->GetRuntime()->GetNodeId(0);
        // ui64 secondNodeId = server->GetRuntime()->GetNodeId(1);

        bool remoteScanDetected = false;

        ui64 result = 0;

        auto captureEvents = [&](TAutoPtr<IEventHandle> &ev) {

            switch (ev->GetTypeRewrite()) {
                /*
                 * Trick executor to think that all datashard are located on node 1.
                 */
                case NKqp::TKqpExecuterEvents::EvShardsResolveStatus: {
                    auto* msg = ev->Get<NKqp::NShardResolver::TEvShardsResolveStatus>();
                    for (auto& [shardId, nodeId]: msg->ShardNodes) {
                        nodeId = firstNodeId;
                    }
                    break;
                }

                /*
                 * Respond to streamData with acks. Without that execution pipeline will stop
                 * producing new tuples.
                 */
                case NKqp::TKqpExecuterEvents::EvStreamData: {
                    auto& record = ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>()->Record;

                    Cerr << (TStringBuilder() << "-- EvStreamData(from: " << ev->Sender << ", to: " << ev->Recipient << "): " << record.AsJSON() << Endl);

                    Y_ASSERT(record.GetResultSet().rows().size() == 1);
                    Y_ASSERT(record.GetResultSet().rows().at(0).items().size() == 1);
                    result = record.GetResultSet().rows().at(0).items().at(0).uint64_value();

                    auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>(record.GetSeqNo(), record.GetChannelId());
                    resp->Record.SetEnough(false);
                    resp->Record.SetFreeSpace(100);
                    runtime.Send(new IEventHandle(ev->Sender, sender, resp.Release()));
                    return TTestActorRuntime::EEventAction::DROP;
                }

                /*
                 * Check that remote scan actually happened.
                 */
                case NKqp::TKqpComputeEvents::EvScanData:
                case TEvDataShard::EvRead: {
                    remoteScanDetected = remoteScanDetected || ev->Sender.NodeId() != ev->Recipient.NodeId();
                    break;
                }

                default:
                    break;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(captureEvents);

        auto streamSender = runtime.AllocateEdgeActor();
        SendRequest(runtime, streamSender, MakeStreamRequest(streamSender, "SELECT sum(value) FROM `/Root/table-1`;", false));
        auto ev = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(streamSender);

        UNIT_ASSERT(remoteScanDetected);
        UNIT_ASSERT_VALUES_EQUAL(result, 596400);
    }

    Y_UNIT_TEST(ScanDuringSplit10) {
       NKikimrConfig::TAppConfig appCfg;

        auto* rm = appCfg.MutableTableServiceConfig()->MutableResourceManager();
        rm->SetChannelBufferSize(100);
        rm->SetMinChannelBufferSize(100);

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetNodeCount(2)
            .SetAppConfig(appCfg)
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();
        auto senderSplit = runtime.AllocateEdgeActor();

        EnableLogging(runtime);

        SetSplitMergePartCountLimit(&runtime, -1);

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        ExecSQL(server, sender, FillTableQuery());

        auto shards = GetTableShards(server, sender, "/Root/table-1");
        for (const auto& shard: shards) {
            Cerr << (TStringBuilder() << "-- shardId=" << shard << Endl);
            Cerr.Flush();
        }

        TSet<TActorId> scans;
        TActorId firstScanActor;
        ui64 tabletId = 0;

        ui64 result = 0;

        auto captureEvents = [&](TAutoPtr<IEventHandle> &ev) {
            switch (ev->GetTypeRewrite()) {
                case NKqp::TKqpExecuterEvents::EvShardsResolveStatus: {
                    auto* msg = ev->Get<NKqp::NShardResolver::TEvShardsResolveStatus>();
                    for (auto& [shardId, nodeId]: msg->ShardNodes) {
                        tabletId = shardId;
                        Cerr << (TStringBuilder() << "-- tabletId= " << tabletId << Endl);
                        Cerr.Flush();
                    }
                    break;
                }

                case TEvDataShard::EvKqpScan: {
                    Cerr << (TStringBuilder() << "-- EvScan " << ev->Sender << " -> " << ev->Recipient << Endl);
                    Cerr.Flush();
                    break;
                }

                /*
                 * Respond to streamData with acks. Without that execution pipeline will stop
                 * producing new tuples.
                 */
                case NKqp::TKqpExecuterEvents::EvStreamData: {
                    auto& record = ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>()->Record;

                    Cerr << (TStringBuilder() << "-- EvStreamData: " << record.AsJSON() << Endl);
                    Cerr.Flush();

                    Y_ASSERT(record.GetResultSet().rows().size() == 1);
                    Y_ASSERT(record.GetResultSet().rows().at(0).items().size() == 1);
                    result = record.GetResultSet().rows().at(0).items().at(0).uint64_value();

                    auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>(record.GetSeqNo(), record.GetChannelId());
                    resp->Record.SetEnough(false);
                    resp->Record.SetFreeSpace(100);

                    runtime.Send(new IEventHandle(ev->Sender, sender, resp.Release()));

                    return TTestActorRuntime::EEventAction::DROP;
                }

                /* Drop message and kill tablet if we already had seen this tablet */
                case NKqp::TKqpComputeEvents::EvScanData: {
                    if (!firstScanActor) {
                        firstScanActor = ev->Sender;
                        AsyncSplitTable(server, senderSplit, "/Root/table-1", tabletId, 10 /* splitKey */);
                        Cerr << (TStringBuilder() << "-- EvScanData from old tablet " << ev->Sender << ": pass and split" << Endl);
                        Cerr.Flush();
                    } else if (firstScanActor == ev->Sender) {
                        // data from old table scan, drop it
                        Cerr << (TStringBuilder() << "-- EvScanData from old tablet " << ev->Sender << ": drop" << Endl);
                        Cerr.Flush();
                        return TTestActorRuntime::EEventAction::DROP;
                    } else {
                        // data from new tablet scan, pass it
                        Cerr << (TStringBuilder() << "-- EvScanData from new tablet" << ev->Sender << ": pass" << Endl);
                        Cerr.Flush();
                    }

                    break;
                }

                default:
                    break;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(captureEvents);

        auto streamSender = runtime.AllocateEdgeActor();
        SendRequest(runtime, streamSender, MakeStreamRequest(streamSender, "SELECT sum(value) FROM `/Root/table-1`;", false));
        auto ev = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(streamSender);

        UNIT_ASSERT_VALUES_EQUAL(result, 596400);
    }

    Y_UNIT_TEST(ScanDuringSplitThenMerge) {
       NKikimrConfig::TAppConfig appCfg;

        auto* rm = appCfg.MutableTableServiceConfig()->MutableResourceManager();
        rm->SetChannelBufferSize(100);
        rm->SetMinChannelBufferSize(100);

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetNodeCount(2)
            .SetAppConfig(appCfg)
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();
        auto senderSplit = runtime.AllocateEdgeActor();

        EnableLogging(runtime);

        SetSplitMergePartCountLimit(&runtime, -1);

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        ExecSQL(server, sender, FillTableQuery());

        auto shards = GetTableShards(server, sender, "/Root/table-1");
        for (const auto& shard: shards) {
            Cerr << (TStringBuilder() << "-- shardId=" << shard << Endl);
            Cerr.Flush();
        }

        TSet<TActorId> scans;
        TActorId firstScanActor;
        TActorId secondScanActor;
        TActorId kqpScanActor;
        ui64 tabletId = 0;

        ui64 result = 0;

        auto captureEvents = [&](TAutoPtr<IEventHandle> &ev) {
            switch (ev->GetTypeRewrite()) {
                case NKqp::TKqpExecuterEvents::EvShardsResolveStatus: {
                    auto* msg = ev->Get<NKqp::NShardResolver::TEvShardsResolveStatus>();
                    for (auto& [shardId, nodeId]: msg->ShardNodes) {
                        tabletId = shardId;
                        Cerr << (TStringBuilder() << "-- tabletId= " << tabletId << Endl);
                        Cerr.Flush();
                    }
                    break;
                }

                case TEvDataShard::EvKqpScan: {
                    Cerr << (TStringBuilder() << "-- EvScan " << ev->Sender << " -> " << ev->Recipient << Endl);
                    Cerr.Flush();
                    break;
                }

                /*
                 * Respond to streamData with acks. Without that execution pipeline will stop
                 * producing new tuples.
                 */
                case NKqp::TKqpExecuterEvents::EvStreamData: {
                    auto& record = ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>()->Record;

                    Cerr << (TStringBuilder() << "-- EvStreamData: " << record.AsJSON() << Endl);
                    Cerr.Flush();

                    Y_ASSERT(record.GetResultSet().rows().size() == 1);
                    Y_ASSERT(record.GetResultSet().rows().at(0).items().size() == 1);
                    result = record.GetResultSet().rows().at(0).items().at(0).uint64_value();

                    auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>(record.GetSeqNo(), record.GetChannelId());
                    resp->Record.SetEnough(false);
                    resp->Record.SetFreeSpace(100);

                    runtime.Send(new IEventHandle(ev->Sender, sender, resp.Release()));

                    return TTestActorRuntime::EEventAction::DROP;
                }

                /* Drop message and kill tablet if we already had seen this tablet */
                case NKqp::TKqpComputeEvents::EvScanData: {
                    if (!firstScanActor) {
                        firstScanActor = ev->Sender;
                        kqpScanActor = ev->Recipient;
                        AsyncSplitTable(server, senderSplit, "/Root/table-1", tabletId, 55 /* splitKey */);
                        Cerr << (TStringBuilder() << "-- EvScanData from old tablet " << ev->Sender << ": pass and split" << Endl);
                        Cerr.Flush();
                    } else if (firstScanActor == ev->Sender) {
                        // data from old table scan, drop it
                        Cerr << (TStringBuilder() << "-- EvScanData from old tablet " << ev->Sender << ": drop" << Endl);
                        Cerr.Flush();
                        return TTestActorRuntime::EEventAction::DROP;
                    } else if (!secondScanActor && ev->Recipient == kqpScanActor) {
                        secondScanActor = ev->Sender;
                        for(auto shard: GetTableShards(server, senderSplit, "/Root/table-1")) {
                            auto [tables, ownerId] = GetTables(server, shard);
                            CompactTable(runtime, shard, TTableId(ownerId, tables["table-1"].GetPathId()), true);
                        }
                        AsyncMergeTable(server, senderSplit, "/Root/table-1", GetTableShards(server, senderSplit, "/Root/table-1"));
                        Cerr << (TStringBuilder() << "-- EvScanData from second old tablet " << ev->Sender << ": pass and merge" << Endl);
                        // data from new tablet scan, pass it
                        Cerr.Flush();
                    } else if (secondScanActor == ev->Sender) {
                        Cerr << (TStringBuilder() << "-- EvScanData from old tablet " << ev->Sender << ": drop" << Endl);
                        Cerr.Flush();
                        return TTestActorRuntime::EEventAction::DROP;
                    } else {
                        Cerr << (TStringBuilder() << "-- EvScanData from new tablet" << ev->Sender << ": pass" << Endl);
                        Cerr.Flush();
                    }

                    break;
                }

                default:
                    break;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(captureEvents);

        auto streamSender = runtime.AllocateEdgeActor();
        SendRequest(runtime, streamSender, MakeStreamRequest(streamSender, "SELECT sum(value) FROM `/Root/table-1`;", false));
        auto ev = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(streamSender);

        UNIT_ASSERT_VALUES_EQUAL(result, 596400);
    }

    Y_UNIT_TEST(ScanDuringSplit) {
       NKikimrConfig::TAppConfig appCfg;

        auto* rm = appCfg.MutableTableServiceConfig()->MutableResourceManager();
        rm->SetChannelBufferSize(100);
        rm->SetMinChannelBufferSize(100);

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetNodeCount(2)
            .SetAppConfig(appCfg)
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();
        auto senderSplit = runtime.AllocateEdgeActor();

        EnableLogging(runtime);

        SetSplitMergePartCountLimit(&runtime, -1);

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        ExecSQL(server, sender, FillTableQuery());

        auto shards = GetTableShards(server, sender, "/Root/table-1");
        for (const auto& shard: shards) {
            Cerr << (TStringBuilder() << "-- shardId=" << shard << Endl);
            Cerr.Flush();
        }

        TSet<TActorId> scans;
        TActorId firstScanActor;
        ui64 tabletId = 0;

        ui64 result = 0;

        auto captureEvents = [&](TAutoPtr<IEventHandle> &ev) {
            switch (ev->GetTypeRewrite()) {
                case NKqp::TKqpExecuterEvents::EvShardsResolveStatus: {
                    auto* msg = ev->Get<NKqp::NShardResolver::TEvShardsResolveStatus>();
                    for (auto& [shardId, nodeId]: msg->ShardNodes) {
                        tabletId = shardId;
                        Cerr << (TStringBuilder() << "-- tabletId= " << tabletId << Endl);
                        Cerr.Flush();
                    }
                    break;
                }

                case TEvDataShard::EvKqpScan: {
                    Cerr << (TStringBuilder() << "-- EvScan " << ev->Sender << " -> " << ev->Recipient << Endl);
                    Cerr.Flush();
                    break;
                }

                /*
                 * Respond to streamData with acks. Without that execution pipeline will stop
                 * producing new tuples.
                 */
                case NKqp::TKqpExecuterEvents::EvStreamData: {
                    auto& record = ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>()->Record;

                    Cerr << (TStringBuilder() << "-- EvStreamData: " << record.AsJSON() << Endl);
                    Cerr.Flush();

                    Y_ASSERT(record.GetResultSet().rows().size() == 1);
                    Y_ASSERT(record.GetResultSet().rows().at(0).items().size() == 1);
                    result = record.GetResultSet().rows().at(0).items().at(0).uint64_value();

                    auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>(record.GetSeqNo(), record.GetChannelId());
                    resp->Record.SetEnough(false);
                    resp->Record.SetFreeSpace(100);

                    runtime.Send(new IEventHandle(ev->Sender, sender, resp.Release()));

                    return TTestActorRuntime::EEventAction::DROP;
                }

                /* Drop message and kill tablet if we already had seen this tablet */
                case NKqp::TKqpComputeEvents::EvScanData: {
                    if (!firstScanActor) {
                        firstScanActor = ev->Sender;
                        AsyncSplitTable(server, senderSplit, "/Root/table-1", tabletId, 55 /* splitKey */);
                        Cerr << (TStringBuilder() << "-- EvScanData from old tablet " << ev->Sender << ": pass and split" << Endl);
                        Cerr.Flush();
                    } else if (firstScanActor == ev->Sender) {
                        // data from old table scan, drop it
                        Cerr << (TStringBuilder() << "-- EvScanData from old tablet " << ev->Sender << ": drop" << Endl);
                        Cerr.Flush();
                        return TTestActorRuntime::EEventAction::DROP;
                    } else {
                        // data from new tablet scan, pass it
                        Cerr << (TStringBuilder() << "-- EvScanData from new tablet" << ev->Sender << ": pass" << Endl);
                        Cerr.Flush();
                    }

                    break;
                }

                default:
                    break;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(captureEvents);

        auto streamSender = runtime.AllocateEdgeActor();
        SendRequest(runtime, streamSender, MakeStreamRequest(streamSender, "SELECT sum(value) FROM `/Root/table-1`;", false));
        auto ev = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(streamSender);

        UNIT_ASSERT_VALUES_EQUAL(result, 596400);
    }

    Y_UNIT_TEST(ScanRetryReadRanges) {
        Y_UNUSED(EnableLogging);

        NKikimrConfig::TAppConfig appCfg;

        auto* rm = appCfg.MutableTableServiceConfig()->MutableResourceManager();
        rm->SetChannelBufferSize(100);
        rm->SetMinChannelBufferSize(100);

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetNodeCount(2)
            .SetAppConfig(appCfg)
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        // EnableLogging(runtime);

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        ExecSQL(server, sender, FillTableQuery());

        TSet<TActorId> scans;
        TSet<TActorId> killedTablets;

        ui64 result = 0;
        ui64 incomingRangesSize = 0;

        auto captureEvents = [&](TAutoPtr<IEventHandle> &ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                /*
                 * Trick executor to think that all datashard are located on node 1.
                 */
                case NKqp::TKqpExecuterEvents::EvShardsResolveStatus: {
                    auto* msg = ev->Get<NKqp::NShardResolver::TEvShardsResolveStatus>();
                    for (auto& [shardId, nodeId]: msg->ShardNodes) {
                        Cerr << "-- nodeId: " << nodeId << Endl;
                        nodeId = runtime.GetNodeId(0);
                    }
                    break;
                }

                case TEvDataShard::EvKqpScan: {
                    Cerr << (TStringBuilder() << "-- EvScan " << ev->Sender << " -> " << ev->Recipient << Endl);

                    if (!incomingRangesSize) {
                        auto& request = ev->Get<TEvDataShard::TEvKqpScan>()->Record;
                        incomingRangesSize = request.RangesSize();
                    }

                    break;
                }
                case TEvDataShard::EvRead: {
                    if (!incomingRangesSize) {
                        auto& request = ev->Get<TEvDataShard::TEvRead>()->Record;
                        incomingRangesSize = request.RangesSize();
                    }
                    break;
                }

                /*
                 * Respond to streamData with acks. Without that execution pipeline will stop
                 * producing new tuples.
                 */
                case NKqp::TKqpExecuterEvents::EvStreamData: {
                    auto& record = ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>()->Record;

                    Cerr << (TStringBuilder() << "-- EvStreamData: " << record.AsJSON() << Endl);

                    // Empty message can come on finish
                    if (!record.GetResultSet().rows().empty()) {
                        Y_ASSERT(record.GetResultSet().rows().at(0).items().size() == 2);
                        for (int i = 0; i < record.GetResultSet().rows().size(); ++i) {
                            // Get value from (key, value)
                            auto val = record.GetResultSet().rows().at(i).items().at(1).uint32_value();
                            result += val;
                        }
                    }

                    auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>(record.GetSeqNo(), record.GetChannelId());
                    resp->Record.SetEnough(false);
                    resp->Record.SetFreeSpace(100);
                    runtime.Send(new IEventHandle(ev->Sender, sender, resp.Release()));
                    return TTestActorRuntime::EEventAction::DROP;
                }

                /* Drop message and kill tablet if we already had seen this tablet */
                case NKqp::TKqpComputeEvents::EvScanData: {
                    if (scans.contains(ev->Sender)) {
                        if (killedTablets.empty()) { // do only 1 kill per test
                            runtime.Send(new IEventHandle(ev->Sender, ev->Sender, new NKqp::TEvKqpCompute::TEvKillScanTablet));
                            Cerr << (TStringBuilder() << "-- EvScanData from " << ev->Sender << ": hijack event, kill tablet " << ev->Sender << Endl);
                            Cerr.Flush();
                        }
                    } else {
                        scans.insert(ev->Sender);
                        runtime.EnableScheduleForActor(ev->Sender);

                        Cerr << (TStringBuilder() << "-- EvScanData from " << ev->Sender << ": pass" << Endl);

                        auto scanEvent = ev->Get<NKqp::TEvKqpCompute::TEvScanData>();

                        for (auto& item: scanEvent->Rows) {
                            // Row consists of 'key', 'value'
                            ui32 key = item[0].AsValue<ui32>();

                            // Check that key correspond to query
                            bool inRange = (key > 1 && key < 3) || (key > 20 && key < 30) || (key >= 40 && key <= 50);
                            UNIT_ASSERT_C(inRange, TStringBuilder() << "Key " << key << "not in query range");
                        }
                    }

                    break;
                }

                default:
                    break;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(captureEvents);

        auto query = TString(R"(
            --!syntax_v1
            SELECT key, value FROM `/Root/table-1`
            WHERE
                (key > 1 AND key < 3) OR
                (key > 20 AND key < 30) OR
                (key >= 40 AND key <= 50)
            ORDER BY key;
        )");

        auto streamSender = runtime.AllocateEdgeActor();
        SendRequest(runtime, streamSender, MakeStreamRequest(streamSender, query, false));
        auto ev = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(streamSender);

        UNIT_ASSERT_VALUES_EQUAL(result, 72742);
        UNIT_ASSERT_VALUES_EQUAL(incomingRangesSize, 3);
    }

    Y_UNIT_TEST(ScanAfterSplitSlowMetaRead) {
        NKikimrConfig::TAppConfig appCfg;

        auto* rm = appCfg.MutableTableServiceConfig()->MutableResourceManager();
        rm->SetChannelBufferSize(100);
        rm->SetMinChannelBufferSize(100);

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetNodeCount(2)
            .SetAppConfig(appCfg)
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        EnableLogging(runtime);

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        ExecSQL(server, sender, FillTableQuery());

        auto waitFor = [&](const auto& condition, const TString& description) {
            if (!condition()) {
                Cerr << "... waiting for " << description << Endl;
                TDispatchOptions options;
                options.CustomFinalCondition = [&]() {
                    return condition();
                };
                runtime.DispatchEvents(options);
                UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
            }
        };

        std::optional<int> result;
        std::optional<Ydb::StatusIds::StatusCode> status;
        auto streamSender = runtime.Register(new TLambdaActor([&](TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
            Cerr << "... response " << ev->GetTypeRewrite() << " " << ev->GetTypeName() << " " << ev->ToString() << Endl;
            switch (ev->GetTypeRewrite()) {
                case NKqp::TEvKqpExecuter::TEvStreamData::EventType: {
                    auto* msg = ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>();
                    auto& record = msg->Record;
                    Y_ASSERT(record.GetResultSet().rows().size() == 1);
                    Y_ASSERT(record.GetResultSet().rows().at(0).items().size() == 1);
                    result = record.GetResultSet().rows().at(0).items().at(0).uint64_value();

                    auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>(record.GetSeqNo(), record.GetChannelId());
                    resp->Record.SetEnough(false);
                    resp->Record.SetFreeSpace(100);
                    ctx.Send(ev->Sender, resp.Release());
                    break;
                }
                case NKqp::TEvKqp::TEvQueryResponse::EventType: {
                    auto* msg = ev->Get<NKqp::TEvKqp::TEvQueryResponse>();
                    auto& record = msg->Record.GetRef();
                    status = record.GetYdbStatus();
                    break;
                }
            }
        }));

        SendRequest(runtime, streamSender, MakeStreamRequest(streamSender, "SELECT sum(value) FROM `/Root/table-1`;", false));
        waitFor([&]{ return bool(status); }, "request status");

        UNIT_ASSERT_VALUES_EQUAL(*status, Ydb::StatusIds::SUCCESS);

        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(*result, 596400);

        SetSplitMergePartCountLimit(&runtime, -1);

        auto shards = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TVector<THolder<IEventHandle>> blockedGets;
        TVector<THolder<IEventHandle>> blockedSnapshots;
        auto blockGetObserver = [&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case NKqp::TEvKqpSnapshot::TEvCreateSnapshotResponse::EventType: {
                    Cerr << "... blocking snapshot response" << Endl;
                    blockedSnapshots.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                case TEvBlobStorage::TEvGet::EventType: {
                    auto* msg = ev->Get<TEvBlobStorage::TEvGet>();
                    bool block = false;
                    for (ui32 i = 0; i < msg->QuerySize; ++i) {
                        if (msg->Queries[i].Id.TabletID() == shards.at(0)) {
                            Cerr << "... blocking get request to " << msg->Queries[i].Id << Endl;
                            block = true;
                        }
                    }
                    if (block) {
                        blockedGets.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(blockGetObserver);

        result = {};
        status = {};
        SendRequest(runtime, streamSender, MakeStreamRequest(streamSender, "SELECT sum(value) FROM `/Root/table-1`;", false));

        waitFor([&]{ return blockedSnapshots.size() > 0; }, "snapshot response");

        // Start a split, surprisingly it will succeed despite blocked events
        auto senderSplit = runtime.AllocateEdgeActor();
        ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", shards.at(0), 55 /* splitKey */);
        WaitTxNotification(server, senderSplit, txId);

        // Unblock snapshot and try waiting for results
        runtime.SetObserverFunc(prevObserverFunc);
        for (auto& ev : blockedSnapshots) {
            ui32 nodeIdx = ev->GetRecipientRewrite().NodeId() - runtime.GetNodeId(0);
            Cerr << "... unblocking snapshot" << Endl;
            runtime.Send(ev.Release(), nodeIdx, true);
        }
        blockedSnapshots.clear();

        SimulateSleep(runtime, TDuration::Seconds(1));
        UNIT_ASSERT_C(!status, "Query finished with status: " << *status);
        UNIT_ASSERT_C(!result, "Query returned unexpected result: " << *result);

        // Unblock storage reads and wait for result
        for (auto& ev : blockedGets) {
            ui32 nodeIdx = ev->GetRecipientRewrite().NodeId() - runtime.GetNodeId(0);
            Cerr << "... unblocking get" << Endl;
            runtime.Send(ev.Release(), nodeIdx, true);
        }
        blockedGets.clear();

        waitFor([&]{ return bool(status); }, "request finish");
        UNIT_ASSERT_VALUES_EQUAL(*status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(*result, 596400);
    }

}

} // namespace NKqp
} // namespace NKikimr
