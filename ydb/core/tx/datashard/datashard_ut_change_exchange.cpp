#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "datashard_ut_common_kqp.h"

#include <ydb/core/base/path.h>
#include <ydb/core/change_exchange/change_sender_common_ops.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/user_info.h>
#include <ydb/core/persqueue/write_meta.h>
#include <ydb/core/tx/scheme_board/events.h>
#include <ydb/public/sdk/cpp/client/ydb_datastreams/datastreams.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <util/generic/algorithm.h>
#include <util/generic/size_literals.h>
#include <util/string/join.h>
#include <util/string/printf.h>
#include <util/string/strip.h>

namespace NKikimr {

using namespace NDataShard;
using namespace NDataShard::NKqpHelpers;
using namespace Tests;

Y_UNIT_TEST_SUITE(AsyncIndexChangeExchange) {
    void SenderShouldBeActivated(const TString& path, const TShardedTableOptions& opts) {
        const auto pathParts = SplitPath(path);
        UNIT_ASSERT(pathParts.size() > 1);

        const auto domainName = pathParts.at(0);
        const auto workingDir = CombinePath(pathParts.begin(), pathParts.begin() + pathParts.size() - 1);
        const auto tableName = pathParts.at(pathParts.size() - 1);

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName(domainName)
            .SetUseRealThreads(false)
            .SetEnableDataColumnForIndexTable(true);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::CHANGE_EXCHANGE, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        bool activated = false;
        runtime.SetObserverFunc([&activated](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvChangeExchange::EvActivateSender) {
                activated = true;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        CreateShardedTable(server, sender, workingDir, tableName, opts);

        if (!activated) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&activated](IEventHandle&) {
                return activated;
            });
            server->GetRuntime()->DispatchEvents(opts);
        }
    }

    TShardedTableOptions TableWoIndexes() {
        return TShardedTableOptions()
            .Columns({
                {"pkey", "Uint32", true, false},
                {"ikey", "Uint32", false, false},
            });
    }

    TShardedTableOptions::TIndex SimpleSyncIndex() {
        return TShardedTableOptions::TIndex{
            "by_ikey", {"ikey"}, {}, NKikimrSchemeOp::EIndexTypeGlobal
        };
    }

    TShardedTableOptions::TIndex SimpleAsyncIndex() {
        return TShardedTableOptions::TIndex{
            "by_ikey", {"ikey"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync
        };
    }

    TShardedTableOptions TableWithIndex(const TShardedTableOptions::TIndex& index) {
        return TShardedTableOptions()
            .Columns({
                {"pkey", "Uint32", true, false},
                {"ikey", "Uint32", false, false},
            })
            .Indexes({
                index
            });
    }

    Y_UNIT_TEST(SenderShouldBeActivatedOnTableWoIndexes) {
        SenderShouldBeActivated("/Root/Table", TableWoIndexes());
    }

    Y_UNIT_TEST(SenderShouldBeActivatedOnTableWithSyncIndex) {
        SenderShouldBeActivated("/Root/Table", TableWithIndex(SimpleSyncIndex()));
    }

    Y_UNIT_TEST(SenderShouldBeActivatedOnTableWithAsyncIndex) {
        SenderShouldBeActivated("/Root/Table", TableWithIndex(SimpleAsyncIndex()));
    }

    void SenderShouldShakeHands(const TString& path, ui32 times, const TShardedTableOptions& opts,
            TMaybe<TShardedTableOptions::TIndex> addIndex, const TString& query)
    {
        const auto pathParts = SplitPath(path);
        UNIT_ASSERT(pathParts.size() > 1);

        const auto domainName = pathParts.at(0);
        const auto workingDir = CombinePath(pathParts.begin(), pathParts.begin() + pathParts.size() - 1);
        const auto tableName = pathParts.at(pathParts.size() - 1);

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName(domainName)
            .SetUseRealThreads(false)
            .SetEnableDataColumnForIndexTable(true);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::CHANGE_EXCHANGE, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        ui32 counter = 0;
        runtime.SetObserverFunc([&counter](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvChangeExchange::EvHandshake) {
                ++counter;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        CreateShardedTable(server, sender, workingDir, tableName, opts);
        if (addIndex) {
            WaitTxNotification(server, sender, AsyncAlterAddIndex(server, domainName, path, *addIndex));
        }

        // trigger initialization
        ExecSQL(server, sender, query);

        if (counter != times) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&](IEventHandle&) {
                return counter == times;
            });
            server->GetRuntime()->DispatchEvents(opts);
        }
    }

    Y_UNIT_TEST(SenderShouldShakeHandsOnce) {
        SenderShouldShakeHands("/Root/Table", 1, TableWithIndex(SimpleAsyncIndex()), {},
            "UPSERT INTO `/Root/Table` (pkey, ikey) VALUES (1, 10);");
    }

    Y_UNIT_TEST(SenderShouldShakeHandsTwice) {
        SenderShouldShakeHands("/Root/Table", 2, TShardedTableOptions()
            .Columns({
                {"pkey", "Uint32", true, false},
                {"i1key", "Uint32", false, false},
                {"i2key", "Uint32", false, false},
            })
            .Indexes({
                {"by_i1key", {"i1key"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync},
                {"by_i2key", {"i2key"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync},
            }), {},
            "UPSERT INTO `/Root/Table` (pkey, i1key, i2key) VALUES (1, 10, 100);"
        );
    }

    Y_UNIT_TEST(SenderShouldShakeHandsAfterAddingIndex) {
        SenderShouldShakeHands("/Root/Table", 1, TableWoIndexes(), SimpleAsyncIndex(),
            "UPSERT INTO `/Root/Table` (pkey, ikey) VALUES (1, 10);");
    }

    void ShouldDeliverChanges(const TString& path, const TShardedTableOptions& opts,
            TMaybe<TShardedTableOptions::TIndex> addIndex, const TVector<TString>& queries)
    {
        const auto pathParts = SplitPath(path);
        UNIT_ASSERT(pathParts.size() > 1);

        const auto domainName = pathParts.at(0);
        const auto workingDir = CombinePath(pathParts.begin(), pathParts.begin() + pathParts.size() - 1);
        const auto tableName = pathParts.at(pathParts.size() - 1);

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName(domainName)
            .SetUseRealThreads(false)
            .SetEnableDataColumnForIndexTable(true);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::CHANGE_EXCHANGE, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        THashSet<ui64> enqueued;
        THashSet<ui64> requested;
        THashSet<ui64> removed;

        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case NChangeExchange::TEvChangeExchange::EvEnqueueRecords:
                for (const auto& record : ev->Get<NChangeExchange::TEvChangeExchange::TEvEnqueueRecords>()->Records) {
                    enqueued.insert(record.Order);
                }
                break;

            case NChangeExchange::TEvChangeExchange::EvRequestRecords:
                for (const auto& record : ev->Get<NChangeExchange::TEvChangeExchange::TEvRequestRecords>()->Records) {
                    requested.insert(record.Order);
                }
                break;

            case NChangeExchange::TEvChangeExchange::EvRemoveRecords:
                for (const auto& record : ev->Get<NChangeExchange::TEvChangeExchange::TEvRemoveRecords>()->Records) {
                    removed.insert(record);
                }
                break;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        CreateShardedTable(server, sender, workingDir, tableName, opts);
        if (addIndex) {
            WaitTxNotification(server, sender, AsyncAlterAddIndex(server, domainName, path, *addIndex));
        }

        for (const auto& query : queries) {
            ExecSQL(server, sender, query);
        }

        if (removed.size() != queries.size()) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&removed, expected = queries.size()](IEventHandle&) {
                return removed.size() == expected;
            });
            server->GetRuntime()->DispatchEvents(opts);
        }

        UNIT_ASSERT_VALUES_EQUAL(enqueued.size(), requested.size());
        UNIT_ASSERT_VALUES_EQUAL(enqueued.size(), removed.size());
    }

    Y_UNIT_TEST(ShouldDeliverChangesOnFreshTable) {
        ShouldDeliverChanges("/Root/Table", TableWithIndex(SimpleAsyncIndex()), Nothing(), {
            "INSERT INTO `/Root/Table` (pkey, ikey) VALUES (1, 10);",
            "UPSERT INTO `/Root/Table` (pkey, ikey) VALUES (2, 20);",
        });
    }

    Y_UNIT_TEST(ShouldDeliverChangesOnAlteredTable) {
        ShouldDeliverChanges("/Root/Table", TableWoIndexes(), SimpleAsyncIndex(), {
            "INSERT INTO `/Root/Table` (pkey, ikey) VALUES (1, 10);",
            "DELETE FROM `/Root/Table` WHERE pkey = 1;",
        });
    }

    Y_UNIT_TEST(ShouldRemoveRecordsAfterDroppingIndex) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableDataColumnForIndexTable(true);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::CHANGE_EXCHANGE, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        bool preventActivation = true;
        TVector<THolder<IEventHandle>> activations;

        THashSet<ui64> enqueued;
        THashSet<ui64> removed;

        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TEvChangeExchange::EvActivateSender:
                if (preventActivation) {
                    activations.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                } else {
                    return TTestActorRuntime::EEventAction::PROCESS;
                }

            case NChangeExchange::TEvChangeExchange::EvEnqueueRecords:
                for (const auto& record : ev->Get<NChangeExchange::TEvChangeExchange::TEvEnqueueRecords>()->Records) {
                    enqueued.insert(record.Order);
                }
                break;

            case NChangeExchange::TEvChangeExchange::EvRemoveRecords:
                for (const auto& record : ev->Get<NChangeExchange::TEvChangeExchange::TEvRemoveRecords>()->Records) {
                    removed.insert(record);
                }
                break;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        CreateShardedTable(server, sender, "/Root", "Table", TableWithIndex(SimpleAsyncIndex()));
        ExecSQL(server, sender, "INSERT INTO `/Root/Table` (pkey, ikey) VALUES (1, 10);");
        WaitTxNotification(server, sender, AsyncAlterDropIndex(server, "/Root", "Table", "by_ikey"));

        if (activations.size() != 2 /* main + index */) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&activations](IEventHandle&) {
                return activations.size() == 2;
            });
            server->GetRuntime()->DispatchEvents(opts);
        }

        preventActivation = false;
        for (auto& ev : activations) {
            server->GetRuntime()->Send(ev.Release(), 0, true);
        }

        if (removed.size() != 1) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&removed](IEventHandle&) {
                return removed.size() == 1;
            });
            server->GetRuntime()->DispatchEvents(opts);
        }

        UNIT_ASSERT_VALUES_EQUAL(enqueued.size(), removed.size());
    }

    Y_UNIT_TEST(ShouldRemoveRecordsAfterCancelIndexBuild) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableDataColumnForIndexTable(true);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::CHANGE_EXCHANGE, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        TVector<THolder<IEventHandle>> delayed;
        bool inited = false;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case NChangeExchange::TEvChangeExchange::EvEnqueueRecords:
                delayed.emplace_back(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;

            case TEvDataShard::EvBuildIndexCreateRequest:
                inited = true;
                return TTestActorRuntime::EEventAction::DROP;

            default:
                return TTestActorRuntime::EEventAction::PROCESS;
            }
        });

        CreateShardedTable(server, sender, "/Root", "Table", TableWoIndexes());
        ExecSQL(server, sender, R"(INSERT INTO `/Root/Table` (pkey, ikey) VALUES
            (1, 10),
            (2, 20),
            (3, 30);
        )");

        const auto buildIndexId = AsyncAlterAddIndex(server, "/Root", "/Root/Table", SimpleAsyncIndex());
        if (!inited) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&inited](IEventHandle&) {
                return inited;
            });
            runtime.DispatchEvents(opts);
        }

        ExecSQL(server, sender, "INSERT INTO `/Root/Table` (pkey, ikey) VALUES (4, 40);");
        if (delayed.empty()) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&delayed](IEventHandle&) {
                return !delayed.empty();
            });
            runtime.DispatchEvents(opts);
        }

        CancelAddIndex(server, "/Root", buildIndexId);
        WaitTxNotification(server, sender, buildIndexId);

        THashSet<ui64> enqueued;
        THashSet<ui64> removed;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case NChangeExchange::TEvChangeExchange::EvEnqueueRecords:
                for (const auto& record : ev->Get<NChangeExchange::TEvChangeExchange::TEvEnqueueRecords>()->Records) {
                    enqueued.insert(record.Order);
                }
                break;

            case NChangeExchange::TEvChangeExchange::EvRemoveRecords:
                for (const auto& record : ev->Get<NChangeExchange::TEvChangeExchange::TEvRemoveRecords>()->Records) {
                    removed.insert(record);
                }
                break;

            default:
                break;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        for (auto& ev : std::exchange(delayed, TVector<THolder<IEventHandle>>())) {
            runtime.Send(ev.Release(), 0, true);
        }

        if (removed.empty()) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&enqueued, &removed](IEventHandle&) {
                return removed && enqueued == removed;
            });
            runtime.DispatchEvents(opts);
        }
    }

    void WaitForContent(TServer::TPtr server, const TString& tablePath, const TString& expected) {
        while (true) {
            auto content = ReadShardedTable(server, tablePath);
            if (StripInPlace(content) == expected) {
                break;
            } else {
                SimulateSleep(server, TDuration::Seconds(1));
            }
        }
    }

    Y_UNIT_TEST(ShouldDeliverChangesOnSplitMerge) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableDataColumnForIndexTable(true);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::CHANGE_EXCHANGE, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        bool preventEnqueueing = true;
        TVector<THolder<IEventHandle>> enqueued;
        THashMap<ui64, ui32> splitAcks;
        ui32 allowedRejects = Max<ui32>();

        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case NChangeExchange::TEvChangeExchange::EvEnqueueRecords:
                if (preventEnqueueing) {
                    enqueued.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                } else {
                    return TTestActorRuntime::EEventAction::PROCESS;
                }

            case TEvChangeExchange::EvStatus:
                if (ev->Get<TEvChangeExchange::TEvStatus>()->Record.GetStatus() == NKikimrChangeExchange::TEvStatus::STATUS_REJECT) {
                    if (!allowedRejects) {
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    --allowedRejects;
                }
                break;

            case TEvDataShard::EvSplitAck:
                ++splitAcks[ev->Get<TEvDataShard::TEvSplitAck>()->Record.GetOperationCookie()];
                break;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto waitForSplitAcks = [&](ui64 txId, ui32 count) {
            if (splitAcks[txId] != count) {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) {
                    return splitAcks[txId] == count;
                });
                server->GetRuntime()->DispatchEvents(opts);
            }
        };

        auto sendEnqueued = [&]() {
            preventEnqueueing = false;
            for (auto& ev : std::exchange(enqueued, TVector<THolder<IEventHandle>>())) {
                server->GetRuntime()->Send(ev.Release(), 0, true);
            }
        };

        CreateShardedTable(server, sender, "/Root", "Table", TableWithIndex(SimpleAsyncIndex()));
        SimulateSleep(server, TDuration::Seconds(1));
        SetSplitMergePartCountLimit(&runtime, -1);

        // split of main table
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/Table` (pkey, ikey) VALUES
            (1, 10),
            (2, 20),
            (3, 30);
        )");

        auto tabletIds = GetTableShards(server, sender, "/Root/Table");
        UNIT_ASSERT_VALUES_EQUAL(tabletIds.size(), 1);

        auto txId = AsyncSplitTable(server, sender, "/Root/Table", tabletIds.at(0), 4);
        waitForSplitAcks(txId, 1);
        sendEnqueued();

        WaitTxNotification(server, sender, txId);
        WaitForContent(server, "/Root/Table/by_ikey/indexImplTable",
            "ikey = 10, pkey = 1\nikey = 20, pkey = 2\nikey = 30, pkey = 3");

        // merge of main table
        preventEnqueueing = true;
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/Table` (pkey, ikey) VALUES
            (1, 11),
            (2, 21),
            (3, 31);
        )");

        tabletIds = GetTableShards(server, sender, "/Root/Table");
        UNIT_ASSERT_VALUES_EQUAL(tabletIds.size(), 2);

        txId = AsyncMergeTable(server, sender, "/Root/Table", tabletIds);
        waitForSplitAcks(txId, 2);

        ExecSQL(server, sender, "UPSERT INTO `/Root/Table` (pkey, ikey) VALUES (3, 32);");
        sendEnqueued();

        WaitTxNotification(server, sender, txId);
        WaitForContent(server, "/Root/Table/by_ikey/indexImplTable",
            "ikey = 11, pkey = 1\nikey = 21, pkey = 2\nikey = 32, pkey = 3");

        // split of index table
        preventEnqueueing = true;
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/Table` (pkey, ikey) VALUES
            (1, 13),
            (2, 23),
            (3, 33);
        )");

        tabletIds = GetTableShards(server, sender, "/Root/Table/by_ikey/indexImplTable");
        UNIT_ASSERT_VALUES_EQUAL(tabletIds.size(), 1);

        txId = AsyncSplitTable(server, sender, "/Root/Table/by_ikey/indexImplTable", tabletIds.at(0), 40);
        waitForSplitAcks(txId, 1);

        ExecSQL(server, sender, "UPSERT INTO `/Root/Table` (pkey, ikey) VALUES (4, 44);");
        sendEnqueued();

        WaitTxNotification(server, sender, txId);
        WaitForContent(server, "/Root/Table/by_ikey/indexImplTable",
            "ikey = 13, pkey = 1\nikey = 23, pkey = 2\nikey = 33, pkey = 3\nikey = 44, pkey = 4");

        // merge of index table
        preventEnqueueing = true;
        allowedRejects = 1; // skip 2nd reject from index shard
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/Table` (pkey, ikey) VALUES
            (1, 15),
            (2, 25),
            (3, 35),
            (4, 45);
        )");

        tabletIds = GetTableShards(server, sender, "/Root/Table/by_ikey/indexImplTable");
        UNIT_ASSERT_VALUES_EQUAL(tabletIds.size(), 2);

        txId = AsyncMergeTable(server, sender, "/Root/Table/by_ikey/indexImplTable", tabletIds);
        waitForSplitAcks(txId, 2);
        sendEnqueued();

        WaitTxNotification(server, sender, txId);
        WaitForContent(server, "/Root/Table/by_ikey/indexImplTable",
            "ikey = 15, pkey = 1\nikey = 25, pkey = 2\nikey = 35, pkey = 3\nikey = 45, pkey = 4");
    }

    using TSetQueueLimitFunc = std::function<void(TServerSettings&)>;
    void ShouldRejectChangesOnQueueOverflow(TSetQueueLimitFunc setLimit) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableDataColumnForIndexTable(true);
        setLimit(serverSettings);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::CHANGE_EXCHANGE, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        bool preventEnqueueing = true;
        TVector<THolder<IEventHandle>> enqueued;

        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case NChangeExchange::TEvChangeExchange::EvEnqueueRecords:
                if (preventEnqueueing) {
                    enqueued.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                } else {
                    return TTestActorRuntime::EEventAction::PROCESS;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto sendEnqueued = [&]() {
            preventEnqueueing = false;
            for (auto& ev : std::exchange(enqueued, TVector<THolder<IEventHandle>>())) {
                server->GetRuntime()->Send(ev.Release(), 0, true);
            }
        };

        CreateShardedTable(server, sender, "/Root", "Table", TableWithIndex(SimpleAsyncIndex()));

        ExecSQL(server, sender, "UPSERT INTO `/Root/Table` (pkey, ikey) VALUES (1, 10);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/Table` (pkey, ikey) VALUES (2, 20);", true, Ydb::StatusIds::OVERLOADED);

        sendEnqueued();
        WaitForContent(server, "/Root/Table/by_ikey/indexImplTable",
            "ikey = 10, pkey = 1");

        ExecSQL(server, sender, "UPSERT INTO `/Root/Table` (pkey, ikey) VALUES (2, 20);");
        WaitForContent(server, "/Root/Table/by_ikey/indexImplTable",
            "ikey = 10, pkey = 1\nikey = 20, pkey = 2");
    }

    Y_UNIT_TEST(ShouldRejectChangesOnQueueOverflowByCount) {
        ShouldRejectChangesOnQueueOverflow([](TServerSettings& opts) {
            opts.SetChangesQueueItemsLimit(1);
        });
    }

    Y_UNIT_TEST(ShouldRejectChangesOnQueueOverflowBySize) {
        ShouldRejectChangesOnQueueOverflow([](TServerSettings& opts) {
            opts.SetChangesQueueBytesLimit(1);
        });
    }

} // AsyncIndexChangeExchange

Y_UNIT_TEST_SUITE(Cdc) {
    using namespace NYdb::NPersQueue;
    using namespace NYdb::NDataStreams::V1;

    using TCdcStream = TShardedTableOptions::TCdcStream;

    static NKikimrPQ::TPQConfig DefaultPQConfig() {
        NKikimrPQ::TPQConfig pqConfig;
        pqConfig.SetEnabled(true);
        pqConfig.SetEnableProtoSourceIdInfo(true);
        pqConfig.SetTopicsAreFirstClassCitizen(true);
        pqConfig.SetMaxReadCookies(10);
        pqConfig.AddClientServiceType()->SetName("data-streams");
        pqConfig.SetCheckACL(false);
        pqConfig.SetRequireCredentialsInNewProtocol(false);
        pqConfig.MutableQuotingConfig()->SetEnableQuoting(false);
        return pqConfig;
    }

    static void SetupLogging(TTestActorRuntime& runtime) {
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::CHANGE_EXCHANGE, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::PERSQUEUE, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::PQ_READ_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::PQ_METACACHE, NLog::PRI_DEBUG);
    }

    template <typename TDerived, typename TClient>
    class TTestEnv {
    public:
        explicit TTestEnv(
            const TShardedTableOptions& tableDesc,
            const TCdcStream& streamDesc,
            bool useRealThreads = true,
            const TString& root = "Root",
            const TString& tableName = "Table")
        {
            auto settings = TServerSettings(PortManager.GetPort(2134), {}, DefaultPQConfig())
                .SetUseRealThreads(useRealThreads)
                .SetDomainName(root)
                .SetGrpcPort(PortManager.GetPort(2135))
                .SetEnableChangefeedDynamoDBStreamsFormat(true)
                .SetEnableChangefeedDebeziumJsonFormat(true)
                .SetEnableTopicMessageMeta(true)
                .SetEnableChangefeedInitialScan(true)
                .SetEnableUuidAsPrimaryKey(true);

            Server = new TServer(settings);
            if (useRealThreads) {
                Server->EnableGRpc(settings.GrpcPort);
            }

            const auto database = JoinPath({root});
            auto& runtime = *Server->GetRuntime();
            EdgeActor = runtime.AllocateEdgeActor();

            SetupLogging(runtime);
            InitRoot(Server, EdgeActor);

            CreateShardedTable(Server, EdgeActor, database, tableName, tableDesc);
            WaitTxNotification(Server, EdgeActor, AsyncAlterAddStream(Server, database, tableName, streamDesc));

            if (useRealThreads) {
                Client = TDerived::MakeClient(Server->GetDriver(), database);
            }
        }

        TServer::TPtr GetServer() {
            UNIT_ASSERT(Server);
            return Server;
        }

        TClient& GetClient() {
            UNIT_ASSERT(Client);
            return *Client;
        }

        const TActorId& GetEdgeActor() const {
            return EdgeActor;
        }

    private:
        TPortManager PortManager;
        TServer::TPtr Server;
        TActorId EdgeActor;
        THolder<TClient> Client;

    }; // TTestEnv

    class TTestPqEnv: public TTestEnv<TTestPqEnv, TPersQueueClient> {
    public:
        using TTestEnv<TTestPqEnv, TPersQueueClient>::TTestEnv;

        static THolder<TPersQueueClient> MakeClient(const NYdb::TDriver& driver, const TString& database) {
            return MakeHolder<TPersQueueClient>(driver, TPersQueueClientSettings().Database(database));
        }
    };

    class TTestYdsEnv: public TTestEnv<TTestYdsEnv, TDataStreamsClient> {
    public:
        using TTestEnv<TTestYdsEnv, TDataStreamsClient>::TTestEnv;

        static THolder<TDataStreamsClient> MakeClient(const NYdb::TDriver& driver, const TString& database) {
            return MakeHolder<TDataStreamsClient>(driver, NYdb::TCommonClientSettings().Database(database));
        }
    };

    class TTestTopicEnv: public TTestEnv<TTestTopicEnv, NYdb::NTopic::TTopicClient> {
    public:
        using TTestEnv<TTestTopicEnv, NYdb::NTopic::TTopicClient>::TTestEnv;

        static THolder<NYdb::NTopic::TTopicClient> MakeClient(const NYdb::TDriver& driver, const TString& database) {
            return MakeHolder<NYdb::NTopic::TTopicClient>(driver, NYdb::NTopic::TTopicClientSettings().Database(database));
        }
    };

    TShardedTableOptions SimpleTable() {
        return TShardedTableOptions()
            .Columns({
                {"key", "Uint32", true, false},
                {"value", "Uint32", false, false},
            });
    }

    TShardedTableOptions UuidTable() {
        return TShardedTableOptions()
            .Columns({
                {"key", "Uuid", true, false},
                {"value", "Uint32", false, false},
            });
    }

    TCdcStream KeysOnly(NKikimrSchemeOp::ECdcStreamFormat format, const TString& name = "Stream") {
        return TCdcStream{
            .Name = name,
            .Mode = NKikimrSchemeOp::ECdcStreamModeKeysOnly,
            .Format = format,
        };
    }

    TCdcStream Updates(NKikimrSchemeOp::ECdcStreamFormat format, const TString& name = "Stream") {
        return TCdcStream{
            .Name = name,
            .Mode = NKikimrSchemeOp::ECdcStreamModeUpdate,
            .Format = format,
        };
    }

    TCdcStream NewAndOldImages(NKikimrSchemeOp::ECdcStreamFormat format, const TString& name = "Stream") {
        return TCdcStream{
            .Name = name,
            .Mode = NKikimrSchemeOp::ECdcStreamModeNewAndOldImages,
            .Format = format,
        };
    }

    TCdcStream OldImage(NKikimrSchemeOp::ECdcStreamFormat format, const TString& name = "Stream") {
        return TCdcStream{
            .Name = name,
            .Mode = NKikimrSchemeOp::ECdcStreamModeOldImage,
            .Format = format,
        };
    }

    TCdcStream NewImage(NKikimrSchemeOp::ECdcStreamFormat format, const TString& name = "Stream") {
        return TCdcStream{
            .Name = name,
            .Mode = NKikimrSchemeOp::ECdcStreamModeNewImage,
            .Format = format,
        };
    }

    TCdcStream WithVirtualTimestamps(TCdcStream streamDesc) {
        streamDesc.VirtualTimestamps = true;
        return streamDesc;
    }

    TCdcStream WithResolvedTimestamps(TDuration interval, TCdcStream streamDesc) {
        streamDesc.ResolvedTimestamps = interval;
        return streamDesc;
    }

    TCdcStream WithInitialScan(TCdcStream streamDesc) {
        streamDesc.InitialState = NKikimrSchemeOp::ECdcStreamStateScan;
        return streamDesc;
    }

    TCdcStream WithAwsRegion(const TString& awsRegion, TCdcStream streamDesc) {
        streamDesc.AwsRegion = awsRegion;
        return streamDesc;
    }

    TString CalcPartitionKey(const TString& data) {
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(data, &json));

        NJson::TJsonValue::TMapType root;
        UNIT_ASSERT(json.GetMap(&root));

        UNIT_ASSERT(root.contains("key"));
        return MD5::Calc(root.at("key").GetStringRobust());
    }

    static bool AreJsonsEqual(const TString& actual, const TString& expected, bool assertOnParseError = true) {
        bool parseResult;
        NJson::TJsonValue actualJson;
        parseResult = NJson::ReadJsonTree(actual, &actualJson);
        if (assertOnParseError) {
            UNIT_ASSERT(parseResult);
        } else if (!parseResult) {
            return false;
        }

        NJson::TJsonValue expectedJson;
        parseResult = NJson::ReadJsonTree(expected, &expectedJson);
        if (assertOnParseError) {
            UNIT_ASSERT(parseResult);
        } else if (!parseResult) {
            return false;
        }

        class TScanner: public NJson::IScanCallback {
            NJson::TJsonValue& Actual;
            bool Success = true;

        public:
            explicit TScanner(NJson::TJsonValue& actual)
                : Actual(actual)
            {}

            bool Do(const TString& path, NJson::TJsonValue*, NJson::TJsonValue& expectedValue) override {
                // Skip if not "***"
                if (expectedValue.GetStringRobust() != "***") {
                    return true;
                }

                // Discrepancy in path format here.
                // GetValueByPath expects ".array.[0]" while Scanner provides with ".array[0]".
                // Don't use "***" inside a non-root array.
                UNIT_ASSERT_C(!path.Contains("["), TStringBuilder()
                    << "Please don't use \"***\" inside an array. Seems like " << path << " has array on the way");

                NJson::TJsonValue actualValue;
                // If "***", find a corresponding actual value
                if (!Actual.GetValueByPath(path, actualValue)) {
                    // Couldn't find an actual value for "***"
                    Success = false;
                    return false;
                }

                // Replace "***" with actual value
                expectedValue = actualValue;
                return true;
            }

            bool IsSuccess() const {
                return Success;
            }
        };

        TScanner scanner(actualJson);
        expectedJson.Scan(scanner);

        if (!scanner.IsSuccess()) {
            return false; // actualJson is missing a path to ***
        }

        return actualJson == expectedJson;
    }

    static void AssertJsonsEqual(const TString& actual, const TString& expected) {
        UNIT_ASSERT_C(AreJsonsEqual(actual, expected), TStringBuilder()
            << "Jsons are different: " << actual << " != " << expected);
    }

    static bool CheckJsonsEqual(const TString& actual, const TString& expected) {
        return AreJsonsEqual(actual, expected, false);
    }

    Y_UNIT_TEST(AreJsonsEqualReturnsTrueOnEqual) {
        UNIT_ASSERT(AreJsonsEqual("{}", "{}"));
        UNIT_ASSERT(AreJsonsEqual("[]", "[]"));
        UNIT_ASSERT(AreJsonsEqual("1", "1"));
        UNIT_ASSERT(AreJsonsEqual("null", "null"));
        UNIT_ASSERT(AreJsonsEqual(R"({"a":"b","c":"d","e":[1,2,"3"]})", R"({"a":"b","c":"d","e":[1,2,"3"]})"));
        UNIT_ASSERT(AreJsonsEqual(R"({"update":{},"key":[1]})", R"({"update":{},"key":[1]})"));
        UNIT_ASSERT(AreJsonsEqual(R"({"update":{},"key":[1,2]})", R"({"update":{},"key":[1,2]})"));
        // Root wildcard
        UNIT_ASSERT(AreJsonsEqual("{}", R"("***")"));
        UNIT_ASSERT(AreJsonsEqual("1", R"("***")"));
        UNIT_ASSERT(AreJsonsEqual(R"({"a": "b"})", R"("***")"));
        UNIT_ASSERT(AreJsonsEqual("[1,2,3]", R"("***")"));
        // Deep wildcard
        UNIT_ASSERT(AreJsonsEqual(R"({"a":"b","c":"d","e":[1,2,"3"]})", R"({"a":"b","c":"***","e":"***"})"));
        UNIT_ASSERT(AreJsonsEqual(R"({"update":{},"key":[1]})", R"({"update":{},"key":"***"})"));
        UNIT_ASSERT(AreJsonsEqual(R"({"update":{},"ts":[1,2]})", R"({"update":{},"ts":"***"})"));
    };

    Y_UNIT_TEST(AreJsonsEqualReturnsFalseOnDifferent) {
        // Simple cases
        UNIT_ASSERT(!AreJsonsEqual("{}", "[]"));
        UNIT_ASSERT(!AreJsonsEqual("[]", "{}"));
        UNIT_ASSERT(!AreJsonsEqual("1", "2"));
        UNIT_ASSERT(!AreJsonsEqual("null", "[]"));
        UNIT_ASSERT(!AreJsonsEqual("null", "{}"));
        UNIT_ASSERT(!AreJsonsEqual("[]", "null"));
        UNIT_ASSERT(!AreJsonsEqual("{}", "null"));
        UNIT_ASSERT(!AreJsonsEqual(R"({"a":"b","c":"d","e":[1,2,"3"]})", R"({"a":"b","c":"d","e":[9,2,"3"]})"));
        UNIT_ASSERT(!AreJsonsEqual(R"({"update":{},"key":[1]})", R"({"update":[],"key":[1]})"));
        UNIT_ASSERT(!AreJsonsEqual(R"({"update":{},"ts":[1,2]})", R"({"update":{},"key":[9,2]})"));
        // Wildcart in actual value shouldn't be treated as a wildcard
        UNIT_ASSERT(!AreJsonsEqual(R"("***")", "{}"));
        UNIT_ASSERT(!AreJsonsEqual(R"({"a":"***"})", R"({"a":"b"})"));
        // Deep wildcard
        UNIT_ASSERT(!AreJsonsEqual(R"({"a":"z","c":"d","e":[1,2,"3"]})", R"({"a":"b","c":"***","e":"***"})"));
        UNIT_ASSERT(!AreJsonsEqual(R"({"update":{"a":"b"},"key":[1]})", R"({"update":{},"key":"***"})"));
        UNIT_ASSERT(!AreJsonsEqual(R"({"update":{},"key":{},"ts":[1,2]})", R"({"update":{},"ts":"***"})"));
    };

    Y_UNIT_TEST(AreJsonsEqualFailsOnWildcardInArray) {
        // Wildcard in a not-root array is not supported because of a bug in code
        UNIT_ASSERT_TEST_FAILS(AreJsonsEqual(R"({"a":[1,{"a":"b"}]})", R"({"a":[1,{"a":"***"}]})"));
        UNIT_ASSERT_TEST_FAILS(AreJsonsEqual(R"({"a":[1]})", R"({"a":["***"]})"));
        UNIT_ASSERT_TEST_FAILS(AreJsonsEqual(R"([1])", R"(["***"])"));
    }

    struct PqRunner {
        static void Read(const TShardedTableOptions& tableDesc, const TCdcStream& streamDesc,
                const TVector<TString>& queries, const TVector<TString>& records, bool checkKey = true)
        {
            TTestPqEnv env(tableDesc, streamDesc);

            for (const auto& query : queries) {
                ExecSQL(env.GetServer(), env.GetEdgeActor(), query);
            }

            auto& client = env.GetClient();

            // add consumer
            {
                auto res = client.AddReadRule("/Root/Table/Stream", TAddReadRuleSettings()
                    .ReadRule(TReadRuleSettings().ConsumerName("user"))).ExtractValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
            }

            // get records
            auto reader = client.CreateReadSession(TReadSessionSettings()
                .AppendTopics(TString("/Root/Table/Stream"))
                .ConsumerName("user")
                .DisableClusterDiscovery(true)
            );

            ui32 reads = 0;
            while (reads < records.size()) {
                auto ev = reader->GetEvent(true);
                UNIT_ASSERT(ev);

                TPartitionStream::TPtr pStream;
                if (auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*ev)) {
                    pStream = data->GetPartitionStream();
                    for (const auto& item : data->GetMessages()) {
                        const auto& record = records.at(reads++);
                        AssertJsonsEqual(item.GetData(), record);
                        if (checkKey) {
                            UNIT_ASSERT_VALUES_EQUAL(item.GetPartitionKey(), CalcPartitionKey(record));
                        }
                    }
                } else if (auto* create = std::get_if<TReadSessionEvent::TCreatePartitionStreamEvent>(&*ev)) {
                    pStream = create->GetPartitionStream();
                    create->Confirm();
                } else if (auto* destroy = std::get_if<TReadSessionEvent::TDestroyPartitionStreamEvent>(&*ev)) {
                    pStream = destroy->GetPartitionStream();
                    destroy->Confirm();
                } else if (std::get_if<TSessionClosedEvent>(&*ev)) {
                    break;
                }

                if (pStream) {
                    UNIT_ASSERT_VALUES_EQUAL(pStream->GetTopicPath(), "/Root/Table/Stream");
                }
            }

            // remove consumer
            {
                auto res = client.RemoveReadRule("/Root/Table/Stream", TRemoveReadRuleSettings()
                    .ConsumerName("user")).ExtractValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
            }
        }

        static void Write(const TShardedTableOptions& tableDesc, const TCdcStream& streamDesc) {
            TTestPqEnv env(tableDesc, streamDesc);

            auto session = env.GetClient().CreateSimpleBlockingWriteSession(TWriteSessionSettings()
                .Path("/Root/Table/Stream")
                .MessageGroupId("user")
                .ClusterDiscoveryMode(EClusterDiscoveryMode::Off)
            );

            const bool failed = !session->Write("message-1");
            UNIT_ASSERT(failed);

            session->Close();
        }

        static void Drop(const TShardedTableOptions& tableDesc, const TCdcStream& streamDesc) {
            TTestPqEnv env(tableDesc, streamDesc);

            auto res = env.GetClient().DropTopic("/Root/Table/Stream").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }
    };

    struct YdsRunner {
        static void Read(const TShardedTableOptions& tableDesc, const TCdcStream& streamDesc,
                const TVector<TString>& queries, const TVector<TString>& records, bool checkKey = true)
        {
            TTestYdsEnv env(tableDesc, streamDesc);

            for (const auto& query : queries) {
                ExecSQL(env.GetServer(), env.GetEdgeActor(), query);
            }

            auto& client = env.GetClient();

            // add consumer
            {
                auto res = client.RegisterStreamConsumer("/Root/Table/Stream", "user").ExtractValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
            }

            // list consumers
            {
                auto res = client.ListStreamConsumers("/Root/Table/Stream", TListStreamConsumersSettings()
                    .MaxResults(100)).ExtractValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(res.GetResult().consumers().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(res.GetResult().consumers().begin()->consumer_name(), "user");
            }

            // get shards
            TString shardId;
            {
                auto res = client.ListShards("/Root/Table/Stream", {}).ExtractValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(res.GetResult().shards().size(), 1);
                shardId = res.GetResult().shards().begin()->shard_id();
            }

            // get iterator
            TString shardIt;
            {
                auto res = client.GetShardIterator("/Root/Table/Stream", shardId, Ydb::DataStreams::V1::ShardIteratorType::TRIM_HORIZON).ExtractValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
                shardIt = res.GetResult().shard_iterator();
            }

            // get records
            {
                auto res = client.GetRecords(shardIt).ExtractValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(res.GetResult().records().size(), records.size());

                for (ui32 i = 0; i < records.size(); ++i) {
                    const auto& actual = res.GetResult().records().at(i);
                    const auto& expected = records.at(i);
                    AssertJsonsEqual(actual.data(), expected);
                    if (checkKey) {
                        UNIT_ASSERT_VALUES_EQUAL(actual.partition_key(), CalcPartitionKey(expected));
                    }
                }
            }

            // remove consumer
            {
                auto res = client.DeregisterStreamConsumer("/Root/Table/Stream", "user").ExtractValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
            }
        }

        static void Write(const TShardedTableOptions& tableDesc, const TCdcStream& streamDesc) {
            TTestYdsEnv env(tableDesc, streamDesc);

            auto res = env.GetClient().PutRecord("/Root/Table/Stream", {"data", "key", ""}).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }

        static void Drop(const TShardedTableOptions& tableDesc, const TCdcStream& streamDesc) {
            TTestYdsEnv env(tableDesc, streamDesc);

            auto res = env.GetClient().DeleteStream("/Root/Table/Stream").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }
    };

    struct TopicRunner {
    private:
        using TMessageMeta = TVector<std::pair<TString, TString>>;

        static TString DumpMessageMeta(TMessageMeta messageMeta) {
            std::stable_sort(messageMeta.begin(), messageMeta.end());
            return JoinSeq(", ", messageMeta);
        }

        static void AssertMessageMetaContains(const TMessageMeta& actual, const TMessageMeta& expected) {
            for (const auto& e : expected) {
                auto it = std::find_if(actual.begin(), actual.end(), [&e](const auto& a) {
                    return a.first == e.first && CheckJsonsEqual(a.second, e.second);
                });
                UNIT_ASSERT_C(it != actual.end(), TStringBuilder() << "Message meta '" << e << "' was expected"
                    << ": actual# " << DumpMessageMeta(actual)
                    << ", expected# " << DumpMessageMeta(expected));
            }
        }

    public:
        static void WaitForContent(NYdb::NTopic::IReadSession* reader, const TVector<std::pair<TString, TMessageMeta>>& records) {
            ui32 reads = 0;
            while (reads < records.size()) {
                auto ev = reader->GetEvent(true);
                UNIT_ASSERT(ev);

                NYdb::NTopic::TPartitionSession::TPtr pStream;
                if (auto* data = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*ev)) {
                    pStream = data->GetPartitionSession();
                    for (const auto& item : data->GetMessages()) {
                        const auto& [body, meta] = records.at(reads++);
                        AssertJsonsEqual(item.GetData(), body);
                        AssertMessageMetaContains(item.GetMessageMeta()->Fields, meta);
                    }
                } else if (auto* create = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*ev)) {
                    pStream = create->GetPartitionSession();
                    create->Confirm();
                } else if (auto* destroy = std::get_if<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&*ev)) {
                    pStream = destroy->GetPartitionSession();
                    destroy->Confirm();
                } else if (std::get_if<NYdb::NTopic::TSessionClosedEvent>(&*ev)) {
                    break;
                }

                if (pStream) {
                    UNIT_ASSERT_VALUES_EQUAL(pStream->GetTopicPath(), "/Root/Table/Stream");
                }
            }
        }

        static void Read(const TShardedTableOptions& tableDesc, const TCdcStream& streamDesc,
                const TVector<TString>& queries, const TVector<std::pair<TString, TMessageMeta>>& records)
        {
            TTestTopicEnv env(tableDesc, streamDesc);

            for (const auto& query : queries) {
                ExecSQL(env.GetServer(), env.GetEdgeActor(), query);
            }

            auto& client = env.GetClient();

            // add consumer
            {
                auto res = client.AlterTopic("/Root/Table/Stream", NYdb::NTopic::TAlterTopicSettings()
                    .BeginAddConsumer("user").EndAddConsumer()).ExtractValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
            }

            // create reader
            auto reader = client.CreateReadSession(NYdb::NTopic::TReadSessionSettings()
                .AppendTopics(TString("/Root/Table/Stream"))
                .ConsumerName("user")
            );

            // get records
            WaitForContent(reader.get(), records);

            // remove consumer
            {
                auto res = client.AlterTopic("/Root/Table/Stream", NYdb::NTopic::TAlterTopicSettings()
                    .AppendDropConsumers("user")).ExtractValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
            }
        }

        static void Read(const TShardedTableOptions& tableDesc, const TCdcStream& streamDesc,
                const TVector<TString>& queries, const TVector<TString>& records, bool checkKey = true)
        {
            Y_UNUSED(checkKey);

            TVector<std::pair<TString, TMessageMeta>> recordsWithMetadata(Reserve(records.size()));
            for (const auto& record : records) {
                recordsWithMetadata.emplace_back(record, TMessageMeta());
            }

            Read(tableDesc, streamDesc, queries, recordsWithMetadata);
        }

        static void Write(const TShardedTableOptions& tableDesc, const TCdcStream& streamDesc) {
            TTestPqEnv env(tableDesc, streamDesc);

            auto session = env.GetClient().CreateSimpleBlockingWriteSession(TWriteSessionSettings()
                .Path("/Root/Table/Stream")
                .MessageGroupId("user")
                .ClusterDiscoveryMode(EClusterDiscoveryMode::Off)
            );

            const bool failed = !session->Write("message-1");
            UNIT_ASSERT(failed);

            session->Close();
        }

        static void Drop(const TShardedTableOptions& tableDesc, const TCdcStream& streamDesc) {
            TTestTopicEnv env(tableDesc, streamDesc);

            auto res = env.GetClient().DropTopic("/Root/Table/Stream").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }
    };

    static TString DebeziumBody(const char* op, const char* before, const char* after, bool snapshot = false) {
        NJsonWriter::TBuf body;
        auto root = body.BeginObject();
        auto payload = root.WriteKey("payload").BeginObject();

        payload
            .WriteKey("op").WriteString(op)
            .WriteKey("source")
                .BeginObject()
                    .WriteKey("connector").WriteString("ydb")
                    .WriteKey("version").WriteString("1.0.0")
                    .WriteKey("step").WriteString("***")
                    .WriteKey("txId").WriteString("***")
                    .WriteKey("ts_ms").WriteString("***")
                    .WriteKey("snapshot").WriteBool(snapshot)
                .EndObject();

        if (before) {
            payload.WriteKey("before").UnsafeWriteValue(before);
        }

        if (after) {
            payload.WriteKey("after").UnsafeWriteValue(after);
        }

        payload.EndObject();
        root.EndObject();
        return body.Str();
    }

    #define Y_UNIT_TEST_TRIPLET(N, VAR1, VAR2, VAR3)                                                                   \
        template<typename TRunner> void N(NUnitTest::TTestContext&);                                                   \
        struct TTestRegistration##N {                                                                                  \
            TTestRegistration##N() {                                                                                   \
                TCurrentTest::AddTest(#N "[" #VAR1 "]", static_cast<void (*)(NUnitTest::TTestContext&)>(&N<VAR1>), false); \
                TCurrentTest::AddTest(#N "[" #VAR2 "]", static_cast<void (*)(NUnitTest::TTestContext&)>(&N<VAR2>), false); \
                TCurrentTest::AddTest(#N "[" #VAR3 "]", static_cast<void (*)(NUnitTest::TTestContext&)>(&N<VAR3>), false); \
            }                                                                                                          \
        };                                                                                                             \
        static TTestRegistration##N testRegistration##N;                                                               \
        template<typename TRunner>                                                                                     \
        void N(NUnitTest::TTestContext&)

    Y_UNIT_TEST_TRIPLET(KeysOnlyLog, PqRunner, YdsRunner, TopicRunner) {
        TRunner::Read(SimpleTable(), KeysOnly(NKikimrSchemeOp::ECdcStreamFormatJson), {R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 10),
            (2, 20),
            (3, 30);
        )", R"(
            DELETE FROM `/Root/Table` WHERE key = 1;
        )"}, {
            R"({"update":{},"key":[1]})",
            R"({"update":{},"key":[2]})",
            R"({"update":{},"key":[3]})",
            R"({"erase":{},"key":[1]})",
        });
    }

    Y_UNIT_TEST_TRIPLET(UuidExchange, PqRunner, YdsRunner, TopicRunner) {
        TRunner::Read(UuidTable(), KeysOnly(NKikimrSchemeOp::ECdcStreamFormatJson), {R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (Uuid("65df1ec1-a97d-47b2-ae56-3c023da6ee8c"), 10),
            (Uuid("65df1ec2-a97d-47b2-ae56-3c023da6ee8c"), 20),
            (Uuid("65df1ec3-a97d-47b2-ae56-3c023da6ee8c"), 30);
        )", R"(
            DELETE FROM `/Root/Table` WHERE key = Uuid("65df1ec1-a97d-47b2-ae56-3c023da6ee8c");
        )"}, {
            R"({"update":{},"key":["65df1ec1-a97d-47b2-ae56-3c023da6ee8c"]})",
            R"({"update":{},"key":["65df1ec2-a97d-47b2-ae56-3c023da6ee8c"]})",
            R"({"update":{},"key":["65df1ec3-a97d-47b2-ae56-3c023da6ee8c"]})",
            R"({"erase":{},"key":["65df1ec1-a97d-47b2-ae56-3c023da6ee8c"]})",
        });
    }

    Y_UNIT_TEST(KeysOnlyLogDebezium) {
        TopicRunner::Read(SimpleTable(), KeysOnly(NKikimrSchemeOp::ECdcStreamFormatDebeziumJson), {R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 10),
            (2, 20),
            (3, 30);
        )", R"(
            DELETE FROM `/Root/Table` WHERE key = 1;
        )"}, {
            {DebeziumBody("u", nullptr, nullptr), {{"__key", R"({"payload":{"key":1}})"}}},
            {DebeziumBody("u", nullptr, nullptr), {{"__key", R"({"payload":{"key":2}})"}}},
            {DebeziumBody("u", nullptr, nullptr), {{"__key", R"({"payload":{"key":3}})"}}},
            {DebeziumBody("d", nullptr, nullptr), {{"__key", R"({"payload":{"key":1}})"}}},
        });
    }

    Y_UNIT_TEST_TRIPLET(UpdatesLog, PqRunner, YdsRunner, TopicRunner) {
        TRunner::Read(SimpleTable(), Updates(NKikimrSchemeOp::ECdcStreamFormatJson), {R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 10),
            (2, 20),
            (3, 30);
        )", R"(
            DELETE FROM `/Root/Table` WHERE key = 1;
        )"}, {
            R"({"update":{"value":10},"key":[1]})",
            R"({"update":{"value":20},"key":[2]})",
            R"({"update":{"value":30},"key":[3]})",
            R"({"erase":{},"key":[1]})",
        });
    }

    Y_UNIT_TEST_TRIPLET(NewAndOldImagesLog, PqRunner, YdsRunner, TopicRunner) {
        TRunner::Read(SimpleTable(), NewAndOldImages(NKikimrSchemeOp::ECdcStreamFormatJson), {R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 10),
            (2, 20),
            (3, 30);
        )", R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 100),
            (2, 200),
            (3, 300);
        )", R"(
            DELETE FROM `/Root/Table` WHERE key = 1;
        )"}, {
            R"({"update":{},"newImage":{"value":10},"key":[1]})",
            R"({"update":{},"newImage":{"value":20},"key":[2]})",
            R"({"update":{},"newImage":{"value":30},"key":[3]})",
            R"({"update":{},"newImage":{"value":100},"key":[1],"oldImage":{"value":10}})",
            R"({"update":{},"newImage":{"value":200},"key":[2],"oldImage":{"value":20}})",
            R"({"update":{},"newImage":{"value":300},"key":[3],"oldImage":{"value":30}})",
            R"({"erase":{},"key":[1],"oldImage":{"value":100}})",
        });
    }

    Y_UNIT_TEST(NewAndOldImagesLogDebezium) {
        TopicRunner::Read(SimpleTable(), NewAndOldImages(NKikimrSchemeOp::ECdcStreamFormatDebeziumJson), {R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 10),
            (2, 20),
            (3, 30);
        )", R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 100),
            (2, 200),
            (3, 300);
        )", R"(
            DELETE FROM `/Root/Table` WHERE key = 1;
        )"}, {
            {DebeziumBody("c", nullptr, R"({"key":1,"value":10})"), {{"__key", R"({"payload":{"key":1}})"}}},
            {DebeziumBody("c", nullptr, R"({"key":2,"value":20})"), {{"__key", R"({"payload":{"key":2}})"}}},
            {DebeziumBody("c", nullptr, R"({"key":3,"value":30})"), {{"__key", R"({"payload":{"key":3}})"}}},
            {DebeziumBody("u", R"({"key":1,"value":10})", R"({"key":1,"value":100})"), {{"__key", R"({"payload":{"key":1}})"}}},
            {DebeziumBody("u", R"({"key":2,"value":20})", R"({"key":2,"value":200})"), {{"__key", R"({"payload":{"key":2}})"}}},
            {DebeziumBody("u", R"({"key":3,"value":30})", R"({"key":3,"value":300})"), {{"__key", R"({"payload":{"key":3}})"}}},
            {DebeziumBody("d", R"({"key":1,"value":100})", nullptr), {{"__key", R"({"payload":{"key":1}})"}}},
        });
    }

    Y_UNIT_TEST(OldImageLogDebezium) {
        TopicRunner::Read(SimpleTable(), OldImage(NKikimrSchemeOp::ECdcStreamFormatDebeziumJson), {R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 10),
            (2, 20),
            (3, 30);
        )", R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 100),
            (2, 200),
            (3, 300);
        )", R"(
            DELETE FROM `/Root/Table` WHERE key = 1;
        )"}, {
            {DebeziumBody("u", nullptr, nullptr), {{"__key", R"({"payload":{"key":1}})"}}},
            {DebeziumBody("u", nullptr, nullptr), {{"__key", R"({"payload":{"key":2}})"}}},
            {DebeziumBody("u", nullptr, nullptr), {{"__key", R"({"payload":{"key":3}})"}}},
            {DebeziumBody("u", R"({"key":1,"value":10})", nullptr), {{"__key", R"({"payload":{"key":1}})"}}},
            {DebeziumBody("u", R"({"key":2,"value":20})", nullptr), {{"__key", R"({"payload":{"key":2}})"}}},
            {DebeziumBody("u", R"({"key":3,"value":30})", nullptr), {{"__key", R"({"payload":{"key":3}})"}}},
            {DebeziumBody("d", R"({"key":1,"value":100})", nullptr), {{"__key", R"({"payload":{"key":1}})"}}},
        });
    }

    Y_UNIT_TEST(NewImageLogDebezium) {
        TopicRunner::Read(SimpleTable(), NewImage(NKikimrSchemeOp::ECdcStreamFormatDebeziumJson), {R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 10),
            (2, 20),
            (3, 30);
        )", R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 100),
            (2, 200),
            (3, 300);
        )", R"(
            DELETE FROM `/Root/Table` WHERE key = 1;
        )"}, {
            {DebeziumBody("u", nullptr, R"({"key":1,"value":10})"), {{"__key", R"({"payload":{"key":1}})"}}},
            {DebeziumBody("u", nullptr, R"({"key":2,"value":20})"), {{"__key", R"({"payload":{"key":2}})"}}},
            {DebeziumBody("u", nullptr, R"({"key":3,"value":30})"), {{"__key", R"({"payload":{"key":3}})"}}},
            {DebeziumBody("u", nullptr, R"({"key":1,"value":100})"), {{"__key", R"({"payload":{"key":1}})"}}},
            {DebeziumBody("u", nullptr, R"({"key":2,"value":200})"), {{"__key", R"({"payload":{"key":2}})"}}},
            {DebeziumBody("u", nullptr, R"({"key":3,"value":300})"), {{"__key", R"({"payload":{"key":3}})"}}},
            {DebeziumBody("d", nullptr, nullptr), {{"__key", R"({"payload":{"key":1}})"}}},
        });
    }

    Y_UNIT_TEST_TRIPLET(VirtualTimestamps, PqRunner, YdsRunner, TopicRunner) {
        TRunner::Read(SimpleTable(), WithVirtualTimestamps(KeysOnly(NKikimrSchemeOp::ECdcStreamFormatJson)), {R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 10),
            (2, 20),
            (3, 30);
        )"}, {
            R"({"update":{},"key":[1],"ts":"***"})",
            R"({"update":{},"key":[2],"ts":"***"})",
            R"({"update":{},"key":[3],"ts":"***"})",
        });
    }

    TShardedTableOptions DocApiTable() {
        return TShardedTableOptions()
            .Columns({
                {"__Hash", "Uint64", true, false},
                {"id_shard", "Utf8", true, false},
                {"id_sort", "Utf8", true, false},
                {"__RowData", "JsonDocument", false, false},
                {"extra", "Bool", false, false},
            })
            .Attributes({
                {"__document_api_version", "1"},
            });
    }

    Y_UNIT_TEST_TRIPLET(DocApi, PqRunner, YdsRunner, TopicRunner) {
        TRunner::Read(DocApiTable(), KeysOnly(NKikimrSchemeOp::ECdcStreamFormatDynamoDBStreamsJson), {R"(
            UPSERT INTO `/Root/Table` (__Hash, id_shard, id_sort, __RowData) VALUES (
                1, "10", "100", JsonDocument('{"M":{"color":{"S":"pink"},"weight":{"N":"4.5"}}}')
            );
        )"}, {
            WriteJson(NJson::TJsonMap({
                {"awsRegion", ""},
                {"dynamodb", NJson::TJsonMap({
                    {"ApproximateCreationDateTime", "***"},
                    {"Keys", NJson::TJsonMap({
                        {"id_shard", NJson::TJsonMap({{"S", "10"}})},
                        {"id_sort", NJson::TJsonMap({{"S", "100"}})},
                    })},
                    {"SequenceNumber", "000000000000000000001"},
                    {"StreamViewType", "KEYS_ONLY"},
                })},
                {"eventID", "***"},
                {"eventName", "MODIFY"},
                {"eventSource", "ydb:document-table"},
                {"eventVersion", "1.0"},
            }), false),
        }, false /* do not check key */);

        TRunner::Read(DocApiTable(), NewAndOldImages(NKikimrSchemeOp::ECdcStreamFormatDynamoDBStreamsJson), {R"(
            UPSERT INTO `/Root/Table` (__Hash, id_shard, id_sort, __RowData, extra) VALUES (
                1, "10", "100", JsonDocument('{"M":{"color":{"S":"pink"},"weight":{"N":"4.5"}}}'), true
            );
        )", R"(
            UPSERT INTO `/Root/Table` (__Hash, id_shard, id_sort, __RowData, extra) VALUES (
                1, "10", "100", JsonDocument('{"M":{"color":{"S":"yellow"},"weight":{"N":"5.4"}}}'), false
            );
        )", R"(
            DELETE FROM `/Root/Table` WHERE __Hash = 1;
        )"}, {
            WriteJson(NJson::TJsonMap({
                {"awsRegion", ""},
                {"dynamodb", NJson::TJsonMap({
                    {"ApproximateCreationDateTime", "***"},
                    {"Keys", NJson::TJsonMap({
                        {"id_shard", NJson::TJsonMap({{"S", "10"}})},
                        {"id_sort", NJson::TJsonMap({{"S", "100"}})},
                    })},
                    {"NewImage", NJson::TJsonMap({
                        {"id_shard", NJson::TJsonMap({{"S", "10"}})},
                        {"id_sort", NJson::TJsonMap({{"S", "100"}})},
                        {"color", NJson::TJsonMap({{"S", "pink"}})},
                        {"weight", NJson::TJsonMap({{"N", "4.5"}})},
                        {"extra", NJson::TJsonMap({{"BOOL", true}})},
                    })},
                    {"SequenceNumber", "000000000000000000001"},
                    {"StreamViewType", "NEW_AND_OLD_IMAGES"},
                })},
                {"eventID", "***"},
                {"eventName", "INSERT"},
                {"eventSource", "ydb:document-table"},
                {"eventVersion", "1.0"},
            }), false),
            WriteJson(NJson::TJsonMap({
                {"awsRegion", ""},
                {"dynamodb", NJson::TJsonMap({
                    {"ApproximateCreationDateTime", "***"},
                    {"Keys", NJson::TJsonMap({
                        {"id_shard", NJson::TJsonMap({{"S", "10"}})},
                        {"id_sort", NJson::TJsonMap({{"S", "100"}})},
                    })},
                    {"OldImage", NJson::TJsonMap({
                        {"id_shard", NJson::TJsonMap({{"S", "10"}})},
                        {"id_sort", NJson::TJsonMap({{"S", "100"}})},
                        {"color", NJson::TJsonMap({{"S", "pink"}})},
                        {"weight", NJson::TJsonMap({{"N", "4.5"}})},
                        {"extra", NJson::TJsonMap({{"BOOL", true}})},
                    })},
                    {"NewImage", NJson::TJsonMap({
                        {"id_shard", NJson::TJsonMap({{"S", "10"}})},
                        {"id_sort", NJson::TJsonMap({{"S", "100"}})},
                        {"color", NJson::TJsonMap({{"S", "yellow"}})},
                        {"weight", NJson::TJsonMap({{"N", "5.4"}})},
                        {"extra", NJson::TJsonMap({{"BOOL", false}})},
                    })},
                    {"SequenceNumber", "000000000000000000002"},
                    {"StreamViewType", "NEW_AND_OLD_IMAGES"},
                })},
                {"eventID", "***"},
                {"eventName", "MODIFY"},
                {"eventSource", "ydb:document-table"},
                {"eventVersion", "1.0"},
            }), false),
            WriteJson(NJson::TJsonMap({
                {"awsRegion", ""},
                {"dynamodb", NJson::TJsonMap({
                    {"ApproximateCreationDateTime", "***"},
                    {"Keys", NJson::TJsonMap({
                        {"id_shard", NJson::TJsonMap({{"S", "10"}})},
                        {"id_sort", NJson::TJsonMap({{"S", "100"}})},
                    })},
                    {"OldImage", NJson::TJsonMap({
                        {"id_shard", NJson::TJsonMap({{"S", "10"}})},
                        {"id_sort", NJson::TJsonMap({{"S", "100"}})},
                        {"color", NJson::TJsonMap({{"S", "yellow"}})},
                        {"weight", NJson::TJsonMap({{"N", "5.4"}})},
                        {"extra", NJson::TJsonMap({{"BOOL", false}})},
                    })},
                    {"SequenceNumber", "000000000000000000003"},
                    {"StreamViewType", "NEW_AND_OLD_IMAGES"},
                })},
                {"eventID", "***"},
                {"eventName", "REMOVE"},
                {"eventSource", "ydb:document-table"},
                {"eventVersion", "1.0"},
            }), false),
        }, false /* do not check key */);
    }

    Y_UNIT_TEST_TRIPLET(NaN, PqRunner, YdsRunner, TopicRunner) {
        const auto variants = std::vector<std::pair<const char*, const char*>>{
            {"Double", ""},
            {"Float", "f"},
        };

        for (const auto& [type, s] : variants) {
            const auto table = TShardedTableOptions()
                .Columns({
                    {"key", "Uint32", true, false},
                    {"value", type, false, false},
                });

            TRunner::Read(table, Updates(NKikimrSchemeOp::ECdcStreamFormatJson), {Sprintf(R"(
                UPSERT INTO `/Root/Table` (key, value) VALUES
                (1, 0.0%s/0.0%s),
                (2, 1.0%s/0.0%s),
                (3, -1.0%s/0.0%s);
            )", s, s, s, s, s, s)}, {
                R"({"update":{"value":"nan"},"key":[1]})",
                R"({"update":{"value":"inf"},"key":[2]})",
                R"({"update":{"value":"-inf"},"key":[3]})",
            });
        }
    }

    Y_UNIT_TEST_TRIPLET(HugeKey, PqRunner, YdsRunner, TopicRunner) {
        const auto key = TString(512_KB, 'A');
        const auto table = TShardedTableOptions()
            .Columns({
                {"key", "Utf8", true, false},
                {"value", "Uint32", false, false},
            });

        TRunner::Read(table, KeysOnly(NKikimrSchemeOp::ECdcStreamFormatJson), {Sprintf(R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            ("%s", 1);
        )", key.c_str())}, {
            Sprintf(R"({"update":{},"key":["%s"]})", key.c_str()),
        });
    }

    Y_UNIT_TEST(HugeKeyDebezium) {
        const auto key = TString(512_KB, 'A');
        const auto table = TShardedTableOptions()
            .Columns({
                {"key", "Utf8", true, false},
                {"value", "Uint32", false, false},
            });

        TopicRunner::Read(table, KeysOnly(NKikimrSchemeOp::ECdcStreamFormatDebeziumJson), {Sprintf(R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            ("%s", 1);
        )", key.c_str())}, {
            {DebeziumBody("u", nullptr, nullptr), {{"__key", Sprintf(R"({"payload":{"key":"%s"}})", key.c_str())}}},
        });
    }

    Y_UNIT_TEST_TRIPLET(Write, PqRunner, YdsRunner, TopicRunner) {
        TRunner::Write(SimpleTable(), KeysOnly(NKikimrSchemeOp::ECdcStreamFormatJson));
    }

    Y_UNIT_TEST_TRIPLET(Drop, PqRunner, YdsRunner, TopicRunner) {
        TRunner::Drop(SimpleTable(), KeysOnly(NKikimrSchemeOp::ECdcStreamFormatJson));
    }

    Y_UNIT_TEST(AlterViaTopicService) {
        TTestTopicEnv env(SimpleTable(), KeysOnly(NKikimrSchemeOp::ECdcStreamFormatJson));
        auto& client = env.GetClient();

        // try to update partitions count
        {
            auto res = client.AlterTopic("/Root/Table/Stream", NYdb::NTopic::TAlterTopicSettings()
                .AlterPartitioningSettings(5, 5)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NYdb::EStatus::BAD_REQUEST);
        }

        // try to update retention period
        {
            auto res = client.AlterTopic("/Root/Table/Stream", NYdb::NTopic::TAlterTopicSettings()
                .SetRetentionPeriod(TDuration::Hours(48))).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NYdb::EStatus::BAD_REQUEST);
        }
        // try to update supported codecs
        {
            auto res = client.AlterTopic("/Root/Table/Stream", NYdb::NTopic::TAlterTopicSettings()
                .AppendSetSupportedCodecs(NYdb::NTopic::ECodec(5))).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NYdb::EStatus::BAD_REQUEST);
        }
        // try to update retention storage
        {
            auto res = client.AlterTopic("/Root/Table/Stream", NYdb::NTopic::TAlterTopicSettings()
                .SetRetentionStorageMb(1)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NYdb::EStatus::BAD_REQUEST);
        }

        // try to update speed
        {
            auto res = client.AlterTopic("/Root/Table/Stream", NYdb::NTopic::TAlterTopicSettings()
                .SetPartitionWriteSpeedBytesPerSecond(1_MB)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NYdb::EStatus::BAD_REQUEST);
        }

        // try to update write burst
        {
            auto res = client.AlterTopic("/Root/Table/Stream", NYdb::NTopic::TAlterTopicSettings()
                .SetPartitionWriteBurstBytes(1_MB)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NYdb::EStatus::BAD_REQUEST);
        }

        // try to update attributes
        {
            auto res = client.AlterTopic("/Root/Table/Stream", NYdb::NTopic::TAlterTopicSettings()
                .BeginAlterAttributes().Add("key", "value").EndAlterAttributes()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NYdb::EStatus::BAD_REQUEST);
        }
    }

    // Pq specific
    Y_UNIT_TEST(Alter) {
        TTestPqEnv env(SimpleTable(), KeysOnly(NKikimrSchemeOp::ECdcStreamFormatJson));
        auto& client = env.GetClient();

        auto desc = client.DescribeTopic("/Root/Table/Stream").ExtractValueSync();
        UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());

        // try to update partitions count
        {
            UNIT_ASSERT_VALUES_EQUAL(desc.TopicSettings().PartitionsCount(), 1);
            auto res = client.AlterTopic("/Root/Table/Stream", TAlterTopicSettings()
                .SetSettings(desc.TopicSettings()).PartitionsCount(2)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }

        // try to update retention period
        {
            UNIT_ASSERT_VALUES_EQUAL(desc.TopicSettings().RetentionPeriod().Hours(), 24);
            auto res = client.AlterTopic("/Root/Table/Stream", TAlterTopicSettings()
                .SetSettings(desc.TopicSettings()).RetentionPeriod(TDuration::Hours(48))).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }
    }

    // Yds specific
    Y_UNIT_TEST(DescribeStream) {
        TTestYdsEnv env(SimpleTable(), KeysOnly(NKikimrSchemeOp::ECdcStreamFormatJson));

        auto res = env.GetClient().DescribeStream("/Root/Table/Stream").ExtractValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(res.GetResult().stream_description().stream_name(), "/Root/Table/Stream");
    }

    Y_UNIT_TEST(UpdateStream) {
        TTestYdsEnv env(SimpleTable(), KeysOnly(NKikimrSchemeOp::ECdcStreamFormatJson));

        auto res = env.GetClient().UpdateStream("/Root/Table/Stream", TUpdateStreamSettings()
            .RetentionPeriodHours(8).TargetShardCount(2).WriteQuotaKbPerSec(128)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
    }

    Y_UNIT_TEST(UpdateShardCount) {
        TTestYdsEnv env(SimpleTable(), KeysOnly(NKikimrSchemeOp::ECdcStreamFormatJson));

        auto res = env.GetClient().UpdateShardCount("/Root/Table/Stream", TUpdateShardCountSettings()
            .TargetShardCount(2)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
    }

    Y_UNIT_TEST(UpdateRetentionPeriod) {
        TTestYdsEnv env(SimpleTable(), KeysOnly(NKikimrSchemeOp::ECdcStreamFormatJson));
        auto& client = env.GetClient();

        {
            auto res = client.DecreaseStreamRetentionPeriod("/Root/Table/Stream", TDecreaseStreamRetentionPeriodSettings()
                .RetentionPeriodHours(12)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }

        {
            auto res = client.IncreaseStreamRetentionPeriod("/Root/Table/Stream", TIncreaseStreamRetentionPeriodSettings()
                .RetentionPeriodHours(48)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }
    }

    // Schema snapshots
    using TActionFunc = std::function<ui64(TServer::TPtr)>;

    ui64 ResolvePqTablet(TTestActorRuntime& runtime, const TActorId& sender, const TString& path, ui32 partitionId) {
        auto streamDesc = Ls(runtime, sender, path);

        const auto& streamEntry = streamDesc->ResultSet.at(0);
        UNIT_ASSERT(streamEntry.ListNodeEntry);

        const auto& children = streamEntry.ListNodeEntry->Children;
        UNIT_ASSERT_VALUES_EQUAL(children.size(), 1);

        auto topicDesc = Navigate(runtime, sender, JoinPath(ChildPath(SplitPath(path), children.at(0).Name)),
            NSchemeCache::TSchemeCacheNavigate::EOp::OpTopic);

        const auto& topicEntry = topicDesc->ResultSet.at(0);
        UNIT_ASSERT(topicEntry.PQGroupInfo);

        const auto& pqDesc = topicEntry.PQGroupInfo->Description;
        for (const auto& partition : pqDesc.GetPartitions()) {
            if (partitionId == partition.GetPartitionId()) {
                return partition.GetTabletId();
            }
        }

        UNIT_ASSERT_C(false, "Cannot find partition: " << partitionId);
        return 0;
    }

    auto GetRecords(TTestActorRuntime& runtime, const TActorId& sender, const TString& path, ui32 partitionId) {
        NKikimrClient::TPersQueueRequest request;
        request.MutablePartitionRequest()->SetTopic(path);
        request.MutablePartitionRequest()->SetPartition(partitionId);

        auto& cmd = *request.MutablePartitionRequest()->MutableCmdRead();
        cmd.SetClientId(NKikimr::NPQ::CLIENTID_WITHOUT_CONSUMER);
        cmd.SetCount(10000);
        cmd.SetOffset(0);
        cmd.SetReadTimestampMs(0);
        cmd.SetExternalOperation(true);

        auto req = MakeHolder<TEvPersQueue::TEvRequest>();
        req->Record = std::move(request);
        ForwardToTablet(runtime, ResolvePqTablet(runtime, sender, path, partitionId), sender, req.Release());

        auto resp = runtime.GrabEdgeEventRethrow<TEvPersQueue::TEvResponse>(sender);
        UNIT_ASSERT(resp);

        TVector<std::pair<TString, TString>> result;
        for (const auto& r : resp->Get()->Record.GetPartitionResponse().GetCmdReadResult().GetResult()) {
            const auto data = NKikimr::GetDeserializedData(r.GetData());
            result.emplace_back(r.GetPartitionKey(), data.GetData());
        }

        return result;
    }

    void WaitForContent(TServer::TPtr server, const TActorId& sender, const TString& path, const TVector<TString>& expected) {
        while (true) {
            const auto records = GetRecords(*server->GetRuntime(), sender, path, 0);
            for (ui32 i = 0; i < std::min(records.size(), expected.size()); ++i) {
                AssertJsonsEqual(records.at(i).second, expected.at(i));
            }

            if (records.size() >= expected.size()) {
                UNIT_ASSERT_VALUES_EQUAL_C(records.size(), expected.size(),
                    "Unexpected record: " << records.at(expected.size()).second);
                break;
            }

            SimulateSleep(server, TDuration::Seconds(1));
        }
    }

    void ShouldDeliverChanges(const TShardedTableOptions& tableDesc, const TCdcStream& streamDesc, TActionFunc action,
            const TVector<TString>& queriesBefore, const TVector<TString>& queriesAfter, const TVector<TString>& records)
    {
        TTestPqEnv env(tableDesc, streamDesc, false);

        bool preventEnqueueing = true;
        TVector<THolder<IEventHandle>> enqueued;

        env.GetServer()->GetRuntime()->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case NChangeExchange::TEvChangeExchange::EvEnqueueRecords:
                if (preventEnqueueing) {
                    enqueued.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                } else {
                    return TTestActorRuntime::EEventAction::PROCESS;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto sendEnqueued = [&]() {
            preventEnqueueing = false;
            for (auto& ev : std::exchange(enqueued, TVector<THolder<IEventHandle>>())) {
                env.GetServer()->GetRuntime()->Send(ev.Release(), 0, true);
            }
        };

        for (const auto& query : queriesBefore) {
            ExecSQL(env.GetServer(), env.GetEdgeActor(), query);
        }

        WaitTxNotification(env.GetServer(), env.GetEdgeActor(), action(env.GetServer()));

        for (const auto& query : queriesAfter) {
            ExecSQL(env.GetServer(), env.GetEdgeActor(), query);
        }

        sendEnqueued();
        WaitForContent(env.GetServer(), env.GetEdgeActor(), "/Root/Table/Stream", records);
    }

    TShardedTableOptions WithExtraColumn() {
        return TShardedTableOptions()
            .Columns({
                {"key", "Uint32", true, false},
                {"value", "Uint32", false, false},
                {"extra", "Uint32", false, false},
            });
    }

    TShardedTableOptions::TIndex SimpleIndex() {
        return TShardedTableOptions::TIndex{
            "by_value", {"value"}, {}, NKikimrSchemeOp::EIndexTypeGlobal
        };
    }

    TShardedTableOptions WithSimpleIndex() {
        return SimpleTable()
            .Indexes({
                SimpleIndex()
            });
    }

    Y_UNIT_TEST(AddColumn) {
        auto action = [](TServer::TPtr server) {
            return AsyncAlterAddExtraColumn(server, "/Root", "Table");
        };

        ShouldDeliverChanges(SimpleTable(), Updates(NKikimrSchemeOp::ECdcStreamFormatJson), action, {
            R"(UPSERT INTO `/Root/Table` (key, value) VALUES (1, 10);)",
        }, {
            R"(UPSERT INTO `/Root/Table` (key, value, extra) VALUES (2, 20, 200);)",
        }, {
            R"({"update":{"value":10},"key":[1]})",
            R"({"update":{"extra":200,"value":20},"key":[2]})",
        });
    }

    Y_UNIT_TEST(DropColumn) {
        auto action = [](TServer::TPtr server) {
            return AsyncAlterDropColumn(server, "/Root", "Table", "extra");
        };

        ShouldDeliverChanges(WithExtraColumn(), Updates(NKikimrSchemeOp::ECdcStreamFormatJson), action, {
            R"(UPSERT INTO `/Root/Table` (key, value, extra) VALUES (1, 10, 100);)",
        }, {
            R"(UPSERT INTO `/Root/Table` (key, value) VALUES (2, 20);)",
        }, {
            R"({"update":{"extra":100,"value":10},"key":[1]})",
            R"({"update":{"value":20},"key":[2]})",
        });
    }

    Y_UNIT_TEST(AddIndex) {
        auto action = [](TServer::TPtr server) {
            return AsyncAlterAddIndex(server, "/Root", "/Root/Table", SimpleIndex());
        };

        ShouldDeliverChanges(SimpleTable(), Updates(NKikimrSchemeOp::ECdcStreamFormatJson), action, {
            R"(UPSERT INTO `/Root/Table` (key, value) VALUES (1, 10);)",
        }, {
            R"(UPSERT INTO `/Root/Table` (key, value) VALUES (2, 20);)",
        }, {
            R"({"update":{"value":10},"key":[1]})",
            R"({"update":{"value":20},"key":[2]})",
        });
    }

    Y_UNIT_TEST(DropIndex) {
        auto action = [](TServer::TPtr server) {
            return AsyncAlterDropIndex(server, "/Root", "Table", SimpleIndex().Name);
        };

        ShouldDeliverChanges(WithSimpleIndex(), Updates(NKikimrSchemeOp::ECdcStreamFormatJson), action, {
            R"(UPSERT INTO `/Root/Table` (key, value) VALUES (1, 10);)",
        }, {
            R"(UPSERT INTO `/Root/Table` (key, value) VALUES (2, 20);)",
        }, {
            R"({"update":{"value":10},"key":[1]})",
            R"({"update":{"value":20},"key":[2]})",
        });
    }

    Y_UNIT_TEST(AddStream) {
        auto action = [](TServer::TPtr server) {
            return AsyncAlterAddStream(server, "/Root", "Table",
                KeysOnly(NKikimrSchemeOp::ECdcStreamFormatJson, "AnotherStream"));
        };

        ShouldDeliverChanges(SimpleTable(), Updates(NKikimrSchemeOp::ECdcStreamFormatJson), action, {
            R"(UPSERT INTO `/Root/Table` (key, value) VALUES (1, 10);)",
        }, {
            R"(UPSERT INTO `/Root/Table` (key, value) VALUES (2, 20);)",
        }, {
            R"({"update":{"value":10},"key":[1]})",
            R"({"update":{"value":20},"key":[2]})",
        });
    }

    Y_UNIT_TEST(DisableStream) {
        auto action = [](TServer::TPtr server) {
            return AsyncAlterDisableStream(server, "/Root", "Table", "Stream");
        };

        ShouldDeliverChanges(SimpleTable(), Updates(NKikimrSchemeOp::ECdcStreamFormatJson), action, {
            R"(UPSERT INTO `/Root/Table` (key, value) VALUES (1, 10);)",
        }, {
            R"(UPSERT INTO `/Root/Table` (key, value) VALUES (2, 20);)",
        }, {
            R"({"update":{"value":10},"key":[1]})",
        });
    }

    Y_UNIT_TEST(RacyRebootAndSplitWithTxInflight) {
        TTestPqEnv env(SimpleTable(), Updates(NKikimrSchemeOp::ECdcStreamFormatJson), false);
        auto& runtime = *env.GetServer()->GetRuntime();

        CreateShardedTable(env.GetServer(), env.GetEdgeActor(), "/Root", "TableAux", SimpleTable());
        ExecSQL(env.GetServer(), env.GetEdgeActor(), R"(
            UPSERT INTO `/Root/TableAux` (key, value)
            VALUES (1, 10);
        )");

        SetSplitMergePartCountLimit(&runtime, -1);
        const auto tabletIds = GetTableShards(env.GetServer(), env.GetEdgeActor(), "/Root/Table");
        UNIT_ASSERT_VALUES_EQUAL(tabletIds.size(), 1);

        ui32 readSets = 0;
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TEvTxProcessing::EvReadSet:
                ++readSets;
                return TTestActorRuntime::EEventAction::DROP;
            default:
                return TTestActorRuntime::EEventAction::PROCESS;
            }
        });

        SendSQL(env.GetServer(), env.GetEdgeActor(), R"(
            UPSERT INTO `/Root/Table` (key, value)
            SELECT key, value FROM `/Root/TableAux` WHERE key = 1;
        )");
        if (readSets < 1) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&readSets](IEventHandle&) {
                return readSets >= 1;
            });
            runtime.DispatchEvents(opts);
        }

        const auto splitTxId = AsyncSplitTable(env.GetServer(), env.GetEdgeActor(), "/Root/Table", tabletIds.at(0), 2);
        SimulateSleep(env.GetServer(), TDuration::Seconds(1));

        THolder<IEventHandle> getOwnership;
        bool txCompleted = false;
        bool splitStarted = false;
        bool splitAcked = false;

        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TEvPersQueue::EvRequest:
                if (auto* msg = ev->Get<TEvPersQueue::TEvRequest>()) {
                    if (msg->Record.GetPartitionRequest().HasCmdGetOwnership()) {
                        getOwnership.Reset(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    } else if (msg->Record.GetPartitionRequest().HasCmdSplitMessageGroup()) {
                        splitStarted = true;
                    }
                }
                return TTestActorRuntime::EEventAction::PROCESS;

            case TEvDataShard::EvProposeTransactionResult:
                if (auto* msg = ev->Get<TEvDataShard::TEvProposeTransactionResult>()) {
                    if (msg->GetStatus() == NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE) {
                        txCompleted = true;
                    }
                }
                return TTestActorRuntime::EEventAction::PROCESS;

            case TEvChangeExchange::EvSplitAck:
                splitAcked = true;
                return TTestActorRuntime::EEventAction::PROCESS;

            default:
                return TTestActorRuntime::EEventAction::PROCESS;
            }
        });

        RebootTablet(runtime, tabletIds.at(0), env.GetEdgeActor());

        if (!txCompleted) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&txCompleted](IEventHandle&) {
                return txCompleted;
            });
            runtime.DispatchEvents(opts);
        }

        if (splitStarted && !splitAcked) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&splitAcked](IEventHandle&) {
                return splitAcked;
            });
            runtime.DispatchEvents(opts);
        }

        runtime.SetObserverFunc(prevObserver);
        runtime.Send(getOwnership.Release(), 0, true);

        WaitTxNotification(env.GetServer(), env.GetEdgeActor(), splitTxId);
        WaitForContent(env.GetServer(), env.GetEdgeActor(), "/Root/Table/Stream", {
            R"({"update":{"value":10},"key":[1]})",
        });
    }

    // Split/merge
    Y_UNIT_TEST(ShouldDeliverChangesOnSplitMerge) {
        TTestPqEnv env(SimpleTable(), Updates(NKikimrSchemeOp::ECdcStreamFormatJson), false);

        bool preventEnqueueing = true;
        TVector<THolder<IEventHandle>> enqueued;
        THashMap<ui64, ui32> splitAcks;

        env.GetServer()->GetRuntime()->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case NChangeExchange::TEvChangeExchange::EvEnqueueRecords:
                if (preventEnqueueing) {
                    enqueued.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                } else {
                    return TTestActorRuntime::EEventAction::PROCESS;
                }

            case TEvDataShard::EvSplitAck:
                ++splitAcks[ev->Get<TEvDataShard::TEvSplitAck>()->Record.GetOperationCookie()];
                break;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto waitForSplitAcks = [&](ui64 txId, ui32 count) {
            if (splitAcks[txId] != count) {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) {
                    return splitAcks[txId] == count;
                });
                env.GetServer()->GetRuntime()->DispatchEvents(opts);
            }
        };

        auto sendEnqueued = [&]() {
            preventEnqueueing = false;
            for (auto& ev : std::exchange(enqueued, TVector<THolder<IEventHandle>>())) {
                env.GetServer()->GetRuntime()->Send(ev.Release(), 0, true);
            }
        };

        SetSplitMergePartCountLimit(env.GetServer()->GetRuntime(), -1);

        // split
        ExecSQL(env.GetServer(), env.GetEdgeActor(), R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 10),
            (2, 20);
        )");

        auto tabletIds = GetTableShards(env.GetServer(), env.GetEdgeActor(), "/Root/Table");
        UNIT_ASSERT_VALUES_EQUAL(tabletIds.size(), 1);

        auto txId = AsyncSplitTable(env.GetServer(), env.GetEdgeActor(), "/Root/Table", tabletIds.at(0), 4);
        waitForSplitAcks(txId, 1);
        sendEnqueued();

        WaitTxNotification(env.GetServer(), env.GetEdgeActor(), txId);
        WaitForContent(env.GetServer(), env.GetEdgeActor(), "/Root/Table/Stream", {
            R"({"update":{"value":10},"key":[1]})",
            R"({"update":{"value":20},"key":[2]})",
        });

        // reboot original shard
        RebootTablet(*env.GetServer()->GetRuntime(), tabletIds.at(0), env.GetEdgeActor());

        // merge
        preventEnqueueing = true;
        ExecSQL(env.GetServer(), env.GetEdgeActor(), R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 11),
            (2, 21);
        )");

        tabletIds = GetTableShards(env.GetServer(), env.GetEdgeActor(), "/Root/Table");
        UNIT_ASSERT_VALUES_EQUAL(tabletIds.size(), 2);

        txId = AsyncMergeTable(env.GetServer(), env.GetEdgeActor(), "/Root/Table", tabletIds);
        waitForSplitAcks(txId, 2);

        ExecSQL(env.GetServer(), env.GetEdgeActor(), "UPSERT INTO `/Root/Table` (key, value) VALUES (3, 32);");
        sendEnqueued();

        WaitTxNotification(env.GetServer(), env.GetEdgeActor(), txId);
        WaitForContent(env.GetServer(), env.GetEdgeActor(), "/Root/Table/Stream", {
            R"({"update":{"value":10},"key":[1]})",
            R"({"update":{"value":20},"key":[2]})",
            R"({"update":{"value":11},"key":[1]})",
            R"({"update":{"value":21},"key":[2]})",
            R"({"update":{"value":32},"key":[3]})",
        });
    }

    Y_UNIT_TEST(RacyActivateAndEnqueue) {
        TTestPqEnv env(SimpleTable(), Updates(NKikimrSchemeOp::ECdcStreamFormatJson), false);

        TMaybe<TActorId> preventEnqueueingOnSpecificSender;
        bool preventEnqueueing = true;
        TVector<THolder<IEventHandle>> enqueued;
        bool preventActivation = false;
        TVector<THolder<IEventHandle>> activations;
        THashMap<ui64, ui32> splitAcks;

        env.GetServer()->GetRuntime()->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case NChangeExchange::TEvChangeExchange::EvEnqueueRecords:
                if (preventEnqueueing || (preventEnqueueingOnSpecificSender && *preventEnqueueingOnSpecificSender == ev->Recipient)) {
                    enqueued.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                } else {
                    return TTestActorRuntime::EEventAction::PROCESS;
                }

            case TEvChangeExchange::EvActivateSender:
                if (preventActivation && !ev->Get<TEvChangeExchange::TEvActivateSender>()->Record.HasOrigin()) {
                    // local activation event
                    activations.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                } else {
                    return TTestActorRuntime::EEventAction::PROCESS;
                }

            case TEvDataShard::EvSplitAck:
                ++splitAcks[ev->Get<TEvDataShard::TEvSplitAck>()->Record.GetOperationCookie()];
                break;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto waitForSplitAcks = [&](ui64 txId, ui32 count) {
            if (splitAcks[txId] != count) {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&](IEventHandle&) {
                    return splitAcks[txId] == count;
                });
                env.GetServer()->GetRuntime()->DispatchEvents(opts);
            }
        };

        auto sendDelayed = [&](bool& toggleFlag, TVector<THolder<IEventHandle>>& delayed) {
            toggleFlag = false;
            for (auto& ev : std::exchange(delayed, TVector<THolder<IEventHandle>>())) {
                env.GetServer()->GetRuntime()->Send(ev.Release(), 0, true);
            }
        };

        SetSplitMergePartCountLimit(env.GetServer()->GetRuntime(), -1);

        // split
        auto tabletIds = GetTableShards(env.GetServer(), env.GetEdgeActor(), "/Root/Table");
        UNIT_ASSERT_VALUES_EQUAL(tabletIds.size(), 1);

        WaitTxNotification(env.GetServer(), env.GetEdgeActor(),
            AsyncSplitTable(env.GetServer(), env.GetEdgeActor(), "/Root/Table", tabletIds.at(0), 4));

        // execute on old partitions
        ExecSQL(env.GetServer(), env.GetEdgeActor(), R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 10),
            (2, 20);
        )");

        // merge
        tabletIds = GetTableShards(env.GetServer(), env.GetEdgeActor(), "/Root/Table");
        UNIT_ASSERT_VALUES_EQUAL(tabletIds.size(), 2);

        auto txId = AsyncMergeTable(env.GetServer(), env.GetEdgeActor(), "/Root/Table", tabletIds);
        waitForSplitAcks(txId, 2);

        // execute on new partition & enqueue on inactive sender
        preventEnqueueing = false;
        ExecSQL(env.GetServer(), env.GetEdgeActor(), "UPSERT INTO `/Root/Table` (key, value) VALUES (3, 31);");

        // send previously enqueued records & wait for activation msg
        preventActivation = true;
        sendDelayed(preventEnqueueing, enqueued);
        {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&activations](IEventHandle&) {
                return !activations.empty();
            });
            env.GetServer()->GetRuntime()->DispatchEvents(opts);
        }

        WaitTxNotification(env.GetServer(), env.GetEdgeActor(), txId);
        UNIT_ASSERT_VALUES_EQUAL(activations.size(), 1);
        preventEnqueueingOnSpecificSender = activations[0]->Recipient;

        // generate one more record
        ExecSQL(env.GetServer(), env.GetEdgeActor(), "UPSERT INTO `/Root/Table` (key, value) VALUES (4, 42);");

        // activate
        sendDelayed(preventActivation, activations);
        WaitForContent(env.GetServer(), env.GetEdgeActor(), "/Root/Table/Stream", {
            R"({"update":{"value":10},"key":[1]})",
            R"({"update":{"value":20},"key":[2]})",
            R"({"update":{"value":31},"key":[3]})",
        });

        UNIT_ASSERT_VALUES_EQUAL(enqueued.size(), 1);
        preventEnqueueingOnSpecificSender.Clear();

        sendDelayed(preventEnqueueing, enqueued);
        WaitForContent(env.GetServer(), env.GetEdgeActor(), "/Root/Table/Stream", {
            R"({"update":{"value":10},"key":[1]})",
            R"({"update":{"value":20},"key":[2]})",
            R"({"update":{"value":31},"key":[3]})",
            R"({"update":{"value":42},"key":[4]})",
        });
    }

    Y_UNIT_TEST(RacyCreateAndSend) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
        );

        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);
        CreateShardedTable(server, edgeActor, "/Root", "Table", SimpleTable());

        bool added = false;
        TVector<THolder<IEventHandle>> delayed;

        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TEvChangeExchange::EvAddSender:
                added = true;
                break;

            case TSchemeBoardEvents::EvUpdate:
                if (auto* msg = ev->Get<TSchemeBoardEvents::TEvUpdate>()) {
                    NKikimrScheme::TEvDescribeSchemeResult desc;
                    Y_ABORT_UNLESS(ParseFromStringNoSizeLimit(desc, *msg->GetRecord().GetDescribeSchemeResultSerialized().begin()));
                    if (desc.GetPath() == "/Root/Table/Stream" && desc.GetPathDescription().GetSelf().GetCreateFinished()) {
                        delayed.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                break;

            default:
                break;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        const auto txId = AsyncAlterAddStream(server, "/Root", "Table",
            Updates(NKikimrSchemeOp::ECdcStreamFormatJson));

        if (!added) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&added](IEventHandle&) {
                return added;
            });
            runtime.DispatchEvents(opts);
        }

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 10),
            (2, 20),
            (3, 30);
        )");

        if (!delayed.size() || delayed.size() % 3) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&delayed](IEventHandle&) {
                return delayed.size() && (delayed.size() % 3 == 0);
            });
            runtime.DispatchEvents(opts);
        }

        runtime.SetObserverFunc(prevObserver);
        for (auto& ev : delayed) {
            runtime.Send(ev.Release(), 0, true);
        }

        WaitTxNotification(server, edgeActor, txId);
        WaitForContent(server, edgeActor, "/Root/Table/Stream", {
            R"({"update":{"value":10},"key":[1]})",
            R"({"update":{"value":20},"key":[2]})",
            R"({"update":{"value":30},"key":[3]})",
        });
    }

    Y_UNIT_TEST(RacySplitAndDropTable) {
        TTestPqEnv env(SimpleTable(), Updates(NKikimrSchemeOp::ECdcStreamFormatJson), false);
        auto& runtime = *env.GetServer()->GetRuntime();

        TVector<THolder<IEventHandle>> enqueued;
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NChangeExchange::TEvChangeExchange::EvEnqueueRecords) {
                enqueued.emplace_back(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        ExecSQL(env.GetServer(), env.GetEdgeActor(), R"(
            UPSERT INTO `/Root/Table` (key, value)
            VALUES (1, 10);
        )");

        SetSplitMergePartCountLimit(&runtime, -1);
        const auto tabletIds = GetTableShards(env.GetServer(), env.GetEdgeActor(), "/Root/Table");
        UNIT_ASSERT_VALUES_EQUAL(tabletIds.size(), 1);
        AsyncSplitTable(env.GetServer(), env.GetEdgeActor(), "/Root/Table", tabletIds.at(0), 2);

        const auto dropTxId = AsyncDropTable(env.GetServer(), env.GetEdgeActor(), "/Root", "Table");

        runtime.SetObserverFunc(prevObserver);
        for (auto& ev : std::exchange(enqueued, {})) {
            runtime.Send(ev.Release(), 0, true);
        }

        WaitTxNotification(env.GetServer(), env.GetEdgeActor(), dropTxId);
    }

    Y_UNIT_TEST(RenameTable) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
        );

        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);

        THashSet<ui64> enqueued;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NChangeExchange::TEvChangeExchange::EvEnqueueRecords) {
                for (const auto& record : ev->Get<NChangeExchange::TEvChangeExchange::TEvEnqueueRecords>()->Records) {
                    enqueued.insert(record.Order);
                }

                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        CreateShardedTable(server, edgeActor, "/Root", "Table", SimpleTable());
        WaitTxNotification(server, edgeActor, AsyncAlterAddStream(server, "/Root", "Table",
            Updates(NKikimrSchemeOp::ECdcStreamFormatJson)));

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 10),
            (2, 20),
            (3, 30);
        )");

        if (!enqueued) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&enqueued](IEventHandle&) {
                return bool(enqueued);
            });
            runtime.DispatchEvents(opts);
        }

        THashSet<ui64> removed;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NChangeExchange::TEvChangeExchange::EvRemoveRecords) {
                for (const auto& record : ev->Get<NChangeExchange::TEvChangeExchange::TEvRemoveRecords>()->Records) {
                    removed.insert(record);
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        WaitTxNotification(server, edgeActor, AsyncAlterDropStream(server, "/Root", "Table", "Stream"));
        WaitTxNotification(server, edgeActor, AsyncMoveTable(server, "/Root/Table", "/Root/MovedTable"));

        if (enqueued != removed) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&enqueued, &removed](IEventHandle&) {
                return enqueued == removed;
            });
            runtime.DispatchEvents(opts);
        }
    }

    Y_UNIT_TEST(InitialScan) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
            .SetEnableChangefeedInitialScan(true)
        );

        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);
        CreateShardedTable(server, edgeActor, "/Root", "Table", SimpleTable());

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 10),
            (2, 20),
            (3, 30);
        )");

        WaitTxNotification(server, edgeActor, AsyncAlterAddStream(server, "/Root", "Table",
            WithInitialScan(Updates(NKikimrSchemeOp::ECdcStreamFormatJson))));

        WaitForContent(server, edgeActor, "/Root/Table/Stream", {
            R"({"update":{"value":10},"key":[1]})",
            R"({"update":{"value":20},"key":[2]})",
            R"({"update":{"value":30},"key":[3]})",
        });

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 100),
            (2, 200),
            (3, 300);
        )");

        WaitForContent(server, edgeActor, "/Root/Table/Stream", {
            R"({"update":{"value":10},"key":[1]})",
            R"({"update":{"value":20},"key":[2]})",
            R"({"update":{"value":30},"key":[3]})",
            R"({"update":{"value":100},"key":[1]})",
            R"({"update":{"value":200},"key":[2]})",
            R"({"update":{"value":300},"key":[3]})",
        });
    }

    Y_UNIT_TEST(InitialScanDebezium) {
        TTestTopicEnv env(SimpleTable(), KeysOnly(NKikimrSchemeOp::ECdcStreamFormatDebeziumJson, "UnusedStream"));
        auto& client = env.GetClient();

        // Populate data
        ExecSQL(env.GetServer(), env.GetEdgeActor(), R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 10),
            (2, 20),
            (3, 30);
        )");

        // add a stream with initial scan
        WaitTxNotification(env.GetServer(), env.GetEdgeActor(), AsyncAlterAddStream(env.GetServer(), "/Root", "Table",
            WithInitialScan(NewAndOldImages(NKikimrSchemeOp::ECdcStreamFormatDebeziumJson))));

        // add consumer
        {
            auto res = client.AlterTopic("/Root/Table/Stream", NYdb::NTopic::TAlterTopicSettings()
                .BeginAddConsumer("user").EndAddConsumer()).ExtractValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        // create reader
        auto reader = client.CreateReadSession(NYdb::NTopic::TReadSessionSettings()
            .AppendTopics(TString("/Root/Table/Stream"))
            .ConsumerName("user")
        );

        // Wait for initial scan records
        TopicRunner::WaitForContent(reader.get(), {
            {DebeziumBody("r", nullptr, R"({"key":1,"value":10})", true), {{"__key", R"({"payload":{"key":1}})"}}},
            {DebeziumBody("r", nullptr, R"({"key":2,"value":20})", true), {{"__key", R"({"payload":{"key":2}})"}}},
            {DebeziumBody("r", nullptr, R"({"key":3,"value":30})", true), {{"__key", R"({"payload":{"key":3}})"}}},
        });

        // Perform update after initial scan
        ExecSQL(env.GetServer(), env.GetEdgeActor(), R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 100),
            (2, 200),
            (3, 300),
            (4, 400);
        )");

        // Wait for update records
        TopicRunner::WaitForContent(reader.get(), {
            {DebeziumBody("u", R"({"key":1,"value":10})", R"({"key":1,"value":100})"), {{"__key", R"({"payload":{"key":1}})"}}},
            {DebeziumBody("u", R"({"key":2,"value":20})", R"({"key":2,"value":200})"), {{"__key", R"({"payload":{"key":2}})"}}},
            {DebeziumBody("u", R"({"key":3,"value":30})", R"({"key":3,"value":300})"), {{"__key", R"({"payload":{"key":3}})"}}},
            {DebeziumBody("c", nullptr, R"({"key":4,"value":400})"), {{"__key", R"({"payload":{"key":4}})"}}},
        });
    }

    Y_UNIT_TEST(InitialScanRacyCompleteAndRequest) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
            .SetEnableChangefeedInitialScan(true)
        );

        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);
        CreateShardedTable(server, edgeActor, "/Root", "Table", SimpleTable());

        std::unique_ptr<IEventHandle> doneResponse;
        auto blockDone = runtime.AddObserver<TEvDataShard::TEvCdcStreamScanResponse>(
            [&](TEvDataShard::TEvCdcStreamScanResponse::TPtr& ev) {
                if (ev->Get()->Record.GetStatus() == NKikimrTxDataShard::TEvCdcStreamScanResponse::DONE) {
                    doneResponse.reset(ev.Release());
                }
            }
        );

        WaitTxNotification(server, edgeActor, AsyncAlterAddStream(server, "/Root", "Table",
            WithInitialScan(Updates(NKikimrSchemeOp::ECdcStreamFormatJson))));
        WaitFor(runtime, [&]{ return bool(doneResponse); }, "doneResponse");
        blockDone.Remove();

        bool done = false;
        auto waitDone = runtime.AddObserver<TEvDataShard::TEvCdcStreamScanResponse>(
            [&](TEvDataShard::TEvCdcStreamScanResponse::TPtr& ev) {
                if (ev->Get()->Record.GetStatus() == NKikimrTxDataShard::TEvCdcStreamScanResponse::DONE) {
                    done = true;
                }
            }
        );

        const auto& record = doneResponse->Get<TEvDataShard::TEvCdcStreamScanResponse>()->Record;
        RebootTablet(runtime, record.GetTablePathId().GetOwnerId(), edgeActor);
        WaitFor(runtime, [&]{ return done; }, "done");
    }

    Y_UNIT_TEST(InitialScanUpdatedRows) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
            .SetEnableChangefeedInitialScan(true)
        );

        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);
        CreateShardedTable(server, edgeActor, "/Root", "Table", SimpleTable());

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 10),
            (2, 20),
            (3, 30);
        )");

        TVector<THolder<IEventHandle>> delayed;
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::EvCdcStreamScanRequest) {
                delayed.emplace_back(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        WaitTxNotification(server, edgeActor, AsyncAlterAddStream(server, "/Root", "Table",
            WithInitialScan(Updates(NKikimrSchemeOp::ECdcStreamFormatJson))));

        if (delayed.empty()) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&delayed](IEventHandle&) {
                return !delayed.empty();
            });
            runtime.DispatchEvents(opts);
        }

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 100),
            (4, 40);
        )");

        ExecSQL(server, edgeActor, R"(
            DELETE FROM `/Root/Table` WHERE key = 2;
        )");

        WaitForContent(server, edgeActor, "/Root/Table/Stream", {
            R"({"update":{"value":10},"key":[1]})",
            R"({"update":{"value":100},"key":[1]})",
            R"({"update":{"value":40},"key":[4]})",
            R"({"update":{"value":20},"key":[2]})",
            R"({"erase":{},"key":[2]})",
        });

        runtime.SetObserverFunc(prevObserver);
        for (auto& ev : std::exchange(delayed, TVector<THolder<IEventHandle>>())) {
            runtime.Send(ev.Release(), 0, true);
        }

        WaitForContent(server, edgeActor, "/Root/Table/Stream", {
            R"({"update":{"value":10},"key":[1]})",
            R"({"update":{"value":100},"key":[1]})",
            R"({"update":{"value":40},"key":[4]})",
            R"({"update":{"value":20},"key":[2]})",
            R"({"erase":{},"key":[2]})",
            R"({"update":{"value":30},"key":[3]})",
        });
    }

    Y_UNIT_TEST(InitialScanAndLimits) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
            .SetEnableChangefeedInitialScan(true)
            .SetChangesQueueItemsLimit(1)
        );

        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);
        CreateShardedTable(server, edgeActor, "/Root", "Table", SimpleTable());

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 10),
            (2, 20),
            (3, 30);
        )");

        TVector<THolder<IEventHandle>> delayed;
        ui32 progressCount = 0;

        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            static constexpr ui32 EvCdcStreamScanProgress = EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 24;

            switch (ev->GetTypeRewrite()) {
            case TEvDataShard::EvCdcStreamScanRequest:
                if (auto* msg = ev->Get<TEvDataShard::TEvCdcStreamScanRequest>()) {
                    msg->Record.MutableLimits()->SetBatchMaxRows(1);
                } else {
                    UNIT_ASSERT(false);
                }
                break;

            case NChangeExchange::TEvChangeExchange::EvEnqueueRecords:
                delayed.emplace_back(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;

            case EvCdcStreamScanProgress:
                ++progressCount;
                break;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        WaitTxNotification(server, edgeActor, AsyncAlterAddStream(server, "/Root", "Table",
            WithInitialScan(Updates(NKikimrSchemeOp::ECdcStreamFormatJson))));

        if (delayed.empty()) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&delayed, &progressCount](IEventHandle&) {
                return !delayed.empty() && progressCount >= 2;
            });
            runtime.DispatchEvents(opts);
        }

        runtime.SetObserverFunc(prevObserver);
        for (auto& ev : std::exchange(delayed, TVector<THolder<IEventHandle>>())) {
            runtime.Send(ev.Release(), 0, true);
        }

        WaitForContent(server, edgeActor, "/Root/Table/Stream", {
            R"({"update":{"value":10},"key":[1]})",
            R"({"update":{"value":20},"key":[2]})",
            R"({"update":{"value":30},"key":[3]})",
        });
    }

    Y_UNIT_TEST(InitialScanComplete) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
            .SetEnableChangefeedInitialScan(true)
        );

        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);
        CreateShardedTable(server, edgeActor, "/Root", "Table", SimpleTable());

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 10),
            (2, 20);
        )");

        THolder<IEventHandle> delayed;
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NSchemeShard::TEvSchemeShard::EvModifySchemeTransaction) {
                auto* msg = ev->Get<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction>();
                const auto& tx = msg->Record.GetTransaction(0);
                if (tx.HasAlterCdcStream() && tx.GetAlterCdcStream().HasGetReady()) {
                    delayed.Reset(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        WaitTxNotification(server, edgeActor, AsyncAlterAddStream(server, "/Root", "Table",
            WithInitialScan(Updates(NKikimrSchemeOp::ECdcStreamFormatJson))));

        if (!delayed) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&delayed](IEventHandle&) {
                return bool(delayed);
            });
            runtime.DispatchEvents(opts);
        }

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (3, 30),
            (4, 40);
        )");

        runtime.SetObserverFunc(prevObserver);
        runtime.Send(delayed.Release(), 0, true);

        WaitForContent(server, edgeActor, "/Root/Table/Stream", {
            R"({"update":{"value":10},"key":[1]})",
            R"({"update":{"value":20},"key":[2]})",
            R"({"update":{"value":30},"key":[3]})",
            R"({"update":{"value":40},"key":[4]})",
        });
    }

    Y_UNIT_TEST(InitialScanRacyProgressAndDrop) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
            .SetEnableChangefeedInitialScan(true)
            .SetChangesQueueItemsLimit(1)
        );

        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);
        CreateShardedTable(server, edgeActor, "/Root", "Table", SimpleTable());

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 10),
            (2, 20),
            (3, 30);
        )");

        bool delayProgress = true;
        ui32 progressCount = 0;
        TVector<THolder<IEventHandle>> delayed;

        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            static constexpr ui32 EvCdcStreamScanProgress = EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 24;
            if (ev->GetTypeRewrite() == EvCdcStreamScanProgress) {
                ++progressCount;
                if (delayProgress) {
                    delayed.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto waitProgress = [&](ui32 count) {
            if (progressCount != count) {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&progressCount, count](IEventHandle&) {
                    return progressCount == count;
                });
                runtime.DispatchEvents(opts);
            }
        };

        WaitTxNotification(server, edgeActor, AsyncAlterAddStream(server, "/Root", "Table",
            WithInitialScan(Updates(NKikimrSchemeOp::ECdcStreamFormatJson))));

        waitProgress(1);
        WaitTxNotification(server, edgeActor, AsyncAlterDropStream(server, "/Root", "Table", "Stream"));

        delayProgress = false;
        for (auto& ev : std::exchange(delayed, TVector<THolder<IEventHandle>>())) {
            runtime.Send(ev.Release(), 0, true);
        }

        waitProgress(2);
    }

    Y_UNIT_TEST(EnqueueRequestProcessSend) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
        );

        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);
        CreateShardedTable(server, edgeActor, "/Root", "Table", TShardedTableOptions()
            .Columns({
                {"key", "Uint32", true, false},
                {"value", "Utf8", false, false},
            })
        );

        bool ready = false;
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NChangeExchange::TEvChangeExchangePrivate::EvReady) {
                ready = true;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        WaitTxNotification(server, edgeActor, AsyncAlterAddStream(server, "/Root", "Table",
            Updates(NKikimrSchemeOp::ECdcStreamFormatJson)));

        if (!ready) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&ready](IEventHandle&) {
                return ready;
            });
            server->GetRuntime()->DispatchEvents(opts);
        }

        runtime.SetObserverFunc(prevObserver);

        THolder<IEventHandle> delayed;
        prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NChangeExchange::TEvChangeExchangePrivate::EvReady) {
                delayed.Reset(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        const auto value = TString(200_KB, 'A');
        // make sender busy
        ExecSQL(server, edgeActor, Sprintf(R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, "%s");
        )", value.c_str()));

        if (!delayed) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&delayed](IEventHandle&) {
                return bool(delayed);
            });
            server->GetRuntime()->DispatchEvents(opts);
        }

        ExecSQL(server, edgeActor, Sprintf(R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (2, "%s"),
            (3, "%s");
        )", value.c_str(), value.c_str()));

        runtime.SetObserverFunc(prevObserver);
        runtime.Send(delayed.Release(), 0, true);

        WaitForContent(server, edgeActor, "/Root/Table/Stream", {
            Sprintf(R"({"update":{"value":"%s"},"key":[1]})", value.c_str()),
            Sprintf(R"({"update":{"value":"%s"},"key":[2]})", value.c_str()),
            Sprintf(R"({"update":{"value":"%s"},"key":[3]})", value.c_str()),
        });
    }

    Y_UNIT_TEST(AwsRegion) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
            .SetAwsRegion("defaultRegion")
            .SetEnableChangefeedDynamoDBStreamsFormat(true)
        );

        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);
        CreateShardedTable(server, edgeActor, "/Root", "Table", DocApiTable());

        WaitTxNotification(server, edgeActor, AsyncAlterAddStream(server, "/Root", "Table",
            KeysOnly(NKikimrSchemeOp::ECdcStreamFormatDynamoDBStreamsJson, "Stream1")));
        WaitTxNotification(server, edgeActor, AsyncAlterAddStream(server, "/Root", "Table",
            WithAwsRegion("customRegion", KeysOnly(NKikimrSchemeOp::ECdcStreamFormatDynamoDBStreamsJson, "Stream2"))));

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (__Hash, id_shard, id_sort, __RowData) VALUES (
                1, "10", "100", JsonDocument('{"M":{"color":{"S":"pink"},"weight":{"N":"4.5"}}}')
            );
        )");

        auto checkAwsRegion = [&](const TString& path, const char* awsRegion) {
            while (true) {
                const auto records = GetRecords(runtime, edgeActor, path, 0);
                if (records.size() >= 1) {
                    for (const auto& [_, record] : records) {
                        UNIT_ASSERT_STRING_CONTAINS(record, Sprintf(R"("awsRegion":"%s")", awsRegion));
                    }

                    break;
                }

                SimulateSleep(server, TDuration::Seconds(1));
            }
        };

        checkAwsRegion("/Root/Table/Stream1", "defaultRegion");
        checkAwsRegion("/Root/Table/Stream2", "customRegion");
    }

    Y_UNIT_TEST(ResolvedTimestamps) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
        );

        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);
        CreateShardedTable(server, edgeActor, "/Root", "Table", SimpleTable());

        WaitTxNotification(server, edgeActor, AsyncAlterAddStream(server, "/Root", "Table",
            WithResolvedTimestamps(TDuration::Seconds(3), Updates(NKikimrSchemeOp::ECdcStreamFormatJson))));

        WaitForContent(server, edgeActor, "/Root/Table/Stream", {
            R"({"resolved":"***"})",
        });

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 10),
            (2, 20),
            (3, 30);
        )");

        WaitForContent(server, edgeActor, "/Root/Table/Stream", {
            R"({"resolved":"***"})",
            R"({"update":{"value":10},"key":[1]})",
            R"({"update":{"value":20},"key":[2]})",
            R"({"update":{"value":30},"key":[3]})",
            R"({"resolved":"***"})",
        });

        // split table
        const auto tabletIds = GetTableShards(server, edgeActor, "/Root/Table");
        UNIT_ASSERT_VALUES_EQUAL(tabletIds.size(), 1);

        SetSplitMergePartCountLimit(&runtime, -1);
        WaitTxNotification(server, edgeActor, AsyncSplitTable(server, edgeActor, "/Root/Table", tabletIds.at(0), 2));

        WaitForContent(server, edgeActor, "/Root/Table/Stream", {
            R"({"resolved":"***"})",
            R"({"update":{"value":10},"key":[1]})",
            R"({"update":{"value":20},"key":[2]})",
            R"({"update":{"value":30},"key":[3]})",
            R"({"resolved":"***"})",
            R"({"resolved":"***"})",
        });

        // disable stream
        WaitTxNotification(server, edgeActor, AsyncAlterDisableStream(server, "/Root", "Table", "Stream"));
        SimulateSleep(server, TDuration::Seconds(5));

        WaitForContent(server, edgeActor, "/Root/Table/Stream", {
            R"({"resolved":"***"})",
            R"({"update":{"value":10},"key":[1]})",
            R"({"update":{"value":20},"key":[2]})",
            R"({"update":{"value":30},"key":[3]})",
            R"({"resolved":"***"})",
            R"({"resolved":"***"})",
        });
    }

    Y_UNIT_TEST(ResolvedTimestampsMultiplePartitions) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
        );

        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);
        CreateShardedTable(server, edgeActor, "/Root", "Table", TShardedTableOptions().Shards(2));

        WaitTxNotification(server, edgeActor, AsyncAlterAddStream(server, "/Root", "Table",
            WithResolvedTimestamps(TDuration::Seconds(3), Updates(NKikimrSchemeOp::ECdcStreamFormatJson))));

        TVector<TVector<std::pair<TString, TString>>> records(2); // partition to records
        while (true) {
            for (ui32 i = 0; i < records.size(); ++i) {
                records[i] = GetRecords(*server->GetRuntime(), edgeActor, "/Root/Table/Stream", i);
            }

            if (AllOf(records, [](const auto& x) { return !x.empty(); })) {
                break;
            }

            SimulateSleep(server, TDuration::Seconds(1));
        }

        UNIT_ASSERT(records.size() > 1);
        UNIT_ASSERT(!records[0].empty());
        AssertJsonsEqual(records[0][0].second, R"({"resolved":"***"})");

        for (ui32 i = 1; i < records.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(records[i][0].second, records[0][0].second);
        }
    }

    Y_UNIT_TEST(InitialScanAndResolvedTimestamps) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
            .SetEnableChangefeedInitialScan(true)
        );

        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);
        CreateShardedTable(server, edgeActor, "/Root", "Table", SimpleTable());

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 10),
            (2, 20),
            (3, 30);
        )");

        THolder<IEventHandle> delayed;
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::EvCdcStreamScanRequest) {
                delayed.Reset(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        WaitTxNotification(server, edgeActor, AsyncAlterAddStream(server, "/Root", "Table",
            WithResolvedTimestamps(TDuration::Seconds(3),
                WithInitialScan(Updates(NKikimrSchemeOp::ECdcStreamFormatJson))
            )
        ));

        if (!delayed) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&delayed](IEventHandle&) {
                return bool(delayed);
            });
            runtime.DispatchEvents(opts);
        }

        SimulateSleep(server, TDuration::Seconds(5));
        runtime.SetObserverFunc(prevObserver);
        runtime.Send(delayed.Release(), 0, true);

        WaitForContent(server, edgeActor, "/Root/Table/Stream", {
            R"({"update":{"value":10},"key":[1]})",
            R"({"update":{"value":20},"key":[2]})",
            R"({"update":{"value":30},"key":[3]})",
            R"({"resolved":"***"})",
        });
    }

    Y_UNIT_TEST(ResolvedTimestampsVolatileOutOfOrder) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
            .SetEnableDataShardVolatileTransactions(true)
        );

        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);
        CreateShardedTable(server, edgeActor, "/Root", "Table1", SimpleTable());
        CreateShardedTable(server, edgeActor, "/Root", "Table2", SimpleTable());

        WaitTxNotification(server, edgeActor, AsyncAlterAddStream(server, "/Root", "Table1",
            WithResolvedTimestamps(TDuration::Seconds(3), Updates(NKikimrSchemeOp::ECdcStreamFormatJson))));
        WaitTxNotification(server, edgeActor, AsyncAlterAddStream(server, "/Root", "Table2",
            WithResolvedTimestamps(TDuration::Seconds(3), Updates(NKikimrSchemeOp::ECdcStreamFormatJson))));

        WaitForContent(server, edgeActor, "/Root/Table1/Stream", {
            R"({"resolved":"***"})",
        });
        WaitForContent(server, edgeActor, "/Root/Table2/Stream", {
            R"({"resolved":"***"})",
        });

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table1` (key, value) VALUES (1, 10);
            UPSERT INTO `/Root/Table2` (key, value) VALUES (2, 20);
        )");

        WaitForContent(server, edgeActor, "/Root/Table1/Stream", {
            R"({"resolved":"***"})",
            R"({"update":{"value":10},"key":[1]})",
            R"({"resolved":"***"})",
        });
        WaitForContent(server, edgeActor, "/Root/Table2/Stream", {
            R"({"resolved":"***"})",
            R"({"update":{"value":20},"key":[2]})",
            R"({"resolved":"***"})",
        });

        // Block readset exchange
        std::vector<std::unique_ptr<IEventHandle>> readSets;
        auto blockReadSets = runtime.AddObserver<TEvTxProcessing::TEvReadSet>([&](TEvTxProcessing::TEvReadSet::TPtr& ev) {
            readSets.emplace_back(ev.Release());
        });

        // Start a distributed write to both tables
        TString sessionId = CreateSessionRPC(runtime, "/Root");
        auto upsertResult = SendRequest(
            runtime,
            MakeSimpleRequestRPC(R"(
                UPSERT INTO `/Root/Table1` (key, value) VALUES (3, 30);
                UPSERT INTO `/Root/Table2` (key, value) VALUES (4, 40);
                )", sessionId, /* txId */ "", /* commitTx */ true),
            "/Root");
        WaitFor(runtime, [&]{ return readSets.size() >= 4; }, "readsets");

        // Stop blocking further readsets
        blockReadSets.Remove();

        // Start another distributed write to both tables, it should succeed
        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table1` (key, value) VALUES (5, 50);
            UPSERT INTO `/Root/Table2` (key, value) VALUES (6, 60);
        )");

        runtime.SimulateSleep(TDuration::Seconds(10));

        // Unblock readsets
        for (auto& ev : readSets) {
            runtime.Send(ev.release(), 0, true);
        }
        readSets.clear();

        // There should be only one resolved timestamp after out of order writes
        WaitForContent(server, edgeActor, "/Root/Table1/Stream", {
            R"({"resolved":"***"})",
            R"({"update":{"value":10},"key":[1]})",
            R"({"resolved":"***"})",
            R"({"update":{"value":50},"key":[5]})",
            R"({"update":{"value":30},"key":[3]})",
            R"({"resolved":"***"})",
        });
        WaitForContent(server, edgeActor, "/Root/Table2/Stream", {
            R"({"resolved":"***"})",
            R"({"update":{"value":20},"key":[2]})",
            R"({"resolved":"***"})",
            R"({"update":{"value":60},"key":[6]})",
            R"({"update":{"value":40},"key":[4]})",
            R"({"resolved":"***"})",
        });
    }

    Y_UNIT_TEST(SequentialSplitMerge) {
        TTestPqEnv env(SimpleTable(), Updates(NKikimrSchemeOp::ECdcStreamFormatJson), false);
        SetSplitMergePartCountLimit(env.GetServer()->GetRuntime(), -1);

        // split
        auto tabletIds = GetTableShards(env.GetServer(), env.GetEdgeActor(), "/Root/Table");
        UNIT_ASSERT_VALUES_EQUAL(tabletIds.size(), 1);

        WaitTxNotification(env.GetServer(), env.GetEdgeActor(),
            AsyncSplitTable(env.GetServer(), env.GetEdgeActor(), "/Root/Table", tabletIds.at(0), 4));

        // merge
        tabletIds = GetTableShards(env.GetServer(), env.GetEdgeActor(), "/Root/Table");
        UNIT_ASSERT_VALUES_EQUAL(tabletIds.size(), 2);

        WaitTxNotification(env.GetServer(), env.GetEdgeActor(),
            AsyncMergeTable(env.GetServer(), env.GetEdgeActor(), "/Root/Table", tabletIds));

        ExecSQL(env.GetServer(), env.GetEdgeActor(), R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 10),
            (2, 20),
            (3, 30);
        )");

        WaitForContent(env.GetServer(), env.GetEdgeActor(), "/Root/Table/Stream", {
            R"({"update":{"value":10},"key":[1]})",
            R"({"update":{"value":20},"key":[2]})",
            R"({"update":{"value":30},"key":[3]})",
        });
    }

    void MustNotLoseSchemaSnapshot(bool enableVolatileTx) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
            .SetEnableDataShardVolatileTransactions(enableVolatileTx)
        );

        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);
        CreateShardedTable(server, edgeActor, "/Root", "Table", SimpleTable());

        WaitTxNotification(server, edgeActor, AsyncAlterAddStream(server, "/Root", "Table",
            Updates(NKikimrSchemeOp::ECdcStreamFormatJson)));

        auto tabletIds = GetTableShards(server, edgeActor, "/Root/Table");
        UNIT_ASSERT_VALUES_EQUAL(tabletIds.size(), 1);

        std::vector<std::unique_ptr<IEventHandle>> blockedRemoveRecords;
        auto blockRemoveRecords = runtime.AddObserver<NChangeExchange::TEvChangeExchange::TEvRemoveRecords>([&](auto& ev) {
            Cerr << "... blocked remove record" << Endl;
            blockedRemoveRecords.emplace_back(ev.Release());
        });

        Cerr << "... execute first query" << Endl;
        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES (1, 10);
        )");

        WaitFor(runtime, [&]{ return blockedRemoveRecords.size() == 1; }, "blocked remove records");
        blockRemoveRecords.Remove();

        std::vector<std::unique_ptr<IEventHandle>> blockedPlans;
        auto blockPlans = runtime.AddObserver<TEvTxProxy::TEvProposeTransaction>([&](auto& ev) {
            blockedPlans.emplace_back(ev.Release());
        });

        Cerr << "... execute scheme query" << Endl;
        const auto alterTxId = AsyncAlterAddExtraColumn(server, "/Root", "Table");

        WaitFor(runtime, [&]{ return blockedPlans.size() > 0; }, "blocked plans");
        blockPlans.Remove();

        std::vector<std::unique_ptr<IEventHandle>> blockedPutResponses;
        auto blockPutResponses = runtime.AddObserver<TEvBlobStorage::TEvPutResult>([&](auto& ev) {
            auto* msg = ev->Get();
            if (msg->Id.TabletID() == tabletIds[0]) {
                Cerr << "... blocked put response:" << msg->Id << Endl;
                blockedPutResponses.emplace_back(ev.Release());
            }
        });

        Cerr << "... execute second query" << Endl;
        SendSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES (2, 20);
        )");

        WaitFor(runtime, [&]{ return blockedPutResponses.size() > 0; }, "blocked put responses");
        auto wasBlockedPutResponses = blockedPutResponses.size();

        Cerr << "... release blocked plans" << Endl;
        for (auto& ev : std::exchange(blockedPlans, {})) {
            runtime.Send(ev.release(), 0, true);
        }

        WaitFor(runtime, [&]{ return blockedPutResponses.size() > wasBlockedPutResponses; }, "blocked put responses");
        wasBlockedPutResponses = blockedPutResponses.size();

        Cerr << "... release blocked remove records" << Endl;
        for (auto& ev : std::exchange(blockedRemoveRecords, {})) {
            runtime.Send(ev.release(), 0, true);
        }

        WaitFor(runtime, [&]{ return blockedPutResponses.size() > wasBlockedPutResponses; }, "blocked put responses");
        blockPutResponses.Remove();

        Cerr << "... release blocked put responses" << Endl;
        for (auto& ev : std::exchange(blockedPutResponses, {})) {
            runtime.Send(ev.release(), 0, true);
        }

        Cerr << "... finalize" << Endl;
        WaitTxNotification(server, edgeActor, alterTxId);
        WaitForContent(server, edgeActor, "/Root/Table/Stream", {
            R"({"update":{"value":10},"key":[1]})",
            R"({"update":{"value":20},"key":[2]})",
        });
    }

    Y_UNIT_TEST(MustNotLoseSchemaSnapshot) {
        MustNotLoseSchemaSnapshot(false);
    }

    Y_UNIT_TEST(MustNotLoseSchemaSnapshotWithVolatileTx) {
        MustNotLoseSchemaSnapshot(true);
    }

    Y_UNIT_TEST(ResolvedTimestampsContinueAfterMerge) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
        );

        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);
        SetSplitMergePartCountLimit(&runtime, -1);
        CreateShardedTable(server, edgeActor, "/Root", "Table", SimpleTable());

        WaitTxNotification(server, edgeActor, AsyncAlterAddStream(server, "/Root", "Table",
            WithResolvedTimestamps(TDuration::Seconds(3), Updates(NKikimrSchemeOp::ECdcStreamFormatJson))));

        Cerr << "... prepare" << Endl;
        {
            WaitForContent(server, edgeActor, "/Root/Table/Stream", {
                R"({"resolved":"***"})",
            });

            auto tabletIds = GetTableShards(server, edgeActor, "/Root/Table");
            UNIT_ASSERT_VALUES_EQUAL(tabletIds.size(), 1);

            WaitTxNotification(server, edgeActor, AsyncSplitTable(server, edgeActor, "/Root/Table", tabletIds.at(0), 2));
            WaitForContent(server, edgeActor, "/Root/Table/Stream", {
                R"({"resolved":"***"})",
                R"({"resolved":"***"})",
            });
        }

        auto initialTabletIds = GetTableShards(server, edgeActor, "/Root/Table");
        UNIT_ASSERT_VALUES_EQUAL(initialTabletIds.size(), 2);

        std::vector<std::unique_ptr<IEventHandle>> blockedSplitRequests;
        auto blockSplitRequests = runtime.AddObserver<TEvPersQueue::TEvRequest>([&](auto& ev) {
            if (ev->Get()->Record.GetPartitionRequest().HasCmdSplitMessageGroup()) {
                blockedSplitRequests.emplace_back(ev.Release());
            }
        });

        Cerr << "... merge table" << Endl;
        const auto mergeTxId = AsyncMergeTable(server, edgeActor, "/Root/Table", initialTabletIds);
        WaitFor(runtime, [&]{ return blockedSplitRequests.size() == initialTabletIds.size(); }, "blocked split requests");
        blockSplitRequests.Remove();

        std::vector<std::unique_ptr<IEventHandle>> blockedRegisterRequests;
        auto blockRegisterRequests = runtime.AddObserver<TEvPersQueue::TEvRequest>([&](auto& ev) {
            if (ev->Get()->Record.GetPartitionRequest().HasCmdRegisterMessageGroup()) {
                blockedRegisterRequests.emplace_back(ev.Release());
            }
        });

        ui32 splitResponses = 0;
        auto countSplitResponses = runtime.AddObserver<TEvPersQueue::TEvResponse>([&](auto&) {
            ++splitResponses;
        });

        Cerr << "... release split requests" << Endl;
        for (auto& ev : std::exchange(blockedSplitRequests, {})) {
            runtime.Send(ev.release(), 0, true);
            WaitFor(runtime, [prev = splitResponses, &splitResponses]{ return splitResponses > prev; }, "split response");
        }

        Cerr << "... reboot pq tablet" << Endl;
        RebootTablet(runtime, ResolvePqTablet(runtime, edgeActor, "/Root/Table/Stream", 0), edgeActor);
        countSplitResponses.Remove();

        Cerr << "... release register requests" << Endl;
        blockRegisterRequests.Remove();
        for (auto& ev : std::exchange(blockedRegisterRequests, {})) {
            runtime.Send(ev.release(), 0, true);
        }

        Cerr << "... wait for merge tx notification" << Endl;
        WaitTxNotification(server, edgeActor, mergeTxId);

        Cerr << "... wait for final heartbeat" << Endl;
        WaitForContent(server, edgeActor, "/Root/Table/Stream", {
            R"({"resolved":"***"})",
            R"({"resolved":"***"})",
            R"({"resolved":"***"})",
        });
    }

} // Cdc

} // NKikimr

template <>
void Out<std::pair<TString, TString>>(IOutputStream& output, const std::pair<TString, TString>& x) {
    output << x.first << ":" << x.second;
}

void AppendToString(TString& dst, const std::pair<TString, TString>& x) {
    TStringOutput output(dst);
    output << x;
}
