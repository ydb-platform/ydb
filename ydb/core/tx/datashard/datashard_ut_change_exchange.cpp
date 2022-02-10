#include "datashard_ut_common.h"

#include <util/string/strip.h>

namespace NKikimr {

using namespace NDataShard;
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
            .SetEnableDataColumnForIndexTable(true)
            .SetEnableAsyncIndexes(true);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::CHANGE_EXCHANGE, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        bool activated = false;
        runtime.SetObserverFunc([&activated](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
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
            TMaybe<TShardedTableOptions::TIndex> addIndex = Nothing())
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
            .SetEnableDataColumnForIndexTable(true)
            .SetEnableAsyncIndexes(true);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::CHANGE_EXCHANGE, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        ui32 counter = 0;
        runtime.SetObserverFunc([&counter](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvChangeExchange::EvHandshake) {
                ++counter;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        CreateShardedTable(server, sender, workingDir, tableName, opts);
        if (addIndex) {
            AsyncAlterAddIndex(server, domainName, path, *addIndex);
        }

        if (counter != times) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&](IEventHandle&) {
                return counter == times;
            });
            server->GetRuntime()->DispatchEvents(opts);
        }
    }

    Y_UNIT_TEST(SenderShouldShakeHandsOnce) {
        SenderShouldShakeHands("/Root/Table", 1, TableWithIndex(SimpleAsyncIndex()));
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
            })
        );
    }

    Y_UNIT_TEST(SenderShouldShakeHandsAfterAddingIndex) {
        SenderShouldShakeHands("/Root/Table", 1, TableWoIndexes(), SimpleAsyncIndex());
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
            .SetEnableDataColumnForIndexTable(true)
            .SetEnableAsyncIndexes(true);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::CHANGE_EXCHANGE, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        THashSet<ui64> enqueued;
        THashSet<ui64> requested;
        THashSet<ui64> removed;

        runtime.SetObserverFunc([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TEvChangeExchange::EvEnqueueRecords:
                for (const auto& record : ev->Get<TEvChangeExchange::TEvEnqueueRecords>()->Records) {
                    enqueued.insert(record.Order);
                }
                break;

            case TEvChangeExchange::EvRequestRecords:
                for (const auto& record : ev->Get<TEvChangeExchange::TEvRequestRecords>()->Records) {
                    requested.insert(record.Order);
                }
                break;

            case TEvChangeExchange::EvRemoveRecords:
                for (const auto& record : ev->Get<TEvChangeExchange::TEvRemoveRecords>()->Records) {
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
            .SetEnableDataColumnForIndexTable(true)
            .SetEnableAsyncIndexes(true);

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

        runtime.SetObserverFunc([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TEvChangeExchange::EvActivateSender:
                if (preventActivation) {
                    activations.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                } else {
                    return TTestActorRuntime::EEventAction::PROCESS;
                }

            case TEvChangeExchange::EvEnqueueRecords:
                for (const auto& record : ev->Get<TEvChangeExchange::TEvEnqueueRecords>()->Records) {
                    enqueued.insert(record.Order);
                }
                break;

            case TEvChangeExchange::EvRemoveRecords:
                for (const auto& record : ev->Get<TEvChangeExchange::TEvRemoveRecords>()->Records) {
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
            .SetEnableDataColumnForIndexTable(true)
            .SetEnableAsyncIndexes(true);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::CHANGE_EXCHANGE, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        bool preventEnqueueing = true;
        TVector<THolder<IEventHandle>> enqueued;
        THashMap<ui64, ui32> splitAcks;

        runtime.SetObserverFunc([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TEvChangeExchange::EvEnqueueRecords:
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
            .SetEnableDataColumnForIndexTable(true)
            .SetEnableAsyncIndexes(true);
        setLimit(serverSettings);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::CHANGE_EXCHANGE, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        bool preventEnqueueing = true;
        TVector<THolder<IEventHandle>> enqueued;

        runtime.SetObserverFunc([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TEvChangeExchange::EvEnqueueRecords:
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
}

} // NKikimr
