#include <ydb/core/tablet_flat/util_fmt_cell.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_utils.h>

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSchemeShardSplitBySizeTest) {
    Y_UNIT_TEST(Test) {
    }

    Y_UNIT_TEST(ConcurrentSplitOneShard) {
        TTestBasicRuntime runtime;

        TTestEnvOptions opts;
        opts.EnableBackgroundCompaction(false);

        TTestEnv env(runtime, opts);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            Columns { Name: "Key"       Type: "Utf8"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["Key", "Value"]
                            )");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::PartitionKeys({""})});

        TVector<THolder<IEventHandle>> suppressed;
        auto prevObserver = SetSuppressObserver(runtime, suppressed, TEvHive::TEvCreateTablet::EventType);

        TestSplitTable(runtime, ++txId, "/MyRoot/Table", R"(
                            SourceTabletId: 72075186233409546
                            SplitBoundary {
                                KeyPrefix {
                                    Tuple { Optional { Text: "A" } }
                                }
                            })");

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        TestSplitTable(runtime, ++txId, "/MyRoot/Table", R"(
                        SourceTabletId: 72075186233409546
                        SplitBoundary {
                            KeyPrefix {
                                Tuple { Optional { Text: "A" } }
                            }
                        })",
                       {NKikimrScheme::StatusMultipleModifications});

        WaitForSuppressed(runtime, suppressed, 4, prevObserver);

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        env.TestWaitNotification(runtime, {txId-1, txId});
        env.TestWaitTabletDeletion(runtime, TTestTxConfig::FakeHiveTablets); //delete src

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::PartitionKeys({"A", ""})});

    }

    Y_UNIT_TEST(Split10Shards) {
        TTestBasicRuntime runtime;

        TTestEnvOptions opts;
        opts.EnableBackgroundCompaction(false);

        TTestEnv env(runtime, opts);

        ui64 txId = 100;

        NDataShard::gDbStatsReportInterval = TDuration::Seconds(1);
        NDataShard::gDbStatsDataSizeResolution = 10;
        NDataShard::gDbStatsRowCountResolution = 10;

        //runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_CRIT);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_CRIT);


        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                        Name: "Table"
                        Columns { Name: "key"       Type: "Uint64"}
                        Columns { Name: "value"      Type: "Utf8"}
                        KeyColumnNames: ["key"]
                        UniformPartitionsCount: 1
                        )");
        env.TestWaitNotification(runtime, txId);

        auto fnWriteRow = [&] (ui64 tabletId, ui64 key) {
            TString writeQuery = Sprintf(R"(
                (
                    (let key '( '('key (Uint64 '%lu)) ) )
                    (let value '('('value (Utf8 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA)) ) )
                    (return (AsList (UpdateRow '__user__Table key value) ))
                )
            )", key);;
            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL(err, "");
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);;
        };
        for (ui64 key = 0; key < 1000; ++key) {
            fnWriteRow(TTestTxConfig::FakeHiveTablets, key* 1'000'000);
        }

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::PartitionCount(1)});

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                        Name: "Table"
                        PartitionConfig {
                            PartitioningPolicy {
                                MinPartitionsCount: 100
                                MaxPartitionsCount: 100
                                SizeToSplit: 1
                                FastSplitSettings {
                                    SizeThreshold: 10
                                    RowCountThreshold: 10
                                }
                            }
                        }
                    )");
        env.TestWaitNotification(runtime, txId);

        while (true) {
            TVector<THolder<IEventHandle>> suppressed;
            auto prevObserver = SetSuppressObserver(runtime, suppressed, TEvDataShard::TEvGetTableStatsResult::EventType);

            WaitForSuppressed(runtime, suppressed, 1, prevObserver);
            for (auto &msg : suppressed) {
                runtime.Send(msg.Release());
            }
            suppressed.clear();

            bool itIsEnough = false;

            NLs::TCheckFunc checkPartitionCount = [&] (const NKikimrScheme::TEvDescribeSchemeResult& record) {
                if (record.GetPathDescription().TablePartitionsSize() >= 10) {
                    itIsEnough = true;
                }
            };

            TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                               {checkPartitionCount});

            if (itIsEnough) {
                return;
            }
        }
    }

    Y_UNIT_TEST(Merge1KShards) {
        TTestBasicRuntime runtime;

        TTestEnvOptions opts;
        opts.EnableBackgroundCompaction(false);
        opts.DisableStatsBatching(true);

        TTestEnv env(runtime, opts);

        ui64 txId = 100;
        runtime.SetDispatchedEventsLimit(10'000'000);

        NDataShard::gDbStatsReportInterval = TDuration::MilliSeconds(1);

        //runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_CRIT);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_CRIT);


        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                        Name: "Table"
                        Columns { Name: "key"       Type: "Uint64"}
                        Columns { Name: "value"      Type: "Utf8"}
                        KeyColumnNames: ["key"]
                        UniformPartitionsCount: 1000
                        )");
        env.TestWaitNotification(runtime, txId);

        {
            TVector<THolder<IEventHandle>> suppressed;
            auto prevObserver = SetSuppressObserver(runtime, suppressed, TEvDataShard::TEvPeriodicTableStats::EventType);

            WaitForSuppressed(runtime, suppressed, 1000, prevObserver);
            for (auto &msg : suppressed) {
                runtime.Send(msg.Release());
            }
            suppressed.clear();
        }

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::PartitionCount(1000)});

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                        Name: "Table"
                        PartitionConfig {
                            PartitioningPolicy {
                                MinPartitionsCount: 1
                                SizeToSplit: 100500
                            }
                        }
                    )");
        env.TestWaitNotification(runtime, txId);

        {
            TVector<THolder<IEventHandle>> suppressed;
            auto prevObserver = SetSuppressObserver(runtime, suppressed, TEvDataShard::TEvPeriodicTableStats::EventType);

            WaitForSuppressed(runtime, suppressed, 5*1000, prevObserver);
            for (auto &msg : suppressed) {
                runtime.Send(msg.Release());
            }
            suppressed.clear();
        }

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::PartitionCount(1000)});


        env.TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+1000));

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::PartitionCount(1)});
    }

    Y_UNIT_TEST(Merge111Shards) {
        TTestBasicRuntime runtime;

        TTestEnvOptions opts;
        opts.EnableBackgroundCompaction(false);

        TTestEnv env(runtime, opts);

        ui64 txId = 100;

        TVector<THolder<IEventHandle>> suppressed;
        auto prevObserver = SetSuppressObserver(runtime, suppressed, TEvDataShard::TEvPeriodicTableStats::EventType);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                        Name: "Table"
                        Columns { Name: "key"       Type: "Uint64"}
                        Columns { Name: "value"      Type: "Utf8"}
                        KeyColumnNames: ["key"]
                        UniformPartitionsCount: 111
                        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::PartitionCount(111)});

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                        Name: "Table"
                        PartitionConfig {
                            PartitioningPolicy {
                                MinPartitionsCount: 1
                                SizeToSplit: 100500
                            }
                        }
                    )");
        env.TestWaitNotification(runtime, txId);

        WaitForSuppressed(runtime, suppressed, suppressed.size(), prevObserver);
        for (auto &msg : suppressed) {
            runtime.Send(msg.Release());
        }
        suppressed.clear();

        env.TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+111));
        // test requires more txids than cached at start
    }

    Y_UNIT_TEST(AutoMergeInOne) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                                Name: "Table"
                                Columns { Name: "key1"       Type: "Utf8"}
                                Columns { Name: "key2"       Type: "Uint32"}
                                Columns { Name: "Value"      Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "A" } }}}
                                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::PartitionKeys({"A", ""})});
            }

            TVector<THolder<IEventHandle>> suppressed;
            auto prevObserver = SetSuppressObserver(runtime, suppressed, TEvDataShard::TEvPeriodicTableStats::EventType);

            {
                TInactiveZone inactive(activeZone);
                TestAlterTable(runtime, ++t.TxId, "/MyRoot", R"(
                                Name: "Table"
                                PartitionConfig {
                                    PartitioningPolicy {
                                        MinPartitionsCount: 1
                                        SizeToSplit: 100500
                                    }
                                }
                            )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                               {NLs::PartitionKeys({"A", ""})});

            WaitForSuppressed(runtime, suppressed, 1, prevObserver);

            t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+1));

            TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                               {NLs::PartitionKeys({""})});

        }, true);
    }

    void TryMergeWithInflyLimit(TTestActorRuntime &runtime, TTestEnv &env, const ui64 mergeNum, const ui64 remainMergeNum, const ui64 acceptedMergeNum, ui64 &txId) {
        const ui64 shardsNum = mergeNum * 2;
        const ui64 startMergePart = mergeNum - remainMergeNum;
        TSet<ui64> txIds;
        ui64 startTxId = txId;
        for (ui64 i = startMergePart * 2; i < shardsNum; i += 2) {
            AsyncSplitTable(runtime, txId, "/MyRoot/Table",
                                Sprintf(R"(
                                    SourceTabletId: %lu
                                    SourceTabletId: %lu
                                )", TTestTxConfig::FakeHiveTablets + i, TTestTxConfig::FakeHiveTablets + i + 1));
            txIds.insert(txId++);
        }

        for (ui64 i = startTxId; i < startTxId + acceptedMergeNum ; i++)
            TestModificationResult(runtime, i, NKikimrScheme::StatusAccepted);
        for (ui64 i = startTxId + acceptedMergeNum; i < txId; i++)
            TestModificationResult(runtime, i, NKikimrScheme::StatusResourceExhausted);

        env.TestWaitNotification(runtime, txIds);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {
                            NLs::ShardsInsideDomain(mergeNum + remainMergeNum - acceptedMergeNum)
                        });
    };

    void AsyncMergeWithInflyLimit(const ui64 mergeNum, const ui64 mergeLimit) {
        const ui64 shardsNum = mergeNum * 2;
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;
        auto& appData = runtime.GetAppData();

        // set batching only by timeout
        NKikimrConfig::TSchemeShardConfig_TInFlightCounterConfig *inFlightCounter = appData.SchemeShardConfig.AddInFlightCounterConfig();
        inFlightCounter->SetType(NKikimr::NSchemeShard::ESimpleCounters::COUNTER_IN_FLIGHT_OPS_TxSplitTablePartition);
        inFlightCounter->SetInFlightLimit(mergeLimit);
        // apply config via reboot
        TActorId sender = runtime.AllocateEdgeActor();
        GracefulRestartTablet(runtime, TTestTxConfig::SchemeShard, sender);

        TestCreateTable(runtime, txId++, "/MyRoot", Sprintf(R"(
                        Name: "Table"
                        Columns { Name: "key"       Type: "Uint64"}
                        Columns { Name: "value"      Type: "Utf8"}
                        KeyColumnNames: ["key"]
                        UniformPartitionsCount: %lu
                        PartitionConfig {
                            PartitioningPolicy {
                                MinPartitionsCount: 0
                            }
                        })", shardsNum));

        env.TestWaitNotification(runtime, txId - 1);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::IsTable,
                            NLs::ShardsInsideDomain(shardsNum)});
        ui64 remainMergeNum = mergeNum;

        while (remainMergeNum > 0)
        {
            ui64 acceptedMergeNum = mergeLimit == 0
                ? remainMergeNum
                : std::min(remainMergeNum, mergeLimit);
            TryMergeWithInflyLimit(runtime, env, mergeNum, remainMergeNum, acceptedMergeNum, txId);
            remainMergeNum -= acceptedMergeNum;
        }
    }

    Y_UNIT_TEST(Make11MergeOperationsWithInflyLimit10) {
        AsyncMergeWithInflyLimit(11, 10);
    }

    Y_UNIT_TEST(Make20MergeOperationsWithInflyLimit5) {
        AsyncMergeWithInflyLimit(20, 5);
    }

    Y_UNIT_TEST(Make20MergeOperationsWithoutLimit) {
        AsyncMergeWithInflyLimit(20, 0);
    }

}

namespace {

using NFmt::TPrintableTypedCells;

TString ToSerialized(ui64 key) {
    const auto cell = TCell::Make(key);
    const TSerializedCellVec saved(TArrayRef<const TCell>(&cell, 1));
    return TString(saved.GetBuffer());
}

ui64 FromSerialized(const TString& buf) {
    TSerializedCellVec saved(buf);
    // Cerr << "TEST FromSerialized, " << TPrintableTypedCells(saved.GetCells(), {NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)}) << Endl;
    auto& cell = saved.GetCells()[0];
    // Cerr << "TEST FromSerialized, cell " << cell.IsInline() << ", " << cell.IsNull() << ", " << cell.Size() << Endl;
    return cell.IsNull() ? 0 : cell.AsValue<ui64>();
}

void HistogramAddBucket(NKikimrTableStats::THistogram& hist, ui64 key, ui64 value) {
    auto bucket = hist.AddBuckets();
    bucket->SetKey(ToSerialized(key));
    bucket->SetValue(value);
};

constexpr ui64 CpuLoadMicroseconds(const ui64 percent) {
    return percent * 1000000 / 100;
}

// const ui64 CpuLoadPercent(const ui64 microseconds) {
//     return microseconds * 100 / 1000000;
// }

// Quick and dirty simulator for cpu overload and key range splitting of datashards.
// Should be used in test runtime EventObservers.
//
// Assumed index configuration: 1 initial datashard, Uint64 key.
//
struct TLoadAndSplitSimulator {
    NKikimrTabletBase::TMetrics MetricsPatch;
    NKikimrTableStats::THistogram KeyAccessHistogramPatch;
    ui64 TableLocalPathId;

    std::map<ui64, std::pair<ui64, ui64>> DatashardsKeyRanges;
    TInstant LastSplitAckTime;
    ui64 SplitAckCount = 0;
    ui64 PeriodicTableStatsCount = 0;
    ui64 KeyAccessSampleReqCount = 0;
    ui64 SplitReqCount = 0;

    TLoadAndSplitSimulator(ui64 tableLocalPathId, ui64 initialDatashardId, ui64 targetCpuLoadPercent)
        : TableLocalPathId(tableLocalPathId)
    {
        MetricsPatch.SetCPU(CpuLoadMicroseconds(targetCpuLoadPercent));

        //NOTE: histogram must have at least 3 buckets with different keys to be able to produce split key
        // (see ydb/core/tx/schemeshard/schemeshard__table_stats_histogram.cpp, DoFindSplitKey() and ChooseSplitKeyByKeySample())
        HistogramAddBucket(KeyAccessHistogramPatch, 999998, 1000);
        HistogramAddBucket(KeyAccessHistogramPatch, 999999, 1000);
        HistogramAddBucket(KeyAccessHistogramPatch, 1000000, 1000);

        DatashardsKeyRanges[initialDatashardId] = std::make_pair(0, 1000000);

        Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
            << ", target CPU load " << targetCpuLoadPercent << "%"
            << Endl;
    }

    void ChangeEvent(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            case TEvDataShard::EvPeriodicTableStats:
                // replace real stats with the simulated ones
                {
                    auto x = reinterpret_cast<TEvDataShard::TEvPeriodicTableStats::TPtr*>(&ev);
                    auto& record = (*x).Get()->Get()->Record;

                    if (record.GetTableLocalId() != TableLocalPathId) {
                        return;
                    }

                    auto prevCPU = record.GetTabletMetrics().GetCPU();
                    record.MutableTabletMetrics()->MergeFrom(MetricsPatch);
                    auto newCPU = record.GetTabletMetrics().GetCPU();

                    Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                        << ", intercept EvPeriodicTableStats, from datashard " << record.GetDatashardId()
                        << ", patch CPU: " << prevCPU << "->" << newCPU
                        << Endl;

                    ++PeriodicTableStatsCount;
                }
                break;
            case TEvDataShard::EvGetTableStats:
                // count requests for key access samples, as they indicate consideration of performing a split
                {
                    auto x = reinterpret_cast<TEvDataShard::TEvGetTableStats::TPtr*>(&ev);
                    auto& record = (*x).Get()->Get()->Record;

                    if (record.GetTableId() != TableLocalPathId) {
                        return;
                    }

                    // Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                    //     << ", intercept EvGetTableStats" << Endl;

                    if (record.GetCollectKeySample()) {
                        ++KeyAccessSampleReqCount;
                    }
                }
                break;
            case TEvDataShard::EvGetTableStatsResult:
                // replace real key access samples with the simulated ones
                {
                    auto x = reinterpret_cast<TEvDataShard::TEvGetTableStatsResult::TPtr*>(&ev);
                    auto& record = (*x).Get()->Get()->Record;

                    if (record.GetTableLocalId() != TableLocalPathId) {
                        return;
                    }

                    record.MutableTableStats()->MutableKeyAccessSample()->CopyFrom(KeyAccessHistogramPatch);

                    auto [start, end] = DatashardsKeyRanges[record.GetDatashardId()];
                    //NOTE: zero end means infinity -- this is a final shard
                    if (end == 0) {
                        end = 1000000;
                    }
                    ui64 splitPoint = (end - start) / 2;
                    record.MutableTableStats()->MutableKeyAccessSample()->MutableBuckets(0)->SetKey(ToSerialized(splitPoint - 1));
                    record.MutableTableStats()->MutableKeyAccessSample()->MutableBuckets(1)->SetKey(ToSerialized(splitPoint));
                    record.MutableTableStats()->MutableKeyAccessSample()->MutableBuckets(2)->SetKey(ToSerialized(splitPoint + 1));

                    Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                        << ", intercept EvGetTableStatsResult, from datashard " << record.GetDatashardId()
                        << ", patch KeyAccessSample: split point " << splitPoint
                        << Endl;
                }
                break;
            case TEvDataShard::EvSplit:
                // save key ranges of the new datashards
                {
                    auto x = reinterpret_cast<TEvDataShard::TEvSplit::TPtr*>(&ev);
                    auto& record = (*x).Get()->Get()->Record;

                    // remove info for the source shard(s) that will be splitted
                    // (split will have single source range)
                    for (const auto& i : record.GetSplitDescription().GetSourceRanges()) {
                        DatashardsKeyRanges.erase(i.GetTabletID());
                    }
                    // add info for destination shards
                    for (const auto& i : record.GetSplitDescription().GetDestinationRanges()) {
                        auto& [start, end] = DatashardsKeyRanges[i.GetTabletID()];
                        start = FromSerialized(i.GetKeyRangeBegin());
                        //NOTE: empty KeyRangeEnd means infinity
                        auto keyRangeEnd = i.GetKeyRangeEnd();
                        end = (keyRangeEnd.size() > 0) ? FromSerialized(keyRangeEnd) : 0;
                    }

                    Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                        << ", intercept EvSplit, to datashard " << record.GetSplitDescription().GetSourceRanges(0).GetTabletID()
                        << ", event:"
                        << Endl
                        << record.DebugString()
                        << Endl;

                    ++SplitReqCount;
                }
                break;
            case TEvDataShard::EvSplitAck:
                // count splits
                {
                    auto x = reinterpret_cast<TEvDataShard::TEvSplitAck::TPtr*>(&ev);
                    auto& record = (*x).Get()->Get()->Record;

                    auto now = TInstant::Now();
                    auto elapsed = now - LastSplitAckTime;
                    LastSplitAckTime = now;

                    Cerr << "TEST TLoadAndSplitSimulator for table id " << TableLocalPathId
                        << ", intercept EvSplitAck, from datashard " << record.GetTabletId()
                        << ", " << elapsed << " since last split ack"
                        << Endl;

                    ++SplitAckCount;
                }
                break;
        }
    };
};

TTestEnv SetupEnv(TTestBasicRuntime &runtime) {
    TTestEnvOptions opts;
    opts.EnableBackgroundCompaction(false);

    TTestEnv env(runtime, opts);

    NDataShard::gDbStatsReportInterval = TDuration::Seconds(0);
    NDataShard::gDbStatsDataSizeResolution = 10;
    NDataShard::gDbStatsRowCountResolution = 10;

    {
        auto& appData = runtime.GetAppData();

        appData.FeatureFlags.SetEnablePersistentPartitionStats(true);

        // disable batching
        appData.SchemeShardConfig.SetStatsBatchTimeoutMs(0);
        appData.SchemeShardConfig.SetStatsMaxBatchSize(0);
    }

    runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

    // apply config changes to schemeshard via reboot
    //FIXME: make it possible to set config before initial boot
    GracefulRestartTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

    return env;
}

}  // anonymous namespace

Y_UNIT_TEST_SUITE(TSchemeShardSplitByLoad) {

    void SplitByLoad(TTestBasicRuntime &runtime, const TString &tablePath, ui32 targetCpuLoadPercent) {
        auto tableInfo = DescribePrivatePath(runtime, tablePath, true, true);
        Cerr << "TEST table initial state:" << Endl << tableInfo.DebugString() << Endl;

        const ui64 tableLocalPathId = tableInfo.GetPathDescription().GetSelf().GetPathId();
        const ui64 initialDatashardId = tableInfo.GetPathDescription().GetTablePartitions(0).GetDatashardId();

        TLoadAndSplitSimulator simulator(tableLocalPathId, initialDatashardId, targetCpuLoadPercent);

        runtime.SetObserverFunc([&simulator](TAutoPtr<IEventHandle>& event) {
            simulator.ChangeEvent(event);
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        {
            TDispatchOptions opts;
            opts.CustomFinalCondition = [&simulator]() {
                auto now = TInstant::Now();
                // Cerr << "TEST SplitByLoad, CustomFinalCondition, SplitAckCount " << simulator.SplitAckCount << ", " << (now - simulator.LastSplitAckTime) << " since last split" << Endl;
                return simulator.SplitAckCount > 0 && (now - simulator.LastSplitAckTime) > TDuration::Seconds(3);
            };
            runtime.DispatchEvents(opts/*, TDuration::Seconds(120)*/);
        }
        Cerr << "TEST SplitByLoad, splitted " << simulator.SplitAckCount << " times"
            << ", datashard count " << simulator.DatashardsKeyRanges.size()
            << Endl;
        // Cerr << "TEST SplitByLoad, PeriodicTableStats " << simulator.PeriodicTableStatsCount << Endl;
        // Cerr << "TEST SplitByLoad, KeyAccessSampleReq " << simulator.KeyAccessSampleReqCount << Endl;
        // Cerr << "TEST SplitByLoad, SplitReq " << simulator.SplitReqCount << Endl;
    }

    void NoSplitByLoad(TTestBasicRuntime &runtime, const TString &tablePath, ui32 targetCpuLoadPercent) {
        auto tableInfo = DescribePrivatePath(runtime, tablePath, true, true);
        Cerr << "TEST table initial state:" << Endl << tableInfo.DebugString() << Endl;

        const ui64 tableLocalPathId = tableInfo.GetPathDescription().GetSelf().GetPathId();
        const ui64 initialDatashardId = tableInfo.GetPathDescription().GetTablePartitions(0).GetDatashardId();

        TLoadAndSplitSimulator simulator(tableLocalPathId, initialDatashardId, targetCpuLoadPercent);

        runtime.SetObserverFunc([&simulator](TAutoPtr<IEventHandle>& event) {
            simulator.ChangeEvent(event);
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        {
            TDispatchOptions opts;
            opts.CustomFinalCondition = [&simulator]() {
                // Cerr << "TEST SplitByLoad, PeriodicTableStats " << simulator.PeriodicTableStatsCount << ", KeyAccessSampleReq " << simulator.KeyAccessSampleReqCount << Endl;
                return simulator.PeriodicTableStatsCount > 10 && simulator.KeyAccessSampleReqCount == 0;
            };
            runtime.DispatchEvents(opts, TDuration::Seconds(60));
        }
        Cerr << "TEST SplitByLoad, splitted " << simulator.SplitAckCount << " times"
            << ", datashard count " << simulator.DatashardsKeyRanges.size()
            << Endl;
        // Cerr << "TEST SplitByLoad, PeriodicTableStats " << simulator.PeriodicTableStatsCount << Endl;
        // Cerr << "TEST SplitByLoad, KeyAccessSampleReq " << simulator.KeyAccessSampleReqCount << Endl;
        // Cerr << "TEST SplitByLoad, SplitReq " << simulator.SplitReqCount << Endl;
    }

    Y_UNIT_TEST(TableSplitsUpToMaxPartitionsCount) {
        TTestBasicRuntime runtime;
        auto env = SetupEnv(runtime);

        const ui32 expectedPartitionCount = 5;
        const ui32 cpuLoadThreshold = 1;    // percents
        const ui64 cpuLoadSimulated = 100;  // percents

        const auto tableScheme = Sprintf(
            R"(
                Name: "Table"
                Columns { Name: "key"       Type: "Uint64"}
                Columns { Name: "value"     Type: "Uint64"}
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 1
                PartitionConfig {
                    PartitioningPolicy {

                        MaxPartitionsCount: %d  # replacement field for required number of partitions

                        SplitByLoadSettings: {
                            Enabled: true

                            CpuPercentageThreshold: %d  # replacement field for cpu load split threshold, percents

                        }
                    }
                }
            )",
            expectedPartitionCount,
            cpuLoadThreshold
        );

        ui64 txId = 100;
        TestCreateTable(runtime, ++txId, "/MyRoot", tableScheme);
        env.TestWaitNotification(runtime, txId);

        SplitByLoad(runtime, "/MyRoot/Table", cpuLoadSimulated);

        auto tableInfo = DescribePrivatePath(runtime, "/MyRoot/Table", true, true);
        Cerr << "TEST table final state:" << Endl << tableInfo.DebugString() << Endl;
        TestDescribeResult(tableInfo, {NLs::PartitionCount(expectedPartitionCount)});
    }

    Y_UNIT_TEST(IndexTableSplitsUpToMainTableCurrentPartitionCount) {
        TTestBasicRuntime runtime;
        auto env = SetupEnv(runtime);

        const ui32 expectedPartitionCount = 5;
        const ui32 cpuLoadThreshold = 1;    // percents
        const ui64 cpuLoadSimulated = 100;  // percents

        const auto mainTableScheme = Sprintf(
            R"(
                TableDescription {
                    Name: "Table"
                    Columns { Name: "key"       Type: "Uint64"}
                    Columns { Name: "value"     Type: "Uint64"}
                    KeyColumnNames: ["key"]

                    UniformPartitionsCount: %d  # replacement field for required number of partitions

                    PartitionConfig {
                        PartitioningPolicy {
                            MaxPartitionsCount: 10
                            SplitByLoadSettings: {
                                Enabled: true

                                CpuPercentageThreshold: %d  # replacement field for cpu load split threshold, percents

                            }
                        }
                    }
                }
                IndexDescription {
                    Name: "by-value"
                    KeyColumnNames: ["value"]
                }
            )",
            expectedPartitionCount,
            cpuLoadThreshold
        );

        ui64 txId = 100;
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", mainTableScheme);
        env.TestWaitNotification(runtime, txId);

        SplitByLoad(runtime, "/MyRoot/Table/by-value/indexImplTable", cpuLoadSimulated);

        auto tableInfo = DescribePrivatePath(runtime, "/MyRoot/Table/by-value/indexImplTable", true, true);
        Cerr << "TEST table final state:" << Endl << tableInfo.DebugString() << Endl;
        TestDescribeResult(tableInfo, {NLs::PartitionCount(expectedPartitionCount)});
    }

    Y_UNIT_TEST(IndexTableDoesNotSplitsIfDisabledByMainTable) {
        TTestBasicRuntime runtime;
        auto env = SetupEnv(runtime);

        const ui32 cpuLoadThreshold = 1;    // percents
        const ui64 cpuLoadSimulated = 100;  // percents

        const auto mainTableScheme = Sprintf(
            R"(
                TableDescription {
                    Name: "Table"
                    Columns { Name: "key"       Type: "Uint64"}
                    Columns { Name: "value"     Type: "Uint64"}
                    KeyColumnNames: ["key"]

                    UniformPartitionsCount: 5

                    PartitionConfig {
                        PartitioningPolicy {
                            MaxPartitionsCount: 10
                            SplitByLoadSettings: {
                                Enabled: false

                                CpuPercentageThreshold: %d  # replacement field for cpu load split threshold, percents

                            }
                        }
                    }
                }
                IndexDescription {
                    Name: "by-value"
                    KeyColumnNames: ["value"]
                }
            )",
            cpuLoadThreshold
        );

        ui64 txId = 100;
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", mainTableScheme);
        env.TestWaitNotification(runtime, txId);

        NoSplitByLoad(runtime, "/MyRoot/Table/by-value/indexImplTable", cpuLoadSimulated);

        auto tableInfo = DescribePrivatePath(runtime, "/MyRoot/Table/by-value/indexImplTable", true, true);
        Cerr << "TEST table final state:" << Endl << tableInfo.DebugString() << Endl;
        TestDescribeResult(tableInfo, {NLs::PartitionCount(1)});
    }
}
