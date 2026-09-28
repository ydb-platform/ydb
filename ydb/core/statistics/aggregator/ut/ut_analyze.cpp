#include <ydb/core/statistics/ut_common/ut_common.h>
#include <ydb/core/statistics/aggregator/aggregator_impl.h>

#include <ydb/library/testlib/helpers.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <ydb/core/kqp/node_service/kqp_node_service.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/scheme_board/events_internal.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>

#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/tx_helpers.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/service/service.h>

#include <util/string/cast.h>

namespace NKikimr {
namespace NStat {

struct TStatisticsAggregatorTestAccess {
    using TTraversalTick = TStatisticsAggregator::TEvPrivate::TEvScheduleTraversal;
    using TTraversalWakeup = TStatisticsAggregator::TEvPrivate::TEvScheduleForceTraversal;
};

namespace {

void CheckTableSummaryRowCount(TTestActorRuntime& runtime, const TPathId& pathId, ui64 expected) {
    auto responses = GetStatistics(runtime, pathId, EStatType::TABLE_SUMMARY, {std::nullopt});
    UNIT_ASSERT_VALUES_EQUAL(responses.size(), 1);
    UNIT_ASSERT_C(responses[0].Success, "TABLE_SUMMARY was not saved");
    UNIT_ASSERT(responses[0].TableSummary.Data);
    UNIT_ASSERT_VALUES_EQUAL(responses[0].TableSummary.Data->GetRowCount(), expected);
}

TTableInfo ResolveRowTable(TTestActorRuntime& runtime, const TString& path) {
    TTableInfo tableInfo;
    tableInfo.Path = path;
    tableInfo.PathId = ResolvePathId(runtime, path, &tableInfo.DomainKey, &tableInfo.SaTabletId);
    return tableInfo;
}

void WaitForAggregatorCommit(TTestActorRuntime& runtime, ui64 tabletId) {
    NTabletFlatScheme::TSchemeChanges scheme;
    TString error;
    UNIT_ASSERT_VALUES_EQUAL_C(
        LocalSchemeTx(runtime, tabletId, "", /*dryRun=*/true, scheme, error),
        NKikimrProto::OK, error);
}

void CheckTraversalSchedulerRate(TTestActorRuntime& runtime, ui64 tabletId, bool started = true) {
    const auto aggregator = ResolveTablet(runtime, tabletId);
    size_t ticks = 0;
    auto observer = runtime.AddObserver<TStatisticsAggregatorTestAccess::TTraversalTick>([&](auto& ev) {
        if (ev->GetRecipientRewrite() == aggregator) {
            ++ticks;
        }
    });
    const auto startedAt = runtime.GetCurrentTime();
    runtime.SimulateSleep(TDuration::Seconds(5));
    const auto elapsed = runtime.GetCurrentTime() - startedAt;
    if (started) {
        UNIT_ASSERT_GT(ticks, 0);
        UNIT_ASSERT_C(ticks <= elapsed.Seconds() + 1,
            "Expected one periodic traversal chain, got " << ticks << " ticks in " << elapsed);
    } else {
        UNIT_ASSERT_VALUES_EQUAL(ticks, 0);
    }
}

void SetAggregatorStatisticsConfig(TTestEnv& env, ui64 tabletId, bool enabled, bool background) {
    auto& runtime = *env.GetServer().GetRuntime();
    const auto& settings = env.GetServer().GetSettings();
    auto request = std::make_unique<NConsole::TEvConsole::TEvConfigNotificationRequest>();
    auto* config = request->Record.MutableConfig();
    *config->MutableFeatureFlags() = settings.FeatureFlags;
    config->MutableFeatureFlags()->SetEnableColumnStatistics(enabled);
    *config->MutableStatisticsConfig() = settings.AppConfig->GetStatisticsConfig();
    config->MutableStatisticsConfig()->SetEnableBackgroundColumnStatsCollection(background);
    const auto sender = runtime.AllocateEdgeActor();
    runtime.SendToPipe(tabletId, sender, request.release());
    UNIT_ASSERT(runtime.GrabEdgeEventRethrow<NConsole::TEvConsole::TEvConfigNotificationResponse>(sender));
}

} // namespace

Y_UNIT_TEST_SUITE(AnalyzeStatistics) {

    Y_UNIT_TEST_TWIN(Analyze, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTable(env, "Database", "Table", ColumnShard);

        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});

        ValidateStatistics(runtime, tableInfo.PathId);
    }

    Y_UNIT_TEST_TWIN(AnalyzeMultiColumnStatistics, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareMultiColumnTable(env, "Database", "Table", ColumnShard);

        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});

        CheckMultiColumnStatisticsProbes(env, runtime, tableInfo.PathId, {2, 3});
    }

    Y_UNIT_TEST_TWIN(AnalyzeEqHeightHistogram, ColumnShard) {
        TTestEnv env(1, 1, false, [](Tests::TServerSettings& settings) {
            auto* cfg = settings.AppConfig->MutableStatisticsConfig();
            cfg->SetAnalyzeCollectPrimaryKeyHistogram(true);
            if constexpr (ColumnShard) {
                cfg->SetAnalyzeColumnTableWholeTableScanMaxBytes(0);
            } else {
                cfg->SetAnalyzeRowTableWholeTableScanMaxBytes(0);
            }
        });
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareMultiColumnTable(env, "Database", "Table", ColumnShard);

        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});

        // Auto-PK over Key. DataShard: exact (PARTITION_AT_KEYS). ColumnShard: within 2N/(f·B).
        const bool optionalKey = !ColumnShard;
        std::vector<TString> sourceKeys;
        sourceKeys.reserve(ColumnTableRowsNumber);
        for (ui64 i = 0; i < ColumnTableRowsNumber; ++i) {
            sourceKeys.push_back(MakeUint64PresortKey(i, optionalKey));
        }
        std::vector<TEqHeightHistogramProbe> probes;
        if (optionalKey) {
            probes.push_back({MakeNullPresortKey(), 0});
        }
        probes.push_back({MakeUint64PresortKey(ColumnTableRowsNumber - 1, optionalKey), ColumnTableRowsNumber});
        probes.push_back({MakeUint64PresortKey(ColumnTableRowsNumber, optionalKey), ColumnTableRowsNumber});
        CheckEqHeightHistogram(runtime, tableInfo.PathId, {1},
            ColumnTableRowsNumber,
            /*expectedMinBuckets=*/4,
            probes,
            ColumnShard ? std::optional<bool>() : std::optional<bool>(true),
            sourceKeys,
            ColumnShard ? std::optional<ui64>(EqHeightDesignRankErrorBound()) : std::nullopt);
    }

    Y_UNIT_TEST_TWIN(AnalyzeMultiColumnEqHeightHistogram, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareMultiColumnEqHeightTable(env, "Database", "Table", ColumnShard);

        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});

        std::vector<TString> sourceKeys;
        sourceKeys.reserve(ColumnTableRowsNumber);
        constexpr bool optionalValues = true; // Value1/Value2 are nullable on both table types.
        for (ui64 i = 0; i < ColumnTableRowsNumber; ++i) {
            sourceKeys.push_back(MakeStringTuplePresortKey(
                {ToString(i % 10), ToString(i % 20)}, optionalValues));
        }
        std::vector<TEqHeightHistogramProbe> probes = {
            {MakeNullStringTuplePresortKey(2), 0},
            {MakeStringTuplePresortKey({"zz", "zz"}, optionalValues), ColumnTableRowsNumber},
        };
        CheckEqHeightHistogram(runtime, tableInfo.PathId, {2, 3},
            ColumnTableRowsNumber,
            /*expectedMinBuckets=*/1,
            probes,
            /*requireExact=*/std::nullopt,
            sourceKeys,
            EqHeightDesignRankErrorBound());
    }

    Y_UNIT_TEST_TWIN(AnalyzeAfterAddStatisticsWithStaleSchema, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto table = PrepareTable(env, "Database", "Table", ColumnShard);

        // Prime the aggregator node's cache before adding the statistic.
        const auto sender = runtime.AllocateEdgeActor(1);
        const auto navigate = [&] {
            using TNavigate = NSchemeCache::TSchemeCacheNavigate;
            auto request = std::make_unique<TNavigate>();
            request->DatabaseName = "/Root/Database";
            auto& entry = request->ResultSet.emplace_back();
            entry.TableId = table.PathId;
            entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
            entry.Operation = TNavigate::OpTable;
            runtime.Send(MakeSchemeCacheID(), sender,
                new TEvTxProxySchemeCache::TEvNavigateKeySet(request.release()), 1);
            auto response = runtime.GrabEdgeEventRethrow<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(sender);
            const auto& result = response->Get()->Request->ResultSet.front();
            UNIT_ASSERT_VALUES_EQUAL(result.Status, TNavigate::EStatus::Ok);
            return result.MultiColumnStatistics.size();
        };
        UNIT_ASSERT_VALUES_EQUAL(navigate(), 0);

        // Let ALTER finish, but delay its notification to the aggregator's cache.
        TBlockEvents<TSchemeBoardEvents::TEvNotifyUpdate> updates(runtime, [&](auto& ev) {
            return ev->Get()->PathId == table.PathId
                && ev->GetRecipientRewrite().NodeId() == runtime.GetNodeId(1);
        });
        ExecuteYqlScript(env, R"(
            ALTER TABLE `/Root/Database/Table`
            ADD STATISTICS stat1 ON (Value) WITH (EQ_HEIGHT_HISTOGRAM);
        )");
        UNIT_ASSERT_VALUES_EQUAL(navigate(), 0);

        // A synchronized navigation must receive the pending notification before
        // its sync response, as it would without our artificial delivery delay.
        auto sync = runtime.AddObserver<NSchemeBoard::NInternalEvents::TEvSyncVersionRequest>([&](auto& ev) {
            const auto& record = ev->Get()->Record;
            const TPathId pathId(record.GetPathOwnerId(), record.GetLocalPathId());
            if (ev->Sender.NodeId() == runtime.GetNodeId(1)
                && (pathId == table.PathId || record.GetPath() == table.Path)) {
                updates.Stop().Unblock();
            }
        });

        Analyze(runtime, table.SaTabletId, {table.PathId});
        updates.Stop().Unblock();
        CheckEqHeightHistogram(runtime, table.PathId, {2}, ColumnTableRowsNumber, 1);
    }

    Y_UNIT_TEST_TWIN(AnalyzeAfterAlteringTableStatistics, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTable(env, "Database", "Table", ColumnShard);

        const auto navigateStatistics = [&](ui32 nodeIndex) {
            using TNavigate = NSchemeCache::TSchemeCacheNavigate;
            auto request = std::make_unique<TNavigate>();
            request->DatabaseName = "/Root/Database";
            auto& entry = request->ResultSet.emplace_back();
            entry.TableId = tableInfo.PathId;
            entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
            entry.Operation = TNavigate::OpTable;
            entry.SyncVersion = true;
            const auto sender = runtime.AllocateEdgeActor(nodeIndex);
            runtime.Send(MakeSchemeCacheID(), sender,
                new TEvTxProxySchemeCache::TEvNavigateKeySet(request.release()), nodeIndex);
            const auto response = runtime.GrabEdgeEventRethrow<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(sender);
            UNIT_ASSERT(response);
            const auto& entries = response->Get()->Request->ResultSet;
            UNIT_ASSERT_VALUES_EQUAL(entries.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(entries[0].Status, TNavigate::EStatus::Ok);
            return entries[0].MultiColumnStatistics;
        };

        // Warm both the query node's cache and the aggregator node's cache before ALTER.
        for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
            UNIT_ASSERT(navigateStatistics(nodeIndex).empty());
        }

        ExecuteYqlScript(env, R"(
            ALTER TABLE `/Root/Database/Table`
            ADD STATISTICS multi_stat ON (Key, Value) WITH (EQ_HEIGHT_HISTOGRAM);
        )");
        ExecuteYqlScript(env, "ANALYZE `/Root/Database/Table`;");

        CheckEqHeightHistogram(runtime, tableInfo.PathId, {1, 2}, ColumnTableRowsNumber, 1);
        UNIT_ASSERT_VALUES_EQUAL(CountStatisticsV2Rows(env, "Database", tableInfo.PathId,
            EStatType::EQ_HEIGHT_HISTOGRAM, "1,2"), 1);
        for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
            const auto statistics = navigateStatistics(nodeIndex);
            UNIT_ASSERT_VALUES_EQUAL(statistics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(statistics[0].GetName(), "multi_stat");
        }

        ExecuteYqlScript(env, "ALTER TABLE `/Root/Database/Table` DROP STATISTICS multi_stat;");
        for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
            UNIT_ASSERT_C(navigateStatistics(nodeIndex).empty(), "Dropped statistics remain in the scheme cache");
        }

        ExecuteYqlScript(env, R"(
            ALTER TABLE `/Root/Database/Table`
            ADD STATISTICS multi_stat ON (Value, Key) WITH (EQ_HEIGHT_HISTOGRAM);
        )");
        ExecuteYqlScript(env, "ANALYZE `/Root/Database/Table`;");

        CheckEqHeightHistogram(runtime, tableInfo.PathId, {2, 1}, ColumnTableRowsNumber, 1);
        UNIT_ASSERT_VALUES_EQUAL(CountStatisticsV2Rows(env, "Database", tableInfo.PathId,
            EStatType::EQ_HEIGHT_HISTOGRAM, "2,1"), 1);
        for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
            const auto statistics = navigateStatistics(nodeIndex);
            UNIT_ASSERT_VALUES_EQUAL(statistics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(statistics[0].GetName(), "multi_stat");
            UNIT_ASSERT_VALUES_EQUAL(statistics[0].ColumnNamesSize(), 2);
            UNIT_ASSERT_VALUES_EQUAL(statistics[0].GetColumnNames(0), "Value");
            UNIT_ASSERT_VALUES_EQUAL(statistics[0].GetColumnNames(1), "Key");
        }
    }

    Y_UNIT_TEST_TWIN(AnalyzeEqHeightHistogramDeclaredPkDedup, ColumnShard) {
        // Declared WITH (EQ_HEIGHT_HISTOGRAM) on the PK plus the auto-PK config
        // must produce exactly one stored histogram (the declared descriptor wins).
        TTestEnv env(1, 1, false, [](Tests::TServerSettings& settings) {
            settings.AppConfig->MutableStatisticsConfig()->SetAnalyzeCollectPrimaryKeyHistogram(true);
        });
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareDeclaredPkEqHeightTable(env, "Database", "Table", ColumnShard);

        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});

        const bool optionalKey = !ColumnShard;
        std::vector<TString> sourceKeys;
        sourceKeys.reserve(ColumnTableRowsNumber);
        for (ui64 i = 0; i < ColumnTableRowsNumber; ++i) {
            sourceKeys.push_back(MakeUint64PresortKey(i, optionalKey));
        }
        CheckEqHeightHistogram(runtime, tableInfo.PathId, {1},
            ColumnTableRowsNumber,
            /*expectedMinBuckets=*/4,
            std::nullopt,
            ColumnShard ? std::optional<bool>() : std::optional<bool>(true),
            sourceKeys,
            ColumnShard ? std::optional<ui64>(EqHeightDesignRankErrorBound()) : std::nullopt);
        UNIT_ASSERT_VALUES_EQUAL(
            CountStatisticsV2Rows(env, "Database", tableInfo.PathId, EStatType::EQ_HEIGHT_HISTOGRAM, "1"),
            1);
    }

    Y_UNIT_TEST_TWIN(AnalyzeEqHeightHistogramCompositePk, ColumnShard) {
        TTestEnv env(1, 1, false, [](Tests::TServerSettings& settings) {
            settings.AppConfig->MutableStatisticsConfig()->SetAnalyzeCollectPrimaryKeyHistogram(true);
        });
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareCompositePkTable(env, "Database", "Table", ColumnShard);

        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});

        const bool optionalKey = !ColumnShard;
        std::vector<TString> sourceKeys;
        sourceKeys.reserve(ColumnTableRowsNumber);
        for (ui64 i = 0; i < ColumnTableRowsNumber; ++i) {
            sourceKeys.push_back(MakeUint64StringPresortKey(i, ToString(i % 10), optionalKey));
        }
        CheckEqHeightHistogram(runtime, tableInfo.PathId, {1, 2},
            ColumnTableRowsNumber,
            /*expectedMinBuckets=*/4,
            std::nullopt,
            ColumnShard ? std::optional<bool>() : std::optional<bool>(true),
            sourceKeys,
            ColumnShard ? std::optional<ui64>(EqHeightDesignRankErrorBound()) : std::nullopt);
    }

    Y_UNIT_TEST_TWIN(AnalyzeEqHeightHistogramJsonRejectedAtDdl, ColumnShard) {
        // Declared EQ_HEIGHT_HISTOGRAM on Json must fail at CREATE, not at ANALYZE.
        TTestEnv env(1, 1);
        CreateDatabase(env, "Database");

        const char* script = ColumnShard
            ? R"(
                CREATE TABLE `Root/Database/Table` (
                    Key Uint64 NOT NULL,
                    Js Json,
                    PRIMARY KEY (Key),
                    STATISTICS js_hist ON (Js) WITH (EQ_HEIGHT_HISTOGRAM)
                )
                PARTITION BY HASH(Key)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4);
            )"
            : R"(
                CREATE TABLE `Root/Database/Table` (
                    Key Uint64,
                    Js Json,
                    PRIMARY KEY (Key),
                    STATISTICS js_hist ON (Js) WITH (EQ_HEIGHT_HISTOGRAM)
                )
                WITH ( UNIFORM_PARTITIONS = 4 );
            )";

        auto status = ExecuteYqlScript(env, script, /*mustSucceed=*/false);
        UNIT_ASSERT_VALUES_UNEQUAL_C(status, Ydb::StatusIds::SUCCESS,
            "EQ_HEIGHT_HISTOGRAM on Json must be rejected at DDL");
    }

    Y_UNIT_TEST_TWIN(AnalyzeEqHeightHistogramJsonColumnOk, ColumnShard) {
        // A Json column on the table must not prevent ANALYZE or a histogram
        // over a different, encodable column.
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");

        if constexpr (ColumnShard) {
            ExecuteYqlScript(env, R"(
                CREATE TABLE `Root/Database/Table` (
                    Key Uint64 NOT NULL,
                    Js Json,
                    Value String,
                    PRIMARY KEY (Key),
                    STATISTICS val_hist ON (Value) WITH (EQ_HEIGHT_HISTOGRAM)
                )
                PARTITION BY HASH(Key)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4);
            )");
            runtime.SimulateSleep(TDuration::Seconds(1));
        } else {
            ExecuteYqlScript(env, R"(
                CREATE TABLE `Root/Database/Table` (
                    Key Uint64,
                    Js Json,
                    Value String,
                    PRIMARY KEY (Key),
                    STATISTICS val_hist ON (Value) WITH (EQ_HEIGHT_HISTOGRAM)
                )
                WITH ( UNIFORM_PARTITIONS = 4 );
            )");
        }

        TTableInfo tableInfo;
        tableInfo.Path = "/Root/Database/Table";
        tableInfo.PathId = ResolvePathId(
            runtime, tableInfo.Path, &tableInfo.DomainKey, &tableInfo.SaTabletId);

        InsertDataIntoTable(env, "Database", "Table", ColumnTableRowsNumber, {
            {
                .Name = "Js",
                .TypeId = NScheme::NTypeIds::Json,
                .AddValue = [](ui64 /*key*/, Ydb::Value& row) {
                    row.add_items()->set_text_value("{}");
                },
            },
            {
                .Name = "Value",
                .TypeId = NScheme::NTypeIds::String,
                .AddValue = [](ui64 key, Ydb::Value& row) {
                    row.add_items()->set_bytes_value(ToString(key % 10));
                },
            },
        });

        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});

        auto responses = GetStatisticsMultiColumn(
            runtime, tableInfo.PathId, EStatType::EQ_HEIGHT_HISTOGRAM, {3});
        UNIT_ASSERT_VALUES_EQUAL(responses.size(), 1);
        UNIT_ASSERT(responses[0].Success);
        UNIT_ASSERT(responses[0].EqHeightHistogram.Data);
    }

    Y_UNIT_TEST_TWIN(AnalyzeEqHeightHistogramConfigOff, ColumnShard) {
        // When AnalyzeCollectPrimaryKeyHistogram is false (the default),
        // the auto-PK eq-height histogram must NOT be collected.
        TTestEnv env(1, 1); // default: AnalyzeCollectPrimaryKeyHistogram = false
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareMultiColumnTable(env, "Database", "Table", ColumnShard);

        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});

        // The PK eq-height histogram should not be present.
        // GetStatistics for EQ_HEIGHT_HISTOGRAM on the PK column (tag 1) should
        // return an empty response (no histogram stored), which is indicated by
        // Success = false (see CheckCountMinSketch for the same convention).
        auto responses = GetStatisticsMultiColumn(runtime, tableInfo.PathId, EStatType::EQ_HEIGHT_HISTOGRAM, {1});
        UNIT_ASSERT_VALUES_EQUAL(responses.size(), 1);
        for (const auto& resp : responses) {
            UNIT_ASSERT_C(!resp.Success,
                          "EQ_HEIGHT_HISTOGRAM should not be collected when "
                          "AnalyzeCollectPrimaryKeyHistogram is false");
            UNIT_ASSERT_C(!resp.EqHeightHistogram.Data,
                          "EQ_HEIGHT_HISTOGRAM should not be collected when "
                          "AnalyzeCollectPrimaryKeyHistogram is false");
        }
    }

    Y_UNIT_TEST_TWIN(AnalyzeEmptyTableEqHeightHistogram, ColumnShard) {
        // With AnalyzeCollectPrimaryKeyHistogram enabled, an empty table
        // should produce no eq-height histogram: Finalize() returns nullopt
        // when TotalCount == 0, so no histogram is stored.
        TTestEnv env(1, 1, false, [](Tests::TServerSettings& settings) {
            settings.AppConfig->MutableStatisticsConfig()->SetAnalyzeCollectPrimaryKeyHistogram(true);
        });
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = CreateEmptyTable(env, "Database", "Table", ColumnShard);

        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});

        // No eq-height histogram should be stored for an empty table.
        auto responses = GetStatisticsMultiColumn(runtime, tableInfo.PathId, EStatType::EQ_HEIGHT_HISTOGRAM, {1});
        UNIT_ASSERT_VALUES_EQUAL(responses.size(), 1);
        for (const auto& resp : responses) {
            UNIT_ASSERT_C(!resp.Success,
                          "EQ_HEIGHT_HISTOGRAM should not be collected for an empty table");
            UNIT_ASSERT_C(!resp.EqHeightHistogram.Data,
                          "EQ_HEIGHT_HISTOGRAM should not be collected for an empty table");
        }
    }

    Y_UNIT_TEST_TWIN(AnalyzeEqHeightHistogramWithoutWith, ColumnShard) {
        // STATISTICS without WITH: collect CMS and EQ_HEIGHT (auto-PK off).
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareMultiColumnAllTypesTable(env, "Database", "Table", ColumnShard);

        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});

        CheckMultiColumnStatisticsProbes(env, runtime, tableInfo.PathId, {2, 3});

        std::vector<TString> sourceKeys;
        sourceKeys.reserve(ColumnTableRowsNumber);
        constexpr bool optionalValues = true;
        for (ui64 i = 0; i < ColumnTableRowsNumber; ++i) {
            sourceKeys.push_back(MakeStringTuplePresortKey(
                {ToString(i % 10), ToString(i % 20)}, optionalValues));
        }
        std::vector<TEqHeightHistogramProbe> probes = {
            {MakeNullStringTuplePresortKey(2), 0},
            {MakeStringTuplePresortKey({"zz", "zz"}, optionalValues), ColumnTableRowsNumber},
        };
        CheckEqHeightHistogram(runtime, tableInfo.PathId, {2, 3},
            ColumnTableRowsNumber,
            /*expectedMinBuckets=*/1,
            probes,
            /*requireExact=*/std::nullopt,
            sourceKeys,
            EqHeightDesignRankErrorBound());
    }

    Y_UNIT_TEST_TWIN(AnalyzeEqHeightHistogramWithoutWithOnPk, ColumnShard) {
        // STATISTICS on PK with no WITH, auto-PK off: still collect the PK histogram.
        TTestEnv env(1, 1); // AnalyzeCollectPrimaryKeyHistogram = false
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareDeclaredPkAllTypesTable(env, "Database", "Table", ColumnShard);

        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});

        const bool optionalKey = !ColumnShard;
        std::vector<TString> sourceKeys;
        sourceKeys.reserve(ColumnTableRowsNumber);
        for (ui64 i = 0; i < ColumnTableRowsNumber; ++i) {
            sourceKeys.push_back(MakeUint64PresortKey(i, optionalKey));
        }
        CheckEqHeightHistogram(runtime, tableInfo.PathId, {1},
            ColumnTableRowsNumber,
            /*expectedMinBuckets=*/4,
            std::nullopt,
            ColumnShard ? std::optional<bool>() : std::optional<bool>(true),
            sourceKeys,
            ColumnShard ? std::optional<ui64>(EqHeightDesignRankErrorBound()) : std::nullopt);
        UNIT_ASSERT_VALUES_EQUAL(
            CountStatisticsV2Rows(env, "Database", tableInfo.PathId, EStatType::EQ_HEIGHT_HISTOGRAM, "1"),
            1);
    }

    Y_UNIT_TEST_TWIN(AnalyzeTwoTables, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto table1 = PrepareTable(env, "Database", "Table1", ColumnShard);
        const auto table2 = PrepareTable(env, "Database", "Table2", ColumnShard);

        Analyze(runtime, table1.SaTabletId, {table1.PathId, table2.PathId});

        ValidateStatistics(runtime, table1.PathId);
        ValidateStatistics(runtime, table2.PathId);
    }

    Y_UNIT_TEST_TWIN(AnalyzeEmptyTable, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = CreateEmptyTable(env, "Database", "Table", ColumnShard);

        for (size_t i = 0; i < 10; ++i) {
            Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId}, TStringBuilder() << "operation" << i);
        }
        CheckTraversalSchedulerRate(runtime, tableInfo.SaTabletId);

        // An empty table produces no statistics.
        std::vector<TCountMinSketchProbes> expected = {
            { .Tag = 1, .Probes = std::nullopt },
            { .Tag = 2, .Probes = std::nullopt },
        };
        CheckCountMinSketch(runtime, tableInfo.PathId, expected);
    }

    Y_UNIT_TEST_TWIN(AnalyzeServerless, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Shared", 1, true);
        CreateServerlessDatabase(env, "Database", "/Root/Shared");
        const auto tableInfo = PrepareTable(env, "Database", "Table", ColumnShard);

        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId}, "operationId", "/Root/Database");

        ValidateStatistics(runtime, tableInfo.PathId);
    }

    Y_UNIT_TEST_TWIN(AnalyzeUsesBatchPool, ColumnShard) {
        TTestEnv env(1, 1, ColumnShard, [](Tests::TServerSettings& settings) {
            if constexpr (ColumnShard) {
                using TExecutor = NKikimrConfig::TActorSystemConfig::TExecutor;
                auto& actorSystemConfig = *settings.AppConfig->MutableActorSystemConfig();
                actorSystemConfig.ClearExecutor();

                const auto addPool = [&](const TString& name, const TExecutor::EType type) {
                    auto& executor = *actorSystemConfig.AddExecutor();
                    executor.SetType(type);
                    executor.SetName(name);
                    executor.SetThreads(1);
                    if (type == TExecutor::BASIC) {
                        executor.SetSpinThreshold(1);
                    }
                };

                addPool("System", TExecutor::BASIC);
                actorSystemConfig.SetSysExecutor(0);
                addPool("User", TExecutor::BASIC);
                actorSystemConfig.SetUserExecutor(1);
                addPool("Batch", TExecutor::BASIC);
                actorSystemConfig.SetBatchExecutor(2);
                addPool("IO", TExecutor::IO);
                actorSystemConfig.SetIoExecutor(3);
            }
        });
        auto& runtime = *env.GetServer().GetRuntime();
        if (ColumnShard) {
            for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
                UNIT_ASSERT_VALUES_UNEQUAL(
                    runtime.GetAppData(nodeIndex).UserPoolId, runtime.GetAppData(nodeIndex).BatchPoolId);
            }
        }
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTable(env, "Database", "Table", ColumnShard);

        size_t taskRequests = 0;
        auto tasksObserver = runtime.AddObserver<NKqp::TEvKqpNode::TEvStartKqpTasksRequest>([&](auto& ev) {
            UNIT_ASSERT(ev->Get()->Record.GetUseBatchPool());
            ++taskRequests;
        });
        size_t scans = 0;
        auto scansObserver = runtime.AddObserver<TEvDataShard::TEvKqpScan>([&](auto& ev) {
            UNIT_ASSERT(ev->Get()->Record.GetUseBatchPool());
            ++scans;
        });

        const auto getReceivedConveyorTasks = [&](const ui32 poolId) {
            ui64 result = 0;
            for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
                auto counters = runtime.GetAppData(nodeIndex).Counters
                    ->GetSubgroup("actor_system_pool_id", ::ToString(poolId))
                    ->GetSubgroup("module_id", "COMPOSITE_CONVEYOR");
                if (const auto histogram = counters->FindHistogram("Histogram/ReceiveTask/Duration/Us")) {
                    const auto snapshot = histogram->Snapshot();
                    for (size_t i = 0; i < snapshot->Count(); ++i) {
                        result += snapshot->Value(i);
                    }
                }
            }
            return result;
        };
        const ui32 batchPoolId = runtime.GetAppData().BatchPoolId;
        const ui64 batchTasksBeforeAnalyze = ColumnShard ? getReceivedConveyorTasks(batchPoolId) : 0;

        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});

        if (ColumnShard) {
            UNIT_ASSERT_GT(getReceivedConveyorTasks(batchPoolId), batchTasksBeforeAnalyze);
        } else {
            UNIT_ASSERT_GT(taskRequests, 0);
            UNIT_ASSERT_GT(scans, 0);
        }
    }

    Y_UNIT_TEST_TWIN(QueryDoesNotUseBatchPool, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        PrepareTable(env, "Database", "Table", ColumnShard);

        size_t scans = 0;
        auto scansObserver = runtime.AddObserver<TEvDataShard::TEvKqpScan>([&](auto& ev) {
            UNIT_ASSERT(!ev->Get()->Record.GetUseBatchPool());
            ++scans;
        });
        THashSet<TActorId> userConveyorServices;
        THashSet<TActorId> batchConveyorServices;
        for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
            const auto nodeId = runtime.GetNodeId(nodeIndex);
            userConveyorServices.emplace(NConveyorComposite::TServiceOperator::MakeServiceId(nodeId, false));
            batchConveyorServices.emplace(NConveyorComposite::TServiceOperator::MakeServiceId(nodeId, true));
        }
        THashSet<ui64> userScanProcesses;
        THashSet<ui64> batchScanProcesses;
        auto conveyorProcessesObserver = runtime.AddObserver<NConveyorComposite::TEvExecution::TEvRegisterProcess>([&](auto& ev) {
            if (ev->Get()->GetCategory() != NConveyorComposite::ESpecialTaskCategory::Scan) {
                return;
            }
            if (userConveyorServices.contains(ev->Recipient)) {
                userScanProcesses.emplace(ev->Get()->GetInternalProcessId());
            } else if (batchConveyorServices.contains(ev->Recipient)) {
                batchScanProcesses.emplace(ev->Get()->GetInternalProcessId());
            }
        });
        size_t userScanTasks = 0;
        size_t batchScanTasks = 0;
        auto conveyorTasksObserver = runtime.AddObserver<NConveyorComposite::TEvExecution::TEvNewTask>([&](auto& ev) {
            if (ev->Get()->GetCategory() != NConveyorComposite::ESpecialTaskCategory::Scan) {
                return;
            }
            if (userScanProcesses.contains(ev->Get()->GetInternalProcessId())) {
                UNIT_ASSERT(userConveyorServices.contains(ev->Recipient));
                ++userScanTasks;
            } else if (batchScanProcesses.contains(ev->Get()->GetInternalProcessId())) {
                UNIT_ASSERT(batchConveyorServices.contains(ev->Recipient));
                ++batchScanTasks;
            }
        });

        ExecuteYqlScript(env, "SELECT COUNT(*) FROM `Root/Database/Table`;");

        UNIT_ASSERT_GT(scans, 0);
        if (ColumnShard) {
            UNIT_ASSERT_GT(userScanProcesses.size(), 0);
            UNIT_ASSERT_GT(userScanTasks, 0);
            UNIT_ASSERT_VALUES_EQUAL(batchScanProcesses.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(batchScanTasks, 0);
        }
    }

    Y_UNIT_TEST_TWIN(AnalyzeSpecificColumns, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTable(env, "Database", "Table", ColumnShard);

        Analyze(runtime, tableInfo.SaTabletId, {{tableInfo.PathId, {1, 2}}});

        ValidateStatistics(runtime, tableInfo.PathId);
    }

    Y_UNIT_TEST_TWIN(AnalyzeStatus, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        TBlockEvents<TEvStatistics::TEvSaveStatisticsQueryResponse> block(runtime);
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTable(env, "Database", "Table", ColumnShard);

        const TString operationId = "operationId";
        AnalyzeStatus(runtime, sender, tableInfo.SaTabletId, operationId, NKikimrStat::TEvAnalyzeStatusResponse::STATUS_NO_OPERATION);

        auto analyzeRequest = MakeAnalyzeRequest({{tableInfo.PathId, {1, 2}}}, operationId);
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest.release());

        runtime.WaitFor("TEvSaveStatisticsQueryResponse", [&]{ return block.size(); });

        AnalyzeStatus(runtime, sender, tableInfo.SaTabletId, operationId, NKikimrStat::TEvAnalyzeStatusResponse::STATUS_IN_PROGRESS);

        // Check EvRemoteHttpInfo
        {
            auto httpRequest = std::make_unique<NActors::NMon::TEvRemoteHttpInfo>("/app?");
            runtime.SendToPipe(tableInfo.SaTabletId, sender, httpRequest.release(), 0, {});
            auto httpResponse = runtime.GrabEdgeEventRethrow<NActors::NMon::TEvRemoteHttpInfoRes>(sender);
            TString body = httpResponse->Get()->Html;
            Cerr << body << Endl;
            UNIT_ASSERT(body.size() > 500);
            UNIT_ASSERT(body.Contains("ForceTraversals: 1"));
        }

        SetAggregatorStatisticsConfig(env, tableInfo.SaTabletId, false, false);
        const auto aggregator = ResolveTablet(runtime, tableInfo.SaTabletId);
        size_t wakeups = 0;
        auto observer = runtime.AddObserver<TStatisticsAggregatorTestAccess::TTraversalWakeup>([&](auto& ev) {
            if (ev->GetRecipientRewrite() == aggregator) {
                ++wakeups;
            }
        });
        block.Stop().Unblock();

        auto analyzeResponse = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender, TDuration::Seconds(30));
        UNIT_ASSERT(analyzeResponse);
        UNIT_ASSERT_VALUES_EQUAL(analyzeResponse->Get()->Record.GetOperationId(), operationId);
        UNIT_ASSERT_VALUES_EQUAL(analyzeResponse->Get()->Record.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);

        AnalyzeStatus(runtime, sender, tableInfo.SaTabletId, operationId, NKikimrStat::TEvAnalyzeStatusResponse::STATUS_NO_OPERATION);
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(wakeups, 0);
    }

    Y_UNIT_TEST_TWIN(AnalyzeSameOperationId, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTable(env, "Database", "Table", ColumnShard);
        auto sender = runtime.AllocateEdgeActor();
        const TString operationId = "operationId";

        TBlockEvents<TEvStatistics::TEvSaveStatisticsQueryResponse> block(runtime);

        auto tabletPipe = runtime.ConnectToPipe(tableInfo.SaTabletId, sender, 0, {});

        auto analyzeRequest1 = MakeAnalyzeRequest({tableInfo.PathId}, operationId);
        runtime.SendToPipe(tabletPipe, sender, analyzeRequest1.release());

        runtime.WaitFor("TEvSaveStatisticsQueryResponse", [&]{ return block.size(); });

        for (size_t i = 0; i < 10; ++i) {
            runtime.SendToPipe(tabletPipe, sender, MakeAnalyzeRequest({tableInfo.PathId}, operationId).release());
        }
        WaitForAggregatorCommit(runtime, tableInfo.SaTabletId);

        block.Unblock();
        block.Stop();

        auto response1 = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
        UNIT_ASSERT(response1);
        UNIT_ASSERT_VALUES_EQUAL(response1->Get()->Record.GetOperationId(), operationId);
        UNIT_ASSERT_VALUES_EQUAL(response1->Get()->Record.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);
        CheckTraversalSchedulerRate(runtime, tableInfo.SaTabletId);

        auto response2 = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender, TDuration::Seconds(5));
        UNIT_ASSERT(!response2);

        // Terminal retries must not rescan.
        size_t results = 0;
        auto observer = runtime.AddObserver<TEvStatistics::TEvAnalyzeActorResult>([&](auto&) { ++results; });
        runtime.SendToPipe(tabletPipe, sender, MakeAnalyzeRequest({tableInfo.PathId}, operationId).release());
        const auto replay = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(replay->Get()->Record.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);
        CheckTraversalSchedulerRate(runtime, tableInfo.SaTabletId);
        UNIT_ASSERT_VALUES_EQUAL(results, 0);
    }

    Y_UNIT_TEST_TWIN(AnalyzeCompletionStartsQueuedWork, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto first = CreateEmptyTable(env, "Database", "First", ColumnShard);
        const auto second = CreateEmptyTable(env, "Database", "Second", ColumnShard);
        const auto queued = CreateEmptyTable(env, "Database", "Queued", ColumnShard);
        WaitForSchemeShardStatsUpdate(runtime, first.PathId.OwnerId, true);
        const auto aggregator = ResolveTablet(runtime, first.SaTabletId);
        TBlockEvents<TEvStatistics::TEvAnalyzeActorResult> firstResult(runtime, [&](auto& ev) {
            return ev->GetRecipientRewrite() == aggregator && ev->Get()->Final;
        });
        const auto sender = runtime.AllocateEdgeActor();
        runtime.SendToPipe(first.SaTabletId, sender,
            MakeAnalyzeRequest({first.PathId, second.PathId}, "operationA", "/Root/Database").release());
        runtime.WaitFor("first traversal result", [&] { return !firstResult.empty(); }, TDuration::Seconds(30));

        auto ticks = runtime.AddObserver<TStatisticsAggregatorTestAccess::TTraversalTick>([&](auto& ev) {
            if (ev->GetRecipientRewrite() == aggregator) {
                ev.Reset();
            }
        });

        // Only A's completion may start B.
        TBlockEvents<TStatisticsAggregatorTestAccess::TTraversalWakeup> wakeup(runtime, [&](auto& ev) {
            return ev->GetRecipientRewrite() == aggregator;
        });
        runtime.SendToPipe(first.SaTabletId, sender,
            MakeAnalyzeRequest({queued.PathId}, "operationB", "/Root/Database").release());
        runtime.WaitFor("submission wakeup", [&] { return !wakeup.empty(); }, TDuration::Seconds(30));
        wakeup.Stop();
        UNIT_ASSERT_VALUES_EQUAL(
            TestGetAnalyzeOp(runtime, first.SaTabletId, "/Root/Database", "operationB").GetAnalyzeOperation().GetState(),
            Ydb::Table::AnalyzeState::STATE_ENQUEUED);

        firstResult.Stop().Unblock();
        for (const TString operationId : {"operationA", "operationB"}) {
            const auto response = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(
                sender, TDuration::Seconds(30));
            UNIT_ASSERT_C(response, "Queued traversal did not complete while periodic ticks were held");
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetOperationId(), operationId);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);
        }
        CheckTableSummaryRowCount(runtime, queued.PathId, 0);
        CheckTableSummaryRowCount(runtime, second.PathId, 0);
    }

    Y_UNIT_TEST_TWIN(AnalyzeBackgroundFailuresWaitForPeriodicTick, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto table = PrepareTable(env, "Database", "Table", ColumnShard);
        // Keep the table stale after failed scans.
        WaitForRowCount(runtime, 0, table.PathId, ColumnTableRowsNumber);
        const auto aggregator = ResolveTablet(runtime, table.SaTabletId);
        const auto sender = runtime.AllocateEdgeActor();
        TBlockEvents<TEvStatistics::TEvAnalyzeActorResult> results(runtime, [&](auto& ev) {
            return ev->GetRecipientRewrite() == aggregator && ev->Get()->Final;
        });
        SetAggregatorStatisticsConfig(env, table.SaTabletId, true, true);

        for (bool queuedAnalyze : {false, true}) {
            runtime.WaitFor("background traversal result", [&] { return !results.empty(); }, TDuration::Seconds(30));
            UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);
            using TTraversalTick = TStatisticsAggregatorTestAccess::TTraversalTick;
            TTraversalTick::TPtr tick;
            auto ticks = runtime.AddObserver<TTraversalTick>([&](auto& ev) {
                if (ev->GetRecipientRewrite() == aggregator) {
                    UNIT_ASSERT(!tick);
                    tick = std::move(ev);
                }
            });
            runtime.WaitFor("periodic traversal tick", [&] { return bool(tick); }, TDuration::Seconds(30));

            if (queuedAnalyze) {
                // Drop the submission wakeup to isolate the completion wakeup.
                TBlockEvents<TStatisticsAggregatorTestAccess::TTraversalWakeup> wakeup(runtime, [&](auto& ev) {
                    return ev->GetRecipientRewrite() == aggregator;
                });
                runtime.SendToPipe(table.SaTabletId, sender,
                    MakeAnalyzeRequest({table.PathId}, "operation", "/Root/Database").release());
                runtime.WaitFor("submission wakeup", [&] { return !wakeup.empty(); }, TDuration::Seconds(30));
                UNIT_ASSERT_VALUES_EQUAL(
                    TestGetAnalyzeOp(runtime, table.SaTabletId, "/Root/Database", "operation").GetAnalyzeOperation().GetState(),
                    Ydb::Table::AnalyzeState::STATE_ENQUEUED);
            }

            auto& result = *results.front()->Get();
            UNIT_ASSERT_C(result.Status == TEvStatistics::TEvAnalyzeActorResult::EStatus::Success,
                result.Issues.ToString());
            result.Status = TEvStatistics::TEvAnalyzeActorResult::EStatus::InternalError;
            result.Issues.AddIssue(NYql::TIssue("Injected background scan failure"));
            if (queuedAnalyze) {
                results.Stop();
            }
            results.Unblock();

            if (queuedAnalyze) {
                const auto response = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(
                    sender, TDuration::Seconds(30));
                UNIT_ASSERT_C(response, "Queued ANALYZE did not advance after a failed background traversal");
                UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetOperationId(), "operation");
                UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);
                CheckTableSummaryRowCount(runtime, table.PathId, ColumnTableRowsNumber);
            } else {
                runtime.SimulateSleep(TDuration::Seconds(5));
                UNIT_ASSERT_C(results.empty(), "Failed background traversal retried while periodic ticks were held");
                ticks.Remove();
                runtime.Send(tick.Release(), aggregator.NodeId() - runtime.GetFirstNodeId(), true);
            }
        }
    }

    Y_UNIT_TEST_TWIN(TraversalSchedulerStartsOnceAfterEnable, ColumnShard) {
        TTestEnv env(1, 1, false, [](Tests::TServerSettings& settings) {
            settings.FeatureFlags.SetEnableColumnStatistics(false);
        });
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto table = CreateEmptyTable(env, "Database", "Table", ColumnShard);
        CheckTraversalSchedulerRate(runtime, table.SaTabletId, /*started=*/false);

        SetAggregatorStatisticsConfig(env, table.SaTabletId, true, false);
        CheckTraversalSchedulerRate(runtime, table.SaTabletId);
        for (size_t i = 0; i < 3; ++i) {
            SetAggregatorStatisticsConfig(env, table.SaTabletId, false, false);
            SetAggregatorStatisticsConfig(env, table.SaTabletId, true, false);
        }
        CheckTraversalSchedulerRate(runtime, table.SaTabletId);

        SetAggregatorStatisticsConfig(env, table.SaTabletId, false, false);
        // Let the pending tick stop the periodic chain.
        runtime.SimulateSleep(TDuration::Seconds(2));
        CheckTraversalSchedulerRate(runtime, table.SaTabletId, /*started=*/false);

        const auto sender = runtime.AllocateEdgeActor();
        for (double sampleRate : {1.0, 0.5}) {
            auto request = MakeAnalyzeRequest({table.PathId}, "disabledOperation");
            request->Record.MutableTables(0)->SetSampleRate(sampleRate);
            runtime.SendToPipe(table.SaTabletId, sender, request.release());
            const auto response = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender, TDuration::Seconds(5));
            UNIT_ASSERT(response);
            const auto& record = response->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(record.GetOperationId(), "disabledOperation");
            UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR);
            NYql::TIssues issues;
            NYql::IssuesFromMessage(record.GetIssues(), issues);
            UNIT_ASSERT_C(issues.ToString().Contains("Column statistics are disabled"), issues.ToString());
        }

        SetAggregatorStatisticsConfig(env, table.SaTabletId, true, false);
        CheckTraversalSchedulerRate(runtime, table.SaTabletId);
        Analyze(runtime, table.SaTabletId, {table.PathId});
    }

    Y_UNIT_TEST_TWIN(AnalyzeMultiOperationId, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTable(env, "Database", "Table", ColumnShard);
        auto sender = runtime.AllocateEdgeActor();

        auto GetOperationId = [] (size_t i) { return TStringBuilder() << "operationId" << i; };

        TBlockEvents<TEvStatistics::TEvSaveStatisticsQueryResponse> block(runtime);

        const size_t numEvents = 10;

        auto tabletPipe = runtime.ConnectToPipe(tableInfo.SaTabletId, sender, 0, {});

        for (size_t i = 0; i < numEvents; ++i) {
            auto analyzeRequest = MakeAnalyzeRequest({tableInfo.PathId}, GetOperationId(i));
            runtime.SendToPipe(tabletPipe, sender, analyzeRequest.release());
        }

        block.Unblock();
        block.Stop();

        for (size_t i = 0; i < numEvents; ++i) {
            auto response = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
            UNIT_ASSERT(response);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetOperationId(), GetOperationId(i));
        }
    }

    Y_UNIT_TEST_TWIN(AnalyzeDeadline, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTable(env, "Database", "Table", ColumnShard);
        auto sender = runtime.AllocateEdgeActor();

        size_t finals = 0;
        TActorId saId;
        auto resultWatch = runtime.AddObserver<TEvStatistics::TEvAnalyzeActorResult>(
            [&](auto& ev) {
                saId = ev->GetRecipientRewrite();
                if (ev->Get()->Final) {
                    ++finals;
                }
            });
        Y_UNUSED(resultWatch);

        TBlockEvents<TEvStatistics::TEvSaveStatisticsQueryResponse> block(runtime,
            [&](auto& ev) {
                return saId && ev->GetRecipientRewrite() == saId;
            });

        auto analyzeRequest = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest.release());

        runtime.WaitFor("op1 collected", [&]{ return finals >= 1 && !block.empty(); });
        const size_t op1Saves = block.size();

        // Expire only the first operation.
        runtime.AdvanceCurrentTime(TDuration::Hours(23));
        runtime.SendToPipe(tableInfo.SaTabletId, sender,
            MakeAnalyzeRequest({tableInfo.PathId}, "operationId2").release());
        WaitForAggregatorCommit(runtime, tableInfo.SaTabletId);
        runtime.AdvanceCurrentTime(TDuration::Hours(2));

        auto analyzeResponse = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(
            sender, TDuration::Seconds(30));
        UNIT_ASSERT(analyzeResponse);
        const auto& record = analyzeResponse->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetOperationId(), "operationId");
        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR);
        UNIT_ASSERT(!record.GetIssues().empty());

        runtime.WaitFor("op2 collected", [&]{ return finals >= 2 && block.size() > op1Saves; });

        // Releasing only op1's blocked saves must not complete op2.
        // Stop blocking first so a stale-triggered follow-up save can finish.
        block.Stop();
        block.Unblock(op1Saves);
        auto staleResponse = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(
            sender, TDuration::Seconds(3));
        UNIT_ASSERT(!staleResponse);

        block.Unblock();
        auto response2 = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(
            sender, TDuration::Seconds(30));
        UNIT_ASSERT(response2);
        UNIT_ASSERT_VALUES_EQUAL(response2->Get()->Record.GetOperationId(), "operationId2");
        UNIT_ASSERT_VALUES_EQUAL(
            response2->Get()->Record.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);
    }

    Y_UNIT_TEST_TWIN(AnalyzeCancel, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTable(env, "Database", "Table", ColumnShard);
        auto sender = runtime.AllocateEdgeActor();

        size_t finalResultsCount = 0;
        auto observer = runtime.AddObserver<TEvStatistics::TEvAnalyzeActorResult>([&](auto& ev) {
            if (ev->Get()->Final) {
                ++finalResultsCount;
            }
        });

        TBlockEvents<TEvDataShard::TEvKqpScan> block(runtime);

        auto analyzeRequest = MakeAnalyzeRequest({tableInfo.PathId});
        auto operationId = analyzeRequest->Record.GetOperationId();
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest.release());

        runtime.WaitFor("TEvKqpScan", [&]{ return !block.empty(); });

        const auto sender2 = runtime.AllocateEdgeActor();
        runtime.SendToPipe(tableInfo.SaTabletId, sender2,
            MakeAnalyzeRequest({tableInfo.PathId}, "operationId2").release());
        WaitForAggregatorCommit(runtime, tableInfo.SaTabletId);

        auto cancelRequest = MakeHolder<TEvStatistics::TEvAnalyzeCancel>();
        cancelRequest->Record.SetOperationId(operationId);
        runtime.SendToPipe(tableInfo.SaTabletId, sender, cancelRequest.Release());

        auto analyzeResponse = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
        const auto& record = analyzeResponse->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetOperationId(), "operationId");
        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_CANCELLED);
        block.Unblock();
        block.Stop();

        const auto response2 = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender2);
        UNIT_ASSERT_VALUES_EQUAL(response2->Get()->Record.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);

        // Make sure that only 1 AnalyzeActor successfully finished.
        UNIT_ASSERT_VALUES_EQUAL(finalResultsCount, 1);
    }

    Y_UNIT_TEST_TWIN(AnalyzeRebootSa, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTable(env, "Database", "Table", ColumnShard);
        auto sender = runtime.AllocateEdgeActor();
        const TString operationId = "operationId";

        size_t finalResultsCount = 0;
        auto observer = runtime.AddObserver<TEvStatistics::TEvAnalyzeActorResult>([&](auto& ev) {
            if (ev->Get()->Final) {
                ++finalResultsCount;
            }
        });

        TBlockEvents<TEvDataShard::TEvKqpScan> block(runtime);

        auto analyzeRequest1 = MakeAnalyzeRequest({tableInfo.PathId}, operationId);
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest1.release());

        runtime.WaitFor("TEvKqpScan", [&]{ return !block.empty(); });

        // The scan may start before its operation commits.
        WaitForAggregatorCommit(runtime, tableInfo.SaTabletId);
        RebootTablet(runtime, tableInfo.SaTabletId, sender);

        // After restart, the operation must still appear as IN_PROGRESS, not ENQUEUED.
        AnalyzeStatus(runtime, sender, tableInfo.SaTabletId, operationId,
            NKikimrStat::TEvAnalyzeStatusResponse::STATUS_IN_PROGRESS);

        block.Unblock();
        block.Stop();

        // Make sure that new operations can be performed
        auto analyzeRequest2 = MakeAnalyzeRequest({tableInfo.PathId}, "operationId2");
        auto sender2 = runtime.AllocateEdgeActor();
        runtime.SendToPipe(tableInfo.SaTabletId, sender2, analyzeRequest2.release());
        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender2);

        // Make sure that the old operation is performed after the reattach request
        auto analyzeRequest3 = MakeAnalyzeRequest({tableInfo.PathId}, operationId);
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest3.release());
        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);

        // Check that AnalyzeActor on the initial tablet instance got cancelled and
        // only 2 AnalyzeActors successfully finished.
        UNIT_ASSERT_VALUES_EQUAL(finalResultsCount, 2);

        ValidateStatistics(runtime, tableInfo.PathId);
        CheckTraversalSchedulerRate(runtime, tableInfo.SaTabletId);
    }

    Y_UNIT_TEST_TWIN(AnalyzeAbsentFromSchemeShardSnapshot, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTable(env, "Database", "Table", ColumnShard);

        // A new table is missing from SchemeShard snapshots until the next report.
        // ANALYZE must still succeed.
        bool omitted = false;
        auto hideTable = runtime.AddObserver<TEvStatistics::TEvSchemeShardStats>([&](auto& ev) {
            NKikimrStat::TSchemeShardStats statRecord;
            if (!statRecord.ParseFromString(ev->Get()->Record.GetStats())) {
                return;
            }
            NKikimrStat::TSchemeShardStats filtered;
            if (statRecord.HasAreAllStatsFull()) {
                filtered.SetAreAllStatsFull(statRecord.GetAreAllStatsFull());
            }
            bool omittedThisTable = false;
            for (const auto& entry : statRecord.GetEntries()) {
                if (TPathId::FromProto(entry.GetPathId()) == tableInfo.PathId) {
                    omittedThisTable = true;
                    continue;
                }
                *filtered.AddEntries() = entry;
            }
            if (!omittedThisTable) {
                return;
            }
            TString stats;
            UNIT_ASSERT(filtered.SerializeToString(&stats));
            ev->Get()->Record.SetStats(stats);
            omitted = true;
        });
        Y_UNUSED(hideTable);
        runtime.WaitFor("SchemeShard stats without the analyzed table", [&]{ return omitted; });

        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});
        ValidateStatistics(runtime, tableInfo.PathId);
    }

    Y_UNIT_TEST_TWIN(DropTableNavigateError, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTable(env, "Database", "Table", ColumnShard);

        const std::pair<EStatType, TString> storedKeys[] = {
            {EStatType::TABLE_SUMMARY, ""},
            {EStatType::SIMPLE_COLUMN, "1"},
            {EStatType::SIMPLE_COLUMN, "2"},
            {EStatType::COUNT_MIN_SKETCH, "2"},
        };

        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId}, "whilePresent");
        CheckCountMinSketch(runtime, tableInfo.PathId, {
            {.Tag = 1, .Probes = std::nullopt},
            {.Tag = 2, .Probes = {{{"1", ColumnTableRowsNumber / 10}, {"2", ColumnTableRowsNumber / 10}, {"10", 0}}}},
        });
        for (const auto& [statType, columnTags] : storedKeys) {
            UNIT_ASSERT_VALUES_EQUAL(CountStatisticsV2Rows(
                env, "Database", tableInfo.PathId, statType, columnTags), 1);
        }

        DropTable(env, "Database", "Table");

        auto result = Analyze(
            runtime, tableInfo.SaTabletId, {tableInfo.PathId},
            "operationId", {}, NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR);

        NYql::TIssues issues;
        NYql::IssuesFromMessage(result.GetIssues(), issues);
        UNIT_ASSERT_C(issues.ToString().Contains("Could not find table"), issues.ToString());
        for (const auto& [statType, columnTags] : storedKeys) {
            UNIT_ASSERT_VALUES_EQUAL(CountStatisticsV2Rows(
                env, "Database", tableInfo.PathId, statType, columnTags), 0);
        }

        std::vector<TCountMinSketchProbes> expected = {
            { .Tag = 1, .Probes = std::nullopt },
            { .Tag = 2, .Probes = std::nullopt },
        };
        CheckCountMinSketch(runtime, tableInfo.PathId, expected);
    }

    Y_UNIT_TEST_TWIN(TrickyTableAndColumnNames, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");

        if constexpr (ColumnShard) {
            ExecuteYqlScript(env, R"(
                CREATE TABLE `Root/Database/test\\Test\`test`(
                    key Uint64 NOT NULL,
                    `val-Val` Uint32,
                    PRIMARY KEY (key)
                )
                PARTITION BY HASH(key)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4);
            )");
        } else {
            ExecuteYqlScript(env, R"(
                CREATE TABLE `Root/Database/test\\Test\`test`(
                    key Uint32,
                    `val-Val` Uint32,
                    PRIMARY KEY (key)
                );
            )");
        }

        ui64 saTabletId = 0;
        auto pathId = ResolvePathId(runtime, R"(/Root/Database/test\Test`test)", nullptr, &saTabletId);
        // Check that ANALYZE succeeds with tricky table and column names.
        auto result = Analyze(runtime, saTabletId, {pathId}, "operationId");
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);
    }

    Y_UNIT_TEST_TWIN(DeleteForceTraversalUsesCorrectKey, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto table1 = PrepareTable(env, "Database", "Table1", ColumnShard);
        const auto table2 = PrepareTable(env, "Database", "Table2", ColumnShard);

        auto sender1 = runtime.AllocateEdgeActor();
        auto sender2 = runtime.AllocateEdgeActor();
        auto sender3 = runtime.AllocateEdgeActor();

        TBlockEvents<TEvStatistics::TEvSaveStatisticsQueryResponse> block(runtime);

        auto req1 = MakeAnalyzeRequest({table1.PathId}, "op1");
        runtime.SendToPipe(table1.SaTabletId, sender1, req1.release());
        runtime.WaitFor("TEvSaveStatisticsQueryResponse", [&]{ return block.size() > 0; });

        // Re-send from different sender triggers delete of the queued operation
        auto req2 = MakeAnalyzeRequest({table2.PathId}, "op2");
        runtime.SendToPipe(table1.SaTabletId, sender2, req2.release());
        auto req3 = MakeAnalyzeRequest({table2.PathId}, "op2");
        runtime.SendToPipe(table1.SaTabletId, sender3, req3.release());

        runtime.SimulateSleep(TDuration::MilliSeconds(10));
        RebootTablet(runtime, table1.SaTabletId, sender1);

        block.Unblock();
        block.Stop();

        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender3);

        // op1 must still be enqueued after reboot
        AnalyzeStatus(runtime, sender1, table1.SaTabletId, "op1",
            NKikimrStat::TEvAnalyzeStatusResponse::STATUS_ENQUEUED);
    }

    Y_UNIT_TEST_TWIN(AnalyzeRebootShard, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTable(env, "Database", "Table", ColumnShard);
        auto sender = runtime.AllocateEdgeActor();

        TBlockEvents<TEvDataShard::TEvKqpScan> block(runtime);

        auto analyzeRequest = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest.release());

        runtime.WaitFor("TEvKqpScan", [&]{ return !block.empty(); });
        RebootTablet(runtime, tableInfo.ShardIds[0], sender);
        block.Unblock();
        block.Stop();

        auto response = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);

        ValidateStatistics(runtime, tableInfo.PathId);
    }

    Y_UNIT_TEST(AnalyzeRangeScanCountsNullKeys) {
        TTestEnv env(1, 1, false, [](Tests::TServerSettings& settings) {
            settings.AppConfig->MutableStatisticsConfig()->SetAnalyzeRowTableWholeTableScanMaxBytes(0);
        });
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        ExecuteYqlScript(env, R"(
            CREATE TABLE `Root/Database/Table` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            )
            WITH (PARTITION_AT_KEYS = (10));
        )");
        ExecuteYqlScript(env, R"(
            UPSERT INTO `Root/Database/Table` (Key, Value) VALUES
                (NULL, "null"),
                (1, "one"),
                (11, "eleven");
        )");

        const auto tableInfo = ResolveRowTable(runtime, "/Root/Database/Table");
        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});
        CheckTableSummaryRowCount(runtime, tableInfo.PathId, 3);
    }

    Y_UNIT_TEST(AnalyzePgKeyFallsBackToWholeTableScan) {
        TTestEnv env(1, 1, false, [](Tests::TServerSettings& settings) {
            settings.SetEnableTablePgTypes(true);
            settings.AppConfig->MutableFeatureFlags()->SetEnableTablePgTypes(true);
            settings.AppConfig->MutableStatisticsConfig()->SetAnalyzeRowTableWholeTableScanMaxBytes(0);
        });
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        ExecuteYqlScript(env, R"(
            CREATE TABLE `Root/Database/Table` (
                Key pgint8,
                Value String,
                PRIMARY KEY (Key)
            );
        )");
        ExecuteYqlScript(env, R"(
            UPSERT INTO `Root/Database/Table` (Key, Value) VALUES
                (1pb, "one"),
                (2pb, "two");
        )");

        const auto tableInfo = ResolveRowTable(runtime, "/Root/Database/Table");
        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});
        CheckTableSummaryRowCount(runtime, tableInfo.PathId, 2);
    }
}

} // NStat
} // NKikimr
