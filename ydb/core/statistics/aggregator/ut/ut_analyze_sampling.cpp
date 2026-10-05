#include <ydb/core/statistics/ut_common/ut_common.h>
#include <ydb/core/statistics/aggregator/analyze_actor.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/core/statistics/service/service.h>
#include <ydb/core/base/request_types.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <yql/essentials/core/histogram/eq_width_histogram.h>
#include <algorithm>
#include <limits>
#include <numeric>

namespace NKikimr::NStat {
namespace {

bool IsAnalyzeScan(const NKqp::TEvKqp::TEvQueryRequest::TPtr& ev) {
    // Count the original request, before the proxy forwards it to a session.
    const auto& request = *ev->Get();
    return ev->Sender == request.GetRequestActorId()
        && request.GetRequestType() == NRequestTypes::Analyze
        && request.GetType() == NKikimrKqp::QUERY_TYPE_SQL_SCAN;
}

auto ObserveScannedShards(TTestActorRuntime& runtime, THashSet<ui64>& shards, size_t* selects = nullptr) {
    return runtime.AddObserver<NKqp::TEvKqp::TEvQueryRequest>([&, selects](auto& ev) {
        if (IsAnalyzeScan(ev)) {
            if (selects) {
                ++*selects;
            }
            const auto& query = ev->Get()->GetQuery();
            TStringBuf prefix, tablet;
            if (TStringBuf(query).TrySplit(" WITH TabletId = '", prefix, tablet)) {
                shards.insert(FromString<ui64>(tablet.Before('\'')));
            } else {
                UNIT_ASSERT_STRING_CONTAINS(query, "sampling_rate");
                UNIT_ASSERT_STRING_CONTAINS(query, "sampling_seed");
            }
        }
    });
}

void CheckSamplingMetadata(const NKikimrStat::TSamplingStatistics& sampling,
        bool columnShard, double rate, ui64 selectedUnits = 2) {
    UNIT_ASSERT_VALUES_EQUAL(sampling.GetRequestedRate(), rate);
    if (columnShard) {
        UNIT_ASSERT(sampling.GetMethod() == NKikimrStat::TSamplingStatistics::SHARD_SUBSET);
        UNIT_ASSERT_VALUES_EQUAL(sampling.GetEligibleUnits(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sampling.GetSelectedUnits(), selectedUnits);
    } else {
        UNIT_ASSERT(sampling.GetMethod() == NKikimrStat::TSamplingStatistics::PK_UNIT_BERNOULLI);
        UNIT_ASSERT(sampling.HasSeed());
        UNIT_ASSERT(!sampling.HasEligibleUnits() && !sampling.HasSelectedUnits());
    }
}

THashMap<TString, ui64> ReadExpectedFrequencies(TTestEnv& env, const TString& path, const THashSet<ui64>& shards,
        const NKikimrStat::TSamplingStatistics& sampling) {
    TVector<TString> sources;
    for (const auto shard : shards) {
        sources.push_back(TStringBuilder() << '`' << path << "` WITH TabletId = '" << shard << '\'');
    }
    if (sources.empty()) {
        sources.push_back(TStringBuilder() << '`' << path << "` WITH (sampling_rate = '"
            << sampling.GetRequestedRate() << "', sampling_seed = '" << sampling.GetSeed() << "')");
    }
    THashMap<TString, ui64> frequencies;
    for (const auto& source : sources) {
        env.RunInThreadPool([&] {
            NYdb::NTable::TTableClient client(env.GetDriver());
            auto stream = client.StreamExecuteScanQuery(
                TStringBuilder() << "SELECT Value1, COUNT(*) FROM " << source << " GROUP BY Value1;").GetValueSync();
            UNIT_ASSERT_C(stream.IsSuccess(), stream.GetIssues().ToString());
            for (;;) {
                auto part = stream.ReadNext().GetValueSync();
                if (!part.IsSuccess()) {
                    UNIT_ASSERT_C(part.EOS(), part.GetIssues().ToString());
                    break;
                }
                if (part.HasResultSet()) {
                    NYdb::TResultSetParser rows(part.ExtractResultSet());
                    while (rows.TryNextRow()) {
                        frequencies[rows.ColumnParser(0).GetOptionalString().value()] += rows.ColumnParser(1).GetUint64();
                    }
                }
            }
        });
    }
    return frequencies;
}

TResponse ReadSample(TTestActorRuntime& runtime, const TPathId& pathId, EStatType type,
        TColumnTags columns = {}) {
    auto request = std::make_unique<TEvStatistics::TEvGetStatistics>();
    request->Database = "/Root/Database";
    request->StatType = type;
    request->StatRequests.push_back({.PathId = pathId, .ColumnTags = std::move(columns), .AcceptSampledStatistics = true});
    auto sender = runtime.AllocateEdgeActor(1);
    runtime.Send(MakeStatServiceID(runtime.GetNodeId(1)), sender, request.release(), 1, true);
    auto response = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvGetStatisticsResult>(sender);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->StatResponses.size(), 1);
    return response->Get()->StatResponses.front();
}

TActorId StartSample(TTestActorRuntime& runtime, const TTableInfo& table,
        TString operation, double rate, const TVector<ui32>& columns = {}) {
    auto request = MakeAnalyzeRequest({table.PathId}, operation, "/Root/Database");
    request->Record.MutableTables(0)->SetSampleRate(rate);
    request->Record.MutableTables(0)->SetPath(table.Path);
    request->Record.MutableTables(0)->MutableColumnTags()->Assign(columns.begin(), columns.end());
    const auto sender = runtime.AllocateEdgeActor();
    runtime.SendToPipe(table.SaTabletId, sender, request.release());
    return sender;
}

NKikimrStat::TEvAnalyzeResponse Sample(TTestActorRuntime& runtime, const TTableInfo& table,
        TString operation, double rate, const TVector<ui32>& columns = {}) {
    const auto sender = StartSample(runtime, table, operation, rate, columns);
    return runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender)->Get()->Record;
}

} // namespace

Y_UNIT_TEST_SUITE(AnalyzeSampling) {
    Y_UNIT_TEST(SampleSize) {
        TVector<ui64> tablets(100);
        std::iota(tablets.begin(), tablets.end(), 1000);
        UNIT_ASSERT_VALUES_EQUAL(SelectAnalyzeSample(tablets, 0.05, 1).size(), 5);
        UNIT_ASSERT_VALUES_EQUAL(SelectAnalyzeSample(tablets, 0.07, 1).size(), 7);
        UNIT_ASSERT_VALUES_EQUAL(SelectAnalyzeSample(tablets, 0.0701, 1).size(), 8);
        tablets.resize(8);
        UNIT_ASSERT_VALUES_EQUAL(SelectAnalyzeSample(tablets, 0.05, 1).size(), 1);
        UNIT_ASSERT(SelectAnalyzeSample(tablets, 1, 1) == tablets);
        UNIT_ASSERT(SelectAnalyzeSample({}, 0.05, 1).empty());
    }

    Y_UNIT_TEST(SamplingUsesReadableShards) {
        TTestEnv env(1, 1, false);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto table = PrepareColumnTable(env, "Database", "Table", 4);
        bool updatedSharding = false;
        ui64 closedShard = 0;
        THashSet<ui64> scannedShards;
        auto scans = ObserveScannedShards(runtime, scannedShards);
        auto observer = runtime.AddObserver<TEvTxProxySchemeCache::TEvNavigateKeySetResult>([&](auto& ev) {
            for (auto& entry : ev->Get()->Request->ResultSet) {
                if (!entry.SyncVersion || entry.TableId.PathId != table.PathId || !entry.ColumnTableInfo) {
                    continue;
                }
                auto info = MakeIntrusive<NSchemeCache::TSchemeCacheNavigate::TColumnTableInfo>();
                info->Kind = entry.ColumnTableInfo->Kind;
                info->Description = entry.ColumnTableInfo->Description;
                info->OlapStoreId = entry.ColumnTableInfo->OlapStoreId;
                auto& sharding = *info->Description.MutableSharding();
                closedShard = sharding.GetColumnShards(0);
                sharding.ClearShardsInfo();
                for (size_t i = 0; i < sharding.ColumnShardsSize(); ++i) {
                    auto* shard = sharding.AddShardsInfo();
                    shard->SetTabletId(sharding.GetColumnShards(i));
                    shard->SetSequenceIdx(i);
                    shard->SetShardingVersion(0);
                    shard->SetIsOpenForRead(i != 0);
                    shard->SetIsOpenForWrite(i != 0);
                }
                entry.ColumnTableInfo = std::move(info);
                updatedSharding = true;
            }
        });
        const auto result = Sample(runtime, table, "readable-shards", 0.99);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS, result.DebugString());
        UNIT_ASSERT(updatedSharding);
        UNIT_ASSERT_VALUES_EQUAL(scannedShards.size(), 3);
        UNIT_ASSERT(!scannedShards.contains(closedShard));
        const auto sampled = ReadSample(runtime, table.PathId, EStatType::TABLE_SUMMARY);
        UNIT_ASSERT(sampled.Success && sampled.Sampling);
        UNIT_ASSERT_VALUES_EQUAL(sampled.Sampling->GetEligibleUnits(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sampled.Sampling->GetSelectedUnits(), 3);
        const auto simple = ReadSample(runtime, table.PathId, EStatType::SIMPLE_COLUMN, TColumnTags(1u));
        UNIT_ASSERT(simple.Success && simple.SimpleColumn.Data);
        UNIT_ASSERT(simple.SimpleColumn.Data->HasCountDistinct());
    }

    Y_UNIT_TEST_QUAD(CollectAndPersistStatistics, ColumnShard, SmallHistogramBudget) {
        TTestEnv env(1, 1, false, [](Tests::TServerSettings& settings) {
            auto* config = settings.AppConfig->MutableStatisticsConfig();
            config->SetAnalyzeCollectPrimaryKeyHistogram(true);
            config->SetAnalyzeHistogramOversampleFactor(4);
            if constexpr (SmallHistogramBudget) {
                config->SetAnalyzeHistogramMaxStateBytes(1);
            }
        });
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto table = PrepareMultiColumnTable(env, "Database", "Table", ColumnShard);
        THashSet<ui64> scannedShards;
        size_t selects = 0;
        auto scans = ObserveScannedShards(runtime, scannedShards, &selects);

        ui32 shardsTotal = 0;
        ui32 shardsDone = 0;
        auto progressObserver = runtime.AddObserver<TEvStatistics::TEvAnalyzeActorProgress>([&](auto& ev) {
            shardsTotal = ev->Get()->ShardsTotal;
            shardsDone = ev->Get()->ShardsDone;
        });
        const double rate = 0.5;
        TResponse sampled;
        // Unit sampling can legitimately pick all or no units. Find a partial
        // draw so ignoring the sampling hint cannot pass this test.
        for (ui32 attempt = 0; attempt < 16; ++attempt) {
            selects = 0;
            scannedShards.clear();
            const auto result = Sample(runtime, table, TStringBuilder() << "sample-" << attempt, rate);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS, result.DebugString());
            sampled = ReadSample(runtime, table.PathId, EStatType::TABLE_SUMMARY);
            UNIT_ASSERT(sampled.Success && sampled.Sampling && sampled.TableSummary.Data);
            if (sampled.Sampling->GetSampleRows() > 0 && sampled.Sampling->GetSampleRows() < ColumnTableRowsNumber) {
                break;
            }
        }
        scans.Remove();
        if constexpr (ColumnShard) {
            UNIT_ASSERT_VALUES_EQUAL(scannedShards.size(), 2);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(selects, 2);
        }

        const auto& metadata = *sampled.Sampling;
        UNIT_ASSERT(metadata.GetSampleRows() > 0 && metadata.GetSampleRows() < ColumnTableRowsNumber);
        const auto expectedFrequencies = ReadExpectedFrequencies(env, table.Path, scannedShards, metadata);
        ui64 expectedRows = 0;
        for (const auto& [_, count] : expectedFrequencies) {
            expectedRows += count;
        }
        CheckSamplingMetadata(metadata, ColumnShard, rate);
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetSampleRows(), expectedRows);
        UNIT_ASSERT_VALUES_EQUAL(sampled.TableSummary.Data->GetRowCount(), expectedRows);
        UNIT_ASSERT_VALUES_EQUAL(shardsDone, shardsTotal);

        const auto legacy = GetStatistics(runtime, table.PathId, EStatType::TABLE_SUMMARY, {std::nullopt});
        UNIT_ASSERT_VALUES_EQUAL(legacy.size(), 1);
        UNIT_ASSERT(!legacy.front().Success && !legacy.front().Sampling);

        const auto simple = ReadSample(runtime, table.PathId, EStatType::SIMPLE_COLUMN, TColumnTags(2u));
        UNIT_ASSERT(simple.Success && simple.SimpleColumn.Data && simple.Sampling);
        UNIT_ASSERT_VALUES_EQUAL(simple.SimpleColumn.Data->GetCount(), expectedRows);
        UNIT_ASSERT_VALUES_EQUAL(simple.Sampling->GetSampleRows(), expectedRows);
        UNIT_ASSERT(!simple.SimpleColumn.Data->HasCountDistinct());
        const auto cms = ReadSample(runtime, table.PathId, EStatType::COUNT_MIN_SKETCH, TColumnTags(2u));
        UNIT_ASSERT(cms.Success && cms.CountMinSketch.CountMin && cms.Sampling);
        for (ui32 value = 0; value < 10; ++value) {
            const auto key = ToString(value);
            UNIT_ASSERT_VALUES_EQUAL(cms.CountMinSketch.CountMin->Probe(key.data(), key.size()), expectedFrequencies.Value(key, 0));
        }
        const auto tuple = ReadSample(runtime, table.PathId, EStatType::COUNT_MIN_SKETCH,
            TColumnTags(std::vector<ui32>{2, 3}));
        UNIT_ASSERT(tuple.Success && tuple.CountMinSketch.CountMin);
        UNIT_ASSERT_VALUES_EQUAL(tuple.CountMinSketch.CountMin->GetElementCount(), expectedRows);
        UNIT_ASSERT(!ReadSample(runtime, table.PathId, EStatType::COUNT_MIN_SKETCH, TColumnTags(1u)).Success);
        UNIT_ASSERT_VALUES_EQUAL(ReadSample(runtime, table.PathId, EStatType::EQ_HEIGHT_HISTOGRAM, TColumnTags(1u)).Success,
            !SmallHistogramBudget);
        const auto histogram = ReadSample(runtime, table.PathId, EStatType::EQ_WIDTH_HISTOGRAM, TColumnTags(1u));
        UNIT_ASSERT(histogram.Success && histogram.EqWidthHistogram.Data && histogram.Sampling);
        UNIT_ASSERT_VALUES_EQUAL(histogram.Sampling->SerializeAsString(), metadata.SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(TEqWidthHistogramEstimator(histogram.EqWidthHistogram.Data).GetNumElements(), expectedRows);

        // A later batch can read a different set of rows. Exercise this for both
        // table types by growing the histogram range, then deleting all rows.
        for (bool deleteRows : {false, true}) {
            TBlockEvents<NKqp::TEvKqp::TEvQueryRequest> secondPass(runtime, [](const auto& ev) {
                return IsAnalyzeScan(ev) && ev->Get()->GetQuery().Contains("StatisticsInternal::EWHCreate");
            });
            const double changingRate = 1.0 - std::numeric_limits<double>::epsilon();
            const auto sender = StartSample(runtime, table, TStringBuilder() << "changed-between-passes-" << deleteRows, changingRate);
            runtime.WaitFor("second statistics pass", [&] { return !secondPass.empty(); }, TDuration::Seconds(30));
            if (deleteRows) {
                env.RunInThreadPool([&] {
                    NYdb::NQuery::TQueryClient client(env.GetDriver());
                    const auto deleted = client.ExecuteQuery(TStringBuilder() << "DELETE FROM `" << table.Path << "`;",
                        NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
                    UNIT_ASSERT_C(deleted.IsSuccess(), deleted.GetIssues().ToString());
                });
            } else {
                InsertDataIntoTable(env, "Database", "Table", 2 * ColumnTableRowsNumber, MultiColumnValueColumns());
            }
            secondPass.Stop().Unblock();
            const auto changed = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL(changed->Get()->Record.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);
            const auto firstBatch = ReadSample(runtime, table.PathId, EStatType::TABLE_SUMMARY);
            UNIT_ASSERT(firstBatch.Success && firstBatch.Sampling && firstBatch.TableSummary.Data);
            UNIT_ASSERT_VALUES_EQUAL(firstBatch.Sampling->GetSampleRows(), (deleteRows ? 2 : 1) * ColumnTableRowsNumber);
            const auto secondBatch = ReadSample(runtime, table.PathId, EStatType::COUNT_MIN_SKETCH, TColumnTags(2u));
            UNIT_ASSERT(secondBatch.Success && secondBatch.Sampling && secondBatch.CountMinSketch.CountMin);
            UNIT_ASSERT_VALUES_EQUAL(secondBatch.Sampling->GetSampleRows(), deleteRows ? 0 : 2 * ColumnTableRowsNumber);
            UNIT_ASSERT_VALUES_EQUAL(secondBatch.CountMinSketch.CountMin->GetElementCount(), secondBatch.Sampling->GetSampleRows());
            UNIT_ASSERT(!ReadSample(runtime, table.PathId, EStatType::EQ_WIDTH_HISTOGRAM, TColumnTags(1u)).Success);
        }

        // A newly unsuitable CMS must invalidate the previous sample.
        auto columns = MultiColumnValueColumns();
        columns.front().AddValue = [](ui64 key, Ydb::Value& row) {
            row.add_items()->set_bytes_value(ToString(key));
        };
        InsertDataIntoTable(env, "Database", "Table", 2 * ColumnTableRowsNumber, columns);
        UNIT_ASSERT_VALUES_EQUAL(Sample(runtime, table, "unique-values", rate).GetStatus(),
            NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);
        UNIT_ASSERT(!ReadSample(runtime, table.PathId, EStatType::COUNT_MIN_SKETCH, TColumnTags(2u)).Success);
    }

    Y_UNIT_TEST_TWIN(BackgroundTraversalDoesNotCompleteSample, ColumnShard) {
        TTestEnv env(1, 1, false, [](Tests::TServerSettings& settings) {
            auto* stats = settings.AppConfig->MutableStatisticsConfig();
            stats->SetEnableBackgroundColumnStatsCollection(true);
            stats->SetBaseStatsSendInitialDelaySeconds(3);
        });
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        TBlockEvents<TEvStatistics::TEvAnalyzeActorResult> results(runtime, [](auto& ev) {
            return ev->Get()->Final;
        });
        const auto table = PrepareMultiColumnTable(env, "Database", "Table", ColumnShard);
        runtime.WaitFor("background collection", [&] { return !results.empty(); }, TDuration::Seconds(30));

        const TString operation = "queued-sample";
        const auto sender = StartSample(runtime, table, operation, 0.5);
        AnalyzeStatus(runtime, sender, table.SaTabletId, operation,
            NKikimrStat::TEvAnalyzeStatusResponse::STATUS_ENQUEUED);

        // Keep subsequent completions blocked while checking the queued request.
        results.Unblock();
        runtime.WaitFor("background completed", [&] {
            return GetBackgroundAnalyzeCompletedCount(runtime) > 0;
        }, TDuration::Seconds(30));
        const auto status = TestGetAnalyzeOp(runtime, table.SaTabletId, "/Root/Database", operation);
        UNIT_ASSERT_C(status.GetAnalyzeOperation().GetState() != Ydb::Table::AnalyzeState::STATE_DONE,
            status.ShortDebugString());
        results.Stop().Unblock();
        const auto response = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);

        const auto sampled = ReadSample(runtime, table.PathId, EStatType::TABLE_SUMMARY);
        UNIT_ASSERT(sampled.Success && sampled.Sampling && sampled.TableSummary.Data);
        CheckSamplingMetadata(*sampled.Sampling, ColumnShard, 0.5);
        UNIT_ASSERT(sampled.Sampling->GetSampleRows() <= ColumnTableRowsNumber);
        UNIT_ASSERT_VALUES_EQUAL(sampled.TableSummary.Data->GetRowCount(), sampled.Sampling->GetSampleRows());
    }

    Y_UNIT_TEST_QUAD(PartialAnalyzePreservesOtherStatistics, Restart, ColumnShard) {
        TTestEnv env(1, 1, false, [](Tests::TServerSettings& settings) {
            settings.AppConfig->MutableStatisticsConfig()->SetAnalyzeCollectPrimaryKeyHistogram(true);
        });
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto table = PrepareMultiColumnTable(env, "Database", "Table", ColumnShard);
        UNIT_ASSERT_VALUES_EQUAL(Sample(runtime, table, "full-baseline", 1.0).GetStatus(),
            NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(Sample(runtime, table, "all", 0.5).GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);
        // Compare payloads as well as metadata, including markers for empty samples.
        const auto readUnrequested = [&] {
            return ExecuteYqlScriptWithResult(env, TStringBuilder()
                << "SELECT column_tags, stat_type, data, sampled_data FROM `/Root/Database/.metadata/statistics_v2`"
                << " WHERE owner_id = " << table.PathId.OwnerId << "ul AND local_path_id = " << table.PathId.LocalPathId
                << "ul AND column_tags IN ('1', '2,3') ORDER BY column_tags, stat_type;").SerializeAsString();
        };
        const auto previous = readUnrequested();

        if (Restart) {
            TBlockEvents<TEvStatistics::TEvAnalyzeActorResult> results(runtime);
            const auto sender = runtime.AllocateEdgeActor();
            auto request = MakeAnalyzeRequest({table.PathId}, "partial", "/Root/Database");
            request->Record.MutableTables(0)->SetSampleRate(0.25);
            request->Record.MutableTables(0)->AddColumnTags(2);
            auto retry = std::make_unique<TEvStatistics::TEvAnalyze>();
            retry->Record = request->Record;
            runtime.SendToPipe(table.SaTabletId, sender, request.release());
            runtime.WaitFor("partial collection", [&] { return !results.empty(); }, TDuration::Seconds(30));
            results.Stop();
            RebootTablet(runtime, table.SaTabletId, sender);
            runtime.SendToPipe(table.SaTabletId, sender, retry.release());
            const auto response = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(Sample(runtime, table, "partial", 0.25, {2}).GetStatus(),
                NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);
        }
        UNIT_ASSERT_VALUES_EQUAL(readUnrequested(), previous);
        const auto updated = ReadSample(runtime, table.PathId, EStatType::SIMPLE_COLUMN, TColumnTags(2u));
        UNIT_ASSERT(updated.Success && updated.Sampling && updated.SimpleColumn.Data);
        CheckSamplingMetadata(*updated.Sampling, ColumnShard, 0.25, 1);
        UNIT_ASSERT(!ColumnShard || updated.SimpleColumn.Data->GetCount() > 0);
        UNIT_ASSERT_VALUES_EQUAL(updated.SimpleColumn.Data->GetCount(), updated.Sampling->GetSampleRows());
        // Even an empty row sample must preserve statistics outside the column list.
        UNIT_ASSERT_VALUES_EQUAL(Sample(runtime, table, "tiny", std::numeric_limits<double>::min(), {2}).GetStatus(),
            NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(readUnrequested(), previous);

        // Table statistics are refreshed even for a column list. A sample summary
        // carries its own count and metadata; ordinary readers still see the full count.
        const auto sampledSummary = ReadSample(runtime, table.PathId, EStatType::TABLE_SUMMARY);
        UNIT_ASSERT(sampledSummary.Success && sampledSummary.Sampling && sampledSummary.TableSummary.Data);
        CheckSamplingMetadata(*sampledSummary.Sampling, ColumnShard, std::numeric_limits<double>::min(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sampledSummary.TableSummary.Data->GetRowCount(), sampledSummary.Sampling->GetSampleRows());
        const auto fullSummary = GetStatistics(runtime, table.PathId, EStatType::TABLE_SUMMARY, {std::nullopt});
        UNIT_ASSERT_VALUES_EQUAL(fullSummary.size(), 1);
        UNIT_ASSERT(fullSummary.front().Success && !fullSummary.front().Sampling && fullSummary.front().TableSummary.Data);
        UNIT_ASSERT_VALUES_EQUAL(fullSummary.front().TableSummary.Data->GetRowCount(), ColumnTableRowsNumber);

        // Explicitly requesting all tuple columns still collects their statistic.
        UNIT_ASSERT_VALUES_EQUAL(Sample(runtime, table, "tuple", 1.0, {2, 3}).GetStatus(),
            NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);
        const auto tuple = ReadSample(runtime, table.PathId, EStatType::COUNT_MIN_SKETCH,
            TColumnTags(std::vector<ui32>{2, 3}));
        UNIT_ASSERT(tuple.Success && tuple.CountMinSketch.CountMin && !tuple.Sampling);
        UNIT_ASSERT_VALUES_EQUAL(tuple.CountMinSketch.CountMin->GetElementCount(), ColumnTableRowsNumber);
        UNIT_ASSERT_VALUES_EQUAL(Sample(runtime, table, "key", 1.0, {1}).GetStatus(),
            NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);
        const auto histogram = ReadSample(runtime, table.PathId, EStatType::EQ_HEIGHT_HISTOGRAM, TColumnTags(1u));
        UNIT_ASSERT(histogram.Success && histogram.EqHeightHistogram.Data && !histogram.Sampling);
    }

    Y_UNIT_TEST_TWIN(SamplingDisabled, ColumnShard) {
        TTestEnv env(1, 1, false, [](Tests::TServerSettings& settings) {
            settings.FeatureFlags.SetEnableAnalyzeSampling(false);
        });
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto table = CreateEmptyTable(env, "Database", "Table", ColumnShard);
        const auto result = Sample(runtime, table, "sample", 0.5);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR);
        UNIT_ASSERT_STRING_CONTAINS(result.DebugString(), "ANALYZE sampling is disabled");
        UNIT_ASSERT_VALUES_EQUAL(Sample(runtime, table, "full", 1).GetStatus(),
            NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);
        const auto full = ReadSample(runtime, table.PathId, EStatType::TABLE_SUMMARY);
        UNIT_ASSERT(full.Success && !full.Sampling && full.TableSummary.Data);
        UNIT_ASSERT_VALUES_EQUAL(full.TableSummary.Data->GetRowCount(), 0);
    }

    Y_UNIT_TEST_TWIN(EmptyTable, ColumnShard) {
        TTestEnv env(1, 1, false, [](Tests::TServerSettings& settings) {
            settings.AppConfig->MutableStatisticsConfig()->SetAnalyzeCollectPrimaryKeyHistogram(true);
        });
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto table = CreateEmptyTable(env, "Database", "Table", ColumnShard);
        const auto result = Sample(runtime, table, "empty-sample", 0.5);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS, result.DebugString());
        const auto sampled = ReadSample(runtime, table.PathId, EStatType::TABLE_SUMMARY);
        UNIT_ASSERT(sampled.Success && sampled.Sampling && sampled.TableSummary.Data);
        UNIT_ASSERT_VALUES_EQUAL(sampled.Sampling->GetSampleRows(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sampled.TableSummary.Data->GetRowCount(), 0);
        UNIT_ASSERT(!ReadSample(runtime, table.PathId, EStatType::EQ_HEIGHT_HISTOGRAM, TColumnTags(1u)).Success);
        UNIT_ASSERT(!ReadSample(runtime, table.PathId, EStatType::COUNT_MIN_SKETCH, TColumnTags(2u)).Success);
    }

    Y_UNIT_TEST_TWIN(FailedCollectionKeepsCompletedBatches, ColumnShard) {
        TTestEnv env(1, 1, false);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto table = PrepareMultiColumnTable(env, "Database", "Table", ColumnShard);
        size_t batches = 0;
        std::vector<std::pair<EStatType, TColumnTags>> failedStatistics;
        auto observer = runtime.AddObserver<TEvStatistics::TEvAnalyzeActorResult>([&](auto& ev) {
            if (++batches == 2) {
                for (const auto& item : ev->Get()->Statistics) {
                    failedStatistics.emplace_back(item.Type, item.ColumnTags);
                }
                ev->Get()->Status = TEvStatistics::TEvAnalyzeActorResult::EStatus::InternalError;
                ev->Get()->Final = true;
                ev->Get()->Issues.AddIssue(NYql::TIssue("injected collection failure"));
            }
        });
        // Ensure a nonempty first pass so there is a second batch to fail.
        const double rate = 1.0 - std::numeric_limits<double>::epsilon();
        const auto result = Sample(runtime, table, "failed", rate);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR);
        UNIT_ASSERT_STRING_CONTAINS(result.DebugString(), "injected collection failure");
        UNIT_ASSERT(!failedStatistics.empty());
        runtime.WaitFor("saved first batch", [&] {
            return ReadSample(runtime, table.PathId, EStatType::TABLE_SUMMARY).Success;
        }, TDuration::Seconds(30));
        const auto sampled = ReadSample(runtime, table.PathId, EStatType::TABLE_SUMMARY);
        UNIT_ASSERT(sampled.Sampling);
        UNIT_ASSERT_VALUES_EQUAL(sampled.Sampling->GetRequestedRate(), rate);
        for (const auto& [type, columns] : failedStatistics) {
            UNIT_ASSERT(!ReadSample(runtime, table.PathId, type, columns).Success);
        }
    }

    Y_UNIT_TEST_TWIN(InvalidSampleRate, ColumnShard) {
        TTestEnv env(1, 1, false);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto table = CreateEmptyTable(env, "Database", "Table", ColumnShard);
        for (double rate : {0.0, -0.1, 1.1, std::numeric_limits<double>::infinity(), std::numeric_limits<double>::quiet_NaN()}) {
            const auto result = Sample(runtime, table, "invalid", rate);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR);
            UNIT_ASSERT_STRING_CONTAINS(result.DebugString(), "finite number in (0, 1]");
        }
    }

    Y_UNIT_TEST_TWIN(FinalQueryFailureDiscardsPartialResults, ColumnShard) {
        TTestEnv env(1, 1, false);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto table = PrepareMultiColumnTable(env, "Database", "Table", ColumnShard);
        UNIT_ASSERT_VALUES_EQUAL(Sample(runtime, table, "previous", 0.5).GetStatus(),
            NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);
        const auto previous = ReadSample(runtime, table.PathId, EStatType::TABLE_SUMMARY);
        UNIT_ASSERT(previous.Success && previous.Sampling && previous.TableSummary.Data);
        THashSet<TActorId> scans;
        size_t finalResponses = 0;
        size_t streamedResponses = 0;
        TSaveStatisticsObserver saves(runtime, table.PathId);
        auto queries = runtime.AddObserver<NKqp::TEvKqp::TEvQueryRequest>([&](auto& ev) {
            if (IsAnalyzeScan(ev)) {
                scans.insert(ev->Sender);
            }
        });
        auto streams = runtime.AddObserver<NKqp::TEvKqpExecuter::TEvStreamData>([&](auto& ev) {
            if (scans.contains(ev->GetRecipientRewrite())) {
                ++streamedResponses;
            }
        });
        auto responses = runtime.AddObserver<NKqp::TEvKqp::TEvQueryResponse>([&](auto& ev) {
            if (scans.contains(ev->GetRecipientRewrite()) && ++finalResponses == 1) {
                UNIT_ASSERT_GT(streamedResponses, 0);
                ev->Get()->Record.SetYdbStatus(Ydb::StatusIds::TIMEOUT);
                ev->Get()->Record.MutableResponse()->AddQueryIssues()->set_message("injected query deadline");
            }
        });
        const auto result = Sample(runtime, table, "failed", 0.25);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR, result.DebugString());
        UNIT_ASSERT_STRING_CONTAINS(result.DebugString(), "injected query deadline");
        UNIT_ASSERT_VALUES_EQUAL(finalResponses, 1);
        UNIT_ASSERT_VALUES_EQUAL(saves.GetSaveCount(), 0);
        const auto current = ReadSample(runtime, table.PathId, EStatType::TABLE_SUMMARY);
        UNIT_ASSERT(current.Success && current.Sampling && current.TableSummary.Data);
        UNIT_ASSERT_VALUES_EQUAL(current.Sampling->SerializeAsString(), previous.Sampling->SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(current.TableSummary.Data->SerializeAsString(), previous.TableSummary.Data->SerializeAsString());
    }
}

} // namespace NKikimr::NStat
