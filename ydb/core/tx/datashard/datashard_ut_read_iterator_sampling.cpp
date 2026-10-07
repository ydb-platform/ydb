#include <ydb/core/tx/datashard/read_iterator_sampling.h>
#include <ydb/core/tx/datashard/datashard_ut_common_kqp.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

#include <ydb/core/tablet_flat/flat_page_iface.h>
#include <ydb/core/tablet_flat/shared_cache_events.h>
#include <ydb/core/testlib/actors/block_events.h>

#include <library/cpp/testing/unittest/registar.h>

#include <limits>

namespace NKikimr {
namespace {

using namespace Tests;
using namespace NDataShard;

struct TSamplingRow {
    ui32 Key = 0;
    ui32 Value = 0;

    bool operator==(const TSamplingRow& rhs) const = default;
    bool operator<(const TSamplingRow& rhs) const { return Key < rhs.Key; }
};

struct TSamplingRead {
    TVector<TSamplingRow> Rows;
    NKikimrTxDataShard::TReadSamplingStats Stats;
    TVector<NKikimrTxDataShard::TEvReadResult> Results;
    bool Finished = false;
};

struct TSamplingOptions {
    ui32 Shards = 1;
    bool ForcePartPerCommit = false;
    bool SmallPages = false;
    bool ColdCache = false;
    bool BTreeIndex = true;
};

struct TSamplingTestHelper {
    Tests::TServer::TPtr Server;
    TActorId Sender;
    TTableId TableId;
    TVector<ui64> TabletIds;
    NKikimrTxDataShard::TEvGetInfoResponse::TUserTable UserTable;
    ui64 NextReadId = 1;
    // Reuse one snapshot across reads and layout changes.
    std::optional<TRowVersion> FixedSnapshot;

    explicit TSamplingTestHelper(const TSamplingOptions& options = {}) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);
        if (options.ColdCache) {
            serverSettings.AppConfig->MutableSharedCacheConfig()->SetMemoryLimit(0);
        }
        Server = new TServer(serverSettings);
        auto& runtime = *Server->GetRuntime();
        Sender = runtime.AllocateEdgeActor();
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_ERROR);
        runtime.GetAppData().FeatureFlags.SetEnableLocalDBBtreeIndex(options.BTreeIndex);
        InitRoot(Server, Sender);

        NLocalDb::TCompactionPolicyPtr policy = NLocalDb::CreateDefaultUserTablePolicy();
        policy->InMemForceStepsToSnapshot = options.ForcePartPerCommit ? 1 : 1000000;
        for (auto& gen : policy->Generations) {
            gen.ExtraCompactionPercent = 0;
            gen.ExtraCompactionMinSize = 100;
            gen.ExtraCompactionExpPercent = 0;
            gen.ExtraCompactionExpMaxSize = 0;
            gen.UpliftPartSize = 0;
        }
        if (options.SmallPages) {
            policy->MinDataPageSize = 1;
            policy->MinBTreeIndexNodeSize = 1;
        }
        auto opts = TShardedTableOptions()
            .Shards(options.Shards)
            .Columns({
                {"key", "Uint32", true, false},
                {"value", "Uint32", false, false},
            })
            .Policy(policy.Get());
        if (options.ColdCache) {
            opts.ExecutorCacheSize(1);
        }
        auto [shards, tableId] = CreateShardedTable(Server, Sender, "/Root", "table-1", opts);
        TabletIds = shards;
        TableId = tableId;
        auto [tables, ownerId] = GetTables(Server, TabletIds[0]);
        Y_UNUSED(ownerId);
        UserTable = tables["table-1"];
    }

    TTestActorRuntime& Runtime() const { return *Server->GetRuntime(); }
    ui64 TabletId(ui32 idx = 0) const { return TabletIds[idx]; }

    auto CountDataPages(ui32& pages) const {
        return Runtime().AddObserver<NSharedCache::TEvRequest>(
            [&pages, tabletId = TabletId()](NSharedCache::TEvRequest::TPtr& ev) {
                if (ev->Get()->PageCollection->Label().TabletID() != tabletId) {
                    return;
                }
                for (const auto& location : ev->Get()->Pages) {
                    pages += location.Type == NTable::NPage::EPage::DataPage;
                }
            });
    }

    void Upsert(ui32 key, ui32 value) const {
        ExecSQL(Server, Sender, TStringBuilder()
            << "UPSERT INTO `/Root/table-1` (key, value) VALUES (" << key << ", " << value << ");");
    }

    void UpsertRange(ui32 first, ui32 count, ui32 step = 1) const {
        for (ui32 i = 0; i < count; ++i) {
            Upsert(first + i * step, first + i * step);
        }
    }

    // Keep the batch in one part when each commit forces a snapshot.
    void UpsertBatch(ui32 first, ui32 count, ui32 step = 1) const {
        TStringBuilder sql;
        sql << "UPSERT INTO `/Root/table-1` (key, value) VALUES ";
        for (ui32 i = 0; i < count; ++i) {
            if (i) {
                sql << ", ";
            }
            const ui32 key = first + i * step;
            sql << "(" << key << ", " << key << ")";
        }
        sql << ";";
        ExecSQL(Server, Sender, sql);
    }

    void Delete(ui32 key) const {
        ExecSQL(Server, Sender, TStringBuilder() << "DELETE FROM `/Root/table-1` WHERE key = " << key << ";");
    }

    void Compact(ui32 idx = 0) const {
        CompactTable(Runtime(), TabletId(idx), TableId, false);
    }

    void WaitParts(ui32 count) const {
        WaitTableStats(Runtime(), TabletId(), [count](const NKikimrTableStats::TTableStats& stats) {
            return stats.GetPartCount() >= count;
        });
    }

    std::unique_ptr<TEvDataShard::TEvRead> MakeRead(
            double rate, ui64 seed, bool withSnapshot = true) {
        TRowVersion snapshot = TRowVersion::Min();
        if (withSnapshot) {
            if (FixedSnapshot) {
                snapshot = *FixedSnapshot;
            } else {
                snapshot = CreateVolatileSnapshot(Server, {"/Root/table-1"}, TDuration::Hours(1));
            }
        }
        auto request = GetBaseReadRequest(
            TableId, UserTable.GetDescription(), NextReadId++, NKikimrDataEvents::FORMAT_CELLVEC, snapshot);
        auto* sampling = request->Record.MutableSampling();
        sampling->SetRate(rate);
        sampling->SetSeed(seed);
        return request;
    }

    static TVector<TSamplingRow> RowsOf(const TEvDataShard::TEvReadResult& result) {
        TVector<TSamplingRow> rows;
        for (size_t i = 0; i < result.GetRowsCount(); ++i) {
            auto cells = result.GetCells(i);
            UNIT_ASSERT(cells.size() >= 2);
            rows.push_back({cells[0].AsValue<ui32>(), cells[1].AsValue<ui32>()});
        }
        return rows;
    }

    void Ack(const NKikimrTxDataShard::TEvReadResult& result, ui32 shardIdx = 0, ui32 maxRows = Max<ui32>()) {
        auto* ack = new TEvDataShard::TEvReadAck();
        ack->Record.SetReadId(result.GetReadId());
        ack->Record.SetSeqNo(result.GetSeqNo());
        ack->Record.SetMaxRows(maxRows);
        ack->Record.SetMaxBytes(Max<ui32>());
        Runtime().SendToPipe(TabletId(shardIdx), Sender, ack, 0, GetPipeConfigWithRetries());
    }

    void Cancel(const NKikimrTxDataShard::TEvReadResult& result) {
        auto* cancel = new TEvDataShard::TEvReadCancel();
        cancel->Record.SetReadId(result.GetReadId());
        Runtime().SendToPipe(TabletId(), Sender, cancel, 0, GetPipeConfigWithRetries());
    }

    TSamplingRead Collect(ui32 shardIdx = 0, ui32 stopAfterResults = Max<ui32>(), ui32 maxRows = Max<ui32>()) {
        TSamplingRead out;
        for (ui32 n = 0; n < stopAfterResults; ++n) {
            auto result = WaitReadResult(Server, TDuration::Seconds(60));
            UNIT_ASSERT_C(result, "timed out waiting for TEvReadResult");
            out.Results.push_back(result->Record);
            out.Finished = result->Record.GetFinished();
            if (result->Record.HasSamplingStats()) {
                out.Stats = result->Record.GetSamplingStats();
            }
            if (result->Record.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
                break;
            }
            auto rows = RowsOf(*result);
            out.Rows.insert(out.Rows.end(), rows.begin(), rows.end());
            if (out.Finished || n + 1 == stopAfterResults) {
                break;
            }
            if (result->Record.GetLimitReached()) {
                Ack(result->Record, shardIdx, maxRows);
            }
        }
        return out;
    }

    TSamplingRead ReadShard(std::unique_ptr<TEvDataShard::TEvRead> request, ui32 shardIdx = 0,
            ui32 stopAfterResults = Max<ui32>(), ui32 maxRows = 0, ui32 maxRowsInResult = 0) {
        if (maxRows) {
            request->Record.SetMaxRows(maxRows);
        }
        if (maxRowsInResult) {
            request->Record.SetMaxRowsInResult(maxRowsInResult);
        }
        SendReadAsync(Server, TabletId(shardIdx), request.release(), Sender);
        return Collect(shardIdx, stopAfterResults, maxRows ? maxRows : Max<ui32>());
    }

    TSamplingRead ReadRange(double rate, ui64 seed, ui32 from, bool fromIncl, ui32 to, bool toIncl,
            ui32 shardIdx = 0) {
        auto request = MakeRead(rate, seed);
        AddRangeQuery<ui32>(*request, {from}, fromIncl, {to}, toIncl);
        return ReadShard(std::move(request), shardIdx);
    }

    TSamplingRead ReadAll(double rate, ui64 seed, ui32 shardIdx = 0, ui32 maxRowsInResult = 0) {
        auto request = MakeRead(rate, seed);
        AddFullRangeQuery(*request);
        return ReadShard(std::move(request), shardIdx, Max<ui32>(), 0, maxRowsInResult);
    }

    // Resume at the token key, preserving the snapshot and selected unit.
    TSamplingRead Resume(const NKikimrTxDataShard::TEvReadResult& partial, double rate, ui64 seed,
            ui32 from, bool fromIncl, ui32 to, bool toIncl, ui32 shardIdx = 0) {
        UNIT_ASSERT(partial.HasContinuationToken());
        NKikimrTxDataShard::TReadContinuationToken token;
        UNIT_ASSERT(token.ParseFromString(partial.GetContinuationToken()));
        UNIT_ASSERT(partial.HasSnapshot());
        auto request = MakeRead(rate, seed, false);
        *request->Record.MutableSnapshot() = partial.GetSnapshot();
        if (token.HasSampling()) {
            *request->Record.MutableSampling()->MutableContinuation() = token.GetSampling();
        }
        ui32 start = from;
        bool startIncl = fromIncl;
        if (!token.GetLastProcessedKey().empty()) {
            TSerializedCellVec key(token.GetLastProcessedKey());
            UNIT_ASSERT_VALUES_EQUAL(key.GetCells().size(), 1u);
            start = key.GetCells()[0].AsValue<ui32>();
            startIncl = token.GetSampling().GetLastProcessedKeyInclusive();
        }
        AddRangeQuery<ui32>(*request, {start}, startIncl, {to}, toIncl);
        return ReadShard(std::move(request), shardIdx);
    }
};

void AssertSameRows(const TVector<TSamplingRow>& left, const TVector<TSamplingRow>& right) {
    UNIT_ASSERT_VALUES_EQUAL(left.size(), right.size());
    for (size_t i = 0; i < left.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(left[i].Key, right[i].Key);
        UNIT_ASSERT_VALUES_EQUAL(left[i].Value, right[i].Value);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(DataShardReadIteratorSampling) {

    Y_UNIT_TEST(RejectsInvalidRequests) {
        TSamplingTestHelper helper;
        helper.Upsert(1, 10);
        auto reject = [&](auto configure) {
            auto request = helper.MakeRead(1.0, 1);
            AddFullRangeQuery(*request);
            configure(*request);
            auto got = helper.ReadShard(std::move(request));
            UNIT_ASSERT_VALUES_EQUAL(got.Results[0].GetStatus().GetCode(), Ydb::StatusIds::BAD_REQUEST);
        };
        for (double rate : {0.0, -0.5, 1.5, std::numeric_limits<double>::infinity(), std::numeric_limits<double>::quiet_NaN()}) {
            reject([rate](auto& request) { request.Record.MutableSampling()->SetRate(rate); });
        }
        reject([](auto& request) { request.Record.MutableSampling()->SetMemtableStride(0); });
        reject([](auto& request) { request.Record.SetReverse(true); });
        reject([](auto& request) { request.Record.SetLockTxId(1); });
        reject([](auto& request) { request.Record.MutableVectorTopK(); });
        reject([](auto& request) {
            request.Ranges.clear();
            AddKeyQuery(request, {1u});
        });
        for (const TString& key : {
            TString("invalid"),
            TSerializedCellVec(TVector<TCell>{TCell::Make(ui64(1))}).GetBuffer(),
            TSerializedCellVec(TVector<TCell>{TCell::Make(ui32(1)), TCell::Make(ui32(2))}).GetBuffer(),
        }) {
            reject([&](auto& request) {
                request.Record.MutableSampling()->MutableContinuation()->MutablePendingSelectedUnit()->SetFirstKey(key);
            });
        }
        reject([](auto& request) {
            const auto key = TSerializedCellVec(TVector<TCell>{TCell::Make(ui32(1))}).GetBuffer();
            auto* bounds = request.Record.MutableSampling()->MutableContinuation()->MutablePendingSelectedUnit();
            bounds->SetFirstKey(key);
            bounds->SetLastKey(key);
            bounds->SetFirstInclusive(false);
            bounds->SetLastInclusive(false);
        });
    }

    Y_UNIT_TEST(PendingContinuationResumePosition) {
        TSamplingTestHelper helper;
        helper.UpsertBatch(0, 8);
        const auto key = [](ui32 value) {
            return TSerializedCellVec(TVector<TCell>{TCell::Make(value)});
        };
        auto check = [&](const NTable::TBounds& pending, bool inclusive, Ydb::StatusIds::StatusCode status) {
            auto request = helper.MakeRead(1.0, 1);
            auto* continuation = request->Record.MutableSampling()->MutableContinuation();
            continuation->SetLastProcessedKeyInclusive(inclusive);
            SaveSamplingBounds(pending, *continuation->MutablePendingSelectedUnit());
            AddRangeQuery<ui32>(*request, {3}, inclusive, {5}, true);
            const auto read = helper.ReadShard(std::move(request));
            UNIT_ASSERT_VALUES_EQUAL(read.Results.back().GetStatus().GetCode(), status);
            if (status == Ydb::StatusIds::BAD_REQUEST) {
                UNIT_ASSERT(read.Rows.empty());
            } else {
                UNIT_ASSERT(read.Finished);
                TVector<TSamplingRow> expected;
                for (ui32 value = inclusive ? 3 : 4; value <= 5; ++value) {
                    expected.push_back({value, value});
                }
                AssertSameRows(read.Rows, expected);
            }
        };
        // A gap before the pending interval would silently omit unread rows.
        check({key(4), key(7), true, true}, true, Ydb::StatusIds::BAD_REQUEST);
        check({key(3), key(7), false, true}, true, Ydb::StatusIds::BAD_REQUEST);

        check({key(3), key(7), true, true}, true, Ydb::StatusIds::SUCCESS);
        check({key(3), key(7), false, true}, false, Ydb::StatusIds::SUCCESS);
        check({key(3), key(7), true, true}, false, Ydb::StatusIds::SUCCESS);
        check({key(2), key(7), true, true}, true, Ydb::StatusIds::SUCCESS);
        // Already-consumed intervals are discarded, including an exclusive end at the cursor.
        check({key(1), key(2), true, true}, true, Ydb::StatusIds::SUCCESS);
        check({key(1), key(3), true, false}, true, Ydb::StatusIds::SUCCESS);

        // An inclusive empty start is normalized to the all-null minimum key.
        auto request = helper.MakeRead(1.0, 1);
        AddRangeQuery<ui32>(*request, {}, true, {5}, true);
        const NTable::TBounds pending(TSerializedCellVec(TVector<TCell>{TCell()}), key(7), true, true);
        SaveSamplingBounds(pending,
            *request->Record.MutableSampling()->MutableContinuation()->MutablePendingSelectedUnit());
        const auto read = helper.ReadShard(std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(read.Results.back().GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(read.Finished);
        AssertSameRows(read.Rows, {{0, 0}, {1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}});
    }

    Y_UNIT_TEST(RateOneEqualsFullRead) {
        TSamplingTestHelper helper({.ForcePartPerCommit = true, .SmallPages = true});
        helper.UpsertRange(0, 10, 3);
        helper.UpsertRange(1, 10, 3);
        helper.UpsertRange(2, 10, 3);
        helper.UpsertRange(30, 5, 1);
        auto sampled = helper.ReadAll(1.0, 7, 0, 1);
        auto full = helper.ReadAll(1.0, 7);
        auto plain = helper.MakeRead(1.0, 1);
        plain->Record.ClearSampling();
        AddFullRangeQuery(*plain);
        auto plainRead = helper.ReadShard(std::move(plain));
        UNIT_ASSERT(sampled.Finished);
        AssertSameRows(sampled.Rows, plainRead.Rows);
        AssertSameRows(full.Rows, plainRead.Rows);
        UNIT_ASSERT(sampled.Results.size() > 1);
    }

    Y_UNIT_TEST_TWIN(RestartFromEveryToken, btree) {
        TSamplingTestHelper helper({.SmallPages = true, .BTreeIndex = btree});
        helper.UpsertBatch(0, 24);
        helper.Compact();
        helper.FixedSnapshot = CreateVolatileSnapshot(helper.Server, {"/Root/table-1"}, TDuration::Hours(1));
        for (double rate : {1.0, 0.5}) {
            TSamplingRead once;
            TVector<TSerializedTableRange> ranges;
            ui64 seed = 1;
            for (; seed <= 64; ++seed) {
                auto request = helper.MakeRead(rate, seed);
                AddRangeQuery<ui32>(*request, {0}, true, {7}, true);
                AddRangeQuery<ui32>(*request, {8}, false, {8}, false);
                AddRangeQuery<ui32>(*request, {12}, true, {23}, true);
                ranges = request->Ranges;
                once = helper.ReadShard(std::move(request), 0, Max<ui32>(), 0, 1);
                UNIT_ASSERT(once.Finished);
                if (!once.Rows.empty() && once.Rows.front().Key <= 7 && once.Rows.back().Key >= 12
                    && (rate == 1.0 || once.Stats.GetUnitsSelected() < once.Stats.GetUnitsTotal()))
                {
                    break;
                }
            }
            UNIT_ASSERT_C(seed <= 64, "no sample exercises both ranges and skipped units");
            UNIT_ASSERT(once.Results.size() > 1);
            size_t delivered = 0;
            bool resumedLaterRange = false;
            bool checkedInclusive = false;
            bool checkedExclusive = false;
            for (size_t i = 0; i + 1 < once.Results.size(); ++i) {
                const auto& partial = once.Results[i];
                delivered += partial.GetRowCount();
                NKikimrTxDataShard::TReadContinuationToken token;
                UNIT_ASSERT(token.ParseFromString(partial.GetContinuationToken()));
                UNIT_ASSERT(token.GetFirstUnprocessedQuery() < ranges.size());
                resumedLaterRange |= token.GetFirstUnprocessedQuery() > 0;
                const TVector<TSamplingRow> expected(once.Rows.begin() + delivered, once.Rows.end());
                for (bool inclusive : {false, true}) {
                    auto request = helper.MakeRead(rate, seed, false);
                    *request->Record.MutableSnapshot() = partial.GetSnapshot();
                    auto* continuation = request->Record.MutableSampling()->MutableContinuation();
                    *continuation = token.GetSampling();
                    request->Ranges.assign(ranges.begin() + token.GetFirstUnprocessedQuery(), ranges.end());
                    if (!token.GetLastProcessedKey().empty()) {
                        TSerializedCellVec key(token.GetLastProcessedKey());
                        // Uint32 cursors After(K) and Before(K + 1) have the same unread suffix.
                        ui32 next = key.GetCells()[0].AsValue<ui32>()
                            + !token.GetSampling().GetLastProcessedKeyInclusive();
                        UNIT_ASSERT(next > 0);
                        const ui32 resumeKey = inclusive ? next : next - 1;
                        request->Ranges.front().From = TSerializedCellVec(TVector<TCell>{TCell::Make(resumeKey)});
                        request->Ranges.front().FromInclusive = inclusive;
                        continuation->SetLastProcessedKeyInclusive(inclusive);
                        checkedInclusive |= inclusive;
                        checkedExclusive |= !inclusive;
                    }
                    auto rest = helper.ReadShard(std::move(request));
                    UNIT_ASSERT(rest.Finished);
                    AssertSameRows(rest.Rows, expected);
                }
            }
            UNIT_ASSERT(resumedLaterRange);
            UNIT_ASSERT(checkedInclusive && checkedExclusive);
        }
    }

    Y_UNIT_TEST(SelectorUsesEveryInput) {
        auto decisions = [](ui64 tablet, ui32 table, TStringBuf layout, ui64 seed) {
            TSamplingSelector selector(tablet, table, layout, seed, SamplingThreshold(0.5));
            TString result;
            for (ui32 unit = 0; unit < 128; ++unit) {
                result.push_back(selector.Draw(ToString(unit)) ? '1' : '0');
            }
            return result;
        };
        const auto expected = decisions(1, 2, "layout", 3);
        UNIT_ASSERT_VALUES_EQUAL(expected, decisions(1, 2, "layout", 3));
        UNIT_ASSERT(expected.find('0') != TString::npos);
        UNIT_ASSERT(expected.find('1') != TString::npos);
        UNIT_ASSERT(expected != decisions(2, 2, "layout", 3));
        UNIT_ASSERT(expected != decisions(1, 3, "layout", 3));
        UNIT_ASSERT(expected != decisions(1, 2, "other-layout", 3));
        UNIT_ASSERT(expected != decisions(1, 2, "layout", 4));

        TSamplingSelector quarter(1, 2, "layout", 3, SamplingThreshold(0.25));
        TSamplingSelector all(1, 2, "layout", 3, SamplingThreshold(1.0));
        ui32 quarterCount = 0;
        for (ui32 unit = 0; unit < expected.size(); ++unit) {
            const bool selected = quarter.Draw(ToString(unit));
            UNIT_ASSERT(!selected || expected[unit] == '1');
            quarterCount += selected;
            UNIT_ASSERT(all.Draw(ToString(unit)));
        }
        UNIT_ASSERT(quarterCount > 0);
        UNIT_ASSERT(quarterCount < std::count(expected.begin(), expected.end(), '1'));
    }

    Y_UNIT_TEST_TWIN(PendingUnitSurvivesLayoutChange, split) {
        TSamplingTestHelper helper({.ForcePartPerCommit = true});
        helper.UpsertBatch(0, 32);
        helper.WaitParts(1);
        helper.Upsert(31, 3131);
        helper.WaitParts(2);
        helper.FixedSnapshot = CreateVolatileSnapshot(helper.Server, {"/Root/table-1"}, TDuration::Hours(1));
        const TVector<NScheme::TTypeInfo> types = {NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)};
        const auto key = [](ui32 value) {
            return TSerializedCellVec(TVector<TCell>{TCell::Make(value)});
        };
        TVector<std::pair<ui64, NKikimrTxDataShard::TEvReadResult>> candidates;
        for (ui64 seed = 1; seed <= 64; ++seed) {
            auto request = helper.MakeRead(0.5, seed);
            AddRangeQuery<ui32>(*request, {0}, true, {30}, true);
            auto first = helper.ReadShard(std::move(request), 0, 1, 1, 1);
            UNIT_ASSERT_VALUES_EQUAL(first.Results[0].GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            if (first.Rows.empty()) {
                UNIT_ASSERT(first.Finished);
                continue;
            }
            UNIT_ASSERT(!first.Finished);
            UNIT_ASSERT_VALUES_EQUAL(first.Rows.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(first.Rows[0].Key, 0u);
            NKikimrTxDataShard::TReadContinuationToken token;
            UNIT_ASSERT(token.ParseFromString(first.Results[0].GetContinuationToken()));
            UNIT_ASSERT(token.GetSampling().HasPendingSelectedUnit());
            NTable::TBounds pending;
            TString error;
            UNIT_ASSERT_C(ParseSamplingBounds(token.GetSampling().GetPendingSelectedUnit(), pending, error, types), error);
            UNIT_ASSERT(CompareSamplingPos(SamplingStart(pending), {key(0), true}, types) <= 0);
            UNIT_ASSERT(CompareSamplingPos({key(30), false}, SamplingEnd(pending), types) <= 0);
            candidates.emplace_back(seed, first.Results[0]);
            helper.Cancel(first.Results[0]);
        }
        UNIT_ASSERT(!candidates.empty());
        if (split) {
            SetSplitMergePartCountLimit(&helper.Runtime(), -1);
            WaitTxNotification(helper.Server, helper.Sender,
                AsyncSplitTable(helper.Server, helper.Sender, "/Root/table-1", helper.TabletId(), 16));
            helper.TabletIds = GetTableShards(helper.Server, helper.Sender, "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(helper.TabletIds.size(), 2u);
        } else {
            helper.Compact();
        }
        TVector<TSamplingRow> expected;
        for (ui32 value = 1; value <= 30; ++value) {
            expected.push_back({value, value});
        }
        for (const auto& [seed, partial] : candidates) {
            size_t freshRows = 0;
            for (ui32 shard = 0; shard < helper.TabletIds.size(); ++shard) {
                auto fresh = helper.ReadRange(0.5, seed, 1, true, 30, true, shard);
                UNIT_ASSERT(fresh.Finished);
                freshRows += fresh.Rows.size();
            }
            if (freshRows == expected.size()) {
                continue;
            }
            // A fresh draw skips rows; the saved selection must return the whole suffix.
            UNIT_ASSERT(freshRows < expected.size());
            TVector<TSamplingRow> resumed;
            for (ui32 shard = 0; shard < helper.TabletIds.size(); ++shard) {
                auto rest = helper.Resume(partial, 0.5, seed, 0, true, 30, true, shard);
                UNIT_ASSERT(rest.Finished);
                resumed.insert(resumed.end(), rest.Rows.begin(), rest.Rows.end());
            }
            std::sort(resumed.begin(), resumed.end());
            AssertSameRows(resumed, expected);
            return;
        }
        UNIT_FAIL("no saved sample differs from a fresh draw after the layout change");
    }

    Y_UNIT_TEST(HeadReadFixesVersion) {
        TSamplingTestHelper helper;
        helper.UpsertRange(0, 5);
        auto request = helper.MakeRead(1.0, 1, false);
        AddFullRangeQuery(*request);
        auto first = helper.ReadShard(std::move(request), 0, 1, 1, 1);
        UNIT_ASSERT(!first.Finished);
        const auto& result = first.Results[0];
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(result.HasSnapshot());
        UNIT_ASSERT(result.GetLimitReached());
        helper.Upsert(100, 100);
        helper.Ack(result);
        const auto rest = helper.Collect();
        UNIT_ASSERT(rest.Finished);
        for (const auto& more : rest.Results) {
            UNIT_ASSERT_VALUES_EQUAL(more.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(more.GetSnapshot().GetStep(), result.GetSnapshot().GetStep());
            UNIT_ASSERT_VALUES_EQUAL(more.GetSnapshot().GetTxId(), result.GetSnapshot().GetTxId());
        }
        first.Rows.insert(first.Rows.end(), rest.Rows.begin(), rest.Rows.end());
        UNIT_ASSERT_VALUES_EQUAL(first.Rows.size(), 5u);
        for (ui32 i = 0; i < first.Rows.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(first.Rows[i].Key, i);
        }
    }

    Y_UNIT_TEST(HistoryAndErases) {
        TSamplingTestHelper helper;
        helper.UpsertRange(0, 6);
        auto snapshot = CreateVolatileSnapshot(helper.Server, {"/Root/table-1"}, TDuration::Hours(1));
        helper.Upsert(1, 999);
        helper.Delete(2);
        auto request = helper.MakeRead(1.0, 1, false);
        snapshot.ToProto(request->Record.MutableSnapshot());
        AddFullRangeQuery(*request);
        auto sampled = helper.ReadShard(std::move(request));
        auto plain = GetBaseReadRequest(helper.TableId, helper.UserTable.GetDescription(), helper.NextReadId++, NKikimrDataEvents::FORMAT_CELLVEC, snapshot);
        AddFullRangeQuery(*plain);
        auto full = helper.ReadShard(std::move(plain));
        AssertSameRows(sampled.Rows, full.Rows);
        UNIT_ASSERT_VALUES_EQUAL(sampled.Rows.size(), 6u);
        UNIT_ASSERT_VALUES_EQUAL(sampled.Rows[1].Value, 1u);
    }

    Y_UNIT_TEST(ExternalBlobNeedDataAfterOutput) {
        TPortManager pm;
        TServerSettings::TControls controls;
        controls.MutableDataShardControls()->SetReadIteratorKeysExtBlobsPrecharge(1);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false)
            .AddStoragePool("ssd").AddStoragePool("hdd").AddStoragePool("ext")
            .SetControls(controls);
        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();
        InitRoot(server, sender);
        TShardedTableOptions::TFamily fam;
        fam.Name = "default";
        fam.LogPoolKind = "ssd";
        fam.SysLogPoolKind = "ssd";
        fam.DataPoolKind = "ssd";
        fam.ExternalPoolKind = "ext";
        fam.DataThreshold = 100;
        fam.ExternalThreshold = 1_KB;
        auto opts = TShardedTableOptions()
            .Columns({
                {"key", "Uint32", true, false},
                {"value", "String", false, false},
            })
            .Families({fam});
        CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const auto shard = GetTableShards(server, sender, "/Root/table-1").at(0);
        const auto tableId = ResolveTableId(server, sender, "/Root/table-1");
        const TString payload(2_KB, 'x');
        for (ui32 key = 0; key < 4; ++key) {
            ExecSQL(server, sender, TStringBuilder()
                << "UPSERT INTO `/Root/table-1` (key, value) VALUES (" << key << ", \""
                << (key ? payload : TString("inline")) << "\");");
        }
        CompactTable(runtime, shard, tableId, false);
        auto [tables, owner] = GetTables(server, shard);
        Y_UNUSED(owner);
        const auto snapshot = CreateVolatileSnapshot(server, {"/Root/table-1"}, TDuration::Hours(1));
        auto makeRead = [&](ui64 readId) {
            auto request = GetBaseReadRequest(tableId, tables["table-1"].GetDescription(),
                readId, NKikimrDataEvents::FORMAT_CELLVEC, snapshot);
            AddFullRangeQuery(*request);
            return request;
        };
        // Warm pages through key-only reads so the first missing blob follows an inline row.
        auto warm = makeRead(1);
        warm->Record.ClearColumns();
        warm->Record.AddColumns(tables["table-1"].GetDescription().GetColumns(0).GetId());
        auto warmed = SendRead(server, shard, warm.release(), sender);
        UNIT_ASSERT(warmed->Record.GetFinished());
        UNIT_ASSERT_VALUES_EQUAL(warmed->GetRowsCount(), 4u);

        TBlockEvents<TEvBlobStorage::TEvGet> blobs(runtime, [shard](const auto& ev) {
            const auto* get = ev->Get();
            for (ui32 i = 0; i < get->QuerySize; ++i) {
                if (get->Queries[i].Id.TabletID() == shard) {
                    return true;
                }
            }
            return false;
        });
        auto request = makeRead(2);
        request->Record.MutableSampling()->SetRate(1.0);
        SendReadAsync(server, shard, request.release(), sender);
        auto first = WaitReadResult(server, TDuration::Seconds(60));
        UNIT_ASSERT(first);
        UNIT_ASSERT_VALUES_EQUAL(first->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(first->GetRowsCount(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(first->GetCells(0)[0].AsValue<ui32>(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(first->GetCells(0)[1].AsBuf(), "inline");
        UNIT_ASSERT(!first->Record.GetFinished());
        NKikimrTxDataShard::TReadContinuationToken token;
        UNIT_ASSERT(token.ParseFromString(first->Record.GetContinuationToken()));
        UNIT_ASSERT(token.GetSampling().GetLastProcessedKeyInclusive());
        UNIT_ASSERT(token.GetSampling().HasPendingSelectedUnit());
        UNIT_ASSERT_VALUES_EQUAL(TSerializedCellVec(token.GetLastProcessedKey()).GetCells()[0].AsValue<ui32>(), 1u);
        runtime.WaitFor("external blob fetch", [&] { return !blobs.empty(); });
        blobs.Stop().Unblock();

        ui32 nextKey = 1;
        for (;;) {
            auto result = WaitReadResult(server, TDuration::Seconds(60));
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            for (size_t i = 0; i < result->GetRowsCount(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(result->GetCells(i)[0].AsValue<ui32>(), nextKey++);
                UNIT_ASSERT_VALUES_EQUAL(result->GetCells(i)[1].AsBuf(), payload);
            }
            if (result->Record.GetFinished()) {
                break;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(nextKey, 4u);
    }

    Y_UNIT_TEST_TWIN(ColdCacheNoReplay, btree) {
        TSamplingTestHelper helper({.SmallPages = true, .ColdCache = true, .BTreeIndex = btree});
        helper.UpsertBatch(0, 20);
        helper.Compact();
        helper.FixedSnapshot = CreateVolatileSnapshot(helper.Server, {"/Root/table-1"}, TDuration::Hours(1));
        const auto expected = helper.ReadAll(0.5, 15);
        UNIT_ASSERT(!expected.Rows.empty());
        UNIT_ASSERT(expected.Rows.size() < 20);
        RebootTablet(helper.Runtime(), helper.TabletId(), helper.Sender);
        ui32 dataPages = 0;
        auto observer = helper.CountDataPages(dataPages);
        const auto resumed = helper.ReadAll(0.5, 15, 0, 1);
        UNIT_ASSERT(resumed.Finished);
        UNIT_ASSERT(dataPages > 0);
        AssertSameRows(resumed.Rows, expected.Rows);
        UNIT_ASSERT_VALUES_EQUAL(resumed.Stats.GetUnitsTotal(), expected.Stats.GetUnitsTotal());
        UNIT_ASSERT_VALUES_EQUAL(resumed.Stats.GetUnitsSelected(), expected.Stats.GetUnitsSelected());
        UNIT_ASSERT_VALUES_EQUAL(resumed.Stats.GetUnitsMemtable(), expected.Stats.GetUnitsMemtable());
    }

    Y_UNIT_TEST(FirstRowPrecedesRemainingIndexScan) {
        TSamplingTestHelper helper({.SmallPages = true, .ColdCache = true});
        helper.UpsertBatch(0, 80);
        helper.Compact();
        helper.FixedSnapshot = CreateVolatileSnapshot(helper.Server, {"/Root/table-1"}, TDuration::Hours(1));
        RebootTablet(helper.Runtime(), helper.TabletId(), helper.Sender);
        THashSet<std::pair<TLogoBlobID, NTable::NPage::TPageOffset>> indexPages;
        auto observer = helper.Runtime().AddObserver<NSharedCache::TEvRequest>([&](auto& ev) {
            const auto& pages = *ev->Get();
            if (pages.PageCollection->Label().TabletID() != helper.TabletId()) {
                return;
            }
            for (const auto& location : pages.Pages) {
                if (location.Type == NTable::NPage::EPage::BTreeIndex) {
                    indexPages.emplace(pages.PageCollection->Label(), location.Offset);
                }
            }
        });
        auto request = helper.MakeRead(1.0, 1);
        request->Record.SetMaxRows(0);
        AddFullRangeQuery(*request);
        const auto paused = helper.ReadShard(std::move(request), 0, 1);
        const auto& empty = paused.Results.front();
        UNIT_ASSERT(!paused.Finished);
        UNIT_ASSERT(paused.Rows.empty());
        UNIT_ASSERT(empty.GetLimitReached());
        UNIT_ASSERT_VALUES_EQUAL(paused.Stats.GetUnitsTotal(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(paused.Stats.GetIndexPagesTouched(), 0u);
        UNIT_ASSERT(indexPages.empty());
        NKikimrTxDataShard::TReadContinuationToken token;
        UNIT_ASSERT(token.ParseFromString(empty.GetContinuationToken()));
        UNIT_ASSERT(token.HasSampling());

        helper.Ack(empty, 0, 1);
        auto first = helper.Collect(0, 1);
        AssertSameRows(first.Rows, {{0, 0}});
        UNIT_ASSERT(!first.Finished);
        UNIT_ASSERT(first.Results.back().GetLimitReached());
        const size_t prefixPages = indexPages.size();
        UNIT_ASSERT(prefixPages > 0);
        helper.Ack(first.Results.back());
        const auto rest = helper.Collect();
        UNIT_ASSERT(rest.Finished);
        // A whole-range index pass before the first row would leave no new pages here.
        UNIT_ASSERT_C(indexPages.size() > prefixPages,
            "prefix index pages " << prefixPages << ", total " << indexPages.size());
        first.Rows.insert(first.Rows.end(), rest.Rows.begin(), rest.Rows.end());
        UNIT_ASSERT_VALUES_EQUAL(first.Rows.size(), 80u);
        for (ui32 key = 0; key < first.Rows.size(); ++key) {
            UNIT_ASSERT_VALUES_EQUAL(first.Rows[key].Key, key);
            UNIT_ASSERT_VALUES_EQUAL(first.Rows[key].Value, key);
        }
    }

    Y_UNIT_TEST(PendingUnitSurvivesInitialPageFaultAndCompaction) {
        TSamplingTestHelper helper({.ForcePartPerCommit = true, .ColdCache = true});
        helper.UpsertBatch(0, 32);
        helper.WaitParts(1);
        helper.Upsert(31, 3131);
        helper.WaitParts(2);
        helper.FixedSnapshot = CreateVolatileSnapshot(helper.Server, {"/Root/table-1"}, TDuration::Hours(1));
        RebootTablet(helper.Runtime(), helper.TabletId(), helper.Sender);
        ui32 dataPages = 0;
        auto pages = helper.CountDataPages(dataPages);
        ui32 deliveredRows = 0;
        auto results = helper.Runtime().AddObserver<TEvDataShard::TEvReadResult>([&](auto& ev) {
            deliveredRows += ev->Get()->GetRowsCount();
        });
        TBlockEvents<TEvDataShard::TEvReadContinue> blockedContinue(helper.Runtime());
        auto request = helper.MakeRead(1.0, 1);
        AddRangeQuery<ui32>(*request, {0}, true, {30}, true);
        SendReadAsync(helper.Server, helper.TabletId(), request.release(), helper.Sender);
        helper.Runtime().WaitFor("selected unit's first data-page fault", [&] {
            return dataPages > 0 && !blockedContinue.empty();
        });
        UNIT_ASSERT_VALUES_EQUAL(deliveredRows, 0u);
        helper.Compact();
        blockedContinue.Stop().Unblock();
        const auto read = helper.Collect();
        UNIT_ASSERT(read.Finished);
        UNIT_ASSERT_VALUES_EQUAL(read.Rows.size(), 31u);
        for (ui32 key = 0; key < read.Rows.size(); ++key) {
            UNIT_ASSERT_VALUES_EQUAL(read.Rows[key].Key, key);
            UNIT_ASSERT_VALUES_EQUAL(read.Rows[key].Value, key);
        }
        UNIT_ASSERT_VALUES_EQUAL(read.Stats.GetParts(), 1u);
        // The decision made before any row was available must survive the new layout.
        UNIT_ASSERT_VALUES_EQUAL(read.Stats.GetUnitsTotal(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(read.Stats.GetUnitsSelected(), 1u);
    }

    // Both ranges share a unit and execute without yielding, so the second must seek.
    Y_UNIT_TEST(StraddlingUnitReadInNextRange) {
        TSamplingTestHelper helper;
        helper.UpsertRange(0, 8);
        auto request = helper.MakeRead(1.0, 1);
        request->Record.MutableSampling()->SetMemtableStride(1000000);
        AddRangeQuery<ui32>(*request, {0}, true, {3}, true);
        AddRangeQuery<ui32>(*request, {5}, true, {7}, true);
        auto both = helper.ReadShard(std::move(request));
        UNIT_ASSERT(both.Finished);
        UNIT_ASSERT_VALUES_EQUAL(both.Stats.GetExecutions(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(both.Stats.GetUnitsTotal(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(both.Stats.GetUnitsMemtable(), 2u);
        const ui32 expected[] = {0, 1, 2, 3, 5, 6, 7};
        UNIT_ASSERT_VALUES_EQUAL(both.Rows.size(), std::size(expected));
        for (size_t i = 0; i < both.Rows.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(both.Rows[i].Key, expected[i]);
        }
    }

    Y_UNIT_TEST_TWIN(ProgressDoesNotLeakAcrossRanges, coldCache) {
        TSamplingTestHelper helper({.SmallPages = true, .ColdCache = coldCache});
        helper.UpsertRange(0, 8);
        helper.Compact();
        if (coldCache) {
            RebootTablet(helper.Runtime(), helper.TabletId(), helper.Sender);
        }
        auto request = helper.MakeRead(1.0, 2);
        AddRangeQuery<ui32>(*request, {0}, false, {0}, false);
        AddRangeQuery<ui32>(*request, {0}, true, {3}, true);
        AddRangeQuery<ui32>(*request, {4}, false, {5}, false);
        AddRangeQuery<ui32>(*request, {5}, true, {7}, true);
        auto both = helper.ReadShard(std::move(request), 0, Max<ui32>(), 0, 1);
        auto left = helper.ReadRange(1.0, 2, 0, true, 3, true);
        auto right = helper.ReadRange(1.0, 2, 5, true, 7, true);
        TVector<TSamplingRow> separate = left.Rows;
        separate.insert(separate.end(), right.Rows.begin(), right.Rows.end());
        AssertSameRows(both.Rows, separate);
        for (const auto& result : both.Results) {
            if (!result.HasContinuationToken()) {
                continue;
            }
            NKikimrTxDataShard::TReadContinuationToken token;
            UNIT_ASSERT(token.ParseFromString(result.GetContinuationToken()));
            if (token.GetFirstUnprocessedQuery() > 0 && token.GetLastProcessedKey().empty()) {
                UNIT_ASSERT(!token.GetSampling().HasPendingSelectedUnit());
            }
        }
    }

    Y_UNIT_TEST(CancelAndRestartFromToken) {
        TSamplingTestHelper helper({.SmallPages = true});
        helper.UpsertBatch(0, 10);
        helper.Compact();
        const auto expected = helper.ReadAll(1.0, 6);
        auto request = helper.MakeRead(1.0, 6);
        AddFullRangeQuery(*request);
        auto first = helper.ReadShard(std::move(request), 0, 1, 1, 1);
        UNIT_ASSERT(!first.Finished);
        UNIT_ASSERT(first.Results[0].GetLimitReached());
        const ui64 oldReadId = first.Results[0].GetReadId();
        ui32 oldResults = 0;
        auto observer = helper.Runtime().AddObserver<TEvDataShard::TEvReadResult>([&](auto& ev) {
            if (ev->GetRecipientRewrite() == helper.Sender && ev->Get()->Record.GetReadId() == oldReadId) {
                ++oldResults;
                ev.Reset();
            }
        });
        helper.Cancel(first.Results[0]);
        helper.Runtime().SimulateSleep(TDuration::MilliSeconds(50));
        // ACK must not revive a cancelled iterator.
        helper.Ack(first.Results[0]);
        helper.Upsert(5, 999);
        helper.Upsert(100, 100);
        auto rest = helper.Resume(first.Results[0], 1.0, 6, 0, true, 100, true);
        UNIT_ASSERT(rest.Finished);
        first.Rows.insert(first.Rows.end(), rest.Rows.begin(), rest.Rows.end());
        AssertSameRows(first.Rows, expected.Rows);
        helper.Runtime().SimulateSleep(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(oldResults, 0u);
        Y_UNUSED(observer);
    }

    Y_UNIT_TEST(CancelWhileWaitingForVolatileTransaction) {
        using namespace NKqpHelpers;
        TSamplingTestHelper helper({.Shards = 2});
        helper.Upsert(1, 10);
        helper.Upsert(3000000000u, 20);
        auto& runtime = helper.Runtime();
        runtime.GetAppData().FeatureFlags.SetEnableDataShardVolatileTransactions(true);
        TBlockEvents<TEvTxProcessing::TEvReadSet> blockedReadSets(runtime);
        auto write = KqpSimpleSend(runtime, R"(
            UPSERT INTO `/Root/table-1` (key, value)
            VALUES (2, 30), (3000000001, 40);
        )");
        runtime.WaitFor("volatile readsets", [&] { return blockedReadSets.size() >= 4; });
        ui64 step = 0;
        for (const auto& ev : blockedReadSets) {
            step = Max(step, ev->Get()->Record.GetStep());
        }
        UNIT_ASSERT(step > 0);

        TVector<THolder<IEventHandle>> results;
        auto readSender = runtime.Register(new TLambdaActor([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::TEvReadResult::EventType) {
                results.emplace_back(ev.Release());
            }
        }));
        auto startRead = [&] {
            auto request = helper.MakeRead(1.0, 1, false);
            const ui64 readId = request->Record.GetReadId();
            request->Record.MutableSnapshot()->SetStep(step);
            request->Record.MutableSnapshot()->SetTxId(Max<ui64>());
            AddFullRangeQuery(*request);
            ForwardToTablet(runtime, helper.TabletId(), readSender, request.release());
            return readId;
        };
        const ui64 cancelled = startRead();
        const ui64 surviving = startRead();
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT(results.empty());

        auto cancel = std::make_unique<TEvDataShard::TEvReadCancel>();
        cancel->Record.SetReadId(cancelled);
        ForwardToTablet(runtime, helper.TabletId(), readSender, cancel.release());
        runtime.SimulateSleep(TDuration::Seconds(1));
        blockedReadSets.Stop().Unblock();
        UNIT_ASSERT_VALUES_EQUAL(FormatResult(AwaitResponse(runtime, std::move(write))), "<empty>");
        runtime.WaitFor("uncanceled sampled read", [&] { return !results.empty(); });
        runtime.SimulateSleep(TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1u);
        const auto* result = results.front()->Get<TEvDataShard::TEvReadResult>();
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetReadId(), surviving);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(result->Record.GetFinished());
        AssertSameRows(TSamplingTestHelper::RowsOf(*result), TVector<TSamplingRow>{{1, 10}, {2, 30}});
    }

    Y_UNIT_TEST(SkippedReadDoesNotFetchDataPages) {
        TSamplingTestHelper helper({.SmallPages = true, .ColdCache = true});
        helper.UpsertBatch(0, 80);
        helper.Compact();
        helper.FixedSnapshot = CreateVolatileSnapshot(helper.Server, {"/Root/table-1"}, TDuration::Hours(1));
        RebootTablet(helper.Runtime(), helper.TabletId(), helper.Sender);
        ui32 dataPages = 0;
        auto observer = helper.CountDataPages(dataPages);
        auto request = helper.MakeRead(1e-6, 1);
        AddFullRangeQuery(*request);
        const auto read = helper.ReadShard(std::move(request));
        UNIT_ASSERT(read.Finished);
        UNIT_ASSERT(read.Rows.empty());
        UNIT_ASSERT_VALUES_EQUAL(read.Stats.GetUnitsSelected(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(dataPages, 0u);
        UNIT_ASSERT(read.Stats.GetUnitsTotal() > 64);
        UNIT_ASSERT(read.Stats.GetExecutions() > 1);
    }

    Y_UNIT_TEST(MemtableStrideOneContinuation) {
        TSamplingTestHelper helper;
        constexpr ui32 Rows = 40;
        helper.UpsertBatch(0, Rows);
        auto request = helper.MakeRead(1.0, 1);
        request->Record.MutableSampling()->SetMemtableStride(1);
        AddFullRangeQuery(*request);
        const auto read = helper.ReadShard(std::move(request), 0, Max<ui32>(), 0, 1);
        UNIT_ASSERT(read.Finished);
        UNIT_ASSERT_VALUES_EQUAL(read.Results[0].GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(read.Stats.GetParts(), 0u);
        UNIT_ASSERT(read.Stats.GetMemtables() > 0);
        UNIT_ASSERT_VALUES_EQUAL(read.Stats.GetIndexPagesTouched(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(read.Stats.GetOwnerMainGroupBytes(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(read.Rows.size(), Rows);
        UNIT_ASSERT(read.Results.size() > 1);
        for (ui32 i = 0; i < Rows; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(read.Rows[i].Key, i);
            // The prefix before key 0 is an empty unit.
            const auto& stats = read.Results[i].GetSamplingStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetUnitsTotal(), i + 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetUnitsSelected(), i + 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetUnitsMemtable(), i + 2);
        }
        UNIT_ASSERT_VALUES_EQUAL(read.Stats.GetUnitsTotal(), Rows + 1);
    }

    Y_UNIT_TEST(CompositeNullableKeysAndPrefixBounds) {
        TSamplingTestHelper helper;
        auto [shards, tableId] = CreateShardedTable(helper.Server, helper.Sender, "/Root", "composite",
            TShardedTableOptions().Columns({
                {"key1", "Uint32", true, false},
                {"key2", "Uint32", true, false},
                {"value", "Uint32", false, false},
            }));
        const ui64 shard = shards.at(0);
        ExecSQL(helper.Server, helper.Sender,
            "UPSERT INTO `/Root/composite` (key1, key2, value) VALUES "
            "(0, NULL, 0), (1, NULL, 10), (1, 0, 11), (1, 1, 12), (2, NULL, 20), (2, 1, 21);");
        auto [tables, owner] = GetTables(helper.Server, shard);
        Y_UNUSED(owner);
        const auto snapshot = CreateVolatileSnapshot(helper.Server, {"/Root/composite"}, TDuration::Hours(1));
        auto makeRequest = [&](bool sampled, bool fromInclusive = true, bool toInclusive = false) {
            auto request = GetBaseReadRequest(tableId, tables["composite"].GetDescription(),
                helper.NextReadId++, NKikimrDataEvents::FORMAT_CELLVEC, snapshot);
            if (sampled) {
                request->Record.MutableSampling()->SetRate(1.0);
                request->Record.MutableSampling()->SetSeed(7);
                request->Record.SetMaxRowsInResult(1);
            }
            AddRangeQuery<ui32>(*request, {1}, fromInclusive, {2}, toInclusive);
            return request;
        };
        auto read = [&](std::unique_ptr<TEvDataShard::TEvRead> request) {
            const bool chunked = request->Record.GetMaxRowsInResult() == 1;
            SendReadAsync(helper.Server, shard, request.release(), helper.Sender);
            TVector<TString> rows;
            for (ui32 attempt = 0; attempt < 16; ++attempt) {
                auto result = WaitReadResult(helper.Server, TDuration::Seconds(60));
                UNIT_ASSERT(result);
                UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
                UNIT_ASSERT(!chunked || result->GetRowsCount() <= 1);
                for (size_t i = 0; i < result->GetRowsCount(); ++i) {
                    rows.push_back(TSerializedCellVec::Serialize(result->GetCells(i)));
                }
                if (result->Record.GetFinished()) {
                    return rows;
                }
                if (result->Record.GetLimitReached()) {
                    auto* ack = new TEvDataShard::TEvReadAck();
                    ack->Record.SetReadId(result->Record.GetReadId());
                    ack->Record.SetSeqNo(result->Record.GetSeqNo());
                    ack->Record.SetMaxRows(Max<ui32>());
                    ack->Record.SetMaxBytes(Max<ui32>());
                    helper.Runtime().SendToPipe(shard, helper.Sender, ack, 0, GetPipeConfigWithRetries());
                }
            }
            UNIT_FAIL("composite read did not finish");
            return rows;
        };
        auto row = [](std::optional<ui32> second, ui32 value) {
            return TSerializedCellVec::Serialize(TVector<TCell>{
                TCell::Make(ui32(1)), second ? TCell::Make(*second) : TCell(), TCell::Make(value)});
        };
        const TVector<TString> expected{row(std::nullopt, 10), row(0, 11), row(1, 12)};
        const auto plain = read(makeRequest(false));
        UNIT_ASSERT(plain == expected);
        UNIT_ASSERT(read(makeRequest(true)) == plain);

        // Full exclusive bounds must compare the second cell, including nulls.
        auto exclusive = makeRequest(true);
        exclusive->Ranges.clear();
        AddRangeQuery<ui32>(*exclusive, {1, 0}, false, {2}, false);
        UNIT_ASSERT(read(std::move(exclusive)) == TVector<TString>{row(1, 12)});

        for (bool exclusiveStart : {false, true}) {
            auto rejected = makeRequest(true, !exclusiveStart, !exclusiveStart);
            SendReadAsync(helper.Server, shard, rejected.release(), helper.Sender);
            auto result = WaitReadResult(helper.Server, TDuration::Seconds(60));
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::BAD_REQUEST);
            UNIT_ASSERT_VALUES_EQUAL(result->GetRowsCount(), 0u);
        }
    }

    Y_UNIT_TEST(CursorAndClip) {
        const TVector<NScheme::TTypeInfo> types = {NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)};
        auto key = [](ui32 value) {
            return TSerializedCellVec(TVector<TCell>{TCell::Make(value)});
        };
        const TSamplingPos neg;
        const TSamplingPos before1{key(1), true};
        const TSamplingPos after1{key(1), false};
        const TSamplingPos pos{{}, false};
        UNIT_ASSERT(CompareSamplingPos(neg, before1, types) < 0);
        UNIT_ASSERT(CompareSamplingPos(before1, after1, types) < 0);
        UNIT_ASSERT(CompareSamplingPos(after1, pos, types) < 0);
        UNIT_ASSERT_VALUES_EQUAL(CompareSamplingPos(before1, before1, types), 0);

        NTable::TBounds bounds(key(1), key(5), true, false);
        auto clipped = ClipSamplingBounds(bounds, after1, TSamplingPos{key(5), true}, types);
        UNIT_ASSERT(clipped);
        UNIT_ASSERT(!clipped->FirstInclusive);
        auto empty = ClipSamplingBounds(bounds, TSamplingPos{key(5), true}, pos, types);
        UNIT_ASSERT(!empty);

        NKikimrTxDataShard::TReadSamplingBounds proto;
        SaveSamplingBounds(bounds, proto);
        SaveSamplingBounds({}, proto);
        TString error;
        UNIT_ASSERT_C(ParseSamplingBounds(proto, bounds, error, types), error);
        UNIT_ASSERT(SamplingStart(bounds).IsNegInf());
        UNIT_ASSERT(SamplingEnd(bounds).IsPosInf());

        UNIT_ASSERT_VALUES_EQUAL(SamplingThreshold(1.0), Max<ui64>());
        UNIT_ASSERT_VALUES_EQUAL(SamplingThreshold(0.5), ui64(1) << 63);
        UNIT_ASSERT_VALUES_EQUAL(SamplingThreshold(1e-20), ui64(1));
    }
}

} // NKikimr
