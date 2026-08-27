// Benchmarks the arena block sizes chosen for TEvDataShard::TEvPeriodicTableStats and
// TEvGetTableStatsResult (see ydb/core/tx/datashard/datashard.h) against a non-arena baseline
// over the same protobuf messages. Both sides go through the real TEventPBBase::Load() path
// (parse from a serialized buffer), which is what the schemeshard actually pays on every
// stats report from a datashard.

#include <library/cpp/testing/gbenchmark/benchmark.h>

#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/protos/tablet.pb.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/events.h>

using namespace NKikimr;
using namespace NActors;

namespace {

    // Non-arena baselines over the same wire format as the production events, to isolate the
    // arena's effect on Load() from everything else (parsing logic, message shape).
    struct TEvGetTableStatsResultNoArena
        : public TEventPB<TEvGetTableStatsResultNoArena,
                           NKikimrTxDataShard::TEvGetTableStatsResult,
                           EventSpaceBegin(TEvents::ES_PRIVATE) + 0> {
    };

    struct TEvPeriodicTableStatsNoArena
        : public TEventPB<TEvPeriodicTableStatsNoArena,
                           NKikimrTxDataShard::TEvPeriodicTableStats,
                           EventSpaceBegin(TEvents::ES_PRIVATE) + 1> {
    };

    NKikimrTableStats::THistogram MakeHistogram(size_t buckets) {
        NKikimrTableStats::THistogram hist;
        for (size_t i = 0; i < buckets; ++i) {
            auto* bucket = hist.AddBuckets();
            bucket->SetKey(TString("key_") + ToString(i));
            bucket->SetValue(i * 1000);
        }
        return hist;
    }

    // Scalar counters both TEvGetTableStatsResult and TEvPeriodicTableStats fill on their shared
    // NKikimrTableStats::TTableStats submessage, outside of the histogram/suggested-key and
    // per-channel fields that differ between the two event types.
    void FillCommonTableStats(NKikimrTableStats::TTableStats* stats) {
        stats->SetDataSize(123456789);
        stats->SetRowCount(987654);
        stats->SetIndexSize(45678);
        stats->SetByKeyFilterSize(4096);
        stats->SetInMemSize(890);
        stats->SetLastAccessTime(1700000000000ull);
        stats->SetLastUpdateTime(1700000000000ull);
        stats->SetPartCount(7);
        stats->SetSearchHeight(3);
        stats->SetHasSchemaChanges(false);
        stats->SetLastFullCompactionTs(1699999000ull);
        stats->SetHasLoanedParts(false);
    }

    // A shard always reports at least itself as a user table part owner; right after a
    // split/merge or copy, this list carries several historical owners. Templated because
    // TEvGetTableStatsResult and TEvPeriodicTableStats are unrelated proto messages that happen
    // to expose the same AddUserTablePartOwners/AddSysTablesPartOwners methods.
    template <typename TRec>
    void FillPartOwners(TRec& rec, size_t partOwners) {
        for (ui64 i = 0; i < partOwners; ++i) {
            rec.AddUserTablePartOwners(123456789 + i);
        }
        rec.AddSysTablesPartOwners(100);
        rec.AddSysTablesPartOwners(101);
    }

    // TabletMetrics submessage (resourceMetrics->Fill), shared verbatim between both event types:
    // aggregate CPU/Memory/Network/Storage plus one group read/write throughput+iops record per
    // channel.
    void FillTabletMetrics(NKikimrTabletBase::TMetrics* metrics, size_t channels) {
        metrics->SetCPU(1234567);
        metrics->SetMemory(89012345);
        metrics->SetNetwork(345678);
        metrics->SetStorage(999888777);
        for (size_t channel = 0; channel < channels; ++channel) {
            auto* readTp = metrics->AddGroupReadThroughput();
            readTp->SetChannel(channel);
            readTp->SetGroupID(2181038080 + channel);
            readTp->SetThroughput(10000 + channel);

            auto* writeTp = metrics->AddGroupWriteThroughput();
            writeTp->SetChannel(channel);
            writeTp->SetGroupID(2181038080 + channel);
            writeTp->SetThroughput(5000 + channel);

            auto* readIops = metrics->AddGroupReadIops();
            readIops->SetChannel(channel);
            readIops->SetGroupID(2181038080 + channel);
            readIops->SetIops(100 + channel);

            auto* writeIops = metrics->AddGroupWriteIops();
            writeIops->SetChannel(channel);
            writeIops->SetGroupID(2181038080 + channel);
            writeIops->SetIops(50 + channel);
        }
    }

    // KeyAccessSample is a fixed-size reservoir sample (TKeyAccessSample::SampleCount in
    // flat_stat_table.h), not affected by the requested histogram bucket count -- see
    // TEvGetTableStatsResult's definition comment in datashard.h.
    constexpr size_t KeyAccessSampleSize = 100;

    // Shape mirrors a real TEvGetTableStatsResult report as built by
    // TDataShard::TTxGetTableStats::Execute() (ydb/core/tx/datashard/datashard__stats.cpp): base
    // fields, part-owner lists (`partOwners` entries), the full scalar counter set the real
    // handler fills when stats are ready, a TabletMetrics submessage (resourceMetrics->Fill), and
    // RowCountHistogram (`buckets` entries -- unconditional, filled regardless of split-protocol
    // version).
    //
    // `useSuggestedKey` selects the split-protocol variant (FillSplitBySize/FillSplitByLoad):
    // DataSizeHistogram+KeyAccessSample and SplitBySizeSuggestedKey/SplitByLoadSuggestedKey are
    // both gated by the same dropHistogram flag, so a report carries exactly one of the two
    // shapes, never both --
    //   false: protocol version 0/1 (today's default) -- DataSizeHistogram (`buckets` entries)
    //          + KeyAccessSample (fixed `KeyAccessSampleSize` entries), no suggested-key fields.
    //   true:  protocol version 3 (dropHistogram on -- the near-future default) -- no
    //          DataSizeHistogram/KeyAccessSample, instead SplitBySizeSuggestedKey/
    //          SplitByLoadSuggestedKey.
    // See TEvGetTableStatsResult's definition comment in datashard.h for the full footprint
    // breakdown and the 500-bucket cap.
    TString MakeGetTableStatsResultPayload(size_t buckets, bool useSuggestedKey, size_t partOwners) {
        NKikimrTxDataShard::TEvGetTableStatsResult rec;
        rec.SetDatashardId(123456789);
        rec.SetTableOwnerId(111);
        rec.SetTableLocalId(222);
        rec.SetShardState(2);
        rec.SetFullStatsReady(true);
        rec.SetFollowerId(0);

        auto* stats = rec.MutableTableStats();
        if (useSuggestedKey) {
            stats->SetSplitProtocolVersion(3);
            // Serialized single-column split key, same order of magnitude as a real
            // TSerializedCellVec::Serialize() of one key column.
            stats->SetSplitBySizeSuggestedKey(TString(12, 'k'));
            stats->SetSplitByLoadSuggestedKey(TString(12, 'k'));
        } else {
            stats->SetSplitProtocolVersion(0);
            *stats->MutableDataSizeHistogram() = MakeHistogram(buckets);
            *stats->MutableKeyAccessSample() = MakeHistogram(KeyAccessSampleSize);
        }

        FillCommonTableStats(stats);
        *stats->MutableRowCountHistogram() = MakeHistogram(buckets);

        FillPartOwners(rec, partOwners);
        FillTabletMetrics(rec.MutableTabletMetrics(), /* channels */ 3);

        TString out;
        Y_ENSURE(rec.SerializeToString(&out));
        return out;
    }

    // Shape mirrors a real TEvPeriodicTableStats report as built by SendPeriodicTableStats
    // (ydb/core/tx/datashard/datashard_impl.h): single table, no histograms (per the comment at
    // its definition, those are only ever set on TEvGetTableStatsResult), but with per-channel
    // stats (`channels` entries — typically 3, up to 256), non-empty part-owner lists
    // (`partOwners` entries — always at least the shard itself; more after a split/merge or
    // copy), the full set of scalar counters SendPeriodicTableStats always fills (tx/lock/row
    // counters, timestamps, part/search-height), and a TabletMetrics submessage
    // (resourceMetrics->Fill) with per-channel throughput/iops records — one group per channel,
    // same `channels` count, since that's what a real shard with that many channels reports. No
    // nested `Tables` entries: a periodic report is normally for one table, so that dimension
    // isn't representative of steady-state traffic.
    TString MakePeriodicTableStatsPayload(size_t channels, size_t partOwners) {
        NKikimrTxDataShard::TEvPeriodicTableStats rec;
        rec.SetDatashardId(123456789);
        rec.SetTableOwnerId(111);
        rec.SetTableLocalId(222);
        rec.SetFollowerId(0);
        rec.SetGeneration(3);
        rec.SetRound(42);
        rec.SetShardState(2);
        rec.SetNodeId(17);
        rec.SetStartTime(1700000000000ull);

        auto* stats = rec.MutableTableStats();
        FillCommonTableStats(stats);

        for (size_t channel = 0; channel < channels; ++channel) {
            auto* item = stats->AddChannels();
            item->SetChannel(channel);
            item->SetDataSize(1000000 + channel * 137);
            item->SetIndexSize(50000 + channel * 11);
        }

        stats->SetImmediateTxCompleted(123456);
        stats->SetPlannedTxCompleted(7890);
        stats->SetTxRejectedByOverload(12);
        stats->SetTxRejectedBySpace(0);
        stats->SetTxCompleteLagMsec(5);
        stats->SetInFlightTxCount(2);

        stats->SetRowUpdates(456789);
        stats->SetRowDeletes(1234);
        stats->SetRowReads(987654321);
        stats->SetRangeReads(54321);
        stats->SetRangeReadRows(6543210);

        stats->SetLocksAcquired(321);
        stats->SetLocksWholeShard(2);
        stats->SetLocksBroken(1);

        FillPartOwners(rec, partOwners);
        FillTabletMetrics(rec.MutableTabletMetrics(), channels);

        TString out;
        Y_ENSURE(rec.SerializeToString(&out));
        return out;
    }

    template <typename TEv>
    void BenchLoad(benchmark::State& state, const TString& payload) {
        size_t bytes = 0;
        for (auto _ : state) {
            TEventSerializedData data(TString(payload), TEventSerializationInfo{});
            THolder<TEv> ev(TEv::Load(&data));
            benchmark::DoNotOptimize(ev.Get());
            bytes += payload.size();
        }
        state.SetBytesProcessed(bytes);
    }

    // Isolates destruction cost: parsing happens outside the timed region (PauseTiming), so what's
    // measured is exactly freeing the parsed message tree — one arena block free vs. individually
    // destructing every submessage (histogram buckets, nested tables).
    template <typename TEv>
    void BenchDestroy(benchmark::State& state, const TString& payload) {
        for (auto _ : state) {
            state.PauseTiming();
            TEventSerializedData data(TString(payload), TEventSerializationInfo{});
            TEv* ev = TEv::Load(&data);
            state.ResumeTiming();
            delete ev;
        }
    }

    // range(0) = histogram bucket count, range(1) = split-protocol variant (0 = DataSizeHistogram
    // + KeyAccessSample, 1 = SplitBySizeSuggestedKey/SplitByLoadSuggestedKey), range(2) =
    // UserTablePartOwners count (1 = steady state, 4 = representative post-split, 32 = deep
    // split-chain worst case).
    void LoadGetTableStatsResultArena(benchmark::State& state) {
        const TString payload = MakeGetTableStatsResultPayload(state.range(0), state.range(1), state.range(2));
        BenchLoad<TEvDataShard::TEvGetTableStatsResult>(state, payload);
    }

    void LoadGetTableStatsResultNoArena(benchmark::State& state) {
        const TString payload = MakeGetTableStatsResultPayload(state.range(0), state.range(1), state.range(2));
        BenchLoad<TEvGetTableStatsResultNoArena>(state, payload);
    }

    // range(0) = channel count, range(1) = UserTablePartOwners count (see LoadGetTableStatsResult*).
    void LoadPeriodicTableStatsArena(benchmark::State& state) {
        const TString payload = MakePeriodicTableStatsPayload(state.range(0), state.range(1));
        BenchLoad<TEvDataShard::TEvPeriodicTableStats>(state, payload);
    }

    void LoadPeriodicTableStatsNoArena(benchmark::State& state) {
        const TString payload = MakePeriodicTableStatsPayload(state.range(0), state.range(1));
        BenchLoad<TEvPeriodicTableStatsNoArena>(state, payload);
    }

    // range(0)/(1)/(2) as in LoadGetTableStatsResult*.
    void DestroyGetTableStatsResultArena(benchmark::State& state) {
        const TString payload = MakeGetTableStatsResultPayload(state.range(0), state.range(1), state.range(2));
        BenchDestroy<TEvDataShard::TEvGetTableStatsResult>(state, payload);
    }

    void DestroyGetTableStatsResultNoArena(benchmark::State& state) {
        const TString payload = MakeGetTableStatsResultPayload(state.range(0), state.range(1), state.range(2));
        BenchDestroy<TEvGetTableStatsResultNoArena>(state, payload);
    }

    // range(0)/(1) as in LoadPeriodicTableStats*.
    void DestroyPeriodicTableStatsArena(benchmark::State& state) {
        const TString payload = MakePeriodicTableStatsPayload(state.range(0), state.range(1));
        BenchDestroy<TEvDataShard::TEvPeriodicTableStats>(state, payload);
    }

    void DestroyPeriodicTableStatsNoArena(benchmark::State& state) {
        const TString payload = MakePeriodicTableStatsPayload(state.range(0), state.range(1));
        BenchDestroy<TEvPeriodicTableStatsNoArena>(state, payload);
    }

    // Bucket count: 0, 10 (typical), 50, 500 (the documented worst case) x split-protocol variant
    // (0 = DataSizeHistogram+KeyAccessSample, 1 = SplitBySizeSuggestedKey/SplitByLoadSuggestedKey)
    // x UserTablePartOwners count (1 = steady state, 4 = representative post-split, 32 = deep
    // split-chain worst case).
    BENCHMARK(LoadGetTableStatsResultArena)
        ->ArgsProduct({{0, 10, 50, 500}, {0, 1}, {1, 4, 32}})
        ->Unit(benchmark::kMicrosecond);
    BENCHMARK(LoadGetTableStatsResultNoArena)
        ->ArgsProduct({{0, 10, 50, 500}, {0, 1}, {1, 4, 32}})
        ->Unit(benchmark::kMicrosecond);
    BENCHMARK(DestroyGetTableStatsResultArena)
        ->ArgsProduct({{0, 10, 50, 500}, {0, 1}, {1, 4, 32}})
        ->Unit(benchmark::kMicrosecond);
    BENCHMARK(DestroyGetTableStatsResultNoArena)
        ->ArgsProduct({{0, 10, 50, 500}, {0, 1}, {1, 4, 32}})
        ->Unit(benchmark::kMicrosecond);

    // Channel count: 1 (single-channel table), 3 (typical), 256 (documented worst case) x
    // UserTablePartOwners count (see LoadGetTableStatsResult*).
    BENCHMARK(LoadPeriodicTableStatsArena)
        ->ArgsProduct({{1, 3, 256}, {1, 4, 32}})
        ->Unit(benchmark::kMicrosecond);
    BENCHMARK(LoadPeriodicTableStatsNoArena)
        ->ArgsProduct({{1, 3, 256}, {1, 4, 32}})
        ->Unit(benchmark::kMicrosecond);
    BENCHMARK(DestroyPeriodicTableStatsArena)
        ->ArgsProduct({{1, 3, 256}, {1, 4, 32}})
        ->Unit(benchmark::kMicrosecond);
    BENCHMARK(DestroyPeriodicTableStatsNoArena)
        ->ArgsProduct({{1, 3, 256}, {1, 4, 32}})
        ->Unit(benchmark::kMicrosecond);

} // namespace
