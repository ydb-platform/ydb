#include <ydb/core/tx/schemeshard/schemeshard__pipe_messages.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>

#include <ydb/core/mon_alloc/memory_info.h>
#include <ydb/core/mon_alloc/stats.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/library/actors/core/event_load.h>

#include <library/cpp/malloc/api/malloc.h>

#include <benchmark/benchmark.h>

#include <util/stream/output.h>
#include <util/string/builder.h>

#include <algorithm>
#include <vector>

using namespace NKikimr;
using namespace NKikimr::NSchemeShard;

// Bench-side declarations of the production staging seam (defined in
// schemeshard__operation_common_pq.cpp). There is no header on purpose — the
// bench is the only external consumer; a signature drift fails at link time.
namespace NKikimr::NSchemeShard::NPQState {

TString BuildPQConfigCommonPart(
    const TTopicInfo& pqGroup,
    const TString& topicName,
    const TString& topicPath,
    const TString& cloudId,
    const TString& folderId,
    const TString& databaseId,
    const TString& databasePath,
    const TString& monitoringProjectId
);

TString BuildPQBootstrapPart(const NKikimrPQ::TBootstrapConfig& bootstrapConfig);

TPipeMessageFragments MakePQProposeTransactionFragments(
    const TActorId& selfActorId,
    const THashMap<TShardIdx, TShardInfo>& shardInfos,
    TTxId txId,
    TTxState::ETxType txType,
    const TTopicInfo& pqGroup,
    const TRcBuf& configCommonPart,
    const TRcBuf& bootstrapPart,
    const TTopicTabletInfo& pqShard
);

}

namespace {

// ---------------------------------------------------------------------------
// Fabricating the incident shape without a live schemeshard.
//
// Incident: a cdc-stream-backed topic create with S = 35k pq-tablet shards,
// PartitionPerTablet = 2 (P = 70k), the bootstrap built over the source
// table's datashard list (one ExplicitMessageGroup per datashard).
// See plans/pq_configure_parts_oom.md §1 and plans/pq_configure_parts_oom_bench.md.
//
// What is measured, per one topic TConfigureParts round (one full
// ProgressState execution): allocator-stat deltas while
// (a) building the round's shared parts (common config, serialized bootstrap),
// (b) staging all per-shard messages,
// (c) parked with the round-scoped parts dropped ("steady" — the resident bytes),
// (d) parked after materializing everything (the DoBindMsg form),
// (e) after teardown (sanity, expect ~0).
//
// The staged fragment lists are also walked once to report gross vs unique
// payload bytes: round-shared parts must be counted once, which pins the
// sharing invariant without involving the allocator at all.
// ---------------------------------------------------------------------------

constexpr ui64 OWNER_ID = 1;
constexpr ui64 FIRST_TABLET_ID = 1000;
constexpr double MIB = 1024.0 * 1024.0;

// A serialized TPQTabletConfig padded to a realistic size: the topic-level
// invariant body carried by every shard message (~KBs in production).
static TString MakeTabletConfig(size_t wantedBytes) {
    NKikimrPQ::TPQTabletConfig config;
    config.MutablePartitionConfig()->SetTotalPartitions(1);
    size_t filled = 0;
    ui32 i = 0;
    while (filled < wantedBytes) {
        TString clientId = TStringBuilder() << "consumer-" << (i++) << "-" << TString(96, 'x');
        config.MutablePartitionConfig()->AddImportantClientId_Deprecated(clientId);
        filled += clientId.size() + 4;
    }
    return config.SerializeAsString();
}

static TString MakeBound(size_t bytes) {
    return TString(bytes, 'r');
}

struct TFixture {
    TTopicInfo::TPtr Topic;
    THashMap<TShardIdx, TShardInfo> ShardInfos;
};

static TFixture MakeFixture(ui32 shards, ui32 partsPerTablet) {
    TFixture f;
    f.Topic = MakeIntrusive<TTopicInfo>();
    f.Topic->TotalGroupCount = ui64(shards) * partsPerTablet;
    f.Topic->MaxPartsPerTablet = partsPerTablet;
    f.Topic->TabletConfig = MakeTabletConfig(8 * 1024);

    ui32 pqId = 0;
    for (ui32 s = 0; s < shards; ++s) {
        const TShardIdx idx(OWNER_ID, s);
        for (ui32 k = 0; k < partsPerTablet; ++k, ++pqId) {
            auto* p = new TTopicTabletInfo::TTopicPartitionInfo();
            p->PqId = pqId;
            p->GroupId = 0;
            p->CreateVersion = 1;
            p->Status = NKikimrPQ::ETopicPartitionStatus::Active;
            TTopicTabletInfo::TKeyRange keyRange;
            keyRange.FromBound = MakeBound(48);
            keyRange.ToBound = MakeBound(48);
            p->KeyRange = keyRange;
            f.Topic->AddPartition(idx, p);
        }
        f.ShardInfos[idx].TabletID = TTabletId(FIRST_TABLET_ID + s);
    }
    return f;
}

// The cdc bootstrap: one ExplicitMessageGroup per source-table datashard
// (id = encoded tablet id, key-range bounds) — the ~2-5 MB term of the
// incident at 35k datashards.
static NKikimrPQ::TBootstrapConfig MakeBootstrapConfig(ui64 entries) {
    Y_ABORT_UNLESS(entries > 0);
    NKikimrPQ::TBootstrapConfig bootstrapConfig;
    for (ui64 i = 0; i < entries; ++i) {
        auto& mg = *bootstrapConfig.AddExplicitMessageGroups();
        mg.SetId(NPQ::NSourceIdEncoding::EncodeSimple(ToString(FIRST_TABLET_ID + i)));

        if (i != entries - 1) {
            mg.MutableKeyRange()->SetToBound(MakeBound(48));
        }

        if (i) {
            mg.MutableKeyRange()->SetFromBound(MakeBound(48));
        }
    }
    return bootstrapConfig;
}

// ---------------------------------------------------------------------------
// Measurement.
// ---------------------------------------------------------------------------

using TAllocPoint = NKikimr::IAllocState::TState;

static i64 AllocDiff(const TAllocPoint& after, const TAllocPoint& before) {
    return i64(after.AllocatedMemory) - i64(before.AllocatedMemory);
}

static i64 AnonRss() {
    NKikimr::NMemory::TProcessMemoryInfo info = NKikimr::NMemory::TProcessMemoryInfoProvider{}.Get();
    return info.AnonRss ? i64(*info.AnonRss) : 0;
}

struct TRoundStat {
    i64 build = 0;        // building the round's shared parts (common config, serialized bootstrap)
    i64 stage = 0;        // staging all per-shard messages (per-shard temporaries already recycled)
    i64 steady = 0;       // parked bytes: round-scoped parts dropped, fragments not yet materialized
    i64 materialize = 0;  // parked bytes after materializing everything (DoBindMsg form)
    i64 teardown = 0;     // sanity: after dropping everything (expect ~0)

    i64 rssBuild = 0;
    i64 rssSteady = 0;
    i64 rssMaterialize = 0;

    ui64 grossFragmentBytes = 0;   // Σ fragment sizes over all messages (shared parts counted per message)
    ui64 uniqueFragmentBytes = 0;  // Σ sizes of distinct payload buffers (shared parts counted once)
};

// One parked message in both parked forms: the staged fragment list
// (pre-materialization; production: TPreSerializedMessage::Deferred) and the
// composed TEventSerializedData (post-materialization; production: .Data).
// MaterializeInPlace reproduces TOperation::TPreSerializedMessage::Materialize
// (schemeshard__pipe_messages.cpp) verbatim so the bench has no dependency on
// per-tree operation internals and stays identical on both trees.
struct TParkedMessage {
    TVector<TRcBuf> Fragments;
    TIntrusivePtr<NActors::TEventSerializedData> Data;
};

static void MaterializeInPlace(TParkedMessage& msg) {
    Y_ABORT_UNLESS(!msg.Data);
    Y_ABORT_UNLESS(!msg.Fragments.empty());

    TRope rope;
    for (const auto& fragment : msg.Fragments) {
        if (fragment.GetSize()) {
            rope.Insert(rope.End(), TRope(fragment));
        }
    }

    NActors::TEventSerializationInfo info;
    msg.Data = MakeIntrusive<NActors::TEventSerializedData>(std::move(rope), std::move(info));
    // Production resets the whole Deferred optional, freeing the fragment
    // vector storage — free it here too, or the retained handle capacity
    // pollutes the materialization delta.
    TVector<TRcBuf>().swap(msg.Fragments);
}

// Mirrors the production parked container (TOperation::PipeBindedMessages,
// keyed (tablet, cookie); the cookie here is ui64 instead of TPipeMessageId).
using TParkedMessages = THashMap<TTabletId, TMap<ui64, TParkedMessage>>;

static TRoundStat DoRound(
    const TFixture& f,
    const TString& topicName,
    const TString& topicPath,
    ui64 bootstrapEntries, // 0 for plain topics
    TTxId txId
) {
    TRoundStat stat;

    TTopicInfo& topic = *f.Topic;
    const TTxState::ETxType txType = TTxState::TxCreatePQGroup;
    static const TActorId selfId; // dummy: only the wire field matters, not its bytes

    const TAllocPoint base = NKikimr::TAllocState::Get();
    const i64 rssBase = AnonRss();

    // Declared outside the staging scope: parked entries survive the pass,
    // exactly like the production PipeBindedMessages.
    TParkedMessages parked;

    {
        // Round-scoped shared parts + staging (production: ProgressState).
        // A fresh bootstrap per round, exactly like production: the proto
        // tree and the serialized blob are paid by this round's build delta,
        // and the blob stays alive into the parked phase via the fragment
        // handles.
        NKikimrPQ::TBootstrapConfig bootstrapConfig;
        if (bootstrapEntries) {
            bootstrapConfig = MakeBootstrapConfig(bootstrapEntries);
        }

        TRcBuf configCommonPart(NPQState::BuildPQConfigCommonPart(
            topic,
            topicName,
            topicPath,
            "bench-cloud", "bench-folder", "bench-database", "/Root", ""));

        TRcBuf bootstrapPart;
        if (bootstrapConfig.ExplicitMessageGroupsSize() > 0) {
            bootstrapPart = MakePipeMessageFragment(NPQState::BuildPQBootstrapPart(bootstrapConfig));
        }

        const TAllocPoint afterBuild = NKikimr::TAllocState::Get();
        stat.build = AllocDiff(afterBuild, base);
        stat.rssBuild = AnonRss() ? AnonRss() - rssBase : 0;

        for (const auto& [idx, pqShardPtr] : topic.Shards) {
            auto fragments = NPQState::MakePQProposeTransactionFragments(
                selfId, f.ShardInfos, txId, txType, topic, configCommonPart,
                bootstrapPart, *pqShardPtr);

            const TTabletId tabletId = f.ShardInfos.at(idx).TabletID;
            const ui64 cookie = ui64(idx.GetLocalId());
            TParkedMessage msg;
            msg.Fragments = std::move(fragments.Items);
            parked[tabletId][cookie] = std::move(msg);
        }

        const TAllocPoint afterStage = NKikimr::TAllocState::Get();
        stat.stage = AllocDiff(afterStage, afterBuild);

        // Payload accounting over the parked fragment lists: shared buffers
        // (common part, bootstrap) are counted once in the unique sum.
        // Keyed by pointer IDENTITY: THashSet<const char*> would hash/compare
        // the C-string content, and every message shares the same zero-filled
        // SourceActor prefix, collapsing all bodies into one key.
        THashSet<size_t> seen;
        for (auto& [tabletId, msgs] : parked) {
            Y_UNUSED(tabletId);
            for (auto& [cookie, msg] : msgs) {
                Y_UNUSED(cookie);
                for (const TRcBuf& fragment : msg.Fragments) {
                    stat.grossFragmentBytes += fragment.GetSize();
                    if (seen.insert(reinterpret_cast<size_t>(fragment.GetData())).second) {
                        stat.uniqueFragmentBytes += fragment.GetSize();
                    }
                }
            }
        }
    } // round-scoped parts dropped here (ProgressState returned)

    const TAllocPoint steady = NKikimr::TAllocState::Get();
    stat.steady = AllocDiff(steady, base);
    stat.rssSteady = AnonRss() ? AnonRss() - rssBase : 0;

    // Production: TSideEffects::DoBindMsg materializes on the send.
    for (auto& [tabletId, msgs] : parked) {
        Y_UNUSED(tabletId);
        for (auto& [cookie, msg] : msgs) {
            Y_UNUSED(cookie);
            MaterializeInPlace(msg);
        }
    }

    const TAllocPoint afterMaterialize = NKikimr::TAllocState::Get();
    stat.materialize = AllocDiff(afterMaterialize, steady);
    stat.rssMaterialize = AnonRss() ? AnonRss() - rssBase : 0;

    // Swap with an empty map: clear() destroys the values but retains the
    // outer bucket array, which would show up as an O(shards) residual in
    // the teardown sanity delta (production frees the whole map with the
    // operation).
    TParkedMessages().swap(parked);
    const TAllocPoint afterTeardown = NKikimr::TAllocState::Get();
    stat.teardown = AllocDiff(afterTeardown, base);

    return stat;
}

static double Median(TVector<i64>& values) {
    Y_ABORT_UNLESS(!values.empty());
    std::sort(values.begin(), values.end());
    return double(values[values.size() / 2]);
}

static double MedianU(TVector<ui64>& values) {
    Y_ABORT_UNLESS(!values.empty());
    std::sort(values.begin(), values.end());
    return double(values[values.size() / 2]);
}

static void PrintSummary(
    ui32 shards, ui32 partsPerTablet, ui64 bootstrapEntries,
    const TFixture& fixture,
    const TRoundStat& warmUp,
    TVector<i64>& build, TVector<i64>& stage, TVector<i64>& steady,
    TVector<i64>& materialize, TVector<i64>& teardown,
    TVector<i64>& rssSteady, TVector<i64>& rssMaterialize,
    TVector<ui64>& gross, TVector<ui64>& unique)
{
    Cout << "== configure-parts round: S=" << shards
         << " p=" << partsPerTablet
         << " B=" << bootstrapEntries
         << " ==" << (bootstrapEntries ? " cdc" : " plain") << "\n";
    Cout << "  fixture: tabletConfig=" << fixture.Topic->TabletConfig.size() << " B"
         << " warmUp: build=" << warmUp.build / MIB << " MiB"
         << " stage=" << warmUp.stage / MIB << " MiB"
         << " steady=" << warmUp.steady / MIB << " MiB\n";
    Cout << "  alloc deltas (medians):"
         << " build=" << Median(build) / MIB << " MiB"
         << " stage=" << Median(stage) / MIB << " MiB"
         << " steady(parked)=" << Median(steady) / MIB << " MiB"
         << " materialize=" << Median(materialize) / MIB << " MiB"
         << " teardown=" << Median(teardown) / MIB << " MiB\n";
    Cout << "  per-shard slopes:"
         << " stage=" << Median(stage) / shards << " B/shard"
         << " steady=" << Median(steady) / shards << " B/shard\n";
    Cout << "  fragments: gross=" << MedianU(gross) / MIB << " MiB"
         << " unique=" << MedianU(unique) / MIB << " MiB\n";
    Cout << "  rss deltas (medians):"
         << " steady=" << Median(rssSteady) / MIB << " MiB"
         << " materialize=" << Median(rssMaterialize) / MIB << " MiB\n";
    Cout.Flush();
}

// ---------------------------------------------------------------------------
// The benchmark.
// ---------------------------------------------------------------------------

static void BenchConfigurePartsRound(::benchmark::State& state) {
    const ui32 shards = ui32(state.range(0));
    const ui32 partsPerTablet = ui32(state.range(1));
    const ui64 bootstrapEntries = ui64(state.range(2));
    if (bootstrapEntries) {
        Y_ABORT_UNLESS(bootstrapEntries == shards,
            "the bootstrap is built over the source table's datashard list (B == S)");
    }

    NMalloc::MallocInfo().SetParam("FillMemoryOnAllocation", "false");

    TFixture fixture = MakeFixture(shards, partsPerTablet);

    const TString topicName = "bench-topic";
    const TString topicPath = "/Root/bench-topic";

    // Warm-up round, discarded.
    TRoundStat warmUp = DoRound(fixture, topicName, topicPath, bootstrapEntries, TTxId(42));
    Y_UNUSED(warmUp);

    TVector<i64> build, stage, steady, materialize, teardown;
    TVector<i64> rssSteady, rssMaterialize;
    TVector<ui64> gross, unique;

    for (auto _ : state) {
        Y_UNUSED(_);
        TRoundStat stat = DoRound(fixture, topicName, topicPath, bootstrapEntries, TTxId(42));

        build.push_back(stat.build);
        stage.push_back(stat.stage);
        steady.push_back(stat.steady);
        materialize.push_back(stat.materialize);
        teardown.push_back(stat.teardown);
        rssSteady.push_back(stat.rssSteady);
        rssMaterialize.push_back(stat.rssMaterialize);
        gross.push_back(stat.grossFragmentBytes);
        unique.push_back(stat.uniqueFragmentBytes);
    }

    PrintSummary(shards, partsPerTablet, bootstrapEntries, fixture, warmUp,
                 build, stage, steady, materialize, teardown,
                 rssSteady, rssMaterialize, gross, unique);
}

} // namespace

BENCHMARK(BenchConfigurePartsRound)
    ->ArgNames({"shards", "partsPerTablet", "bootstrapEntries"})
    ->Args({128, 2, 128})        // smoke row
    ->Args({1'000, 2, 0})
    ->Args({1'000, 2, 1'000})
    ->Args({4'000, 2, 0})
    ->Args({4'000, 2, 4'000})
    ->Args({16'000, 2, 0})
    ->Args({16'000, 2, 16'000})
    ->Args({35'000, 2, 0})
    ->Args({35'000, 2, 35'000})
    ->Args({1'000, 100, 0})
    ->Args({1'000, 100, 1'000});
