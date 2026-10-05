#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/hp_timer.h>

using namespace NKikimr;
using namespace NSchemeShard;

namespace {

TVector<TTableShardInfo> MakeShards(ui32 n, ui64 ownerId = 1) {
    TVector<TTableShardInfo> v;
    v.reserve(n);
    for (ui32 i = 0; i < n; ++i) {
        TString range = (i + 1 < n) ? TString(1, char(i + 1)) : TString{};
        v.emplace_back(TShardIdx(ownerId, i), range);
    }
    return v;
}

TTableInfo::TPtr MakeTable() {
    return TTableInfo::TPtr(new TTableInfo());
}

void EnableTTL(TTableInfo& info) {
    info.MutableTTLSettings().MutableEnabled()->SetColumnName("ts");
}

} // namespace

Y_UNIT_TEST_SUITE(TTableInfoTest) {

// --- SetPartitioning ---

Y_UNIT_TEST(SetPartitioning_BasicStructure) {
    auto info = MakeTable();
    info->SetPartitioning(MakeShards(3));

    const auto& parts = info->GetPartitions();
    UNIT_ASSERT_VALUES_EQUAL(parts.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().PartitionStats.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().Aggregated.PartCount, 3u);
    for (ui32 i = 0; i < 3; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(parts[i]->Position, i);
    }
    // No TTL — schedule must be empty.
    UNIT_ASSERT(info->GetInFlightCondErase().empty());
}

Y_UNIT_TEST(SetPartitioning_WithTTL) {
    auto info = MakeTable();
    EnableTTL(*info);
    info->SetPartitioning(MakeShards(4));

    // VerifyConsistency inside SetPartitioning checks schedule size == partition count.
    // With no shards in-flight, all 4 must be in the schedule.
    UNIT_ASSERT(info->GetInFlightCondErase().empty());
    UNIT_ASSERT_VALUES_EQUAL(info->GetPartitions().size(), 4u);
}

Y_UNIT_TEST(SetPartitioning_ExpectedPartitionCount) {
    auto info = MakeTable();
    info->SetPartitioning(MakeShards(5));
    UNIT_ASSERT_VALUES_EQUAL(info->GetExpectedPartitionCount(), 5u);
}

// --- MovePartitioning ---

Y_UNIT_TEST(MovePartitioning_PreservesStats) {
    auto info = MakeTable();
    info->SetPartitioning(MakeShards(3));

    THashSet<TShardIdx> before;
    for (const auto& [idx, _] : info->GetStats().PartitionStats) {
        before.insert(idx);
    }

    info->MovePartitioning(MakeShards(3));

    // Same shard indices must remain in Stats.
    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().PartitionStats.size(), 3u);
    for (const auto& [idx, _] : info->GetStats().PartitionStats) {
        UNIT_ASSERT_C(before.contains(idx), "Unexpected shard in stats after MovePartitioning");
    }
}

Y_UNIT_TEST(MovePartitioning_PreservesStatsValues) {
    auto info = MakeTable();
    info->SetPartitioning(MakeShards(3));

    // Inject non-zero stats into shard 1.
    TPartitionStats s;
    s.SeqNo = TMessageSeqNo{1, 0};
    s.RowCount = 42;
    s.DataSize = 1000;
    TDiskSpaceUsageDelta delta;
    info->UpdateShardStats(&delta, TShardIdx(1, 1), s, TInstant::Zero());

    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().Aggregated.RowCount, 42u);
    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().Aggregated.DataSize, 1000u);

    info->MovePartitioning(MakeShards(3));

    // Stats values must survive the move — MovePartitioning must not touch PartitionStats.
    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().PartitionStats.at(TShardIdx(1, 1)).RowCount, 42u);
    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().PartitionStats.at(TShardIdx(1, 1)).DataSize, 1000u);
    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().Aggregated.RowCount, 42u);
    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().Aggregated.DataSize, 1000u);
}

Y_UNIT_TEST(MovePartitioning_RebuildsPositions) {
    auto info = MakeTable();
    info->SetPartitioning(MakeShards(3));
    info->MovePartitioning(MakeShards(3));

    const auto& parts = info->GetPartitions();
    for (ui32 i = 0; i < parts.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(parts[i]->Position, i);
    }
}

Y_UNIT_TEST(MovePartitioning_TTL_ClearsInFlight) {
    auto info = MakeTable();
    EnableTTL(*info);

    // Shard 0 gets NextCondErase=0 so it will be heap-top.
    TVector<TTableShardInfo> shards;
    shards.emplace_back(TShardIdx(1, 0), TString(1, '\x01'), 0, 0);
    shards.emplace_back(TShardIdx(1, 1), TString(1, '\x02'), 0, 1000);
    shards.emplace_back(TShardIdx(1, 2), TString{},          0, 2000);
    info->SetPartitioning(std::move(shards));

    // Move the earliest shard into in-flight.
    const auto* top = info->GetScheduledCondEraseShard();
    UNIT_ASSERT(top);
    info->AddInFlightCondErase(top->ShardIdx);
    UNIT_ASSERT_VALUES_EQUAL(info->GetInFlightCondErase().size(), 1u);

    TVector<TTableShardInfo> newShards;
    newShards.emplace_back(TShardIdx(1, 0), TString(1, '\x01'), 0, 0);
    newShards.emplace_back(TShardIdx(1, 1), TString(1, '\x02'), 0, 1000);
    newShards.emplace_back(TShardIdx(1, 2), TString{},          0, 2000);
    info->MovePartitioning(std::move(newShards));

    // In-flight must be cleared; VerifyConsistency checks all 3 are scheduled.
    UNIT_ASSERT(info->GetInFlightCondErase().empty());
}

// --- CopyPartitioning ---

Y_UNIT_TEST(CopyPartitioning_ClearsOldStats) {
    auto info = MakeTable();
    info->SetPartitioning(MakeShards(3, 1));  // ownerId=1

    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().PartitionStats.size(), 3u);

    // Copy with entirely different ShardIdx (ownerId=2).
    info->CopyPartitioning(MakeShards(3, 2));

    const auto& stats = info->GetStats().PartitionStats;
    UNIT_ASSERT_VALUES_EQUAL(stats.size(), 3u);
    for (const auto& [idx, _] : stats) {
        UNIT_ASSERT_VALUES_EQUAL(idx.GetOwnerId(), (ui64)2);
    }
}

Y_UNIT_TEST(CopyPartitioning_ResetsPartCount) {
    auto info = MakeTable();
    info->SetPartitioning(MakeShards(3));
    info->CopyPartitioning(MakeShards(5));

    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().Aggregated.PartCount, 5u);
    UNIT_ASSERT_VALUES_EQUAL(info->GetPartitions().size(), 5u);
}

Y_UNIT_TEST(CopyPartitioning_TTL_ReschedulesNewShards) {
    auto info = MakeTable();
    EnableTTL(*info);
    info->SetPartitioning(MakeShards(2, 1));

    info->CopyPartitioning(MakeShards(3, 2));

    // VerifyConsistency inside CopyPartitioning checks all 3 new shards are scheduled.
    UNIT_ASSERT(info->GetInFlightCondErase().empty());
    UNIT_ASSERT_VALUES_EQUAL(info->GetPartitions().size(), 3u);
}

// --- ApplySplitMerge ---

Y_UNIT_TEST(ApplySplitMerge_Split_1to2) {
    auto info = MakeTable();
    // Shards: A(1/0, '\x01'), B(1/1, '\x02'), C(1/2, '')
    info->SetPartitioning(MakeShards(3));

    // Split B (position 1) into B1 and B2.
    TVector<TTableShardInfo> dst;
    dst.emplace_back(TShardIdx(2, 0), TString(1, '\x01'), 0, 0);
    dst.emplace_back(TShardIdx(2, 1), TString(1, '\x02'), 0, 0);
    TVector<TShardIdx> removed = {TShardIdx(1, 1)};

    info->ApplySplitMerge(std::move(dst), removed, /*splitFirstIdx=*/1, TInstant::Zero(), /*trackSplitMergeDemand=*/true,
        /*loadSplitLineage=*/false);

    const auto& parts = info->GetPartitions();
    UNIT_ASSERT_VALUES_EQUAL(parts.size(), 4u);
    for (ui32 i = 0; i < parts.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(parts[i]->Position, i);
    }
    const auto& stats = info->GetStats().PartitionStats;
    UNIT_ASSERT(!stats.contains(TShardIdx(1, 1)));   // B gone
    UNIT_ASSERT(stats.contains(TShardIdx(2, 0)));    // B1 present
    UNIT_ASSERT(stats.contains(TShardIdx(2, 1)));    // B2 present
    UNIT_ASSERT(stats.contains(TShardIdx(1, 0)));    // A preserved
    UNIT_ASSERT(stats.contains(TShardIdx(1, 2)));    // C preserved
}

Y_UNIT_TEST(ApplySplitMerge_Merge_2to1) {
    auto info = MakeTable();
    // Shards: A(1/0, '\x01'), B(1/1, '\x02'), C(1/2, '')
    info->SetPartitioning(MakeShards(3));

    // Merge A+B into AB.
    TVector<TTableShardInfo> dst;
    dst.emplace_back(TShardIdx(2, 0), TString(1, '\x02'), 0, 0);
    TVector<TShardIdx> removed = {TShardIdx(1, 0), TShardIdx(1, 1)};

    info->ApplySplitMerge(std::move(dst), removed, /*splitFirstIdx=*/0, TInstant::Zero(), /*trackSplitMergeDemand=*/true,
        /*loadSplitLineage=*/false);

    const auto& parts = info->GetPartitions();
    UNIT_ASSERT_VALUES_EQUAL(parts.size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(parts[0]->Position, 0u);
    UNIT_ASSERT_VALUES_EQUAL(parts[1]->Position, 1u);
    const auto& stats = info->GetStats().PartitionStats;
    UNIT_ASSERT(!stats.contains(TShardIdx(1, 0)));   // A gone
    UNIT_ASSERT(!stats.contains(TShardIdx(1, 1)));   // B gone
    UNIT_ASSERT(stats.contains(TShardIdx(2, 0)));    // AB present
    UNIT_ASSERT(stats.contains(TShardIdx(1, 2)));    // C preserved
}

Y_UNIT_TEST(ApplySplitMerge_RightShiftPositions) {
    auto info = MakeTable();
    // Shards: A(1/0), B(1/1), C(1/2), D(1/3)
    info->SetPartitioning(MakeShards(4));

    // Split A (position 0) into A1+A2 — B, C, D shift right by 1.
    TVector<TTableShardInfo> dst;
    dst.emplace_back(TShardIdx(2, 0), TString(1, '\x01'), 0, 0);
    dst.emplace_back(TShardIdx(2, 1), TString(1, '\x02'), 0, 0);
    TVector<TShardIdx> removed = {TShardIdx(1, 0)};

    info->ApplySplitMerge(std::move(dst), removed, /*splitFirstIdx=*/0, TInstant::Zero(), /*trackSplitMergeDemand=*/true,
        /*loadSplitLineage=*/false);

    const auto& parts = info->GetPartitions();
    UNIT_ASSERT_VALUES_EQUAL(parts.size(), 5u);
    for (ui32 i = 0; i < parts.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(parts[i]->Position, i);
    }
}

Y_UNIT_TEST(ApplySplitMerge_AggregatedStatsSubtracted) {
    auto info = MakeTable();
    // Shards: A(1/0), B(1/1), C(1/2)
    info->SetPartitioning(MakeShards(3));

    // Inject stats into A and B.
    TDiskSpaceUsageDelta delta;
    TPartitionStats sA;
    sA.SeqNo = TMessageSeqNo{1, 0};
    sA.RowCount = 100;
    sA.DataSize = 500;
    info->UpdateShardStats(&delta, TShardIdx(1, 0), sA, TInstant::Zero());

    TPartitionStats sB;
    sB.SeqNo = TMessageSeqNo{1, 0};
    sB.RowCount = 200;
    sB.DataSize = 300;
    info->UpdateShardStats(&delta, TShardIdx(1, 1), sB, TInstant::Zero());

    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().Aggregated.RowCount, 300u);
    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().Aggregated.DataSize, 800u);

    // Merge A+B into AB — RemoveShardStats must subtract A and B from Aggregated.
    TVector<TTableShardInfo> dst;
    dst.emplace_back(TShardIdx(2, 0), TString(1, '\x02'), 0, 0);
    TVector<TShardIdx> removed = {TShardIdx(1, 0), TShardIdx(1, 1)};

    info->ApplySplitMerge(std::move(dst), removed, /*splitFirstIdx=*/0, TInstant::Zero(), /*trackSplitMergeDemand=*/true,
        /*loadSplitLineage=*/false);

    // AB starts with zero stats, so Aggregated should reflect only the removal.
    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().Aggregated.RowCount, 0u);
    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().Aggregated.DataSize, 0u);
    UNIT_ASSERT_VALUES_EQUAL(info->GetPartitions().size(), 2u);
}

Y_UNIT_TEST(ApplySplitMerge_PreservesRowUpdatesAndDeletes) {
    // Unlike RowCount/DataSize, RowUpdates/RowDeletes are cumulative counters. On
    // split/merge, RemoveShardStats subtracts the removed shard's RowCount/DataSize but
    // must NOT subtract its RowUpdates/RowDeletes, so they survive the reshard.
    auto info = MakeTable();
    // Shards: A(1/0), B(1/1), C(1/2), D(1/3)
    info->SetPartitioning(MakeShards(4));

    TDiskSpaceUsageDelta delta;
    TPartitionStats sA;
    sA.SeqNo = TMessageSeqNo{1, 0};
    sA.RowCount = 100;
    sA.RowUpdates = 700;
    sA.RowDeletes = 150;
    info->UpdateShardStats(&delta, TShardIdx(1, 0), sA, TInstant::Zero());

    TPartitionStats sB;
    sB.SeqNo = TMessageSeqNo{1, 0};
    sB.RowCount = 200;
    sB.RowUpdates = 300;
    sB.RowDeletes = 50;
    info->UpdateShardStats(&delta, TShardIdx(1, 1), sB, TInstant::Zero());

    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().Aggregated.RowCount, 300u);
    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().Aggregated.RowUpdates, 1000u);
    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().Aggregated.RowDeletes, 200u);

    // Split A (position 0) into A1+A2 — B, C, D shift right by 1.
    TVector<TTableShardInfo> dst;
    dst.emplace_back(TShardIdx(2, 0), TString(1, '\x01'), 0, 0);
    dst.emplace_back(TShardIdx(2, 1), TString(1, '\x02'), 0, 0);
    TVector<TShardIdx> removed = {TShardIdx(1, 0)};

    info->ApplySplitMerge(std::move(dst), removed, /*splitFirstIdx=*/0, TInstant::Zero(), /*trackSplitMergeDemand=*/true,
        /*loadSplitLineage=*/false);

    // Current-state metric (RowCount) drops by the removed shard A's contribution...
    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().Aggregated.RowCount, 200u);
    // ...but the cumulative modification counters are preserved across the split.
    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().Aggregated.RowUpdates, 1000u);
    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().Aggregated.RowDeletes, 200u);
}

Y_UNIT_TEST(ApplySplitMerge_TTL_SrcInFlightCleared) {
    auto info = MakeTable();
    EnableTTL(*info);

    // Shard 0 gets NextCondErase=0 so it will be heap-top.
    TVector<TTableShardInfo> shards;
    shards.emplace_back(TShardIdx(1, 0), TString(1, '\x01'), 0, 0);
    shards.emplace_back(TShardIdx(1, 1), TString(1, '\x02'), 0, 1000);
    shards.emplace_back(TShardIdx(1, 2), TString{},          0, 2000);
    info->SetPartitioning(std::move(shards));

    const auto* top = info->GetScheduledCondEraseShard();
    UNIT_ASSERT(top);
    const TShardIdx inFlightShard = top->ShardIdx;
    info->AddInFlightCondErase(inFlightShard);

    // Split the in-flight shard (position 0).
    TVector<TTableShardInfo> dst;
    dst.emplace_back(TShardIdx(2, 0), TString(1, '\x01'), 0, 500);
    dst.emplace_back(TShardIdx(2, 1), TString(1, '\x02'), 0, 600);
    TVector<TShardIdx> removed = {inFlightShard};

    info->ApplySplitMerge(std::move(dst), removed, /*splitFirstIdx=*/0, TInstant::Zero(), /*trackSplitMergeDemand=*/true,
        /*loadSplitLineage=*/false);

    // The in-flight entry for the src shard must be cleared.
    UNIT_ASSERT(!info->GetInFlightCondErase().contains(inFlightShard));
    // VerifyConsistency confirms all 4 remaining shards are covered by the schedule.
    UNIT_ASSERT_VALUES_EQUAL(info->GetPartitions().size(), 4u);
}

Y_UNIT_TEST(ApplySplitMerge_TTL_SrcInSchedule) {
    // Complement to ApplySplitMerge_TTL_SrcInFlightCleared: exercises the path
    // where the src shard is in CondEraseSchedule (not in-flight) when the split arrives.
    auto info = MakeTable();
    EnableTTL(*info);

    TVector<TTableShardInfo> shards;
    shards.emplace_back(TShardIdx(1, 0), TString(1, '\x01'), 0, 0);
    shards.emplace_back(TShardIdx(1, 1), TString(1, '\x02'), 0, 1000);
    shards.emplace_back(TShardIdx(1, 2), TString{},          0, 2000);
    info->SetPartitioning(std::move(shards));

    // All 3 shards are in CondEraseSchedule, none in-flight.
    UNIT_ASSERT(info->GetInFlightCondErase().empty());

    TVector<TTableShardInfo> dst;
    dst.emplace_back(TShardIdx(2, 0), TString(1, '\x01'), 0, 500);
    dst.emplace_back(TShardIdx(2, 1), TString(1, '\x02'), 0, 600);
    TVector<TShardIdx> removed = {TShardIdx(1, 0)};

    info->ApplySplitMerge(std::move(dst), removed, /*splitFirstIdx=*/0, TInstant::Zero(), /*trackSplitMergeDemand=*/true,
        /*loadSplitLineage=*/false);

    // VerifyConsistency checks all 4 resulting shards are covered by schedule.
    UNIT_ASSERT(info->GetInFlightCondErase().empty());
    UNIT_ASSERT_VALUES_EQUAL(info->GetPartitions().size(), 4u);
}

// --- DeepCopy ---

Y_UNIT_TEST(DeepCopy_PointersAreIndependent) {
    auto info = MakeTable();
    info->SetPartitioning(MakeShards(3));

    auto copy = TTableInfo::DeepCopy(*info);

    // Mutate the original — split middle shard.
    TVector<TTableShardInfo> dst;
    dst.emplace_back(TShardIdx(2, 0), TString(1, '\x01'), 0, 0);
    dst.emplace_back(TShardIdx(2, 1), TString(1, '\x02'), 0, 0);
    info->ApplySplitMerge(std::move(dst), {TShardIdx(1, 1)}, /*splitFirstIdx=*/1, TInstant::Zero(), /*trackSplitMergeDemand=*/true,
        /*loadSplitLineage=*/false);
    UNIT_ASSERT_VALUES_EQUAL(info->GetPartitions().size(), 4u);
    info->VerifyConsistency();

    // Copy must be unaffected — its pointers still reach its own PartitionStore.
    UNIT_ASSERT_VALUES_EQUAL(copy->GetPartitions().size(), 3u);
    copy->VerifyConsistency();
}

Y_UNIT_TEST(DeepCopy_PreservesStats) {
    auto info = MakeTable();
    info->SetPartitioning(MakeShards(3));

    TDiskSpaceUsageDelta delta;
    TPartitionStats s;
    s.SeqNo = TMessageSeqNo{1, 0};
    s.RowCount = 77;
    info->UpdateShardStats(&delta, TShardIdx(1, 1), s, TInstant::Zero());

    auto copy = TTableInfo::DeepCopy(*info);

    UNIT_ASSERT_VALUES_EQUAL(copy->GetStats().PartitionStats.at(TShardIdx(1, 1)).RowCount, 77u);
    UNIT_ASSERT_VALUES_EQUAL(copy->GetStats().Aggregated.RowCount, 77u);
}

Y_UNIT_TEST(DeepCopy_WithTTL) {
    auto info = MakeTable();
    EnableTTL(*info);
    info->SetPartitioning(MakeShards(4));

    // Move one shard to in-flight before copy.
    const auto* top = info->GetScheduledCondEraseShard();
    UNIT_ASSERT(top);
    info->AddInFlightCondErase(top->ShardIdx);

    auto copy = TTableInfo::DeepCopy(*info);

    // VerifyConsistency is called inside DeepCopy, but verify the state explicitly.
    UNIT_ASSERT_VALUES_EQUAL(copy->GetInFlightCondErase().size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(copy->GetPartitions().size(), 4u);
    copy->VerifyConsistency();
}

Y_UNIT_TEST(DeepCopy_PreservesPartitionsFormat) {
    auto info = MakeTable();
    info->SetPartitioning(MakeShards(3));

    // test default and explicit values
    {
        auto copy = TTableInfo::DeepCopy(*info);
        UNIT_ASSERT_VALUES_EQUAL(copy->GetPartitions().size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(copy->PartitionsInShardIdxFormat, info->PartitionsInShardIdxFormat);
    }
    info->PartitionsInShardIdxFormat = false;
    {
        auto copy = TTableInfo::DeepCopy(*info);
        UNIT_ASSERT_VALUES_EQUAL(copy->GetPartitions().size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(copy->PartitionsInShardIdxFormat, info->PartitionsInShardIdxFormat);
    }
    info->PartitionsInShardIdxFormat = true;
    {
        auto copy = TTableInfo::DeepCopy(*info);
        UNIT_ASSERT_VALUES_EQUAL(copy->GetPartitions().size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(copy->PartitionsInShardIdxFormat, info->PartitionsInShardIdxFormat);
    }
}

// --- TTL state machine ---

Y_UNIT_TEST(TTL_RescheduleCycle) {
    auto info = MakeTable();
    EnableTTL(*info);

    TVector<TTableShardInfo> shards;
    shards.emplace_back(TShardIdx(1, 0), TString(1, '\x01'), 0, 0);
    shards.emplace_back(TShardIdx(1, 1), TString(1, '\x02'), 0, 1000);
    shards.emplace_back(TShardIdx(1, 2), TString{},          0, 2000);
    info->SetPartitioning(std::move(shards));

    const auto* top = info->GetScheduledCondEraseShard();
    UNIT_ASSERT(top);
    const TShardIdx dispatched = top->ShardIdx;
    info->AddInFlightCondErase(dispatched);
    UNIT_ASSERT_VALUES_EQUAL(info->GetInFlightCondErase().size(), 1u);

    // Reschedule back — simulates the "nothing to erase" response path.
    info->RescheduleCondErase(dispatched);
    UNIT_ASSERT(info->GetInFlightCondErase().empty());

    // VerifyConsistency confirms all 3 are in the schedule.
    info->VerifyConsistency();
}

Y_UNIT_TEST(TTL_ScheduleOrdering) {
    auto info = MakeTable();
    EnableTTL(*info);

    // Shard 1/1 has the earliest NextCondErase (5000 < 10000 < 20000).
    TVector<TTableShardInfo> shards;
    shards.emplace_back(TShardIdx(1, 0), TString(1, '\x01'), 0, 10000);
    shards.emplace_back(TShardIdx(1, 1), TString(1, '\x02'), 0, 5000);
    shards.emplace_back(TShardIdx(1, 2), TString{},          0, 20000);
    info->SetPartitioning(std::move(shards));

    const auto* top = info->GetScheduledCondEraseShard();
    UNIT_ASSERT(top);
    UNIT_ASSERT_VALUES_EQUAL(top->ShardIdx, TShardIdx(1, 1));
    info->AddInFlightCondErase(TShardIdx(1, 1));

    // After work, reschedule with a much later NextCondErase.
    info->UpdateNextCondErase(TShardIdx(1, 1), TInstant::FromValue(50000), TDuration::Seconds(1));
    info->RescheduleCondErase(TShardIdx(1, 1));

    // Shard 1/0 (NextCondErase=10000) must now be next.
    const auto* newTop = info->GetScheduledCondEraseShard();
    UNIT_ASSERT(newTop);
    UNIT_ASSERT_VALUES_EQUAL(newTop->ShardIdx, TShardIdx(1, 0));
}

// --- UpdateShardStats ---

Y_UNIT_TEST(UpdateShardStats_StaleDropped) {
    auto info = MakeTable();
    info->SetPartitioning(MakeShards(2));

    TDiskSpaceUsageDelta delta;
    TPartitionStats current;
    current.SeqNo = TMessageSeqNo{1, 5};
    current.RowCount = 100;
    info->UpdateShardStats(&delta, TShardIdx(1, 0), current, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().Aggregated.RowCount, 100u);

    // Older SeqNo — must be silently dropped.
    TPartitionStats stale;
    stale.SeqNo = TMessageSeqNo{1, 3};
    stale.RowCount = 999;
    info->UpdateShardStats(&delta, TShardIdx(1, 0), stale, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().Aggregated.RowCount, 100u);
}

Y_UNIT_TEST(UpdateShardStats_GenerationRollover) {
    auto info = MakeTable();
    info->SetPartitioning(MakeShards(2));

    TDiskSpaceUsageDelta delta;

    // Generation 1: tablet has processed 100 transactions.
    TPartitionStats gen1;
    gen1.SeqNo = TMessageSeqNo{1, 0};
    gen1.ImmediateTxCompleted = 100;
    info->UpdateShardStats(&delta, TShardIdx(1, 0), gen1, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().Aggregated.ImmediateTxCompleted, 100u);

    // Generation 2: tablet restarted, its own counter reset to 10.
    // Aggregated must preserve the gen1 count and add gen2 from a zero baseline.
    TPartitionStats gen2;
    gen2.SeqNo = TMessageSeqNo{2, 0};
    gen2.ImmediateTxCompleted = 10;
    info->UpdateShardStats(&delta, TShardIdx(1, 0), gen2, TInstant::Zero());

    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().Aggregated.ImmediateTxCompleted, 110u);
}

Y_UNIT_TEST(UpdateShardStats_GenerationRollover_RowUpdatesAndDeletes) {
    // RowUpdates/RowDeletes are cumulative counters kept in the shard's memory, so they
    // reset to zero on tablet restart. A generation bump must re-baseline them so the
    // aggregate is preserved and keeps growing, never decrements.
    auto info = MakeTable();
    info->SetPartitioning(MakeShards(1));

    TDiskSpaceUsageDelta delta;

    // Generation 1: shard has accumulated 1000 updates and 200 deletes.
    TPartitionStats gen1;
    gen1.SeqNo = TMessageSeqNo{1, 0};
    gen1.RowUpdates = 1000;
    gen1.RowDeletes = 200;
    info->UpdateShardStats(&delta, TShardIdx(1, 0), gen1, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().Aggregated.RowUpdates, 1000u);
    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().Aggregated.RowDeletes, 200u);

    // Generation 2: shard restarted, counters restarted from zero and reached 50/10.
    // The aggregate must keep the gen1 totals and add gen2 from a zero baseline.
    TPartitionStats gen2;
    gen2.SeqNo = TMessageSeqNo{2, 0};
    gen2.RowUpdates = 50;
    gen2.RowDeletes = 10;
    info->UpdateShardStats(&delta, TShardIdx(1, 0), gen2, TInstant::Zero());

    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().Aggregated.RowUpdates, 1050u);
    UNIT_ASSERT_VALUES_EQUAL(info->GetStats().Aggregated.RowDeletes, 210u);
}

// --- RemoveShardStats ---

Y_UNIT_TEST(RemoveShardStats_NormalSubtraction) {
    TTableAggregatedStats stats;
    const TShardIdx shard{1, 0};

    TPartitionStats s;
    s.RowCount = 100;
    s.DataSize = 500;
    s.IndexSize = 50;
    s.ByKeyFilterSize = 10;
    s.Memory = 200;
    s.Network = 30;
    s.Storage = 1000;
    s.ReadThroughput = 100;
    s.WriteThroughput = 200;
    s.ReadIops = 5;
    s.WriteIops = 7;
    s.InFlightTxCount = 3;
    stats.PartitionStats[shard] = s;
    stats.Aggregated.RowCount = 100;
    stats.Aggregated.DataSize = 500;
    stats.Aggregated.IndexSize = 50;
    stats.Aggregated.ByKeyFilterSize = 10;
    stats.Aggregated.Memory = 200;
    stats.Aggregated.Network = 30;
    stats.Aggregated.Storage = 1000;
    stats.Aggregated.ReadThroughput = 100;
    stats.Aggregated.WriteThroughput = 200;
    stats.Aggregated.ReadIops = 5;
    stats.Aggregated.WriteIops = 7;
    stats.Aggregated.InFlightTxCount = 3;

    stats.RemoveShardStats({shard}, TInstant::Zero());

    UNIT_ASSERT_VALUES_EQUAL(stats.Aggregated.RowCount, 0u);
    UNIT_ASSERT_VALUES_EQUAL(stats.Aggregated.DataSize, 0u);
    UNIT_ASSERT_VALUES_EQUAL(stats.Aggregated.IndexSize, 0u);
    UNIT_ASSERT_VALUES_EQUAL(stats.Aggregated.ByKeyFilterSize, 0u);
    UNIT_ASSERT_VALUES_EQUAL(stats.Aggregated.Memory, 0u);
    UNIT_ASSERT_VALUES_EQUAL(stats.Aggregated.Network, 0u);
    UNIT_ASSERT_VALUES_EQUAL(stats.Aggregated.Storage, 0u);
    UNIT_ASSERT_VALUES_EQUAL(stats.Aggregated.ReadThroughput, 0u);
    UNIT_ASSERT_VALUES_EQUAL(stats.Aggregated.WriteThroughput, 0u);
    UNIT_ASSERT_VALUES_EQUAL(stats.Aggregated.ReadIops, 0u);
    UNIT_ASSERT_VALUES_EQUAL(stats.Aggregated.WriteIops, 0u);
    UNIT_ASSERT_VALUES_EQUAL(stats.Aggregated.InFlightTxCount, 0u);
    UNIT_ASSERT(!stats.PartitionStats.contains(shard));
}

Y_UNIT_TEST(RemoveShardStats_SaturatesAtZero) {
    // Shard stats exceed the aggregate (invariant drift). Must clamp to 0, not wrap.
    TTableAggregatedStats stats;
    const TShardIdx shard{1, 0};

    TPartitionStats s;
    s.RowCount = 200;
    s.DataSize = 600;
    s.Memory = 999;
    stats.PartitionStats[shard] = s;
    stats.Aggregated.RowCount = 100;
    stats.Aggregated.DataSize = 500;
    stats.Aggregated.Memory = 0;

    stats.RemoveShardStats({shard}, TInstant::Zero());

    UNIT_ASSERT_VALUES_EQUAL(stats.Aggregated.RowCount, 0u);
    UNIT_ASSERT_VALUES_EQUAL(stats.Aggregated.DataSize, 0u);
    UNIT_ASSERT_VALUES_EQUAL(stats.Aggregated.Memory, 0u);
}

Y_UNIT_TEST(RemoveShardStats_UnknownKeyIsNoop) {
    TTableAggregatedStats stats;
    stats.Aggregated.RowCount = 42;

    stats.RemoveShardStats({TShardIdx{9, 9}}, TInstant::Zero());

    UNIT_ASSERT_VALUES_EQUAL(stats.Aggregated.RowCount, 42u);
}

Y_UNIT_TEST(RemoveShardStats_MultipleKeys) {
    TTableAggregatedStats stats;
    const TShardIdx sA{1, 0};
    const TShardIdx sB{1, 1};

    TPartitionStats sa;
    sa.RowCount = 100;
    TPartitionStats sb;
    sb.RowCount = 200;
    stats.PartitionStats[sA] = sa;
    stats.PartitionStats[sB] = sb;
    stats.Aggregated.RowCount = 300;

    stats.RemoveShardStats({sA, sB}, TInstant::Zero());

    UNIT_ASSERT_VALUES_EQUAL(stats.Aggregated.RowCount, 0u);
    UNIT_ASSERT(!stats.PartitionStats.contains(sA));
    UNIT_ASSERT(!stats.PartitionStats.contains(sB));
}

Y_UNIT_TEST(RemoveShardStats_StoragePoolStats_Subtracted) {
    TTableAggregatedStats stats;
    const TShardIdx shard{1, 0};

    TPartitionStats s;
    s.StoragePoolsStats["hdd"] = {.DataSize = 100, .IndexSize = 50};
    stats.PartitionStats[shard] = s;
    stats.Aggregated.StoragePoolsStats["hdd"] = {.DataSize = 200, .IndexSize = 100};

    stats.RemoveShardStats({shard}, TInstant::Zero());

    const auto& pool = stats.Aggregated.StoragePoolsStats.at("hdd");
    UNIT_ASSERT_VALUES_EQUAL(pool.DataSize, 100u);
    UNIT_ASSERT_VALUES_EQUAL(pool.IndexSize, 50u);
}

Y_UNIT_TEST(RemoveShardStats_StoragePoolStats_UnknownPoolNotInserted) {
    // Pool present on the shard but absent from the aggregate must not create a zero entry.
    TTableAggregatedStats stats;
    const TShardIdx shard{1, 0};

    TPartitionStats s;
    s.StoragePoolsStats["nvme"] = {.DataSize = 100, .IndexSize = 50};
    stats.PartitionStats[shard] = s;

    stats.RemoveShardStats({shard}, TInstant::Zero());

    UNIT_ASSERT(!stats.Aggregated.StoragePoolsStats.contains("nvme"));
}

// --- Split/merge partition history (lineage) ---

Y_UNIT_TEST(History_SeededOnSplit) {
    auto info = MakeTable();
    info->SetPartitioning(MakeShards(3));

    // Seed parent B (1/1): pretend its region keeps splitting by load.
    auto& parent = info->MutablePartitionSplitMergeState(TShardIdx(1, 1));
    parent.LastSplitTime = TInstant::Seconds(100);
    parent.LoadSplitLineageDepth = 2;

    TVector<TTableShardInfo> dst;
    dst.emplace_back(TShardIdx(2, 0), TString(1, '\x01'), 0, 0);
    dst.emplace_back(TShardIdx(2, 1), TString(1, '\x02'), 0, 0);
    info->ApplySplitMerge(std::move(dst), {TShardIdx(1, 1)}, /*splitFirstIdx=*/1, TInstant::Seconds(500),
        /*trackSplitMergeDemand=*/true, /*loadSplitLineage=*/true);

    // Parent history erased; children inherit and the producing split is stamped now.
    UNIT_ASSERT(!info->GetPartitionSplitMergeState(TShardIdx(1, 1)));
    for (const auto idx : {TShardIdx(2, 0), TShardIdx(2, 1)}) {
        const auto* h = info->GetPartitionSplitMergeState(idx);
        UNIT_ASSERT(h);
        UNIT_ASSERT_VALUES_EQUAL(h->LastSplitTime, TInstant::Seconds(500));
        // by-load split deepens the lineage: parent depth 2 -> child 3
        UNIT_ASSERT_VALUES_EQUAL(h->LoadSplitLineageDepth, 3u);
    }
}

Y_UNIT_TEST(History_LoadLineageSeededAfterRestart) {
    // After a SchemeShard restart PartitionSplitMergeStates is empty (in-memory only),
    // but the persisted op still carries LoadSplitLineage == true (TxInFlightV2).
    // The propagation must fire anyway, treating the missing parent history as depth 0.
    auto info = MakeTable();
    info->SetPartitioning(MakeShards(3));
    // No MutablePartitionSplitMergeState calls: the history map is empty, as after a reboot.

    TVector<TTableShardInfo> dst;
    dst.emplace_back(TShardIdx(2, 0), TString(1, '\x01'), 0, 0);
    dst.emplace_back(TShardIdx(2, 1), TString(1, '\x02'), 0, 0);
    info->ApplySplitMerge(std::move(dst), {TShardIdx(1, 1)}, /*splitFirstIdx=*/1, TInstant::Seconds(500),
        /*trackSplitMergeDemand=*/true, /*loadSplitLineage=*/true);

    for (const auto idx : {TShardIdx(2, 0), TShardIdx(2, 1)}) {
        const auto* h = info->GetPartitionSplitMergeState(idx);
        UNIT_ASSERT(h);
        UNIT_ASSERT_VALUES_EQUAL(h->LastSplitTime, TInstant::Seconds(500));
        // No parent history survived the restart: base depth 0 + 1 for the by-load split.
        UNIT_ASSERT_VALUES_EQUAL(h->LoadSplitLineageDepth, 1u);
    }
}

Y_UNIT_TEST(Lineage_DepthGrowsOnLoadSplit) {
    // by-size split does not deepen the lineage
    {
        auto info = MakeTable();
        info->SetPartitioning(MakeShards(3));
        auto& parent = info->MutablePartitionSplitMergeState(TShardIdx(1, 1));
        parent.LoadSplitLineageDepth = 2;

        TVector<TTableShardInfo> dst;
        dst.emplace_back(TShardIdx(2, 0), TString(1, '\x01'), 0, 0);
        dst.emplace_back(TShardIdx(2, 1), TString(1, '\x02'), 0, 0);
        info->ApplySplitMerge(std::move(dst), {TShardIdx(1, 1)}, /*splitFirstIdx=*/1, TInstant::Seconds(1),
            /*trackSplitMergeDemand=*/true, /*loadSplitLineage=*/false);

        UNIT_ASSERT_VALUES_EQUAL(info->GetPartitionSplitMergeState(TShardIdx(2, 0))->LoadSplitLineageDepth, 2u);
    }
    // merge relieves the hot region: child depth resets to 0
    {
        auto info = MakeTable();
        info->SetPartitioning(MakeShards(3));
        info->MutablePartitionSplitMergeState(TShardIdx(1, 0)).LoadSplitLineageDepth = 4;
        info->MutablePartitionSplitMergeState(TShardIdx(1, 1)).LoadSplitLineageDepth = 5;

        TVector<TTableShardInfo> dst;
        dst.emplace_back(TShardIdx(2, 0), TString(1, '\x02'), 0, 0);
        info->ApplySplitMerge(std::move(dst), {TShardIdx(1, 0), TShardIdx(1, 1)}, /*splitFirstIdx=*/0,
            TInstant::Seconds(7), /*trackSplitMergeDemand=*/true, /*loadSplitLineage=*/false);

        const auto* h = info->GetPartitionSplitMergeState(TShardIdx(2, 0));
        UNIT_ASSERT(h);
        UNIT_ASSERT_VALUES_EQUAL(h->LoadSplitLineageDepth, 0u);
        UNIT_ASSERT_VALUES_EQUAL(h->LastMergeTime, TInstant::Seconds(7));
    }
}

Y_UNIT_TEST(History_ErasedOnRemove) {
    auto info = MakeTable();
    info->SetPartitioning(MakeShards(3));

    // B (1/1) is a stuck deferred candidate.
    info->MutablePartitionSplitMergeState(TShardIdx(1, 1)).SplitDeferredCount = 1;
    info->MutableTableSplitMergeState().DeferredShards.emplace(TShardIdx(1, 1), /*wantsSplit=*/true);

    TVector<TTableShardInfo> dst;
    dst.emplace_back(TShardIdx(2, 0), TString(1, '\x01'), 0, 0);
    dst.emplace_back(TShardIdx(2, 1), TString(1, '\x02'), 0, 0);
    info->ApplySplitMerge(std::move(dst), {TShardIdx(1, 1)}, /*splitFirstIdx=*/1, TInstant::Seconds(3),
        /*trackSplitMergeDemand=*/true, /*loadSplitLineage=*/false);

    // Removed shard dropped from both the history map and the deferred set (subset invariant).
    UNIT_ASSERT(!info->GetPartitionSplitMergeState(TShardIdx(1, 1)));
    UNIT_ASSERT(!info->GetTableSplitMergeState().DeferredShards.contains(TShardIdx(1, 1)));
}

Y_UNIT_TEST(History_ResetOnApplied) {
    TPartitionSplitMergeState h;
    h.SplitCandidateCount = 3;
    h.MergeCandidateCount = 2;
    h.SplitDeferredCount = 1;
    h.MergeDeferredCount = 4;
    h.LoadSplitLineageDepth = 5;
    h.RecordDeferral(TPartitionSplitMergeState::EDeferralReason::InFlightLimit);

    h.ResetOnSplitMerge();

    UNIT_ASSERT_VALUES_EQUAL(h.SplitCandidateCount, 0u);
    UNIT_ASSERT_VALUES_EQUAL(h.MergeCandidateCount, 0u);
    UNIT_ASSERT_VALUES_EQUAL(h.SplitDeferredCount, 0u);
    UNIT_ASSERT_VALUES_EQUAL(h.MergeDeferredCount, 0u);
    UNIT_ASSERT_VALUES_EQUAL(h.DeferralReasonCounts[0], 0u);
    // LoadSplitLineageDepth survives the applied split/merge -- that is the point of the lineage signal.
    UNIT_ASSERT_VALUES_EQUAL(h.LoadSplitLineageDepth, 5u);
}

Y_UNIT_TEST(History_InnerWeightedPick) {
    auto info = MakeTable();
    info->SetPartitioning(MakeShards(4));
    auto& tableState = info->MutableTableSplitMergeState();

    // Least stuck.
    info->MutablePartitionSplitMergeState(TShardIdx(1, 1)).SplitDeferredCount = 1;
    info->MutablePartitionSplitMergeState(TShardIdx(1, 1)).LastSplitCandidate = TInstant::Seconds(50);
    tableState.DeferredShards.emplace(TShardIdx(1, 1), /*wantsSplit=*/true);

    // Most stuck (weight 5), but newer candidate.
    info->MutablePartitionSplitMergeState(TShardIdx(1, 2)).SplitDeferredCount = 5;
    info->MutablePartitionSplitMergeState(TShardIdx(1, 2)).LastSplitCandidate = TInstant::Seconds(80);
    tableState.DeferredShards.emplace(TShardIdx(1, 2), /*wantsSplit=*/true);

    // Equally stuck (weight 5), older candidate -> wins the tie.
    info->MutablePartitionSplitMergeState(TShardIdx(1, 3)).SplitDeferredCount = 5;
    info->MutablePartitionSplitMergeState(TShardIdx(1, 3)).LastSplitCandidate = TInstant::Seconds(10);
    tableState.DeferredShards.emplace(TShardIdx(1, 3), /*wantsSplit=*/true);

    UNIT_ASSERT_EQUAL(info->PickMostDeferredPartition(), TShardIdx(1, 3));

    // Empty deferred set -> InvalidShardIdx.
    auto empty = MakeTable();
    empty->SetPartitioning(MakeShards(2));
    UNIT_ASSERT_EQUAL(empty->PickMostDeferredPartition(), InvalidShardIdx);
}

Y_UNIT_TEST(History_HotPathScalesLinearly) {
    // Guards the O(1)-per-update claim: the per-stat history write is a hashmap find-or-insert
    // plus a few field writes. A regression introducing an O(N) scan would blow this bound up.
    auto info = MakeTable();
    const ui32 n = 1000;
    info->SetPartitioning(MakeShards(n));

    THPTimer timer;
    for (ui32 iter = 0; iter < 200000; ++iter) {
        auto& h = info->MutablePartitionSplitMergeState(TShardIdx(1, iter % n));
        ++h.SplitCandidateCount;
    }
    const double elapsed = timer.Passed();
    // Assert an operations/sec floor instead of a wall-clock upper bound: an O(N) regression
    // drops throughput by orders of magnitude, while a floor stays stable under CI load.
    constexpr ui32 kOps = 200000;
    const double opsPerSec = kOps / (elapsed > 0.0 ? elapsed : 1e-9);
    UNIT_ASSERT_C(opsPerSec >= 20000.0,
        "hot-path history recording unexpectedly slow: " << opsPerSec << " ops/sec"
        << " (elapsed " << elapsed << "s for " << kOps << " ops)");
}

Y_UNIT_TEST(History_DropDecrementsExactDirection) {
    // The stored direction (not a heuristic) decides which per-table count is decremented:
    // a merge-deferred shard must decrement MergeDemandCount even if its per-shard history
    // says nothing about split.
    auto info = MakeTable();
    info->SetPartitioning(MakeShards(2));
    auto& tableState = info->MutableTableSplitMergeState();

    info->MutablePartitionSplitMergeState(TShardIdx(1, 1)).MergeDeferredCount = 1;
    tableState.DeferredShards.emplace(TShardIdx(1, 1), /*wantsSplit=*/false);
    tableState.MergeDemandCount = 1;

    info->DropFromSplitMergeState(TShardIdx(1, 1));

    UNIT_ASSERT_VALUES_EQUAL(tableState.MergeDemandCount, 0u);
    UNIT_ASSERT_VALUES_EQUAL(tableState.SplitDemandCount, 0u);
    UNIT_ASSERT(!tableState.DeferredShards.contains(TShardIdx(1, 1)));
    // Drain resets the oldest-candidate timestamp.
    UNIT_ASSERT(!tableState.OldestPendingCandidateAt);
}

Y_UNIT_TEST(History_DropWithoutPerShardStateStillDecrements) {
    // Regression: the old heuristic read the per-shard history on drop; if it was already
    // gone, the count leaked (permanent over-count). The stored direction must not depend
    // on the per-shard entry existing.
    auto info = MakeTable();
    info->SetPartitioning(MakeShards(2));
    auto& tableState = info->MutableTableSplitMergeState();

    tableState.DeferredShards.emplace(TShardIdx(1, 1), /*wantsSplit=*/true);
    tableState.SplitDemandCount = 1;

    info->DropFromSplitMergeState(TShardIdx(1, 1));  // no per-shard state exists

    UNIT_ASSERT_VALUES_EQUAL(tableState.SplitDemandCount, 0u);
    UNIT_ASSERT(!tableState.DeferredShards.contains(TShardIdx(1, 1)));
}

Y_UNIT_TEST(History_PickPrunesStaleEntries) {
    // A deferred entry without per-shard state is stale; the pick must prune it (and fix
    // the counts) instead of churning in the revisit queue forever.
    auto info = MakeTable();
    info->SetPartitioning(MakeShards(3));
    auto& tableState = info->MutableTableSplitMergeState();

    // Stale: deferred but no per-shard state.
    tableState.DeferredShards.emplace(TShardIdx(1, 1), /*wantsSplit=*/true);
    tableState.SplitDemandCount = 1;

    // Live: deferred with per-shard state.
    info->MutablePartitionSplitMergeState(TShardIdx(1, 2)).SplitDeferredCount = 2;
    info->MutablePartitionSplitMergeState(TShardIdx(1, 2)).LastSplitCandidate = TInstant::Seconds(30);
    tableState.DeferredShards.emplace(TShardIdx(1, 2), /*wantsSplit=*/true);
    tableState.SplitDemandCount = 2;

    UNIT_ASSERT_EQUAL(info->PickMostDeferredPartition(), TShardIdx(1, 2));

    // The stale entry was pruned and its count returned.
    UNIT_ASSERT(!tableState.DeferredShards.contains(TShardIdx(1, 1)));
    UNIT_ASSERT_VALUES_EQUAL(tableState.SplitDemandCount, 1u);

    // Pruning the last entry resets the oldest-candidate timestamp.
    info->DropFromSplitMergeState(TShardIdx(1, 2));
    UNIT_ASSERT(tableState.DeferredShards.empty());
    UNIT_ASSERT(!tableState.OldestPendingCandidateAt);
}

Y_UNIT_TEST(History_PickCacheFastPath) {
    // The cached pick gives the revisit turn an O(1) fast path: while a shard stays deferred
    // its weight only grows and candidates only move forward, so the winner changes only via
    // UpdateSplitMergePickCache (deferral) or InvalidateSplitMergePickCache (removal).
    auto info = MakeTable();
    info->SetPartitioning(MakeShards(3));
    auto& tableState = info->MutableTableSplitMergeState();

    auto defer = [&](ui32 shard, ui32 weight, TInstant candidate) {
        auto& h = info->MutablePartitionSplitMergeState(TShardIdx(1, shard));
        h.SplitDeferredCount = weight;
        h.LastSplitCandidate = candidate;
        tableState.DeferredShards.emplace(TShardIdx(1, shard), /*wantsSplit=*/true);
        info->UpdateSplitMergePickCache(TShardIdx(1, shard));
    };

    defer(1, 1, TInstant::Seconds(50));
    defer(2, 5, TInstant::Seconds(80));
    defer(3, 5, TInstant::Seconds(10));  // tie on weight, older candidate -> wins
    UNIT_ASSERT_EQUAL(tableState.CachedPickShardIdx, TShardIdx(1, 3));
    UNIT_ASSERT_EQUAL(info->PickMostDeferredPartition(), TShardIdx(1, 3));

    // A heavier deferral overtakes the cached winner.
    defer(1, 7, TInstant::Seconds(90));
    UNIT_ASSERT_EQUAL(tableState.CachedPickShardIdx, TShardIdx(1, 1));
    UNIT_ASSERT_EQUAL(info->PickMostDeferredPartition(), TShardIdx(1, 1));

    // Removing the cached winner invalidates the cache; the pick rescans and re-caches.
    info->DropFromSplitMergeState(TShardIdx(1, 1));
    UNIT_ASSERT_EQUAL(tableState.CachedPickShardIdx, InvalidShardIdx);
    UNIT_ASSERT_EQUAL(info->PickMostDeferredPartition(), TShardIdx(1, 3));
    UNIT_ASSERT_EQUAL(tableState.CachedPickShardIdx, TShardIdx(1, 3));

    // A candidate-timestamp change on the cached winner fails validation -> rescan stays exact.
    info->MutablePartitionSplitMergeState(TShardIdx(1, 3)).LastSplitCandidate = TInstant::Seconds(95);
    UNIT_ASSERT_EQUAL(info->PickMostDeferredPartition(), TShardIdx(1, 2));  // (1,2) now older
    UNIT_ASSERT_EQUAL(tableState.CachedPickShardIdx, TShardIdx(1, 2));
}

}
