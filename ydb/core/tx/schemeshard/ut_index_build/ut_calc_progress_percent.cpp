#include <ydb/core/tx/schemeshard/schemeshard_index_build_info.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NSchemeShard;

namespace {
    TShardIdx MakeShardIdx(ui64 owner, ui64 local) {
        return TShardIdx(TOwnerId(owner), TLocalShardIdx(local));
    }

    void AddShard(TIndexBuildInfo& info, ui64 owner, ui64 local) {
        auto idx = MakeShardIdx(owner, local);
        info.Shards.emplace(idx, TIndexBuildShardStatus{TSerializedTableRange{}, ""});
    }

    void MarkDone(TIndexBuildInfo& info, ui64 owner, ui64 local) {
        info.DoneShards.push_back(MakeShardIdx(owner, local));
    }

} // namespace

Y_UNIT_TEST_SUITE(CalcProgressPercent) {
    // Non-vector index: progress is based purely on done/total shards.
    // Formula: 100 * done / total, or 0 if no shards.

    Y_UNIT_TEST(NonVectorIndex_NoShards) {
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildSecondaryIndex;
        // No shards -> 0%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 0.f, 1e-6f);
    }

    Y_UNIT_TEST(NonVectorIndex_ZeroShardsComplete) {
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildSecondaryIndex;
        AddShard(info, 1, 1);
        AddShard(info, 1, 2);
        // 100 * 0 / 2 = 0%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 0.f, 1e-6f);
    }

    Y_UNIT_TEST(NonVectorIndex_HalfComplete) {
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildSecondaryIndex;
        AddShard(info, 1, 1);
        AddShard(info, 1, 2);
        MarkDone(info, 1, 1);
        // 100 * 1 / 2 = 50%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 50.f, 1e-6f);
    }

    Y_UNIT_TEST(NonVectorIndex_AllComplete) {
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildSecondaryIndex;
        AddShard(info, 1, 1);
        AddShard(info, 1, 2);
        MarkDone(info, 1, 1);
        MarkDone(info, 1, 2);
        // 100 * 2 / 2 = 100%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 100.f, 1e-6f);
    }

    // Vector index: early return 100% when all work is done
    // (!NeedsAnotherLevel() && !NeedsAnotherParent() && toUpload == 0 && inProgress == 0).
    // Otherwise: 100 * (Level - 1 + shardProgress) / Levels,
    // where shardProgress = done/total if total > 0, else 0.

    Y_UNIT_TEST(VectorIndex_Completed) {
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildVectorIndex;
        info.KMeans.Levels = 3;
        info.KMeans.Level = 3;
        // NeedsAnotherLevel() = (3 < 3) = false
        // NeedsAnotherParent() = (Parent < ParentEnd()) = (0 < 0) = false (default values)
        // InProgressShards and ToUploadShards are empty by default
        // -> early return 100%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 100.f, 1e-6f);
    }

    Y_UNIT_TEST(VectorIndex_StartOfFirstLevel_NoShards) {
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildVectorIndex;
        info.KMeans.Levels = 3;
        info.KMeans.Level = 1;
        // Not completed: NeedsAnotherLevel() = (1 < 3) = true
        // shardProgress = 0 (no shards, total == 0)
        // 100 * (1 - 1 + 0) / 3 = 0%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 0.f, 1e-6f);
    }

    Y_UNIT_TEST(VectorIndex_StartOfFirstLevel_WithShards) {
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildVectorIndex;
        info.KMeans.Levels = 3;
        info.KMeans.Level = 1;
        AddShard(info, 1, 1);
        AddShard(info, 1, 2);
        // Not completed: NeedsAnotherLevel() = (1 < 3) = true
        // shardProgress = 0 / 2 = 0.f
        // 100 * (1 - 1 + 0) / 3 = 0%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 0.f, 1e-6f);
    }

    Y_UNIT_TEST(VectorIndex_HalfShardsAtFirstLevel) {
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildVectorIndex;
        info.KMeans.Levels = 3;
        info.KMeans.Level = 1;
        AddShard(info, 1, 1);
        AddShard(info, 1, 2);
        MarkDone(info, 1, 1);
        // Not completed: NeedsAnotherLevel() = (1 < 3) = true
        // shardProgress = 1 / 2 = 0.5f
        // 100 * (1 - 1 + 0.5) / 3 = 100 / 6 = 16.66...%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 100.f / 6.f, 1e-5f);
    }

    Y_UNIT_TEST(VectorIndex_StartOfSecondLevel_NoShards) {
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildVectorIndex;
        info.KMeans.Levels = 3;
        info.KMeans.Level = 2;
        // Not completed: NeedsAnotherLevel() = (2 < 3) = true
        // shardProgress = 0 (no shards, total == 0)
        // 100 * (2 - 1 + 0) / 3 = 33.33...%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 100.f / 3.f, 1e-5f);
    }

    Y_UNIT_TEST(VectorIndex_HalfShardsAtSecondLevel) {
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildVectorIndex;
        info.KMeans.Levels = 3;
        info.KMeans.Level = 2;
        AddShard(info, 1, 1);
        AddShard(info, 1, 2);
        MarkDone(info, 1, 1);
        // Not completed: NeedsAnotherLevel() = (2 < 3) = true
        // shardProgress = 1 / 2 = 0.5f
        // 100 * (2 - 1 + 0.5) / 3 = 150 / 3 = 50%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 50.f, 1e-5f);
    }

    Y_UNIT_TEST(VectorIndex_AllShardsAtSecondLevel) {
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildVectorIndex;
        info.KMeans.Levels = 3;
        info.KMeans.Level = 2;
        AddShard(info, 1, 1);
        AddShard(info, 1, 2);
        MarkDone(info, 1, 1);
        MarkDone(info, 1, 2);
        // Not completed: NeedsAnotherLevel() = (2 < 3) = true
        // shardProgress = 2 / 2 = 1.f
        // 100 * (2 - 1 + 1) / 3 = 200 / 3 = 66.66...%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 200.f / 3.f, 1e-5f);
    }

    Y_UNIT_TEST(VectorIndex_SingleLevel_HalfShards) {
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildVectorIndex;
        info.KMeans.Levels = 1;
        info.KMeans.Level = 1;
        AddShard(info, 1, 1);
        AddShard(info, 1, 2);
        AddShard(info, 1, 3);
        AddShard(info, 1, 4);
        MarkDone(info, 1, 1);
        MarkDone(info, 1, 2);
        // NeedsAnotherLevel() = (1 < 1) = false, NeedsAnotherParent() = false (defaults),
        // toUpload = 0, inProgress = 0 -> early return 100%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 100.f, 1e-6f);
    }
}
