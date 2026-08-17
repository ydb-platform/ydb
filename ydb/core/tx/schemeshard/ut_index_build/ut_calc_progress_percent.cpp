#include <ydb/core/tx/schemeshard/index/index_build_info.h>

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
        // Put remaining shards in ToUploadShards so the early-return is NOT taken,
        // exercising the within-level progress formula instead.
        info.ToUploadShards.push_back(MakeShardIdx(1, 3));
        info.ToUploadShards.push_back(MakeShardIdx(1, 4));
        // NeedsAnotherLevel() = (1 < 1) = false, but toUpload != 0 -> no early return
        // shardProgress = 2 / 4 = 0.5f
        // 100 * (1 - 1 + 0.5) / 1 = 50%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 50.f, 1e-5f);
    }

    // OverlapClusters > 1 with Levels > 1: each level has a build half and a filter half.
    // Within a level: build pass contributes [0, 0.5) and filter pass contributes [0.5, 1).

    Y_UNIT_TEST(VectorIndex_Overlap_BuildHalf_FirstLevel) {
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildVectorIndex;
        info.KMeans.Levels = 2;
        info.KMeans.Level = 1;
        info.KMeans.OverlapClusters = 2;
        // State = Sample (default, not Filter/FilterBorders) -> build half
        AddShard(info, 1, 1);
        AddShard(info, 1, 2);
        MarkDone(info, 1, 1);
        // shardProgress = 1/2 = 0.5, levelProgress = 0.5 * 0.5 = 0.25
        // 100 * (1 - 1 + 0.25) / 2 = 12.5%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 12.5f, 1e-5f);
    }

    Y_UNIT_TEST(VectorIndex_Overlap_FilterHalf_FirstLevel) {
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildVectorIndex;
        info.KMeans.Levels = 2;
        info.KMeans.Level = 1;
        info.KMeans.OverlapClusters = 2;
        info.KMeans.State = TIndexBuildInfo::TKMeans::Filter;
        AddShard(info, 1, 1);
        AddShard(info, 1, 2);
        MarkDone(info, 1, 1);
        // shardProgress = 1/2 = 0.5, levelProgress = 0.5 + 0.5 * 0.5 = 0.75
        // 100 * (1 - 1 + 0.75) / 2 = 37.5%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 37.5f, 1e-5f);
    }

    Y_UNIT_TEST(VectorIndex_Overlap_BuildHalf_SecondLevel) {
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildVectorIndex;
        info.KMeans.Levels = 2;
        info.KMeans.Level = 2;
        info.KMeans.OverlapClusters = 2;
        // State = Sample (default, not Filter/FilterBorders) -> build half
        AddShard(info, 1, 1);
        AddShard(info, 1, 2);
        MarkDone(info, 1, 1);
        // Keep shard 2 in ToUploadShards so the early-return is NOT taken
        info.ToUploadShards.push_back(MakeShardIdx(1, 2));
        // shardProgress = 1/2 = 0.5, levelProgress = 0.5 * 0.5 = 0.25
        // 100 * (2 - 1 + 0.25) / 2 = 62.5%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 62.5f, 1e-5f);
    }

    Y_UNIT_TEST(VectorIndex_Overlap_FilterHalf_SecondLevel) {
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildVectorIndex;
        info.KMeans.Levels = 2;
        info.KMeans.Level = 2;
        info.KMeans.OverlapClusters = 2;
        info.KMeans.State = TIndexBuildInfo::TKMeans::Filter;
        AddShard(info, 1, 1);
        AddShard(info, 1, 2);
        MarkDone(info, 1, 1);
        // Keep shard 2 in ToUploadShards so the early-return is NOT taken
        info.ToUploadShards.push_back(MakeShardIdx(1, 2));
        // shardProgress = 1/2 = 0.5, levelProgress = 0.5 + 0.5 * 0.5 = 0.75
        // 100 * (2 - 1 + 0.75) / 2 = 87.5%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 87.5f, 1e-5f);
    }

    Y_UNIT_TEST(VectorIndex_Overlap_SingleLevel_NoFilterPass) {
        // OverlapClusters > 1 but Levels == 1: no Filter pass, behaves like non-overlap.
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildVectorIndex;
        info.KMeans.Levels = 1;
        info.KMeans.Level = 1;
        info.KMeans.OverlapClusters = 2;
        AddShard(info, 1, 1);
        AddShard(info, 1, 2);
        MarkDone(info, 1, 1);
        info.ToUploadShards.push_back(MakeShardIdx(1, 2));
        // hasFilterPass = false (Levels == 1), so levelProgress = shardProgress = 0.5
        // 100 * (1 - 1 + 0.5) / 1 = 50%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 50.f, 1e-5f);
    }

    // Fulltext relevance index: two shard-scan stages each covering 50%, with instant
    // single-row uploads between them:
    //   Stage 1 (None):                   shard scan -> posting table     [0%,  50%)
    //   Stage 2 (FulltextIndexStats):      upload aggregate stats row      50%
    //   Stage 3 (FulltextIndexDictionary): shard scan -> dictionary table  [50%, 100%)
    //   Stage 4 (FulltextIndexBorders):    upload border rows              100%

    Y_UNIT_TEST(FulltextRelevance_Stage1_NoShards) {
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildFulltext;
        info.IndexType = NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance;
        info.SubState = TIndexBuildInfo::ESubState::None;
        // No shards yet -> 0%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 0.f, 1e-5f);
    }

    Y_UNIT_TEST(FulltextRelevance_Stage1_HalfDone) {
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildFulltext;
        info.IndexType = NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance;
        info.SubState = TIndexBuildInfo::ESubState::None;
        AddShard(info, 1, 1);
        AddShard(info, 1, 2);
        MarkDone(info, 1, 1);
        // shardProgress = 1/2 = 0.5 -> 50 * 0.5 = 25%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 25.f, 1e-5f);
    }

    Y_UNIT_TEST(FulltextRelevance_Stage1_AllDone) {
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildFulltext;
        info.IndexType = NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance;
        info.SubState = TIndexBuildInfo::ESubState::None;
        AddShard(info, 1, 1);
        AddShard(info, 1, 2);
        MarkDone(info, 1, 1);
        MarkDone(info, 1, 2);
        // shardProgress = 1.0 -> 50 * 1.0 = 50%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 50.f, 1e-5f);
    }

    Y_UNIT_TEST(FulltextRelevance_Stage2) {
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildFulltext;
        info.IndexType = NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance;
        info.SubState = TIndexBuildInfo::ESubState::FulltextIndexStats;
        // Instant upload -> fixed 50%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 50.f, 1e-5f);
    }

    Y_UNIT_TEST(FulltextRelevance_Stage3_HalfDone) {
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildFulltext;
        info.IndexType = NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance;
        info.SubState = TIndexBuildInfo::ESubState::FulltextIndexDictionary;
        AddShard(info, 1, 1);
        AddShard(info, 1, 2);
        MarkDone(info, 1, 1);
        // shardProgress = 1/2 = 0.5 -> 50 + 50 * 0.5 = 75%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 75.f, 1e-5f);
    }

    Y_UNIT_TEST(FulltextRelevance_Stage3_AllDone) {
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildFulltext;
        info.IndexType = NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance;
        info.SubState = TIndexBuildInfo::ESubState::FulltextIndexDictionary;
        AddShard(info, 1, 1);
        AddShard(info, 1, 2);
        MarkDone(info, 1, 1);
        MarkDone(info, 1, 2);
        // shardProgress = 1.0 -> 50 + 50 * 1.0 = 100%
        // (stage 4 upload will immediately bring it to 100% too)
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 100.f, 1e-5f);
    }

    Y_UNIT_TEST(FulltextRelevance_Stage4) {
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildFulltext;
        info.IndexType = NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance;
        info.SubState = TIndexBuildInfo::ESubState::FulltextIndexBorders;
        // Instant upload -> fixed 100%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 100.f, 1e-5f);
    }

    // FulltextPlain / Json index: single shard-scan stage, uses plain 100 * done / total.

    Y_UNIT_TEST(FulltextPlain_NoShards) {
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildFulltext;
        info.IndexType = NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain;
        // No shards yet -> 0%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 0.f, 1e-5f);
    }

    Y_UNIT_TEST(FulltextPlain_HalfDone) {
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildFulltext;
        info.IndexType = NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain;
        AddShard(info, 1, 1);
        AddShard(info, 1, 2);
        MarkDone(info, 1, 1);
        // 100 * 1 / 2 = 50%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 50.f, 1e-5f);
    }

    Y_UNIT_TEST(FulltextPlain_AllDone) {
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildFulltext;
        info.IndexType = NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain;
        AddShard(info, 1, 1);
        AddShard(info, 1, 2);
        MarkDone(info, 1, 1);
        MarkDone(info, 1, 2);
        // 100 * 2 / 2 = 100%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 100.f, 1e-5f);
    }

    Y_UNIT_TEST(FulltextNonRelevance_UsesShardProgress) {
        // Non-relevance fulltext index: only 1 stage, uses plain shard-based progress
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildFulltext;
        info.IndexType = NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain;
        AddShard(info, 1, 1);
        AddShard(info, 1, 2);
        MarkDone(info, 1, 1);
        // 100 * 1 / 2 = 50%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 50.f, 1e-5f);
    }

    Y_UNIT_TEST(VectorIndex_Overlap_FilterBorders_CountsAsFilter) {
        TIndexBuildInfo info;
        info.BuildKind = TIndexBuildInfo::EBuildKind::BuildVectorIndex;
        info.KMeans.Levels = 2;
        info.KMeans.Level = 1;
        info.KMeans.OverlapClusters = 2;
        info.KMeans.State = TIndexBuildInfo::TKMeans::FilterBorders;
        AddShard(info, 1, 1);
        AddShard(info, 1, 2);
        MarkDone(info, 1, 1);
        MarkDone(info, 1, 2);
        // All shards done during FilterBorders -> shardProgress = 1.0
        // levelProgress = 0.5 + 0.5 * 1.0 = 1.0
        // 100 * (1 - 1 + 1.0) / 2 = 50%
        UNIT_ASSERT_DOUBLES_EQUAL(info.CalcProgressPercent(), 50.f, 1e-5f);
    }
}
