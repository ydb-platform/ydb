#include "olap_helpers.h"
#include "helpers.h"

namespace NSchemeShardUT_Private {

ui32 CountSharedShardsRows(TTestActorRuntime& runtime, ui64 localShardIdx) {
    NKikimrMiniKQL::TResult result;
    TString err;
    TString rangeBound;
    if (localShardIdx != 0) {
        rangeBound = Sprintf(
            "'('ShardIdx (Uint64 '%lu) (Uint64 '%lu)) "
            "'('OwnerPathId (Null) (Void)) "
            "'('LocalPathId (Null) (Void))",
            localShardIdx, localShardIdx);
    } else {
        rangeBound =
            "'('ShardIdx (Null) (Void)) "
            "'('OwnerPathId (Null) (Void)) "
            "'('LocalPathId (Null) (Void))";
    }
    const TString query = Sprintf(R"___(
        (
            (let range '(%s))
            (let select '('ShardIdx 'OwnerPathId 'LocalPathId))
            (let result (SelectRange 'SharedShards range select '()))
            (return (AsList (SetResult 'R result)))
        )
    )___", rangeBound.c_str());
    auto status = LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, query, result, err);
    UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, err);
    return result.GetValue().GetStruct(0).GetOptional().GetStruct(0).ListSize();
}

ui64 GetShardOwnerLocalPathId(TTestActorRuntime& runtime, ui64 localShardIdx) {
    NKikimrMiniKQL::TResult result;
    TString err;
    const TString query = Sprintf(R"___(
        (
            (let range '('('ShardIdx (Uint64 '%lu) (Uint64 '%lu))))
            (let select '('ShardIdx 'PathId 'OwnerPathId))
            (let result (SelectRange 'Shards range select '()))
            (return (AsList (SetResult 'R result)))
        )
    )___", localShardIdx, localShardIdx);
    auto status = LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, query, result, err);
    UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, err);
    const auto& list = result.GetValue().GetStruct(0).GetOptional().GetStruct(0);
    if (list.ListSize() == 0) {
        return 0;
    }
    return list.GetList(0).GetStruct(1).GetOptional().GetUint64();
}

TVector<ui64> GetColumnShardTabletIds(TTestActorRuntime& runtime, const TString& path) {
    auto desc = DescribePath(runtime, path);
    const auto& sharding = desc.GetPathDescription().GetColumnTableDescription().GetSharding();
    TVector<ui64> ids;
    ids.reserve(sharding.ColumnShardsSize());
    for (ui64 id : sharding.GetColumnShards()) {
        ids.push_back(id);
    }
    return ids;
}

ui64 ResolveLocalShardIdxByTabletId(TTestActorRuntime& runtime, ui64 tabletId) {
    NKikimrMiniKQL::TResult result;
    TString err;
    const TString query = R"___(
        (
            (let range '('('ShardIdx (Uint64 '0) (Void))))
            (let select '('ShardIdx 'TabletId))
            (let result (SelectRange 'Shards range select '()))
            (return (AsList (SetResult 'R result)))
        )
    )___";
    auto status = LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, query, result, err);
    UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, err);
    const auto& list = result.GetValue().GetStruct(0).GetOptional().GetStruct(0);
    for (ui32 i = 0; i < list.ListSize(); ++i) {
        const auto& row = list.GetList(i);
        ui64 idx = row.GetStruct(0).GetOptional().GetUint64();
        ui64 tid = row.GetStruct(1).GetOptional().GetUint64();
        if (tid == tabletId) {
            return idx;
        }
    }
    return 0;
}

ui32 CountShardsTableRows(TTestActorRuntime& runtime) {
    NKikimrMiniKQL::TResult result;
    TString err;
    const TString query = R"___(
        (
            (let range '('('ShardIdx (Uint64 '0) (Void))))
            (let select '('ShardIdx))
            (let result (SelectRange 'Shards range select '()))
            (return (AsList (SetResult 'R result)))
        )
    )___";
    auto status = LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, query, result, err);
    UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, err);
    return result.GetValue().GetStruct(0).GetOptional().GetStruct(0).ListSize();
}

ui64 GetLocalPathId(TTestActorRuntime& runtime, const TString& path) {
    auto desc = DescribePath(runtime, path);
    return desc.GetPathDescription().GetSelf().GetPathId();
}

} // namespace NSchemeShardUT_Private
