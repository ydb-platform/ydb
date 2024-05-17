#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>
#include <ydb/core/formats/arrow/size_calcer.h>

namespace NKikimrSchemeOp {
class TColumnTableSharding;
}

namespace NKikimr::NSharding {

struct TExternalTableColumn;

class TShardingBase {
private:
    mutable std::vector<ui64> ShardIds;
    YDB_READONLY_DEF(std::set<ui64>, ClosedWritingShardIds);
    YDB_READONLY_DEF(std::set<ui64>, ClosedReadingShardIds);
    YDB_READONLY(ui64, Version, 1);
    TConclusionStatus DeserializeFromProto(const NKikimrSchemeOp::TColumnTableSharding& proto);

protected:
    void SetShardIds(const std::vector<ui64>& ids) const {
        ShardIds = ids;
    }

    const std::vector<ui64>& GetShardIds() const {
        return ShardIds;
    }

    bool HasReadClosedShards() const {
        return ClosedReadingShardIds.size();
    }

    bool HasWriteClosedShards() const {
        return ClosedWritingShardIds.size();
    }

    ui64 GetShardId(const ui32 i) const {
        AFL_VERIFY(i < ShardIds.size());
        return ShardIds[i];
    }

    virtual void DoSerializeToProto(NKikimrSchemeOp::TColumnTableSharding& proto) const = 0;
    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrSchemeOp::TColumnTableSharding& proto) = 0;
    virtual TConclusionStatus DoApplyModification(const NKikimrSchemeOp::TShardingModification& proto) = 0;
    virtual TConclusion<std::vector<NKikimrSchemeOp::TAlterShards>> DoBuildSplitShardsModifiers(const std::vector<ui64>& newTabletIds) const = 0;
    virtual TConclusionStatus DoOnAfterModification() = 0;
    virtual TConclusionStatus DoOnBeforeModification() = 0;
public:
    using TColumn = TExternalTableColumn;
public:
    TShardingBase() = default;

    TConclusionStatus OnAfterModification() {
        return DoOnAfterModification();
    }

    TConclusionStatus OnBeforeModification() {
        return DoOnBeforeModification();
    }

    TConclusionStatus ApplyModification(const NKikimrSchemeOp::TShardingModification& proto);

    TConclusion<std::vector<NKikimrSchemeOp::TAlterShards>> BuildSplitShardsModifiers(const std::vector<ui64>& newTabletIds) const {
        return DoBuildSplitShardsModifiers(newTabletIds);
    }

    TConclusion<std::vector<NKikimrSchemeOp::TAlterShards>> BuildAddShardsModifiers(const std::vector<ui64>& newTabletIds) const;

    void CloseShardWriting(const ui64 shardId) {
        AFL_VERIFY(IsShardExists(shardId));
        ClosedWritingShardIds.emplace(shardId);
    }

    void CloseShardReading(const ui64 shardId) {
        AFL_VERIFY(IsShardExists(shardId));
        ClosedReadingShardIds.emplace(shardId);
    }

    bool IsActiveForWrite(const ui64 tabletId) const {
        AFL_VERIFY(IsShardExists(tabletId));
        return !ClosedWritingShardIds.contains(tabletId);
    }

    bool IsActiveForRead(const ui64 tabletId) const {
        AFL_VERIFY(IsShardExists(tabletId));
        return !ClosedReadingShardIds.contains(tabletId);
    }

    std::vector<ui64> GetActiveReadShardIds() const {
        std::vector<ui64> result;
        for (auto&& i : ShardIds) {
            if (IsActiveForRead(i)) {
                result.emplace_back(i);
            }
        }
        return result;
    }

    bool IsShardExists(const ui64 shardId) const {
        for (auto&& i : ShardIds) {
            if (i == shardId) {
                return true;
            }
        }
        return false;
    }

    bool IsShardClosedForRead(const ui64 shardId) const {
        return ClosedReadingShardIds.contains(shardId);
    }

    bool IsShardClosedForWrite(const ui64 shardId) const {
        return ClosedWritingShardIds.contains(shardId);
    }

    static TConclusionStatus ValidateBehaviour(const NSchemeShard::TOlapSchema& schema, const NKikimrSchemeOp::TColumnTableSharding& shardingInfo);
    static TConclusion<std::unique_ptr<TShardingBase>> BuildFromProto(const NSchemeShard::TOlapSchema& schema, const NKikimrSchemeOp::TColumnTableSharding& shardingInfo) {
        return BuildFromProto(&schema, shardingInfo);
    }
    static TConclusion<std::unique_ptr<TShardingBase>> BuildFromProto(const NSchemeShard::TOlapSchema* schema, const NKikimrSchemeOp::TColumnTableSharding& shardingInfo);
    static TConclusion<std::unique_ptr<TShardingBase>> BuildFromProto(const NKikimrSchemeOp::TColumnTableSharding& shardingInfo) {
        return BuildFromProto(nullptr, shardingInfo);
    }

    TShardingBase(const std::vector<ui64>& shardIds)
        : ShardIds(shardIds) {

    }

    ui32 GetShardsCount() const {
        return ShardIds.size();
    }

    NKikimrSchemeOp::TColumnTableSharding SerializeToProto() const;

    virtual THashMap<ui64, std::vector<ui32>> MakeSharding(const std::shared_ptr<arrow::RecordBatch>& batch) const = 0;

    TConclusion<THashMap<ui64, std::vector<NArrow::TSerializedBatch>>> SplitByShards(const std::shared_ptr<arrow::RecordBatch>& batch, const ui64 chunkBytesLimit);

    virtual TString DebugString() const;

    virtual ~TShardingBase() = default;
};

}
