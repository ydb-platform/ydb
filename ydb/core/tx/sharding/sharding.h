#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>
#include <ydb/core/formats/arrow/size_calcer.h>

namespace NKikimrSchemeOp {
class TColumnTableSharding;
class TGranuleShardingLogicContainer;
}

namespace NKikimr::NSharding {

struct TExternalTableColumn;

class IGranuleShardingLogic {
public:
    using TProto = NKikimrSchemeOp::TGranuleShardingLogicContainer;
    using TFactory = NObjectFactory::TObjectFactory<IGranuleShardingLogic, TString>;

private:
    virtual NArrow::TColumnFilter DoGetFilter(const std::shared_ptr<arrow::Table>& table) const = 0;
    virtual std::set<TString> DoGetColumnNames() const = 0;
    virtual void DoSerializeToProto(TProto& proto) const = 0;
    virtual TConclusionStatus DoDeserializeFromProto(const TProto& proto) = 0;

public:
    IGranuleShardingLogic() = default;
    virtual ~IGranuleShardingLogic() = default;

    NArrow::TColumnFilter GetFilter(const std::shared_ptr<arrow::Table>& table) const {
        return DoGetFilter(table);
    }
    std::set<TString> GetColumnNames() const {
        return DoGetColumnNames();
    }

    virtual TString GetClassName() const = 0;

    void SerializeToProto(TProto& proto) const {
        DoSerializeToProto(proto);
    }
    TConclusionStatus DeserializeFromProto(const TProto& proto) {
        return DoDeserializeFromProto(proto);
    }
};

class TGranuleShardingLogicContainer: public NBackgroundTasks::TInterfaceProtoContainer<IGranuleShardingLogic> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoContainer<IGranuleShardingLogic>;
    using TProto = IGranuleShardingLogic::TProto;
public:
    using TBase::TBase;
};

class IShardingBase {
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
    virtual std::set<ui64> DoGetModifiedShardIds(const NKikimrSchemeOp::TShardingModification& proto) const = 0;
    virtual TConclusion<std::vector<NKikimrSchemeOp::TAlterShards>> DoBuildSplitShardsModifiers(const std::vector<ui64>& newTabletIds) const = 0;
    virtual TConclusionStatus DoOnAfterModification() = 0;
    virtual TConclusionStatus DoOnBeforeModification() = 0;
    virtual std::shared_ptr<IGranuleShardingLogic> DoGetTabletShardingInfoOptional(const ui64 tabletId) const = 0;

public:
    using TColumn = TExternalTableColumn;
public:
    IShardingBase() = default;

    TConclusionStatus OnAfterModification() {
        return DoOnAfterModification();
    }

    TConclusionStatus OnBeforeModification() {
        return DoOnBeforeModification();
    }

    TGranuleShardingLogicContainer GetTabletShardingInfoOptional(const ui64 tabletId) const {
        if (IsShardClosedForWrite(tabletId)) {
            return TGranuleShardingLogicContainer();
        }
        return TGranuleShardingLogicContainer(DoGetTabletShardingInfoOptional(tabletId));
    }

    TConclusionStatus ApplyModification(const NKikimrSchemeOp::TShardingModification& proto);
    std::set<ui64> GetModifiedShardIds(const NKikimrSchemeOp::TShardingModification& proto) const {
        return DoGetModifiedShardIds(proto);
    }

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
    static TConclusion<std::unique_ptr<IShardingBase>> BuildFromProto(const NSchemeShard::TOlapSchema& schema, const NKikimrSchemeOp::TColumnTableSharding& shardingInfo) {
        return BuildFromProto(&schema, shardingInfo);
    }
    static TConclusion<std::unique_ptr<IShardingBase>> BuildFromProto(const NSchemeShard::TOlapSchema* schema, const NKikimrSchemeOp::TColumnTableSharding& shardingInfo);
    static TConclusion<std::unique_ptr<IShardingBase>> BuildFromProto(const NKikimrSchemeOp::TColumnTableSharding& shardingInfo) {
        return BuildFromProto(nullptr, shardingInfo);
    }

    IShardingBase(const std::vector<ui64>& shardIds)
        : ShardIds(shardIds) {

    }

    ui32 GetShardsCount() const {
        return ShardIds.size();
    }

    NKikimrSchemeOp::TColumnTableSharding SerializeToProto() const;

    virtual THashMap<ui64, std::vector<ui32>> MakeSharding(const std::shared_ptr<arrow::RecordBatch>& batch) const = 0;

    TConclusion<THashMap<ui64, std::vector<NArrow::TSerializedBatch>>> SplitByShards(const std::shared_ptr<arrow::RecordBatch>& batch, const ui64 chunkBytesLimit);

    virtual TString DebugString() const;

    virtual ~IShardingBase() = default;
};

}
