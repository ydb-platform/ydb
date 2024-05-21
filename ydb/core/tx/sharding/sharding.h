#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>

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
    virtual std::shared_ptr<NArrow::TColumnFilter> DoGetFilter(const std::shared_ptr<arrow::Table>& table) const = 0;
    virtual std::set<TString> DoGetColumnNames() const = 0;
    virtual void DoSerializeToProto(TProto& proto) const = 0;
    virtual TConclusionStatus DoDeserializeFromProto(const TProto& proto) = 0;

public:
    IGranuleShardingLogic() = default;
    virtual ~IGranuleShardingLogic() = default;

    std::shared_ptr<NArrow::TColumnFilter> GetFilter(const std::shared_ptr<arrow::Table>& table) const {
        return DoGetFilter(table);
    }

    std::shared_ptr<NArrow::TColumnFilter> GetFilter(const std::shared_ptr<arrow::RecordBatch>& rb) const {
        return DoGetFilter(NArrow::TStatusValidator::GetValid(arrow::Table::FromRecordBatches({ rb })));
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

class TShardInfo {
private:
    YDB_READONLY(ui64, TabletId, 0);
    YDB_ACCESSOR(ui32, SequenceIdx, 0);
    YDB_ACCESSOR(bool, IsOpenForRead, true);
    YDB_ACCESSOR(bool, IsOpenForWrite, true);
    YDB_ACCESSOR_DEF(std::optional<NKikimr::NOlap::TSnapshot>, OpenForWriteSnapshot);
    YDB_READONLY(ui32, ShardingVersion, 0);

    TShardInfo() = default;

    TConclusionStatus DeserializeFromProto(const NKikimrSchemeOp::TGranuleShardInfo& proto);
public:
    TShardInfo(const ui64 tabletId, const ui32 seqIdx)
        : TabletId(tabletId)
        , SequenceIdx(seqIdx)
    {

    }

    const NKikimr::NOlap::TSnapshot& GetOpenForWriteSnapshotVerified() const {
        AFL_VERIFY(OpenForWriteSnapshot);
        return *OpenForWriteSnapshot;
    }

    void IncrementVersion() {
        ++ShardingVersion;
    }

    NKikimrSchemeOp::TGranuleShardInfo SerializeToProto() const;

    static TConclusion<TShardInfo> BuildFromProto(const NKikimrSchemeOp::TGranuleShardInfo& proto) {
        TShardInfo result;
        auto conclusion = result.DeserializeFromProto(proto);
        if (conclusion.IsFail()) {
            return conclusion;
        }
        return result;
    }
};

class IShardingBase {
private:
    std::vector<ui64> OrderedShardIds;
    THashMap<ui64, TShardInfo> Shards;
    TConclusionStatus DeserializeFromProto(const NKikimrSchemeOp::TColumnTableSharding& proto);

    void InitializeFromOrdered(const std::vector<ui64>& orderedIds);

protected:
    std::set<ui64> GetClosedWritingShardIds() const {
        std::set<ui64> result;
        for (auto&& i : Shards) {
            if (!i.second.GetIsOpenForWrite()) {
                result.emplace(i.first);
            }
        }
        return result;
    }

    std::set<ui64> GetClosedReadingShardIds() const {
        std::set<ui64> result;
        for (auto&& i : Shards) {
            if (!i.second.GetIsOpenForRead()) {
                result.emplace(i.first);
            }
        }
        return result;
    }

    void SetOrderedShardIds(const std::vector<ui64>& ids) {
        ui32 idx = 0;
        for (auto&& i : ids) {
            auto* shardInfo = GetShardInfo(i);
            AFL_VERIFY(shardInfo);
            shardInfo->SetSequenceIdx(idx++);
        }
        OrderedShardIds = ids;
        AFL_VERIFY(OrderedShardIds.size() == Shards.size());
    }

    const std::vector<ui64>& GetOrderedShardIds() const {
        return OrderedShardIds;
    }

    bool HasReadClosedShards() const {
        for (auto&& [_, i] : Shards) {
            if (!i.GetIsOpenForRead()) {
                return true;
            }
        }
        return false;
    }

    bool HasWriteClosedShards() const {
        for (auto&& [_, i] : Shards) {
            if (!i.GetIsOpenForWrite()) {
                return true;
            }
        }
        return false;
    }
    virtual TString GetClassName() const = 0;
    virtual void DoSerializeToProto(NKikimrSchemeOp::TColumnTableSharding& proto) const = 0;
    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrSchemeOp::TColumnTableSharding& proto) = 0;
    virtual TConclusionStatus DoApplyModification(const NKikimrSchemeOp::TShardingModification& proto) = 0;
    virtual std::set<ui64> DoGetModifiedShardIds(const NKikimrSchemeOp::TShardingModification& proto) const = 0;
    virtual TConclusion<std::vector<NKikimrSchemeOp::TAlterShards>> DoBuildSplitShardsModifiers(const std::vector<ui64>& /*newTabletIds*/) const {
        return TConclusionStatus::Fail("shards splitting not implemented for " + GetClassName());
    }
    virtual TConclusion<std::vector<NKikimrSchemeOp::TAlterShards>> DoBuildMergeShardsModifiers(const std::vector<ui64>& /*newTabletIds*/) const {
        return TConclusionStatus::Fail("shards merging not implemented for " + GetClassName());
    }
    virtual TConclusionStatus DoOnAfterModification() = 0;
    virtual TConclusionStatus DoOnBeforeModification() = 0;
    virtual std::shared_ptr<IGranuleShardingLogic> DoGetTabletShardingInfoOptional(const ui64 tabletId) const = 0;

public:
    using TColumn = TExternalTableColumn;
public:
    IShardingBase() = default;

    TShardInfo& GetShardInfoVerified(const ui64 tabletId) {
        auto it = Shards.find(tabletId);
        AFL_VERIFY(it != Shards.end());
        return it->second;
    }

    const TShardInfo& GetShardInfoVerified(const ui64 tabletId) const {
        auto it = Shards.find(tabletId);
        AFL_VERIFY(it != Shards.end());
        return it->second;
    }

    TShardInfo* GetShardInfo(const ui64 tabletId) {
        auto it = Shards.find(tabletId);
        if (it == Shards.end()) {
            return nullptr;
        }
        return &it->second;
    }

    const TShardInfo* GetShardInfo(const ui64 tabletId) const {
        auto it = Shards.find(tabletId);
        if (it == Shards.end()) {
            return nullptr;
        }
        return &it->second;
    }

    void SetShardingOpenSnapshotVerified(const ui64 tabletId, const NKikimr::NOlap::TSnapshot& ss) {
        GetShardInfoVerified(tabletId).SetOpenForWriteSnapshot(ss);
    }

    NKikimr::NOlap::TSnapshot GetShardingOpenSnapshotVerified(const ui64 tabletId) const {
        return GetShardInfoVerified(tabletId).GetOpenForWriteSnapshotVerified();
    }

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

    TConclusion<std::vector<NKikimrSchemeOp::TAlterShards>> BuildMergeShardsModifiers(const std::vector<ui64>& newTabletIds) const {
        return DoBuildMergeShardsModifiers(newTabletIds);
    }

    TConclusion<std::vector<NKikimrSchemeOp::TAlterShards>> BuildAddShardsModifiers(const std::vector<ui64>& newTabletIds) const;
    TConclusion<std::vector<NKikimrSchemeOp::TAlterShards>> BuildReduceShardsModifiers(const std::vector<ui64>& newTabletIds) const;

    void CloseShardWriting(const ui64 shardId) {
        GetShardInfoVerified(shardId).SetIsOpenForWrite(false);
    }

    void CloseShardReading(const ui64 shardId) {
        GetShardInfoVerified(shardId).SetIsOpenForRead(false);
    }

    bool IsActiveForWrite(const ui64 tabletId) const {
        return GetShardInfoVerified(tabletId).GetIsOpenForWrite();
    }

    bool IsActiveForRead(const ui64 tabletId) const {
        return GetShardInfoVerified(tabletId).GetIsOpenForRead();
    }

    std::vector<ui64> GetActiveReadShardIds() const {
        std::vector<ui64> result;
        for (auto&& [_, i] : Shards) {
            if (i.GetIsOpenForRead()) {
                result.emplace_back(i.GetTabletId());
            }
        }
        return result;
    }

    ui64 GetShardIdByOrderIdx(const ui32 shardIdx) const {
        AFL_VERIFY(shardIdx < OrderedShardIds.size());
        return OrderedShardIds[shardIdx];
    }

    bool IsShardExists(const ui64 shardId) const {
        return Shards.contains(shardId);
    }

    bool IsShardClosedForRead(const ui64 shardId) const {
        return !IsActiveForRead(shardId);
    }

    bool IsShardClosedForWrite(const ui64 shardId) const {
        return !IsActiveForWrite(shardId);
    }

    static TConclusionStatus ValidateBehaviour(const NSchemeShard::TOlapSchema& schema, const NKikimrSchemeOp::TColumnTableSharding& shardingInfo);
    static TConclusion<std::unique_ptr<IShardingBase>> BuildFromProto(const NSchemeShard::TOlapSchema& schema, const NKikimrSchemeOp::TColumnTableSharding& shardingInfo) {
        return BuildFromProto(&schema, shardingInfo);
    }
    static TConclusion<std::unique_ptr<IShardingBase>> BuildFromProto(const NSchemeShard::TOlapSchema* schema, const NKikimrSchemeOp::TColumnTableSharding& shardingInfo);
    static TConclusion<std::unique_ptr<IShardingBase>> BuildFromProto(const NKikimrSchemeOp::TColumnTableSharding& shardingInfo) {
        return BuildFromProto(nullptr, shardingInfo);
    }

    IShardingBase(const std::vector<ui64>& shardIds) {
        InitializeFromOrdered(shardIds);
    }

    ui32 GetShardsCount() const {
        return Shards.size();
    }

    NKikimrSchemeOp::TColumnTableSharding SerializeToProto() const;

    virtual THashMap<ui64, std::vector<ui32>> MakeSharding(const std::shared_ptr<arrow::RecordBatch>& batch) const = 0;

    TConclusion<THashMap<ui64, std::vector<NArrow::TSerializedBatch>>> SplitByShards(const std::shared_ptr<arrow::RecordBatch>& batch, const ui64 chunkBytesLimit);

    virtual TString DebugString() const;

    virtual ~IShardingBase() = default;
};

}
