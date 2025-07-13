#pragma once
#include <ydb/core/tx/columnshard/engines/metadata_accessor.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NAbstract {

class TAccessor: public ITableMetadataAccessor {
private:
    using TBase = ITableMetadataAccessor;

    const NColumnShard::TUnifiedPathId PathId;

protected:
    enum class EPathType {
        Unsupported,
        Table,
        Store
    };
    const EPathType PathType;

    std::optional<NColumnShard::TInternalPathId> GetTableFilterPathId() const {
        return PathType == EPathType::Table ? std::optional{ PathId.GetInternalPathId() } : std::nullopt;
    }

public:
    TAccessor(const TString& tablePath, const NColumnShard::TUnifiedPathId& pathId, const EPathType pathType)
        : TBase(tablePath)
        , PathId(pathId)
        , PathType(pathType)
    {
        AFL_VERIFY(pathType != EPathType::Unsupported);
    }

    virtual std::optional<NColumnShard::TUnifiedPathId> GetPathId() const override {
        return PathId;
    }

    virtual TString GetOverridenScanType(const TString& /*defScanType*/) const override {
        return "SIMPLE";
    }

    virtual bool NeedStalenessChecker() const override {
        return false;
    }

    virtual std::optional<TGranuleShardingInfo> GetShardingInfo(
        const std::shared_ptr<const TVersionedIndex>& /*indexVersionsPointer*/, const NOlap::TSnapshot& /*ss*/) const override {
        return std::nullopt;
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NAbstract
