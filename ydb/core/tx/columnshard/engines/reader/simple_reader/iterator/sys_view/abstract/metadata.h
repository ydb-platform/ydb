#pragma once
#include <ydb/core/tx/columnshard/engines/metadata_accessor.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NAbstract {

class TAccessor: public ITableMetadataAccessor {
private:
    using TBase = ITableMetadataAccessor;

    const NColumnShard::TUnifiedOptionalPathId PathId;

protected:
    std::optional<NColumnShard::TInternalPathId> GetTableFilterPathId() const {
        return PathId.GetInternalPathIdOptional();
    }

public:
    TAccessor(const TString& path, const NColumnShard::TUnifiedOptionalPathId pathId)
        : TBase(path)
        , PathId(pathId) {
    }

    virtual std::optional<NColumnShard::TUnifiedOptionalPathId> GetPathId() const override {
        return PathId;
    }

    virtual TString GetOverridenScanType(const TString& /*defScanType*/) const override {
        return "SIMPLE";
    }

    virtual bool NeedDuplicateFiltering() const override {
        return false;
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
