#pragma once
#include <ydb/core/tx/columnshard/engines/metadata_accessor.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NAbstract {

class TAccessor: public ITableMetadataAccessor {
private:
    using TBase = ITableMetadataAccessor;

    const NColumnShard::TUnifiedPathId PathId;

    enum class EPathType {
        Unsupported,
        Table,
        Store
    };
    const EPathType PathType;

    static EPathType GetPathType(const TString& tablePath, const char (&tableSuffix)[], const char (&storeSuffix)[]) {
        if (tablePath.EndsWith(tableSuffix)) {
            return EPathType::Table;
        } else if (tablePath.EndsWith(storeSuffix)) {
            return EPathType::Store;
        } else {
            return EPathType::Unsupported;
        }
    }

protected:
    std::optional<NColumnShard::TInternalPathId> GetTableFilterPathId() const {
        return PathType == EPathType::Table ? std::optional{ PathId.GetInternalPathId() } : std::nullopt;
    }

public:
    TAccessor(const TString& path, const NColumnShard::TUnifiedPathId& pathId, const char (&tableSuffix)[], const char (&storeSuffix)[])
        : TBase(path)
        , PathId(pathId)
        , PathType(GetPathType(path, tableSuffix, storeSuffix))
    {
        AFL_VERIFY(PathType != EPathType::Unsupported)("path", path);
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
