#pragma once
#include <ydb/core/tx/columnshard/common/path_id.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap {

class ITableMetadataAccessor {
private:
    YDB_READONLY_DEF(TString, TablePath);

public:
    ITableMetadataAccessor(const TString& tablePath);

    TString GetTableName() const;
    std::unique_ptr<ISourcesConstructor> SelectMetadata() {
        if (readDescription.ReadNothing) {
            SelectInfo = std::make_shared<TSelectInfo>();
        } else {
            AFL_VERIFY(readDescription.PKRangesFilter);
            SelectInfo = readDescription.TableMetadataAccessor->SelectMetadata(Index);
            SelectInfo =
                Index->Select(readDescription.PathId.InternalPathId, readDescription.GetSnapshot(), *readDescription.PKRangesFilter, !!LockId);

            SelectInfo = dataAccessor.Select(readDescription, !!LockId);
        }
    }

};

class TSysViewTableAccessor: public ITableMetadataAccessor {
private:
    using TBase = ITableMetadataAccessor;

public:
    TSysViewTableAccessor(const TString& tableName);
    virtual std::unique_ptr<ISourcesConstructor> SelectMetadata(
        const IColumnEngine& /*engine*/, const TReadDescription& /*readDescription*/, const bool /*withUncommitted*/) override {
        AFL_VERIFY(false);
        return {};
    }
};

class TUserTableAccessor: public ITableMetadataAccessor {
private:
    using TBase = ITableMetadataAccessor;
    YDB_READONLY_DEF(NColumnShard::TUnifiedPathId, PathId);

public:
    TUserTableAccessor(const TString& tableName, const NColumnShard::TUnifiedPathId& pathId);

    virtual std::unique_ptr<ISourcesConstructor> SelectMetadata(
        const IColumnEngine& engine, const TReadDescription& readDescription, const bool withUncommitted) override;
};

class TAbsentTableAccessor: public ITableMetadataAccessor {
private:
    using TBase = ITableMetadataAccessor;
    YDB_READONLY_DEF(NColumnShard::TUnifiedPathId, PathId);

public:
    TAbsentTableAccessor(const TString& tableName, const NColumnShard::TUnifiedPathId& pathId)
        : TBase(tableName)
        , PathId(pathId) {
    }

    std::unique_ptr<ISourcesConstructor> SelectMetadata(const IColumnEngine& /*engine*/, const TReadDescription& /*readDescription*/, const bool /*withUncommitted*/) {
        return std::make_unique<TNotSortedPortionsSources>({});
    }
};

}   // namespace NKikimr::NOlap
