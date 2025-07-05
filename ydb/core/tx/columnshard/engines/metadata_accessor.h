#pragma once
#include <ydb/core/tx/columnshard/common/path_id.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NReader {
class TReadDescription;
}

namespace NKikimr::NOlap {
class ISourcesConstructor;
class IColumnEngine;
class ITableMetadataAccessor {
private:
    YDB_READONLY_DEF(TString, TablePath);

public:
    ITableMetadataAccessor(const TString& tablePath);
    virtual ~ITableMetadataAccessor() = default;

    TString GetTableName() const;
    virtual std::unique_ptr<ISourcesConstructor> SelectMetadata(
        const IColumnEngine& engine, const NReader::TReadDescription& readDescription, const bool withUncommitted) const = 0;
};

class TSysViewTableAccessor: public ITableMetadataAccessor {
private:
    using TBase = ITableMetadataAccessor;

public:
    TSysViewTableAccessor(const TString& tableName);
    virtual std::unique_ptr<ISourcesConstructor> SelectMetadata(
        const IColumnEngine& /*engine*/, const NReader::TReadDescription& /*readDescription*/, const bool /*withUncommitted*/) const override {
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
        const IColumnEngine& engine, const NReader::TReadDescription& readDescription, const bool withUncommitted) const override;
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

    virtual std::unique_ptr<ISourcesConstructor> SelectMetadata(
        const IColumnEngine& /*engine*/, const NReader::TReadDescription& /*readDescription*/, const bool /*withUncommitted*/) const override;
};

}   // namespace NKikimr::NOlap
