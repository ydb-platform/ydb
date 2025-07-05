#pragma once
#include "scheme/versions/versioned_index.h"

#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NReader {
class TReadDescription;
}

namespace NKikimr::NOlap::NReader::NCommon {
class ISourcesConstructor;
}

namespace NKikimr::NOlap {
class IColumnEngine;
class ITableMetadataAccessor {
private:
    YDB_READONLY_DEF(TString, TablePath);

public:
    ITableMetadataAccessor(const TString& tablePath);
    virtual ~ITableMetadataAccessor() = default;
    virtual NColumnShard::TUnifiedPathId GetPathId() const {
        AFL_VERIFY(false);
        return NColumnShard::TUnifiedPathId();
    }
    TString GetTableName() const;
    virtual std::unique_ptr<NReader::NCommon::ISourcesConstructor> SelectMetadata(
        const IColumnEngine& engine, const NReader::TReadDescription& readDescription, const bool withUncommitted, const bool isPlain) const = 0;
    virtual std::optional<TGranuleShardingInfo> GetShardingInfo(
        const std::shared_ptr<TVersionedIndex>& indexVersionsPointer, const NOlap::TSnapshot& ss) const = 0;
};

class TSysViewTableAccessor: public ITableMetadataAccessor {
private:
    using TBase = ITableMetadataAccessor;
    const NColumnShard::TUnifiedPathId PathId;

    virtual NColumnShard::TUnifiedPathId GetPathId() const override {
        return PathId;
    }

public:
    TSysViewTableAccessor(const TString& tableName, const NColumnShard::TUnifiedPathId& pathId);
    virtual std::unique_ptr<NReader::NCommon::ISourcesConstructor> SelectMetadata(const IColumnEngine& engine,
        const NReader::TReadDescription& readDescription, const bool withUncommitted, const bool isPlain) const override;
    virtual std::optional<TGranuleShardingInfo> GetShardingInfo(
        const std::shared_ptr<TVersionedIndex>& /*indexVersionsPointer*/, const NOlap::TSnapshot& /*ss*/) const override {
        return std::nullopt;
    }
};

class TUserTableAccessor: public ITableMetadataAccessor {
private:
    using TBase = ITableMetadataAccessor;
    const NColumnShard::TUnifiedPathId PathId;

    virtual NColumnShard::TUnifiedPathId GetPathId() const override {
        return PathId;
    }

public:
    TUserTableAccessor(const TString& tableName, const NColumnShard::TUnifiedPathId& pathId);

    virtual std::unique_ptr<NReader::NCommon::ISourcesConstructor> SelectMetadata(const IColumnEngine& engine,
        const NReader::TReadDescription& readDescription, const bool withUncommitted, const bool isPlain) const override;
    virtual std::optional<TGranuleShardingInfo> GetShardingInfo(
        const std::shared_ptr<TVersionedIndex>& indexVersionsPointer, const NOlap::TSnapshot& ss) const override {
        return indexVersionsPointer->GetShardingInfoOptional(PathId.GetInternalPathId(), ss);
    }
};

class TAbsentTableAccessor: public ITableMetadataAccessor {
private:
    using TBase = ITableMetadataAccessor;
    const NColumnShard::TUnifiedPathId PathId;

public:
    TAbsentTableAccessor(const TString& tableName, const NColumnShard::TUnifiedPathId& pathId)
        : TBase(tableName)
        , PathId(pathId) {
    }

    virtual NColumnShard::TUnifiedPathId GetPathId() const override {
        return PathId;
    }

    virtual std::optional<TGranuleShardingInfo> GetShardingInfo(
        const std::shared_ptr<TVersionedIndex>& /*indexVersionsPointer*/, const NOlap::TSnapshot& /*ss*/) const override {
        return std::nullopt;
    }
    virtual std::unique_ptr<NReader::NCommon::ISourcesConstructor> SelectMetadata(const IColumnEngine& engine,
        const NReader::TReadDescription& readDescription, const bool withUncommitted, const bool isPlain) const override;
};

}   // namespace NKikimr::NOlap
