#pragma once
#include "scheme/index_info.h"
#include "scheme/versions/preset_schemas.h"
#include "scheme/versions/versioned_index.h"

#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/operations/manager.h>

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
    virtual bool NeedStalenessChecker() const {
        return true;
    }
    virtual ~ITableMetadataAccessor() = default;
    virtual TString GetOverridenScanType(const TString& defScanType) const {
        return defScanType;
    }
    virtual std::optional<NColumnShard::TUnifiedOptionalPathId> GetPathId() const {
        return std::nullopt;
    }
    NColumnShard::TUnifiedPathId GetPathIdVerified() const {
        std::optional<NColumnShard::TUnifiedOptionalPathId> result = GetPathId();
        AFL_VERIFY(result);
        return *result;
    }
    std::vector<TNameTypeInfo> GetPrimaryKeyScheme(const TVersionedPresetSchemas& vSchemas) const {
        return GetSnapshotSchemaVerified(vSchemas, TSnapshot::Max())->GetIndexInfo().GetPrimaryKeyColumns();
    }
    TString GetTableName() const;
    virtual std::shared_ptr<ISnapshotSchema> GetSnapshotSchemaOptional(
        const TVersionedPresetSchemas& vSchemas, const TSnapshot& snapshot) const = 0;
    std::shared_ptr<ISnapshotSchema> GetSnapshotSchemaVerified(const TVersionedPresetSchemas& vSchemas, const TSnapshot& snapshot) const {
        auto result = GetSnapshotSchemaOptional(vSchemas, snapshot);
        AFL_VERIFY(!!result);
        return result;
    }
    virtual std::shared_ptr<const TVersionedIndex> GetVersionedIndexCopyOptional(TVersionedPresetSchemas& vSchemas) const = 0;
    std::shared_ptr<const TVersionedIndex> GetVersionedIndexCopyVerified(TVersionedPresetSchemas& vSchemas) const {
        auto result = GetVersionedIndexCopyOptional(vSchemas);
        AFL_VERIFY(!!result);
        return result;
    }

    class TSelectMetadataContext {
    private:
        const NOlap::IPathIdTranslator& PathIdTranslator;
        const IColumnEngine& Engine;

    public:
        const NOlap::IPathIdTranslator& GetPathIdTranslator() const {
            return PathIdTranslator;
        }
        const IColumnEngine& GetEngine() const {
            return Engine;
        }

        TSelectMetadataContext(const NOlap::IPathIdTranslator& pathIdTranslator, const IColumnEngine& engine)
            : PathIdTranslator(pathIdTranslator)
            , Engine(engine) {
        }
    };

    virtual std::unique_ptr<NReader::NCommon::ISourcesConstructor> SelectMetadata(const TSelectMetadataContext& context,
        const NReader::TReadDescription& readDescription, const NColumnShard::IResolveWriteIdToLockId& resolver, const bool isPlain) const = 0;
    virtual std::optional<TGranuleShardingInfo> GetShardingInfo(
        const std::shared_ptr<const TVersionedIndex>& indexVersionsPointer, const NOlap::TSnapshot& ss) const = 0;
};

class TUserTableAccessor: public ITableMetadataAccessor {
private:
    using TBase = ITableMetadataAccessor;
    const NColumnShard::TUnifiedPathId PathId;
    virtual std::shared_ptr<ISnapshotSchema> GetSnapshotSchemaOptional(
        const TVersionedPresetSchemas& vSchemas, const TSnapshot& snapshot) const override {
        return vSchemas.GetDefaultVersionedIndex().GetSchemaVerified(snapshot);
    }

    virtual std::optional<NColumnShard::TUnifiedOptionalPathId> GetPathId() const override {
        return NColumnShard::TUnifiedOptionalPathId(PathId.GetInternalPathId(), PathId.GetSchemeShardLocalPathId());
    }

public:
    TUserTableAccessor(const TString& tableName, const NColumnShard::TUnifiedPathId& pathId);

    virtual std::shared_ptr<const TVersionedIndex> GetVersionedIndexCopyOptional(TVersionedPresetSchemas& vSchemas) const override {
        return vSchemas.GetDefaultVersionedIndexCopy();
    }

    virtual std::unique_ptr<NReader::NCommon::ISourcesConstructor> SelectMetadata(const TSelectMetadataContext& context,
        const NReader::TReadDescription& readDescription, const NColumnShard::IResolveWriteIdToLockId& resolver,
        const bool isPlain) const override;
    virtual std::optional<TGranuleShardingInfo> GetShardingInfo(
        const std::shared_ptr<const TVersionedIndex>& indexVersionsPointer, const NOlap::TSnapshot& ss) const override {
        return indexVersionsPointer->GetShardingInfoOptional(PathId.GetInternalPathId(), ss);
    }
};

class TAbsentTableAccessor: public ITableMetadataAccessor {
private:
    using TBase = ITableMetadataAccessor;
    const NColumnShard::TUnifiedPathId PathId;

    virtual std::shared_ptr<const TVersionedIndex> GetVersionedIndexCopyOptional(TVersionedPresetSchemas& vSchemas) const override {
        return vSchemas.GetDefaultVersionedIndexCopy();
    }

    virtual std::shared_ptr<ISnapshotSchema> GetSnapshotSchemaOptional(
        const TVersionedPresetSchemas& vSchemas, const TSnapshot& snapshot) const override {
        return vSchemas.GetDefaultVersionedIndex().GetSchemaVerified(snapshot);
    }

public:
    TAbsentTableAccessor(const TString& tableName, const NColumnShard::TUnifiedPathId& pathId)
        : TBase(tableName)
        , PathId(pathId) {
    }

    virtual std::optional<NColumnShard::TUnifiedOptionalPathId> GetPathId() const override {
        return NColumnShard::TUnifiedOptionalPathId(PathId.GetInternalPathId(), PathId.GetSchemeShardLocalPathId());
    }

    virtual std::optional<TGranuleShardingInfo> GetShardingInfo(
        const std::shared_ptr<const TVersionedIndex>& /*indexVersionsPointer*/, const NOlap::TSnapshot& /*ss*/) const override {
        return std::nullopt;
    }
    virtual std::unique_ptr<NReader::NCommon::ISourcesConstructor> SelectMetadata(const TSelectMetadataContext& context,
        const NReader::TReadDescription& readDescription, const NColumnShard::IResolveWriteIdToLockId& resolver,
        const bool isPlain) const override;
};

}   // namespace NKikimr::NOlap
