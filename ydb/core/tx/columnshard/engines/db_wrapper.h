#pragma once
#include "defs.h"
#include "scheme/versions/versioned_index.h"
#include <ydb/core/tx/columnshard/common/blob.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/common/path_id.h>

namespace NKikimrTxColumnShard {
class TIndexPortionMeta;
}

namespace NKikimr::NTable {
class TDatabase;
}

namespace NKikimr::NOlap {

class TColumnChunkLoadContextV2;
class TIndexChunkLoadContext;
class TColumnRecord;
class TIndexChunk;
struct TGranuleRecord;
class IColumnEngine;
class TPortionInfo;
class TPortionInfoConstructor;

class IDbWrapper {
public:
    virtual ~IDbWrapper() = default;

    virtual const IBlobGroupSelector* GetDsGroupSelector() const = 0;
    const IBlobGroupSelector& GetDsGroupSelectorVerified() const {
        const auto* result = GetDsGroupSelector();
        AFL_VERIFY(result);
        return *result;
    }

    virtual void WriteColumns(const NOlap::TPortionInfo& portion, const NKikimrTxColumnShard::TIndexPortionAccessor& proto) = 0;

    virtual void WriteColumn(const TPortionInfo& portion, const TColumnRecord& row, const ui32 firstPKColumnId) = 0;
    virtual void EraseColumn(const TPortionInfo& portion, const TColumnRecord& row) = 0;
    virtual bool LoadColumns(const std::optional<TInternalPathId> pathId, const std::function<void(TColumnChunkLoadContextV2&&)>& callback) = 0;

    virtual void WritePortion(const NOlap::TPortionInfo& portion) = 0;
    virtual void ErasePortion(const NOlap::TPortionInfo& portion) = 0;
    virtual bool LoadPortions(const std::optional<TInternalPathId> pathId,
        const std::function<void(std::unique_ptr<NOlap::TPortionInfoConstructor>&&, const NKikimrTxColumnShard::TIndexPortionMeta&)>&
            callback) = 0;

    virtual void WriteIndex(const TPortionInfo& portion, const TIndexChunk& row) = 0;
    virtual void EraseIndex(const TPortionInfo& portion, const TIndexChunk& row) = 0;
    virtual bool LoadIndexes(const std::optional<TInternalPathId> pathId,
        const std::function<void(const TInternalPathId pathId, const ui64 portionId, TIndexChunkLoadContext&&)>& callback) = 0;

    virtual void WriteCounter(ui32 counterId, ui64 value) = 0;
    virtual bool LoadCounters(const std::function<void(ui32 id, ui64 value)>& callback) = 0;
    virtual TConclusion<THashMap<TInternalPathId, std::map<TSnapshot, TGranuleShardingInfo>>> LoadGranulesShardingInfo() = 0;
};

class TDbWrapper : public IDbWrapper {
public:
    TDbWrapper(NTable::TDatabase& db, const IBlobGroupSelector* dsGroupSelector)
        : Database(db)
        , DsGroupSelector(dsGroupSelector)
    {}

    void WritePortion(const NOlap::TPortionInfo& portion) override;
    void ErasePortion(const NOlap::TPortionInfo& portion) override;
    bool LoadPortions(const std::optional<TInternalPathId> pathId, const std::function<void(std::unique_ptr<NOlap::TPortionInfoConstructor>&&, const NKikimrTxColumnShard::TIndexPortionMeta&)>& callback) override;

    void WriteColumn(const NOlap::TPortionInfo& portion, const TColumnRecord& row, const ui32 firstPKColumnId) override;
    void WriteColumns(const NOlap::TPortionInfo& portion, const NKikimrTxColumnShard::TIndexPortionAccessor& proto) override;
    void EraseColumn(const NOlap::TPortionInfo& portion, const TColumnRecord& row) override;
    bool LoadColumns(const std::optional<TInternalPathId> pathId, const std::function<void(TColumnChunkLoadContextV2&&)>& callback) override;

    virtual void WriteIndex(const TPortionInfo& portion, const TIndexChunk& row) override;
    virtual void EraseIndex(const TPortionInfo& portion, const TIndexChunk& row) override;
    virtual bool LoadIndexes(const std::optional<TInternalPathId> pathId,
        const std::function<void(const TInternalPathId pathId, const ui64 portionId, TIndexChunkLoadContext&&)>& callback) override;

    void WriteCounter(ui32 counterId, ui64 value) override;
    bool LoadCounters(const std::function<void(ui32 id, ui64 value)>& callback) override;

    virtual TConclusion<THashMap<TInternalPathId, std::map<TSnapshot, TGranuleShardingInfo>>> LoadGranulesShardingInfo() override;

    virtual const IBlobGroupSelector* GetDsGroupSelector() const override {
        return DsGroupSelector;
    }

private:
    NTable::TDatabase& Database;
    const IBlobGroupSelector* DsGroupSelector;
};

}
