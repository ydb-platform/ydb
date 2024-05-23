#pragma once
#include "defs.h"
#include "scheme/versions/versioned_index.h"
#include <ydb/core/tx/columnshard/common/blob.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>

namespace NKikimrTxColumnShard {
class TIndexPortionMeta;
}

namespace NKikimr::NTable {
class TDatabase;
}

namespace NKikimr::NOlap {

class TColumnChunkLoadContext;
class TIndexChunkLoadContext;
struct TInsertedData;
class TInsertTableAccessor;
class TColumnRecord;
class TIndexChunk;
struct TGranuleRecord;
class IColumnEngine;
class TPortionInfo;
class TPortionInfoConstructor;

class IDbWrapper {
public:
    virtual ~IDbWrapper() = default;

    virtual void Insert(const TInsertedData& data) = 0;
    virtual void Commit(const TInsertedData& data) = 0;
    virtual void Abort(const TInsertedData& data) = 0;
    virtual void EraseInserted(const TInsertedData& data) = 0;
    virtual void EraseCommitted(const TInsertedData& data) = 0;
    virtual void EraseAborted(const TInsertedData& data) = 0;

    virtual bool Load(TInsertTableAccessor& insertTable, const TInstant& loadTime) = 0;

    virtual void WriteColumn(const TPortionInfo& portion, const TColumnRecord& row, const ui32 firstPKColumnId) = 0;
    virtual void EraseColumn(const TPortionInfo& portion, const TColumnRecord& row) = 0;
    virtual bool LoadColumns(const std::function<void(NOlap::TPortionInfoConstructor&&, const TColumnChunkLoadContext&)>& callback) = 0;

    virtual void WritePortion(const NOlap::TPortionInfo& portion) = 0;
    virtual void ErasePortion(const NOlap::TPortionInfo& portion) = 0;
    virtual bool LoadPortions(const std::function<void(NOlap::TPortionInfoConstructor&&, const NKikimrTxColumnShard::TIndexPortionMeta&)>& callback) = 0;

    virtual void WriteIndex(const TPortionInfo& portion, const TIndexChunk& row) = 0;
    virtual void EraseIndex(const TPortionInfo& portion, const TIndexChunk& row) = 0;
    virtual bool LoadIndexes(const std::function<void(const ui64 pathId, const ui64 portionId, const TIndexChunkLoadContext&)>& callback) = 0;

    virtual void WriteCounter(ui32 counterId, ui64 value) = 0;
    virtual bool LoadCounters(const std::function<void(ui32 id, ui64 value)>& callback) = 0;
    virtual TConclusion<THashMap<ui64, std::map<TSnapshot, TGranuleShardingInfo>>> LoadGranulesShardingInfo() = 0;
};

class TDbWrapper : public IDbWrapper {
public:
    TDbWrapper(NTable::TDatabase& db, const IBlobGroupSelector* dsGroupSelector)
        : Database(db)
        , DsGroupSelector(dsGroupSelector)
    {}

    void Insert(const TInsertedData& data) override;
    void Commit(const TInsertedData& data) override;
    void Abort(const TInsertedData& data) override;
    void EraseInserted(const TInsertedData& data) override;
    void EraseCommitted(const TInsertedData& data) override;
    void EraseAborted(const TInsertedData& data) override;

    bool Load(TInsertTableAccessor& insertTable, const TInstant& loadTime) override;

    void WritePortion(const NOlap::TPortionInfo& portion) override;
    void ErasePortion(const NOlap::TPortionInfo& portion) override;
    bool LoadPortions(const std::function<void(NOlap::TPortionInfoConstructor&&, const NKikimrTxColumnShard::TIndexPortionMeta&)>& callback) override;

    void WriteColumn(const NOlap::TPortionInfo& portion, const TColumnRecord& row, const ui32 firstPKColumnId) override;
    void EraseColumn(const NOlap::TPortionInfo& portion, const TColumnRecord& row) override;
    bool LoadColumns(const std::function<void(NOlap::TPortionInfoConstructor&&, const TColumnChunkLoadContext&)>& callback) override;

    virtual void WriteIndex(const TPortionInfo& portion, const TIndexChunk& row) override;
    virtual void EraseIndex(const TPortionInfo& portion, const TIndexChunk& row) override;
    virtual bool LoadIndexes(const std::function<void(const ui64 pathId, const ui64 portionId, const TIndexChunkLoadContext&)>& callback) override;

    void WriteCounter(ui32 counterId, ui64 value) override;
    bool LoadCounters(const std::function<void(ui32 id, ui64 value)>& callback) override;

    virtual TConclusion<THashMap<ui64, std::map<TSnapshot, TGranuleShardingInfo>>> LoadGranulesShardingInfo() override;

private:
    NTable::TDatabase& Database;
    const IBlobGroupSelector* DsGroupSelector;
};

}
