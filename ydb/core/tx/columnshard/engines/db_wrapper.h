#pragma once
#include "defs.h"

namespace NKikimr::NTable {
class TDatabase;
}

namespace NKikimr::NOlap {

struct TInsertedData;
class TInsertTableAccessor;
struct TColumnRecord;
struct TGranuleRecord;
class IColumnEngine;
struct TPortionInfo;

class IDbWrapper {
public:
    virtual ~IDbWrapper() = default;

    virtual void Insert(const TInsertedData& data) = 0;
    virtual void Commit(const TInsertedData& data) = 0;
    virtual void Abort(const TInsertedData& data) = 0;
    virtual void EraseInserted(const TInsertedData& data) = 0;
    virtual void EraseCommitted(const TInsertedData& data) = 0;
    virtual void EraseAborted(const TInsertedData& data) = 0;

    virtual bool Load(TInsertTableAccessor& insertTable,
                      const TInstant& loadTime) = 0;

    virtual void WriteGranule(ui32 index, const IColumnEngine& engine, const TGranuleRecord& row) = 0;
    virtual void EraseGranule(ui32 index, const IColumnEngine& engine, const TGranuleRecord& row) = 0;
    virtual bool LoadGranules(ui32 index, const IColumnEngine& engine, const std::function<void(const TGranuleRecord&)>& callback) = 0;

    virtual void WriteColumn(ui32 index, const TPortionInfo& portion, const TColumnRecord& row) = 0;
    virtual void EraseColumn(ui32 index, const TPortionInfo& portion, const TColumnRecord& row) = 0;
    virtual bool LoadColumns(ui32 index, const std::function<void(const TPortionInfo&, const TColumnRecord&)>& callback) = 0;

    virtual void WriteCounter(ui32 index, ui32 counterId, ui64 value) = 0;
    virtual bool LoadCounters(ui32 index, const std::function<void(ui32 id, ui64 value)>& callback) = 0;
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

    bool Load(TInsertTableAccessor& insertTable,
              const TInstant& loadTime) override;

    void WriteGranule(ui32 index, const IColumnEngine& engine, const TGranuleRecord& row) override;
    void EraseGranule(ui32 index, const IColumnEngine& engine, const TGranuleRecord& row) override;
    bool LoadGranules(ui32 index, const IColumnEngine& engine, const std::function<void(const TGranuleRecord&)>& callback) override;

    void WriteColumn(ui32 index, const NOlap::TPortionInfo& portion, const TColumnRecord& row) override;
    void EraseColumn(ui32 index, const NOlap::TPortionInfo& portion, const TColumnRecord& row) override;
    bool LoadColumns(ui32 index, const std::function<void(const NOlap::TPortionInfo&, const TColumnRecord&)>& callback) override;

    void WriteCounter(ui32 index, ui32 counterId, ui64 value) override;
    bool LoadCounters(ui32 index, const std::function<void(ui32 id, ui64 value)>& callback) override;

private:
    NTable::TDatabase& Database;
    const IBlobGroupSelector* DsGroupSelector;
};

}
