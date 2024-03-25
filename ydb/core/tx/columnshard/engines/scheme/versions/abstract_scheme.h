#pragma once
#include <ydb/core/tx/columnshard/engines/scheme/abstract/saver.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract/loader.h>

#include <ydb/core/tx/columnshard/common/snapshot.h>

#include <string>

namespace NKikimr::NOlap {

struct TIndexInfo;
class TSaverContext;

class ISnapshotSchema {
protected:
    virtual TString DoDebugString() const = 0;
public:
    using TPtr = std::shared_ptr<ISnapshotSchema>;

    virtual ~ISnapshotSchema() {}
    virtual std::shared_ptr<TColumnLoader> GetColumnLoaderOptional(const ui32 columnId) const = 0;
    std::shared_ptr<TColumnLoader> GetColumnLoaderVerified(const ui32 columnId) const;
    std::shared_ptr<TColumnLoader> GetColumnLoaderOptional(const std::string& columnName) const;
    std::shared_ptr<TColumnLoader> GetColumnLoaderVerified(const std::string& columnName) const;

    virtual TColumnSaver GetColumnSaver(const ui32 columnId) const = 0;
    TColumnSaver GetColumnSaver(const TString& columnName) const {
        return GetColumnSaver(GetColumnId(columnName));
    }
    TColumnSaver GetColumnSaver(const std::string& columnName) const {
        return GetColumnSaver(TString(columnName.data(), columnName.size()));
    }

    virtual std::optional<ui32> GetColumnIdOptional(const std::string& columnName) const = 0;
    virtual int GetFieldIndex(const ui32 columnId) const = 0;

    ui32 GetColumnId(const std::string& columnName) const;
    std::shared_ptr<arrow::Field> GetFieldByIndex(const int index) const;
    std::shared_ptr<arrow::Field> GetFieldByColumnIdOptional(const ui32 columnId) const;
    std::shared_ptr<arrow::Field> GetFieldByColumnIdVerified(const ui32 columnId) const;

    TString DebugString() const {
        return DoDebugString();
    }
    virtual const std::shared_ptr<arrow::Schema>& GetSchema() const = 0;
    virtual const TIndexInfo& GetIndexInfo() const = 0;
    virtual const TSnapshot& GetSnapshot() const = 0;
    virtual ui64 GetVersion() const = 0;
    virtual ui32 GetColumnsCount() const = 0;

    std::set<ui32> GetPkColumnsIds() const;

    std::shared_ptr<arrow::RecordBatch> NormalizeBatch(const ISnapshotSchema& dataSchema, const std::shared_ptr<arrow::RecordBatch> batch) const;
    std::shared_ptr<arrow::RecordBatch> PrepareForInsert(const TString& data, const std::shared_ptr<arrow::Schema>& dataSchema) const;
};

} // namespace NKikimr::NOlap
