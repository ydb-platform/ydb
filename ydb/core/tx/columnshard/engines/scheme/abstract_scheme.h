#pragma once

#include <ydb/core/tx/columnshard/engines/defs.h>
#include <ydb/core/tx/columnshard/engines/column_features.h>

#include <string>

#include <ydb/core/formats/arrow/arrow_helpers.h>

namespace NKikimr::NOlap {

struct TIndexInfo;

class ISnapshotSchema {
public:
    using TPtr = std::shared_ptr<ISnapshotSchema>;

    virtual ~ISnapshotSchema() {}
    virtual std::shared_ptr<TColumnLoader> GetColumnLoader(const ui32 columnId) const = 0;
    std::shared_ptr<TColumnLoader> GetColumnLoader(const TString& columnName) const {
        return GetColumnLoader(std::string(columnName.data(), columnName.size()));
    }
    std::shared_ptr<TColumnLoader> GetColumnLoader(const std::string& columnName) const {
        return GetColumnLoader(GetColumnId(columnName));
    }

    virtual TColumnSaver GetColumnSaver(const ui32 columnId, const TSaverContext& context) const = 0;
    TColumnSaver GetColumnSaver(const TString& columnName, const TSaverContext& context) const {
        return GetColumnSaver(GetColumnId(columnName), context);
    }
    TColumnSaver GetColumnSaver(const std::string& columnName, const TSaverContext& context) const {
        return GetColumnSaver(TString(columnName.data(), columnName.size()), context);
    }

    virtual ui32 GetColumnId(const std::string& columnName) const = 0;
    virtual int GetFieldIndex(const ui32 columnId) const = 0;
    std::shared_ptr<arrow::Field> GetFieldByIndex(const int index) const;
    std::shared_ptr<arrow::Field> GetFieldByColumnId(const ui32 columnId) const;

    virtual const std::shared_ptr<arrow::Schema>& GetSchema() const = 0;
    virtual const TIndexInfo& GetIndexInfo() const = 0;
    virtual const TSnapshot& GetSnapshot() const = 0;
    virtual ui32 GetColumnsCount() const = 0;

    std::shared_ptr<arrow::RecordBatch> NormalizeBatch(const ISnapshotSchema& dataSchema, const std::shared_ptr<arrow::RecordBatch> batch) const;
    std::shared_ptr<arrow::RecordBatch> PrepareForInsert(const TString& data, const std::shared_ptr<arrow::Schema>& dataSchema) const;
};

} // namespace NKikimr::NOlap
