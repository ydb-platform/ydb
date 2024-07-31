#pragma once

#include "abstract_scheme.h"

#include <ydb/core/tx/columnshard/engines/index_info.h>

namespace NKikimr::NOlap {

class TFilteredSnapshotSchema: public ISnapshotSchema {
    ISnapshotSchema::TPtr OriginalSnapshot;
    std::shared_ptr<arrow::Schema> Schema;
    YDB_READONLY_DEF(std::set<ui32>, ColumnIds);
protected:
    virtual TString DoDebugString() const override;
public:
    TFilteredSnapshotSchema(ISnapshotSchema::TPtr originalSnapshot, const std::vector<ui32>& columnIds);
    TFilteredSnapshotSchema(ISnapshotSchema::TPtr originalSnapshot, const std::set<ui32>& columnIds);
    TFilteredSnapshotSchema(ISnapshotSchema::TPtr originalSnapshot, const std::set<std::string>& columnNames);

    TColumnSaver GetColumnSaver(const ui32 columnId) const override;
    std::shared_ptr<TColumnLoader> GetColumnLoaderOptional(const ui32 columnId) const override;
    std::optional<ui32> GetColumnIdOptional(const std::string& columnName) const override;
    int GetFieldIndex(const ui32 columnId) const override;

    const std::shared_ptr<arrow::Schema>& GetSchema() const override;
    const TIndexInfo& GetIndexInfo() const override;
    const TSnapshot& GetSnapshot() const override;
    ui32 GetColumnsCount() const override;
    ui64 GetVersion() const override;
};

}
