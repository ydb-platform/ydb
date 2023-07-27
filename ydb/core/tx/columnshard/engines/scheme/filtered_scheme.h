#pragma once

#include "abstract_scheme.h"

#include <ydb/core/tx/columnshard/engines/index_info.h>

namespace NKikimr::NOlap {

class TFilteredSnapshotSchema: public ISnapshotSchema {
    ISnapshotSchema::TPtr OriginalSnapshot;
    std::shared_ptr<arrow::Schema> Schema;
    std::set<ui32> ColumnIds;
public:
    TFilteredSnapshotSchema(ISnapshotSchema::TPtr originalSnapshot, const std::vector<ui32>& columnIds);
    TFilteredSnapshotSchema(ISnapshotSchema::TPtr originalSnapshot, const std::set<ui32>& columnIds);

    TColumnSaver GetColumnSaver(const ui32 columnId, const TSaverContext& context) const override;
    std::shared_ptr<TColumnLoader> GetColumnLoader(const ui32 columnId) const override;
    ui32 GetColumnId(const std::string& columnName) const override;
    int GetFieldIndex(const ui32 columnId) const override;

    const std::shared_ptr<arrow::Schema>& GetSchema() const override;
    const TIndexInfo& GetIndexInfo() const override;
    const TSnapshot& GetSnapshot() const override;
    ui32 GetColumnsCount() const override;
};

}
