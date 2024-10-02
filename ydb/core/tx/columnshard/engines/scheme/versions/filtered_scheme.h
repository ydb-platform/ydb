#pragma once

#include "abstract_scheme.h"

#include <ydb/core/tx/columnshard/engines/index_info.h>

namespace NKikimr::NOlap {

class TFilteredSnapshotSchema: public ISnapshotSchema {
    ISnapshotSchema::TPtr OriginalSnapshot;
    std::shared_ptr<NArrow::TSchemaLite> Schema;
    std::vector<ui32> ColumnIds;
    THashMap<ui32, ui32> IdIntoIndex;

protected:
    virtual TString DoDebugString() const override;
public:
    TFilteredSnapshotSchema(const ISnapshotSchema::TPtr& originalSnapshot, const std::vector<ui32>& columnIds);
    TFilteredSnapshotSchema(const ISnapshotSchema::TPtr& originalSnapshot, const std::set<ui32>& columnIds);

    virtual const std::vector<ui32>& GetColumnIds() const override {
        return ColumnIds;
    }
    TColumnSaver GetColumnSaver(const ui32 columnId) const override;
    std::shared_ptr<TColumnLoader> GetColumnLoaderOptional(const ui32 columnId) const override;
    std::optional<ui32> GetColumnIdOptional(const std::string& columnName) const override;
    ui32 GetColumnIdVerified(const std::string& columnName) const override;
    int GetFieldIndex(const ui32 columnId) const override;

    const std::shared_ptr<NArrow::TSchemaLite>& GetSchema() const override;
    const TIndexInfo& GetIndexInfo() const override;
    const TSnapshot& GetSnapshot() const override;
    ui32 GetColumnsCount() const override;
    ui64 GetVersion() const override;
};

}
