#pragma once

#include "abstract_scheme.h"

#include <ydb/core/tx/columnshard/engines/index_info.h>

namespace NKikimr::NOlap {

class TSnapshotSchema: public ISnapshotSchema {
private:
    TIndexInfo IndexInfo;
    std::shared_ptr<arrow::Schema> Schema;
    TSnapshot Snapshot;
public:
    TSnapshotSchema(TIndexInfo&& indexInfo, const TSnapshot& snapshot);

    TColumnSaver GetColumnSaver(const ui32 columnId, const TSaverContext& context) const override;
    std::shared_ptr<TColumnLoader> GetColumnLoader(const ui32 columnId) const override;
    ui32 GetColumnId(const std::string& columnName) const override;
    int GetFieldIndex(const ui32 columnId) const override;

    const std::shared_ptr<arrow::Schema>& GetSchema() const override;
    const TIndexInfo& GetIndexInfo() const override;
    const TSnapshot& GetSnapshot() const override;
    ui32 GetColumnsCount() const override;
    ui64 GetVersion() const override;
};

}
