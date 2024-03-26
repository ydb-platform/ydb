#pragma once

#include "abstract_scheme.h"

#include <ydb/core/tx/columnshard/engines/index_info.h>

namespace NKikimr::NOlap {

class TSnapshotSchema: public ISnapshotSchema {
private:
    TIndexInfo IndexInfo;
    std::shared_ptr<arrow::Schema> Schema;
    TSnapshot Snapshot;
protected:
    virtual TString DoDebugString() const override {
        return TStringBuilder() << "("
            "schema=" << Schema->ToString() << ";" <<
            "snapshot=" << Snapshot.DebugString() << ";" <<
            "index_info=" << IndexInfo.DebugString() << ";" <<
            ")"
            ;
    }
public:
    TSnapshotSchema(TIndexInfo&& indexInfo, const TSnapshot& snapshot);

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
