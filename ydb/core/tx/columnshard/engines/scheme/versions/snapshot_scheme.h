#pragma once

#include "abstract_scheme.h"

#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

namespace NKikimr::NOlap {

class TSnapshotSchema: public ISnapshotSchema {
private:
    TIndexInfo IndexInfo;
    std::shared_ptr<NArrow::TSchemaLite> Schema;
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

    virtual const std::vector<ui32>& GetColumnIds() const override {
        return IndexInfo.GetColumnIds();
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

    bool IsReplaceableByNext(const ISnapshotSchema& nextSchema) const override {
        return nextSchema.IsReplaceableByPrev(*this);
    };

    bool IsReplaceableByPrev(const TSnapshotSchema& prevSchema) const override {
        return prevSchema.IsReplaceableByNextVersion(*this);
    };

    // Checks that no columns changed and deleted in the next version,
    // so that current schema version can be safely changed to the next
    bool IsReplaceableByNextVersion(const TSnapshotSchema& nextSchema) const;
};

}
