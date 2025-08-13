#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/tx_columnshard.pb.h>

#include <ydb/library/conclusion/status.h>

namespace NKikimr::NOlap {

struct TIndexInfo;

class TSchemaDiffView {
private:
    ui64 Version = 0;
    const NKikimrSchemeOp::TColumnTableSchemaDiff* OriginalProto = nullptr;
    const NKikimrSchemeOp::TColumnTableSchemeOptions* SchemaOptions = nullptr;
    const NKikimrSchemeOp::TCompressionOptions* CompressionOptions = nullptr;
    std::map<ui32, const NKikimrSchemeOp::TOlapColumnDescription*> ModifiedColumns;
    std::map<ui32, const NKikimrSchemeOp::TOlapIndexDescription*> ModifiedIndexes;

    NKikimrSchemeOp::TColumnTableSchema ApplyDiff(const NKikimrSchemeOp::TColumnTableSchema& schema) const;

    static NKikimrSchemeOp::TColumnTableSchemaDiff ApplyDiff(
        const NKikimrSchemeOp::TColumnTableSchemaDiff& diff0, const NKikimrSchemeOp::TColumnTableSchemaDiff& diff1);

    static NKikimrSchemeOp::TColumnTableSchemaDiff MergeDiffs(const std::vector<NKikimrTxColumnShard::TSchemaPresetVersionInfo>& schemas);

public:
    TSchemaDiffView() = default;

    TString DebugString() const {
        AFL_VERIFY(OriginalProto);
        return OriginalProto->DebugString();
    }

    bool IsCorrectToIgnorePreviouse(const TIndexInfo& indexInfo) const;

    void ApplyForColumns(const std::vector<ui32>& originalColumnIds, const std::function<void(const ui32 originalIndex)>& addFromOriginal,
        const std::function<void(const NKikimrSchemeOp::TOlapColumnDescription& col, const std::optional<ui32> originalIndex)>& addFromDiff)
        const;

    static NKikimrTxColumnShard::TSchemaPresetVersionInfo Merge(const std::vector<NKikimrTxColumnShard::TSchemaPresetVersionInfo>& schemas);

    static NKikimrSchemeOp::TColumnTableSchemaDiff MakeSchemasDiff(
        const NKikimrSchemeOp::TColumnTableSchema& current, const NKikimrSchemeOp::TColumnTableSchema& next);

    const NKikimrSchemeOp::TColumnTableSchemeOptions& GetSchemaOptions() const;
    const NKikimrSchemeOp::TCompressionOptions* GetCompressionOptions() const {
        return CompressionOptions;
    }
    const std::map<ui32, const NKikimrSchemeOp::TOlapColumnDescription*>& GetModifiedColumns() const {
        return ModifiedColumns;
    }
    const std::map<ui32, const NKikimrSchemeOp::TOlapIndexDescription*>& GetModifiedIndexes() const {
        return ModifiedIndexes;
    }

    ui64 GetVersion() const;

    [[nodiscard]] TConclusionStatus DeserializeFromProto(const NKikimrSchemeOp::TColumnTableSchemaDiff& proto);
};

}   // namespace NKikimr::NOlap
