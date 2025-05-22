#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/conclusion/status.h>

namespace NKikimr::NOlap {

class TSchemaDiffView {
private:
    ui64 Version = 0;
    const NKikimrSchemeOp::TColumnTableSchemeOptions* SchemaOptions = nullptr;
    const NKikimrSchemeOp::TCompressionOptions* CompressionOptions = nullptr;
    std::map<ui32, const NKikimrSchemeOp::TOlapColumnDescription*> ModifiedColumns;
    std::map<ui32, const NKikimrSchemeOp::TOlapIndexDescription*> ModifiedIndexes;

public:
    TSchemaDiffView() = default;

    void ApplyForColumns(const std::vector<ui32>& originalColumnIds, 
        const std::function<void(const ui32 originalIndex)>& addFromOriginal,
        const std::function<void(const NKikimrSchemeOp::TOlapColumnDescription& col, const std::optional<ui32> originalIndex)>& addFromDiff) const;

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

    ui64 GetVersion() const;;

    TConclusionStatus DeserializeFromProto(const NKikimrSchemeOp::TColumnTableSchemaDiff& proto);
};

}   // namespace NKikimr::NOlap
