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

    NKikimrSchemeOp::TColumnTableSchema ApplyDiff(const NKikimrSchemeOp::TColumnTableSchema& schema) const {
        NKikimrSchemeOp::TColumnTableSchema result = schema;
        AFL_VERIFY(result.GetVersion() < Version);
        result.SetVersion(Version);
        if (SchemaOptions) {
            *result.MutableOptions() = *SchemaOptions;
        }
        if (CompressionOptions) {
            *result.MutableDefaultCompression() = *CompressionOptions;
        }
        result.ClearColumns();
        result.ClearIndexes();
        {
            auto itDiff = ModifiedColumns.begin();
            auto itSchema = schema.GetColumns().begin();
            while (itDiff != ModifiedColumns.end() || itSchema != schema.GetColumns().end()) {
                if (itDiff == ModifiedColumns.end() || itSchema->GetId() < itDiff->first) {
                    *result.AddColumns() = *itSchema;
                    ++itSchema;
                } else if (itSchema == schema.GetColumns().end() || itDiff->first <= itSchema->GetId()) {
                    if (itDiff->second) {
                        *result.AddColumns() = *itDiff->second;
                    }
                    ++itDiff;
                    if (itSchema != schema.GetColumns().end() && itDiff->first == itSchema->GetId()) {
                        ++itSchema;
                    }
                }
            }
        }
        {
            auto itDiff = ModifiedIndexes.begin();
            auto itSchema = schema.GetIndexes().begin();
            while (itDiff != ModifiedIndexes.end() || itSchema != schema.GetIndexes().end()) {
                if (itDiff == ModifiedIndexes.end() || itSchema->GetId() < itDiff->first) {
                    *result.AddIndexes() = *itSchema;
                    ++itSchema;
                } else if (itSchema == schema.GetIndexes().end() || itDiff->first <= itSchema->GetId()) {
                    if (itDiff->second) {
                        *result.AddIndexes() = *itDiff->second;
                    }
                    ++itDiff;
                    if (itSchema != schema.GetIndexes().end() && itDiff->first == itSchema->GetId()) {
                        ++itSchema;
                    }
                }
            }
        }
        return result;
    }

    static NKikimrSchemeOp::TColumnTableSchemaDiff ApplyDiff(
        const NKikimrSchemeOp::TColumnTableSchemaDiff& diff0, const NKikimrSchemeOp::TColumnTableSchemaDiff& diff1) {
        AFL_VERIFY(diff0.GetVersion() < diff1.GetVersion());
        NKikimrSchemeOp::TColumnTableSchemaDiff result;
        result.SetVersion(diff1.GetVersion());
        if (diff1.HasDefaultCompression()) {
            *result.MutableDefaultCompression() = diff1.GetDefaultCompression();
        } else if (diff0.HasDefaultCompression()) {
            *result.MutableDefaultCompression() = diff0.GetDefaultCompression();
        }
        if (diff1.HasOptions()) {
            *result.MutableOptions() = diff1.GetOptions();
        } else if (diff0.HasOptions()) {
            *result.MutableOptions() = diff0.GetOptions();
        }
        TSchemaDiffView view0;
        view0.DeserializeFromProto(diff0).Validate();
        TSchemaDiffView view1;
        view1.DeserializeFromProto(diff1).Validate();
        {
            auto it0 = view0.ModifiedColumns.begin();
            auto it1 = view1.ModifiedColumns.begin();
            while (it0 != view0.ModifiedColumns.end() || it1 != view1.ModifiedColumns.end()) {
                if (it1 == view1.ModifiedColumns.end() || it0->first < it1->first) {
                    if (it0->second) {
                        *result.AddUpsertColumns() = *it0->second;
                    } else {
                        *result.AddDropColumns() = it0->first;
                    }
                    ++it0;
                } else if (it0 == view0.ModifiedColumns.end() || it1->first <= it0->first) {
                    if (it1->second) {
                        *result.AddUpsertColumns() = *it1->second;
                    } else {
                        *result.AddDropColumns() = it1->first;
                    }
                    ++it1;
                    if (it0 != view0.ModifiedColumns.end() && it1->first == it0->first) {
                        ++it0;
                    }
                }
            }
        }
        {
            auto it0 = view0.ModifiedIndexes.begin();
            auto it1 = view1.ModifiedIndexes.begin();
            while (it0 != view0.ModifiedIndexes.end() || it1 != view1.ModifiedIndexes.end()) {
                if (it1 == view1.ModifiedIndexes.end() || it0->first < it1->first) {
                    if (it0->second) {
                        *result.AddUpsertIndexes() = *it0->second;
                    } else {
                        *result.AddDropIndexes() = it0->first;
                    }
                    ++it0;
                } else if (it0 == view0.ModifiedIndexes.end() || it1->first <= it0->first) {
                    if (it1->second) {
                        *result.AddUpsertIndexes() = *it1->second;
                    } else {
                        *result.AddDropIndexes() = it1->first;
                    }
                    ++it1;
                    if (it0 != view0.ModifiedIndexes.end() && it1->first == it0->first) {
                        ++it0;
                    }
                }
            }
        }
        return result;
    }

    static NKikimrSchemeOp::TColumnTableSchemaDiff MergeDiffs(const std::vector<NKikimrSchemeOp::TSchemaPresetVersionInfo>& schemas) {
        AFL_VERIFY(schemas.size());
        AFL_VERIFY(schemas.front().HasDiff());
        NKikimrSchemeOp::TColumnTableSchemaDiff result = schemas.front().GetDiff();
        for (ui32 i = 1; i < schemas.size(); ++i) {
            AFL_VERIFY(schemas[i].HasDiff());
            const NKikimrSchemeOp::TColumnTableSchemaDiff& localDiff = schemas[i].GetDiff();
            result = ApplyDiff(result, localDiff);
        }
        return result;
    }

public:
    TSchemaDiffView() = default;

    void ApplyForColumns(const std::vector<ui32>& originalColumnIds, 
        const std::function<void(const ui32 originalIndex)>& addFromOriginal,
        const std::function<void(const NKikimrSchemeOp::TOlapColumnDescription& col, const std::optional<ui32> originalIndex)>& addFromDiff) const;

    static NKikimrSchemeOp::TSchemaPresetVersionInfo Merge(const std::vector<NKikimrSchemeOp::TSchemaPresetVersionInfo>& schemas) {
        AFL_VERIFY(schemas.size());
        std::optional<ui32> lastSchemaIdx;
        for (ui32 idx = 0; idx < schemas.size(); ++idx) {
            if (schemas[idx].HasSchema()) {
                lastSchemaIdx = idx;
            }
        }
        NKikimrSchemeOp::TColumnTableSchema result = schemas.back();
        result.ClearSchema();
        result.ClearDiff();
        AFL_VERIFY(!result.HasSchema());
        AFL_VERIFY(!result.HasDiff());
        if (lastSchemaIdx) {
            NKikimrSchemeOp::TColumnTableSchema schema = schemas[*lastSchemaIdx].GetSchema();
            for (ui32 idx = *lastSchemaIdx + 1; idx < schemas.size(); ++idx) {
                schema = ApplyDiff(schema, schemas[idx].GetDiff);
            }
            *result.MutableSchema() = std::move(schema);
        } else {
            *result.MutableDiff() = MergeDiffs(schemas);
        }
        return result;
    }

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

    TConclusionStatus DeserializeFromProto(const NKikimrSchemeOp::TColumnTableSchemaDiff& proto);
};

}   // namespace NKikimr::NOlap
