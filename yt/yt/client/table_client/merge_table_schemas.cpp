#include "merge_table_schemas.h"

#include "check_schema_compatibility.h"
#include "comparator.h"
#include "logical_type.h"
#include "schema.h"

#include <yt/yt/client/complex_types/check_type_compatibility.h>
#include <yt/yt/client/complex_types/merge_complex_types.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NTableClient {

using namespace NComplexTypes;

////////////////////////////////////////////////////////////////////////////////

namespace {

TColumnSchema MakeOptionalSchema(const TColumnSchema& columnSchema)
{
    if (columnSchema.LogicalType()->GetMetatype() == ELogicalMetatype::Optional) {
        return columnSchema;
    }
    auto optionalType = New<TOptionalLogicalType>(columnSchema.LogicalType());
    auto resultSchema = TColumnSchema(
        columnSchema.Name(),
        std::move(optionalType),
        columnSchema.SortOrder());
    resultSchema.SetStableName(columnSchema.StableName());
    return resultSchema;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr MergeTableSchemas(
    const TTableSchemaPtr& firstSchema,
    const TTableSchemaPtr& secondSchema)
{
    std::vector<TColumnSchema> resultColumns;
    resultColumns.reserve(std::max(secondSchema->Columns().size(), firstSchema->Columns().size()));

    for (const auto& secondSchemaColumn : secondSchema->Columns()) {
        const auto* firstSchemaColumn = firstSchema->FindColumn(secondSchemaColumn.Name());

        if (firstSchemaColumn) {
            if (firstSchemaColumn->StableName() != secondSchemaColumn.StableName()) {
                THROW_ERROR_EXCEPTION("Mismatching stable names in column %Qv: %Qv and %Qv",
                    firstSchemaColumn->Name(),
                    firstSchemaColumn->StableName(),
                    secondSchemaColumn.StableName());
            }
            if (firstSchemaColumn->SortOrder() != secondSchemaColumn.SortOrder()) {
                THROW_ERROR_EXCEPTION("Mismatching sort orders in column %Qv: %Qv and %Qv",
                    firstSchemaColumn->Name(),
                    firstSchemaColumn->SortOrder(),
                    secondSchemaColumn.SortOrder());
            }

            try {
                auto mergedType = MergeTypes(
                    firstSchemaColumn->LogicalType(),
                    secondSchemaColumn.LogicalType());

                auto resultSchema = TColumnSchema(
                    firstSchemaColumn->Name(),
                    mergedType,
                    firstSchemaColumn->SortOrder());

                resultSchema.SetStableName(firstSchemaColumn->StableName());
                resultColumns.push_back(std::move(resultSchema));

            } catch(const std::exception& ex) {
                THROW_ERROR_EXCEPTION(
                    "Column %v first schema type is incompatible with second schema type",
                    firstSchemaColumn->GetDiagnosticNameString())
                    << ex;
            }

        } else if (!firstSchema->IsStrict()) {
            THROW_ERROR_EXCEPTION("Column %v is present in second schema and is missing in non-strict first schema",
                secondSchemaColumn.GetDiagnosticNameString());
        } else {
            resultColumns.push_back(MakeOptionalSchema(secondSchemaColumn));
        }
    }

    for (const auto& firstSchemaColumn : firstSchema->Columns()) {
        if (!secondSchema->FindColumn(firstSchemaColumn.Name())) {
            if (!secondSchema->IsStrict()) {
                THROW_ERROR_EXCEPTION("Column %v is present in first schema and is missing in non-strict second schema",
                    firstSchemaColumn.GetDiagnosticNameString());
            }
            resultColumns.push_back(MakeOptionalSchema(firstSchemaColumn));
        }
    }

    auto getDeletedColumnsStableNames = [] (const std::vector<TDeletedColumn>& deletedColumns) {
        THashSet<TColumnStableName> stableNames;
        for (const auto& column : deletedColumns) {
            stableNames.insert(column.StableName());
        }
        return stableNames;
    };

    auto firstDeletedStableNames = getDeletedColumnsStableNames(firstSchema->DeletedColumns());
    auto secondDeletedStableNames = getDeletedColumnsStableNames(secondSchema->DeletedColumns());

    if (firstDeletedStableNames == secondDeletedStableNames) {
        // If the deleted columns completely match, then the table can be teleported.
        return {
            New<TTableSchema>(
                std::move(resultColumns),
                firstSchema->IsStrict() && secondSchema->IsStrict(),
                firstSchema->IsUniqueKeys() && secondSchema->IsUniqueKeys(),
                ETableSchemaModification::None,
                firstSchema->DeletedColumns())
        };
    } else {
        return {
            New<TTableSchema>(
                std::move(resultColumns),
                firstSchema->IsStrict() && secondSchema->IsStrict(),
                firstSchema->IsUniqueKeys() && secondSchema->IsUniqueKeys())
        };
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
