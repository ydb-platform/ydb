#include "check_schema_compatibility.h"
#include "logical_type.h"
#include "schema.h"
#include "comparator.h"

#include <yt/yt/client/complex_types/check_type_compatibility.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

std::pair<ESchemaCompatibility, TError> CheckTableSchemaCompatibilityImpl(
    const TTableSchema& inputSchema,
    const TTableSchema& outputSchema,
    TTableSchemaCompatibilityOptions options)
{
    // If output schema is strict, check that input columns are subset of output columns.
    if (outputSchema.GetStrict()) {
        if (!inputSchema.GetStrict()) {
            return {
                ESchemaCompatibility::Incompatible,
                TError("Incompatible strictness: input schema is not strict while output schema is"),
            };
        }

        for (const auto& inputColumn : inputSchema.Columns()) {
            if (!outputSchema.FindColumn(inputColumn.Name())) {
                return {
                    ESchemaCompatibility::Incompatible,
                    TError("Column %v is found in input schema but is missing in output schema",
                        inputColumn.GetDiagnosticNameString()),
                };
            }
        }
    }

    auto result = std::pair(ESchemaCompatibility::FullyCompatible, TError());

    // Check that columns are the same.
    for (const auto& outputColumn : outputSchema.Columns()) {
        const auto* inputColumn = inputSchema.FindColumn(outputColumn.Name());
        const auto* deletedColumn = inputSchema.FindDeletedColumn(outputColumn.StableName());

        if (deletedColumn) {
            return {
                ESchemaCompatibility::Incompatible,
                TError("Column %v in output schema was deleted in the input schema",
                    outputColumn.GetDiagnosticNameString()),
            };
        }

        if (inputColumn) {
            if (!options.IgnoreStableNamesDifference && inputColumn->StableName() != outputColumn.StableName()) {
                return {
                    ESchemaCompatibility::Incompatible,
                    TError("Column %Qv has stable name %Qv in input and %Qv in output schema",
                        inputColumn->Name(),
                        inputColumn->StableName(),
                        outputColumn.StableName())
                };
            }

            auto currentTypeCompatibility = NComplexTypes::CheckTypeCompatibility(
                inputColumn->LogicalType(), outputColumn.LogicalType());

            if (currentTypeCompatibility.first < result.first) {
                result = {
                    currentTypeCompatibility.first,
                    TError("Column %v input type is incompatible with output type",
                        inputColumn->GetDiagnosticNameString())
                        << currentTypeCompatibility.second
                };
            }

            if (result.first == ESchemaCompatibility::Incompatible) {
                break;
            }

            if (outputColumn.Expression() && inputColumn->Expression() != outputColumn.Expression()) {
                return {
                    ESchemaCompatibility::Incompatible,
                    TError("Column %v expression mismatch",
                        inputColumn->GetDiagnosticNameString()),
                };
            }
            if (outputColumn.Aggregate() && inputColumn->Aggregate() != outputColumn.Aggregate()) {
                return {
                    ESchemaCompatibility::Incompatible,
                    TError("Column %v aggregate mismatch",
                        inputColumn->GetDiagnosticNameString()),
                };
            }
        } else if (options.ForbidExtraComputedColumns && outputColumn.Expression()) {
            return {
                ESchemaCompatibility::Incompatible,
                TError("Unexpected computed column %v in output schema",
                    outputColumn.GetDiagnosticNameString()),
            };
        } else if (!inputSchema.GetStrict()) {
            return {
                ESchemaCompatibility::Incompatible,
                TError("Column %v is present in output schema and is missing in non-strict input schema",
                    outputColumn.GetDiagnosticNameString()),
            };
        } else if (outputColumn.Required()) {
            return {
                ESchemaCompatibility::Incompatible,
                TError("Required column %v is present in output schema and is missing in input schema",
                    outputColumn.GetDiagnosticNameString()),
            };
        }
    }

    for (const auto& deletedOutputColumn : outputSchema.DeletedColumns()) {
        const auto& stableName = deletedOutputColumn.StableName();
        const auto* inputColumn = inputSchema.FindColumnByStableName(stableName);
        const auto* deletedColumn = inputSchema.FindDeletedColumn(stableName);

        if (!inputColumn && !deletedColumn) {
            return {
                ESchemaCompatibility::Incompatible,
                TError("Deleted column %Qv is missing in the input schema",
                    deletedOutputColumn.StableName())
            };
        }
    }

    for (const auto& deletedInputColumn : inputSchema.DeletedColumns()) {
        const auto& stableName = deletedInputColumn.StableName();
        const auto* deletedOutputColumn = outputSchema.FindDeletedColumn(stableName);
        if (!deletedOutputColumn) {
            return {
                ESchemaCompatibility::Incompatible,
                TError("Deleted column %Qv must be deleted in the output schema",
                    deletedInputColumn.StableName())
            };
        }
    }

    // Check that we don't lose complex types.
    // We never want to teleport complex types to schemaless part of the chunk because we want to change their type from
    // EValueType::Composite to EValueType::Any.
    if (!outputSchema.GetStrict()) {
        for (const auto& inputColumn : inputSchema.Columns()) {
            if (!IsV3Composite(inputColumn.LogicalType())) {
                continue;
            }
            if (!outputSchema.FindColumn(inputColumn.Name())) {
                return {
                    ESchemaCompatibility::Incompatible,
                    TError("Column %v of input schema with complex type %Qv is missing in strict part of output schema",
                        inputColumn.GetDiagnosticNameString(),
                        *inputColumn.LogicalType()),
                };
            }
        }
    }

    if (options.IgnoreSortOrder) {
        return result;
    }

    // Check that output key columns form a prefix of input key columns.
    int cmp = outputSchema.GetKeyColumnCount() - inputSchema.GetKeyColumnCount();
    if (cmp > 0) {
        return {
            ESchemaCompatibility::Incompatible,
            TError("Output key columns are wider than input key columns"),
        };
    }

    if (outputSchema.GetUniqueKeys()) {
        if (!inputSchema.GetUniqueKeys()) {
            return {
                ESchemaCompatibility::Incompatible,
                TError("Input schema \"unique_keys\" attribute is false"),
            };
        }
        if (cmp != 0) {
            return {
                ESchemaCompatibility::Incompatible,
                TError("Input key columns are wider than output key columns"),
            };
        }
    }

    auto inputKeySchema = inputSchema.ToKeys();
    auto outputKeySchema = outputSchema.ToKeys();

    for (int index = 0; index < outputKeySchema->GetColumnCount(); ++index) {
        const auto& inputColumn = inputKeySchema->Columns()[index];
        const auto& outputColumn = outputKeySchema->Columns()[index];
        if (!options.IgnoreStableNamesDifference && inputColumn.StableName() != outputColumn.StableName()) {
            return {
                ESchemaCompatibility::Incompatible,
                TError("Key columns do not match: input column %v, output column %v",
                    inputColumn.GetDiagnosticNameString(),
                    outputColumn.GetDiagnosticNameString())
            };
        }
        if (inputColumn.SortOrder() != outputColumn.SortOrder()) {
            return {
                ESchemaCompatibility::Incompatible,
                TError("Sort order of column %v does not match: input sort order %Qlv, output sort order %Qlv",
                    inputColumn.GetDiagnosticNameString(),
                    inputColumn.SortOrder(),
                    outputColumn.SortOrder())
            };
        }
    }

    return result;
}

std::pair<ESchemaCompatibility, TError> CheckTableSchemaCompatibility(
    const TTableSchema& inputSchema,
    const TTableSchema& outputSchema,
    TTableSchemaCompatibilityOptions options)
{
    auto result = CheckTableSchemaCompatibilityImpl(
        inputSchema,
        outputSchema,
        options);
    if (result.first != ESchemaCompatibility::FullyCompatible) {
        result.second = TError(NTableClient::EErrorCode::IncompatibleSchemas, "Table schemas are incompatible")
            << result.second
            << TErrorAttribute("input_table_schema", inputSchema)
            << TErrorAttribute("output_table_schema", outputSchema);
    }
    return result;
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
