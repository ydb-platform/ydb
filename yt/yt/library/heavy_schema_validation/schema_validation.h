#pragma once

#include <yt/yt/client/table_client/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TSchemaUpdateEnabledFeatures
{
    bool EnableStaticTableDropColumn = false;
    bool EnableDynamicTableDropColumn = false;
};

////////////////////////////////////////////////////////////////////////////////

void ValidateColumnSchemaUpdate(
    const TColumnSchema& oldColumn,
    const TColumnSchema& newColumn);

void ValidateTableSchemaUpdateInternal(
    const TTableSchema& oldSchema,
    const TTableSchema& newSchema,
    TSchemaUpdateEnabledFeatures enabledFeatures,
    bool isTableDynamic = false,
    bool isTableEmpty = false);

void ValidateTableSchemaUpdate(
    const TTableSchema& oldSchema,
    const TTableSchema& newSchema,
    bool isTableDynamic = false,
    bool isTableEmpty = false);

//! Compared to #ValidateTableSchema, additionally validates
//! aggregated and computed columns (this involves calling some heavy QL-related
//! stuff which is missing in yt/client).
void ValidateTableSchemaHeavy(
    const TTableSchema& schema,
    bool isTableDynamic);

//! Validates computed columns.
//!
//! Validates that:
//! - Type of a computed column matches the type of its expression.
//! - All referenced columns appear in schema and are not computed.
//! For dynamic tables, additionally validates that all computed and referenced
//! columns are key columns.
void ValidateComputedColumns(
    const TTableSchema& schema,
    bool isTableDynamic);

//! Validates that all computed columns in the outputSchema are present in the
//! inputSchema and have the same expression.
TError ValidateComputedColumnsCompatibility(
    const TTableSchema& inputSchema,
    const TTableSchema& outputSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
