#include "constrained_schema.h"
#include "private.h"

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

TConstrainedTableSchema::TConstrainedTableSchema(const TTableSchema& schema)
    : TableSchema_(schema)
{ }

TConstrainedTableSchema::TConstrainedTableSchema(TTableSchema schema, TColumnNameToConstraintMap columnNameToConstraint)
    : TableSchema_(std::move(schema))
    , ColumnToConstraint_(std::move(columnNameToConstraint))
{ }

TConstrainedTableSchema::TConstrainedTableSchema(
    TTableSchema schema,
    TColumnStableNameToConstraintMap columnStableNameToConstraint,
    int columnToConstraintLogLimit)
    : TableSchema_(std::move(schema))
    , ColumnToConstraint_(MakeColumnNameToConstraintMap(TableSchema_, std::move(columnStableNameToConstraint), columnToConstraintLogLimit))
{ }

////////////////////////////////////////////////////////////////////////////////

TColumnNameToConstraintMap MakeColumnNameToConstraintMap(
    const TTableSchema& schema,
    TColumnStableNameToConstraintMap columnStableNameToConstraint,
    int columnToConstraintLogLimit)
{
    TColumnNameToConstraintMap result;
    for (auto& [stableName, constraint] : columnStableNameToConstraint) {
        const auto* column = schema.FindColumnByStableName(stableName);
        if (!column) {
            YT_LOG_ALERT(
                "No column was found during transforming stable named constraints into named constraints "
                "(ColumnStableName: %v, ColumnStableNameToConstraint: %v)",
                stableName,
                MakeShrunkFormattableView(columnStableNameToConstraint, TDefaultFormatter(), columnToConstraintLogLimit));
            continue;
        }
        result.emplace(column->Name(), std::move(constraint));
    }
    return result;
}

TColumnStableNameToConstraintMap MakeColumnStableNameToConstraintMap(
    const TTableSchema& schema,
    TColumnNameToConstraintMap columnNameToConstraint,
    int columnToConstraintLogLimit)
{
    TColumnStableNameToConstraintMap result;
    for (auto& [name, constraint] : columnNameToConstraint) {
        const auto* column = schema.FindColumn(name);
        if (!column) {
            YT_LOG_ALERT(
                "No column was found during transforming named constraints into stable named constraints "
                "(ColumnStableName: %v, ColumnStableNameToConstraint: %v)",
                name,
                MakeShrunkFormattableView(columnNameToConstraint, TDefaultFormatter(), columnToConstraintLogLimit));
            continue;
        }
        result.emplace(column->StableName(), std::move(constraint));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(TColumnNameToConstraintMap* protoConstraints, const NTableClient::TColumnNameToConstraintMap& constraints)
{
    for (const auto& [column, constraint] : constraints) {
        auto* entry = protoConstraints->add_entries();
        entry->set_name(column);
        entry->set_constraint(constraint);
    }
}

void FromProto(NTableClient::TColumnNameToConstraintMap* constraints, const TColumnNameToConstraintMap& protoConstraints)
{
    constraints->clear();
    constraints->reserve(protoConstraints.entries_size());
    for (const auto& entry : protoConstraints.entries()) {
        auto [it, emplaced] = constraints->emplace(entry.name(), entry.constraint());
        if (!emplaced) {
            THROW_ERROR_EXCEPTION(
                "Received duplicate constraints for column %Qv",
                entry.name())
                << TErrorAttribute("first_conflicting_constraint", entry.constraint())
                << TErrorAttribute("second_conflicting_constraint", it->second);
        }
    }
}

} // namespace NYT::NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
