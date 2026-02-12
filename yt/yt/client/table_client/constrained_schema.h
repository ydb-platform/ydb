#pragma once

#include "public.h"
#include "schema.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TConstrainedTableSchema final
{
public:
    DEFINE_BYREF_RO_PROPERTY(TTableSchema, TableSchema);
    DEFINE_BYREF_RO_PROPERTY(TColumnNameToConstraintMap, ColumnToConstraint);

public:
    TConstrainedTableSchema() = default;
    explicit TConstrainedTableSchema(const TTableSchema& schema);
    TConstrainedTableSchema(TTableSchema schema, TColumnNameToConstraintMap columnNameToConstraint);
    TConstrainedTableSchema(TTableSchema schema, TColumnStableNameToConstraintMap columnStableNameToConstraint, int columnToConstraintLogLimit);
};

DEFINE_REFCOUNTED_TYPE(TConstrainedTableSchema)

////////////////////////////////////////////////////////////////////////////////

TColumnNameToConstraintMap MakeColumnNameToConstraintMap(
    const TTableSchema& schema,
    TColumnStableNameToConstraintMap columnStableNameToConstraint,
    int columnToConstraintLogLimit);

TColumnStableNameToConstraintMap MakeColumnStableNameToConstraintMap(
    const TTableSchema& schema,
    TColumnNameToConstraintMap columnNameToConstraint,
    int columnToConstraintLogLimit);

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TConstrainedTableSchema& schema, NYson::IYsonConsumer* consumer);
void Deserialize(TConstrainedTableSchema& schema, NYTree::INodePtr node);
void Deserialize(TConstrainedTableSchema& schema, NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(TColumnNameToConstraintMap* protoConstraints, const NTableClient::TColumnNameToConstraintMap& constraints);
void FromProto(NTableClient::TColumnNameToConstraintMap* constraints, const TColumnNameToConstraintMap& protoConstraints);

} // namespace NYT::NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
