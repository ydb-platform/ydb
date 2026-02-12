#include "schema_serialization_helpers.h"
#include "comparator.h"
#include "constrained_schema.h"
#include "logical_type.h"

#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/library/heavy_schema_validation/schema_validation.h>

namespace NYT::NTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

struct TSerializableColumnSchema
    : public TMaybeDeletedColumnSchema
    , public NYTree::TYsonStructLite
{
    REGISTER_YSON_STRUCT_LITE(TSerializableColumnSchema);

    static void Register(TRegistrar registrar)
    {
        registrar.BaseClassParameter("name", &TThis::Name_)
            .Default();
        registrar.Parameter("stable_name", &TThis::SerializedStableName_)
            .Default();
        registrar.Parameter("type", &TThis::LogicalTypeV1_)
            .Default(std::nullopt);
        registrar.Parameter("required", &TThis::RequiredV1_)
            .Default(std::nullopt);
        registrar.Parameter("type_v3", &TThis::LogicalTypeV3_)
            .Default();
        registrar.BaseClassParameter("lock", &TThis::Lock_)
            .Default();
        registrar.BaseClassParameter("expression", &TThis::Expression_)
            .Default();
        registrar.BaseClassParameter("materialized", &TThis::Materialized_)
            .Default();
        registrar.BaseClassParameter("aggregate", &TThis::Aggregate_)
            .Default();
        registrar.BaseClassParameter("sort_order", &TThis::SortOrder_)
            .Default();
        registrar.BaseClassParameter("group", &TThis::Group_)
            .Default();
        registrar.BaseClassParameter("max_inline_hunk_size", &TThis::MaxInlineHunkSize_)
            .Default();
        registrar.BaseClassParameter("deleted", &TThis::Deleted_)
            .Default();
        registrar.BaseClassParameter("constraint", &TThis::Constraint_)
            .Default();

        registrar.Postprocessor([] (TSerializableColumnSchema* schema) {
            schema->RunPostprocessor();
        });
    }

    void RunPostprocessor()
    {
        if (Deleted() && *Deleted()) {
            if (!SerializedStableName_) {
                THROW_ERROR_EXCEPTION("Stable name should be set for a deleted column");
            }
            SetStableName(*SerializedStableName_);
            return;
        }

        // Name.
        if (Name().empty()) {
            THROW_ERROR_EXCEPTION("Column name cannot be empty");
        }

        if (SerializedStableName_) {
            ValidateColumnName(SerializedStableName_->Underlying());
            SetStableName(*SerializedStableName_);
        } else {
            SetStableName(TColumnStableName(Name()));
        }

        try {
            int setTypeVersion = 0;
            if (LogicalTypeV3_) {
                SetLogicalType(LogicalTypeV3_->LogicalType);
                setTypeVersion = 3;
            }

            if (LogicalTypeV1_) {
                if (setTypeVersion == 0) {
                    SetLogicalType(MakeLogicalType(*LogicalTypeV1_, RequiredV1_.value_or(false)));
                    setTypeVersion = 1;
                } else if (*LogicalTypeV1_ != CastToV1Type()) {
                    auto versionedType = Format("type_v%v", setTypeVersion);
                    THROW_ERROR_EXCEPTION("%Qv does not match \"type\"", versionedType)
                        << TErrorAttribute(versionedType, Format("%v", *LogicalType()))
                        << TErrorAttribute("type", *LogicalTypeV1_)
                        << TErrorAttribute("expected_type", CastToV1Type());
                }
            }

            if (RequiredV1_ && setTypeVersion > 1 && *RequiredV1_ != Required()) {
                auto versionedType = Format("type_v%v", setTypeVersion);
                THROW_ERROR_EXCEPTION("%Qv does not match \"required\"", versionedType)
                    << TErrorAttribute(versionedType, Format("%v", *LogicalType()))
                    << TErrorAttribute("required", *RequiredV1_);
            }

            if (setTypeVersion == 0) {
                THROW_ERROR_EXCEPTION("Column type is not specified");
            }

            if (*DetagLogicalType(LogicalType()) == *SimpleLogicalType(ESimpleLogicalValueType::Any)) {
                THROW_ERROR_EXCEPTION("Column of type %Qlv cannot be \"required\"",
                    ESimpleLogicalValueType::Any);
            }

            // Lock.
            if (Lock() && Lock()->empty()) {
                THROW_ERROR_EXCEPTION("Lock name cannot be empty");
            }

            // Group.
            if (Group() && Group()->empty()) {
                THROW_ERROR_EXCEPTION("Group name cannot be empty");
            }

            // Constraint.
            if (Constraint_ && Constraint_->empty()) {
                THROW_ERROR_EXCEPTION("Constraint cannot be empty");
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error validating column %Qv in table schema",
                GetDiagnosticNameString())
                << ex;
        }
    }

public:
    void DeserializeFromCursor(NYson::TYsonPullParserCursor* cursor)
    {
        cursor->ParseMap([&] (TYsonPullParserCursor* cursor) {
            EnsureYsonToken("column schema attribute key", *cursor, EYsonItemType::StringValue);
            auto key = (*cursor)->UncheckedAsString();
            if (key == TStringBuf("name")) {
                cursor->Next();
                SetName(ExtractTo<TString>(cursor));
            } else if (key == TStringBuf("required")) {
                cursor->Next();
                RequiredV1_ = ExtractTo<bool>(cursor);
            } else if (key == TStringBuf("type")) {
                cursor->Next();
                LogicalTypeV1_ = ExtractTo<ESimpleLogicalValueType>(cursor);
            } else if (key == TStringBuf("type_v3")) {
                cursor->Next();
                LogicalTypeV3_ = TTypeV3LogicalTypeWrapper();
                Deserialize(*LogicalTypeV3_, cursor);
            } else if (key == TStringBuf("lock")) {
                cursor->Next();
                SetLock(ExtractTo<std::optional<std::string>>(cursor));
            } else if (key == TStringBuf("expression")) {
                cursor->Next();
                SetExpression(ExtractTo<std::optional<std::string>>(cursor));
            } else if (key == TStringBuf("materialized")) {
                cursor->Next();
                SetMaterialized(ExtractTo<std::optional<bool>>(cursor));
            } else if (key == TStringBuf("aggregate")) {
                cursor->Next();
                SetAggregate(ExtractTo<std::optional<std::string>>(cursor));
            } else if (key == TStringBuf("sort_order")) {
                cursor->Next();
                SetSortOrder(ExtractTo<std::optional<ESortOrder>>(cursor));
            } else if (key == TStringBuf("group")) {
                cursor->Next();
                SetGroup(ExtractTo<std::optional<std::string>>(cursor));
            } else if (key == TStringBuf("max_inline_hunk_size")) {
                cursor->Next();
                SetMaxInlineHunkSize(ExtractTo<std::optional<i64>>(cursor));
            } else if (key == TStringBuf("stable_name")) {
                cursor->Next();
                SerializedStableName_ = ExtractTo<TColumnStableName>(cursor);
            } else if (key == TStringBuf("deleted")) {
                cursor->Next();
                Deleted_ = ExtractTo<bool>(cursor);
            } else if (key == TStringBuf("constraint")) {
                cursor->Next();
                Constraint_ = ExtractTo<std::optional<std::string>>(cursor);
            } else {
                cursor->Next();
                cursor->SkipComplexValue();
            }
        });

        RunPostprocessor();
    }

    void SetColumnSchema(const TColumnSchema& columnSchema, std::optional<std::string> constraint)
    {
        static_cast<TColumnSchema&>(*this) = columnSchema;
        if (IsRenamed()) {
            SerializedStableName_ = StableName();
        }
        LogicalTypeV1_ = columnSchema.CastToV1Type();
        RequiredV1_ = columnSchema.Required();
        LogicalTypeV3_ = TTypeV3LogicalTypeWrapper{columnSchema.LogicalType()};
        Constraint_ = std::move(constraint);
    }

    void SetDeletedColumnSchema(const TDeletedColumn& deletedColumnSchema)
    {
        Deleted_ = true;
        StableName_ = deletedColumnSchema.StableName();
    }

private:
    std::optional<TColumnStableName> SerializedStableName_;

    std::optional<ESimpleLogicalValueType> LogicalTypeV1_;
    std::optional<bool> RequiredV1_;

    std::optional<TTypeV3LogicalTypeWrapper> LogicalTypeV3_;
};

////////////////////////////////////////////////////////////////////////////////

void ThrowDuplicateConstraintsForColumn(
    const std::string& column,
    const std::string& firstConstraint,
    const std::string& secondConstraint)
{
    THROW_ERROR_EXCEPTION(
        "Received duplicate constraints for column %Qv",
        column)
        << TErrorAttribute("first_conflicting_constraint", firstConstraint)
        << TErrorAttribute("second_conflicting_constraint", secondConstraint);
}

void Serialize(const TTableSchema& schema, const TColumnNameToConstraintMap& columnNameToConstraint, IYsonConsumer* consumer)
{
    auto position = BuildYsonFluently(consumer)
        .BeginAttributes()
            .Item("strict").Value(schema.IsStrict())
            .Item("unique_keys").Value(schema.IsUniqueKeys())
            .DoIf(schema.HasNontrivialSchemaModification(), [&] (TFluentMap fluent) {
                fluent.Item("schema_modification").Value(schema.GetSchemaModification());
            })
        .EndAttributes()
        .BeginList();

    for (const auto& column : schema.Columns()) {
        std::optional<std::string> constraint;
        auto it = columnNameToConstraint.find(column.Name());
        if (it != columnNameToConstraint.end()) {
            constraint = it->second;
        }
        Serialize(column, constraint, position.Item().GetConsumer());
    }
    for (const auto& deletedColumn : schema.DeletedColumns()) {
        Serialize(deletedColumn, position.Item().GetConsumer());
    }

    position.EndList();
}

void Deserialize(TTableSchema& schema, TColumnNameToConstraintMap& columnNameToConstraint, INodePtr node)
{
    auto childNodes = node->AsList()->GetChildren();

    std::vector<TColumnSchema> columns;
    std::vector<TDeletedColumn> deletedColumns;

    for (auto childNode : childNodes) {
        TSerializableColumnSchema wrapper;
        Deserialize(static_cast<TYsonStructLite&>(wrapper), childNode);
        if (wrapper.Deleted() && *wrapper.Deleted()) {
            deletedColumns.push_back(TDeletedColumn(wrapper.StableName()));
        } else {
            columns.push_back(wrapper);
        }
        if (wrapper.Constraint()) {
            auto [it, emplaced] = columnNameToConstraint.emplace(wrapper.Name(), *wrapper.Constraint());
            if (!emplaced) {
                ThrowDuplicateConstraintsForColumn(wrapper.Name(), *wrapper.Constraint(), it->second);
            }
        }
    }

    schema = TTableSchema(
        columns,
        node->Attributes().Get<bool>("strict", true),
        node->Attributes().Get<bool>("unique_keys", false),
        node->Attributes().Get<ETableSchemaModification>(
            "schema_modification",
            ETableSchemaModification::None),
        deletedColumns);
}

void Deserialize(TTableSchema& schema, TColumnNameToConstraintMap& columnNameToConstraint, TYsonPullParserCursor* cursor)
{
    auto strict = true;
    auto uniqueKeys = false;
    auto modification = ETableSchemaModification::None;

    if ((*cursor)->GetType() == EYsonItemType::BeginAttributes) {
        cursor->ParseAttributes([&] (TYsonPullParserCursor* cursor) {
            EnsureYsonToken(TStringBuf("table schema attribute key"), *cursor, EYsonItemType::StringValue);
            auto key = (*cursor)->UncheckedAsString();
            if (key == TStringBuf("strict")) {
                cursor->Next();
                strict = ExtractTo<bool>(cursor);
            } else if (key == TStringBuf("unique_keys")) {
                cursor->Next();
                uniqueKeys = ExtractTo<bool>(cursor);
            } else if (key == TStringBuf("schema_modification")) {
                cursor->Next();
                modification = ExtractTo<ETableSchemaModification>(cursor);
            } else {
                cursor->Next();
                cursor->SkipComplexValue();
            }
        });
    }

    EnsureYsonToken(TStringBuf("table schema"), *cursor, EYsonItemType::BeginList);

    auto maybeDeletedColumns = ExtractTo<std::vector<TMaybeDeletedColumnSchema>>(cursor);

    std::vector<TColumnSchema> columns;
    std::vector<TDeletedColumn> deletedColumns;

    for (const auto& maybeDeletedColumn : maybeDeletedColumns) {
        if (maybeDeletedColumn.Deleted() && *maybeDeletedColumn.Deleted()) {
            deletedColumns.push_back(maybeDeletedColumn.GetDeletedColumnSchema());
        } else {
            columns.push_back(static_cast<TColumnSchema>(maybeDeletedColumn));
            if (maybeDeletedColumn.Constraint()) {
                auto [it, emplaced] = columnNameToConstraint.emplace(maybeDeletedColumn.Name(), *maybeDeletedColumn.Constraint());
                if (!emplaced) {
                    ThrowDuplicateConstraintsForColumn(maybeDeletedColumn.Name(), *maybeDeletedColumn.Constraint(), it->second);
                }
            }
        }
    }

    schema = TTableSchema(columns, strict, uniqueKeys, modification, deletedColumns);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

TDeletedColumn TMaybeDeletedColumnSchema::GetDeletedColumnSchema() const
{
    return TDeletedColumn(StableName());
}

////////////////////////////////////////////////////////////////////////////////

void Deserialize(TMaybeDeletedColumnSchema& schema, TYsonPullParserCursor* cursor)
{
    TSerializableColumnSchema wrapper;
    wrapper.DeserializeFromCursor(cursor);
    schema = wrapper;
}

void Deserialize(TMaybeDeletedColumnSchema& schema, INodePtr node)
{
    TSerializableColumnSchema wrapper;
    Deserialize(static_cast<TYsonStructLite&>(wrapper), node);
    schema = static_cast<TMaybeDeletedColumnSchema>(wrapper);
}

void Serialize(const TColumnSchema& schema, IYsonConsumer* consumer)
{
    TSerializableColumnSchema wrapper;
    wrapper.SetColumnSchema(schema, std::nullopt);
    Serialize(static_cast<const TYsonStructLite&>(wrapper), consumer);
}

void Serialize(const TColumnSchema& schema, std::optional<std::string> constraint, IYsonConsumer* consumer)
{
    TSerializableColumnSchema wrapper;
    wrapper.SetColumnSchema(schema, std::move(constraint));
    Serialize(static_cast<const TYsonStructLite&>(wrapper), consumer);
}

void Serialize(const TDeletedColumn& schema, IYsonConsumer* consumer)
{
    consumer->OnBeginMap();
    consumer->OnKeyedItem("stable_name");
    consumer->OnStringScalar(schema.StableName().Underlying());
    consumer->OnKeyedItem("deleted");
    consumer->OnBooleanScalar(true);
    consumer->OnEndMap();
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TTableSchema& schema, IYsonConsumer* consumer)
{
    Serialize(schema, /*columnNameToConstraint*/ {}, consumer);
}

void Deserialize(TTableSchema& schema, INodePtr node)
{
    TColumnNameToConstraintMap columnNameToConstraint;
    Deserialize(schema, columnNameToConstraint, node);
}

void Deserialize(TTableSchema& schema, TYsonPullParserCursor* cursor)
{
    TColumnNameToConstraintMap columnNameToConstraint;
    Deserialize(schema, columnNameToConstraint, cursor);
}

void Serialize(const TTableSchemaPtr& schema, IYsonConsumer* consumer)
{
    NYTree::Serialize<TTableSchema>(schema, consumer);
}

void Deserialize(TTableSchemaPtr& schema, INodePtr node)
{
    TTableSchema actualSchema;
    Deserialize(actualSchema, node);
    schema = New<TTableSchema>(std::move(actualSchema));
}

void Deserialize(TTableSchemaPtr& schema, TYsonPullParserCursor* cursor)
{
    TTableSchema actualSchema;
    Deserialize(actualSchema, cursor);
    schema = New<TTableSchema>(std::move(actualSchema));
}

void Serialize(const TConstrainedTableSchema& constrainSchema, IYsonConsumer* consumer)
{
    Serialize(constrainSchema.TableSchema(), constrainSchema.ColumnToConstraint(), consumer);
}

void Deserialize(TConstrainedTableSchema& schema, INodePtr node)
{
    TTableSchema actualSchema;
    TColumnNameToConstraintMap columnNameToConstraint;
    Deserialize(actualSchema, columnNameToConstraint, node);

    schema = TConstrainedTableSchema(std::move(actualSchema), std::move(columnNameToConstraint));
}

void Deserialize(TConstrainedTableSchema& schema, TYsonPullParserCursor* cursor)
{
    TTableSchema actualSchema;
    TColumnNameToConstraintMap columnNameToConstraint;
    Deserialize(actualSchema, columnNameToConstraint, cursor);

    schema = TConstrainedTableSchema(std::move(actualSchema), std::move(columnNameToConstraint));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
