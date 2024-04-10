
#include "comparator.h"

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/client/table_client/schema_serialization_helpers.h>

namespace NYT::NTableClient {

void Deserialize(TMaybeDeletedColumnSchema& schema, NYson::TYsonPullParserCursor* cursor)
{
    TSerializableColumnSchema wrapper;
    wrapper.DeserializeFromCursor(cursor);
    schema = wrapper;
}

void Deserialize(TMaybeDeletedColumnSchema& schema, NYTree::INodePtr node)
{
    TSerializableColumnSchema wrapper;
    Deserialize(static_cast<NYTree::TYsonStructLite&>(wrapper), node);
    schema = static_cast<TMaybeDeletedColumnSchema>(wrapper);
}

TDeletedColumn TMaybeDeletedColumnSchema::GetDeletedColumnSchema() const
{
    return TDeletedColumn(StableName());
}

void TSerializableColumnSchema::Register(TRegistrar registrar)
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
    registrar.BaseClassParameter("aggregate", &TThis::Aggregate_)
        .Default();
    registrar.BaseClassParameter("sort_order", &TThis::SortOrder_)
        .Default();
    registrar.BaseClassParameter("group", &TThis::Group_)
        .Default();
    registrar.BaseClassParameter("max_inline_hunk_size", &TThis::MaxInlineHunkSize_)
        .Default();
    registrar.BaseClassParameter("deleted", &TThis::Deleted_).Default(std::nullopt);

    registrar.Postprocessor([] (TSerializableColumnSchema* schema) {
        schema->RunPostprocessor();
    });
}

void TSerializableColumnSchema::DeserializeFromCursor(NYson::TYsonPullParserCursor* cursor)
{
    cursor->ParseMap([&] (NYson::TYsonPullParserCursor* cursor) {
        EnsureYsonToken("column schema attribute key", *cursor, NYson::EYsonItemType::StringValue);
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
            SetLock(ExtractTo<std::optional<TString>>(cursor));
        } else if (key == TStringBuf("expression")) {
            cursor->Next();
            SetExpression(ExtractTo<std::optional<TString>>(cursor));
        } else if (key == TStringBuf("aggregate")) {
            cursor->Next();
            SetAggregate(ExtractTo<std::optional<TString>>(cursor));
        } else if (key == TStringBuf("sort_order")) {
            cursor->Next();
            SetSortOrder(ExtractTo<std::optional<ESortOrder>>(cursor));
        } else if (key == TStringBuf("group")) {
            cursor->Next();
            SetGroup(ExtractTo<std::optional<TString>>(cursor));
        } else if (key == TStringBuf("max_inline_hunk_size")) {
            cursor->Next();
            SetMaxInlineHunkSize(ExtractTo<std::optional<i64>>(cursor));
        } else if (key == TStringBuf("stable_name")) {
            cursor->Next();
            SerializedStableName_ = ExtractTo<TColumnStableName>(cursor);
        } else if (key == TStringBuf("deleted")) {
            cursor->Next();
            Deleted_ = ExtractTo<bool>(cursor);
        } else {
            cursor->Next();
            cursor->SkipComplexValue();
        }
    });

    RunPostprocessor();
}

void TSerializableColumnSchema::SetColumnSchema(const TColumnSchema& columnSchema)
{
    static_cast<TColumnSchema&>(*this) = columnSchema;
    if (IsRenamed()) {
        SerializedStableName_ = StableName();
    }
    LogicalTypeV1_ = columnSchema.CastToV1Type();
    RequiredV1_ = columnSchema.Required();
    LogicalTypeV3_ = TTypeV3LogicalTypeWrapper{columnSchema.LogicalType()};
}

void TSerializableColumnSchema::SetDeletedColumnSchema(
    const TDeletedColumn& deletedColumnSchema)
{
    Deleted_ = true;
    StableName_ = deletedColumnSchema.StableName();
}

void TSerializableColumnSchema::RunPostprocessor()
{
    if (Deleted() && *Deleted()) {
        if (!SerializedStableName_) {
            THROW_ERROR_EXCEPTION("Stable name should be set for a deleted column");
        }
        SetStableName(*SerializedStableName_);
        return;
    }

    // Name
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
            } else {
                if (*LogicalTypeV1_ != CastToV1Type()) {
                    THROW_ERROR_EXCEPTION(
                        "\"type_v%v\" does not match \"type\"; \"type_v%v\": %Qv \"type\": %Qlv expected \"type\": %Qlv",
                        setTypeVersion,
                        setTypeVersion,
                        *LogicalType(),
                        *LogicalTypeV1_,
                        CastToV1Type());
                }
            }
        }

        if (RequiredV1_ && setTypeVersion > 1 && *RequiredV1_ != Required()) {
            THROW_ERROR_EXCEPTION(
                "\"type_v%v\" does not match \"required\"; \"type_v%v\": %Qv \"required\": %Qlv",
                setTypeVersion,
                setTypeVersion,
                *LogicalType(),
                *RequiredV1_);
        }

        if (setTypeVersion == 0) {
            THROW_ERROR_EXCEPTION("Column type is not specified");
        }

        if (*DetagLogicalType(LogicalType()) == *SimpleLogicalType(ESimpleLogicalValueType::Any)) {
            THROW_ERROR_EXCEPTION("Column of type %Qlv cannot be \"required\"",
                ESimpleLogicalValueType::Any);
        }

        // Lock
        if (Lock() && Lock()->empty()) {
            THROW_ERROR_EXCEPTION("Lock name cannot be empty");
        }

        // Group
        if (Group() && Group()->empty()) {
            THROW_ERROR_EXCEPTION("Group name cannot be empty");
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error validating column %Qv in table schema",
            GetDiagnosticNameString())
            << ex;
    }
}

void Serialize(const TColumnSchema& schema, NYson::IYsonConsumer* consumer)
{
    TSerializableColumnSchema wrapper;
    wrapper.SetColumnSchema(schema);
    Serialize(static_cast<const NYTree::TYsonStructLite&>(wrapper), consumer);
}

void Serialize(const TDeletedColumn& schema, NYson::IYsonConsumer* consumer)
{
    consumer->OnBeginMap();
    consumer->OnKeyedItem("stable_name");
    consumer->OnStringScalar(schema.StableName().Underlying());
    consumer->OnKeyedItem("deleted");
    consumer->OnBooleanScalar(true);
    consumer->OnEndMap();
}

void Serialize(const TTableSchema& schema, NYson::IYsonConsumer* consumer)
{
    auto position = NYTree::BuildYsonFluently(consumer)
        .BeginAttributes()
            .Item("strict").Value(schema.GetStrict())
            .Item("unique_keys").Value(schema.GetUniqueKeys())
            .DoIf(schema.HasNontrivialSchemaModification(), [&] (NYTree::TFluentMap fluent) {
                fluent.Item("schema_modification").Value(schema.GetSchemaModification());
            })
        .EndAttributes()
        .BeginList();

    for (const auto& column : schema.Columns()) {
        Serialize(column, position.Item().GetConsumer());
    }
    for (const auto& deletedColumn : schema.DeletedColumns()) {
        Serialize(deletedColumn, position.Item().GetConsumer());
    }

    position.EndList();
}

void Serialize(const TTableSchemaPtr& schema, NYson::IYsonConsumer* consumer)
{
    Serialize(*schema, consumer);
}

void Deserialize(TTableSchema& schema, NYTree::INodePtr node)
{
    auto childNodes = node->AsList()->GetChildren();

    std::vector<TColumnSchema> columns;
    std::vector<TDeletedColumn> deletedColumns;

    for (auto childNode : childNodes) {
        TSerializableColumnSchema wrapper;
        Deserialize(static_cast<NYTree::TYsonStructLite&>(wrapper), childNode);
        if (wrapper.Deleted() && *wrapper.Deleted()) {
            deletedColumns.push_back(TDeletedColumn(wrapper.StableName()));
        } else {
            columns.push_back(wrapper);
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

void Deserialize(TTableSchema& schema, NYson::TYsonPullParserCursor* cursor)
{
    bool strict = true;
    bool uniqueKeys = false;
    ETableSchemaModification modification = ETableSchemaModification::None;

    if ((*cursor)->GetType() == NYson::EYsonItemType::BeginAttributes) {
        cursor->ParseAttributes([&] (NYson::TYsonPullParserCursor* cursor) {
            EnsureYsonToken(TStringBuf("table schema attribute key"), *cursor, NYson::EYsonItemType::StringValue);
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

    EnsureYsonToken(TStringBuf("table schema"), *cursor, NYson::EYsonItemType::BeginList);

    auto maybeDeletedColumns = ExtractTo<std::vector<TMaybeDeletedColumnSchema>>(cursor);

    std::vector<TColumnSchema> columns;
    std::vector<TDeletedColumn> deletedColumns;

    for (const auto& maybeDeletedColumn : maybeDeletedColumns) {
        if (maybeDeletedColumn.Deleted() && *maybeDeletedColumn.Deleted()) {
            deletedColumns.push_back(maybeDeletedColumn.GetDeletedColumnSchema());
        } else {
            columns.push_back(static_cast<TColumnSchema>(maybeDeletedColumn));
        }
    }

    schema = TTableSchema(columns, strict, uniqueKeys, modification, deletedColumns);
}

void Deserialize(TTableSchemaPtr& schema, NYTree::INodePtr node)
{
    TTableSchema actualSchema;
    Deserialize(actualSchema, node);
    schema = New<TTableSchema>(std::move(actualSchema));
}

void Deserialize(TTableSchemaPtr& schema, NYson::TYsonPullParserCursor* cursor)
{
    TTableSchema actualSchema;
    Deserialize(actualSchema, cursor);
    schema = New<TTableSchema>(std::move(actualSchema));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
