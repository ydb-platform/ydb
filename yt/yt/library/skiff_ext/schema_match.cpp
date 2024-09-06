#include "schema_match.h"

#include "serialize.h"

#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/serialize.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NSkiffExt {

using namespace NYson;
using namespace NYTree;
using namespace NSkiff;

////////////////////////////////////////////////////////////////////////////////

const TString KeySwitchColumnName = "$key_switch";
const TString OtherColumnsName = "$other_columns";
const TString SparseColumnsName = "$sparse_columns";

////////////////////////////////////////////////////////////////////////////////

static void ThrowInvalidSkiffTypeError(const TString& columnName, std::shared_ptr<TSkiffSchema> expectedType, std::shared_ptr<TSkiffSchema> actualType)
{
    THROW_ERROR_EXCEPTION("Column %Qv has unexpected Skiff type: expected %Qv, found type %Qv",
        columnName,
        GetShortDebugString(expectedType),
        GetShortDebugString(actualType));
}

static ERowRangeIndexMode GetRowRangeIndexMode(const std::shared_ptr<TSkiffSchema>& skiffSchema, TStringBuf columnName)
{
    auto throwRowRangeIndexError = [&] {
        THROW_ERROR_EXCEPTION("Column %Qv has unsupported Skiff type %Qv",
            columnName,
            GetShortDebugString(skiffSchema));
    };

    if (skiffSchema->GetWireType() != EWireType::Variant8) {
        throwRowRangeIndexError();
    }

    std::vector<EWireType> children;
    for (const auto& child : skiffSchema->GetChildren()) {
        children.emplace_back(child->GetWireType());
    }

    if (children == std::vector{EWireType::Nothing, EWireType::Int64}) {
        return ERowRangeIndexMode::Incremental;
    } else if (children == std::vector{EWireType::Nothing, EWireType::Int64, EWireType::Nothing}) {
        return ERowRangeIndexMode::IncrementalWithError;
    }
    throwRowRangeIndexError();
    Y_UNREACHABLE();
}

static bool IsSkiffSpecialColumn(
    TStringBuf columnName,
    const TString& rangeIndexColumnName,
    const TString& rowIndexColumnName)
{
    static const THashSet<TString> specialColumns = {
        KeySwitchColumnName,
        OtherColumnsName,
        SparseColumnsName
    };
    return specialColumns.contains(columnName) || columnName == rangeIndexColumnName || columnName == rowIndexColumnName;
}

static std::pair<std::shared_ptr<TSkiffSchema>, bool> DeoptionalizeSchema(std::shared_ptr<TSkiffSchema> skiffSchema)
{
    if (skiffSchema->GetWireType() != EWireType::Variant8) {
        return std::pair(skiffSchema, true);
    }
    auto children = skiffSchema->GetChildren();
    if (children.size() != 2) {
        return std::pair(skiffSchema, true);
    }
    if (children[0]->GetWireType() == EWireType::Nothing) {
        return std::pair(children[1], false);
    } else {
        return std::pair(skiffSchema, true);
    }
}

static TSkiffTableDescription CreateTableDescription(
    const std::shared_ptr<TSkiffSchema>& skiffSchema,
    const TString& rangeIndexColumnName,
    const TString& rowIndexColumnName)
{
    TSkiffTableDescription result;
    THashSet<TString> topLevelNames;
    std::shared_ptr<TSkiffSchema> otherColumnsField;
    std::shared_ptr<TSkiffSchema> sparseColumnsField;

    if (skiffSchema->GetWireType() != EWireType::Tuple) {
        THROW_ERROR_EXCEPTION("Invalid wire type for table row: expected %Qlv, found %Qlv",
            EWireType::Tuple,
            skiffSchema->GetWireType());
    }

    auto children = skiffSchema->GetChildren();

    if (!children.empty() && children.back()->GetName() == OtherColumnsName) {
        otherColumnsField = children.back();
        result.HasOtherColumns = true;
        children.pop_back();

        if (otherColumnsField->GetWireType() != EWireType::Yson32) {
            THROW_ERROR_EXCEPTION("Invalid wire type for column %Qv: expected %Qlv, found %Qlv",
                OtherColumnsName,
                EWireType::Yson32,
                otherColumnsField->GetWireType());
        }
    }

    if (!children.empty() && children.back()->GetName() == SparseColumnsName) {
        sparseColumnsField = children.back();
        children.pop_back();
        if (sparseColumnsField->GetWireType() != EWireType::RepeatedVariant16) {
            THROW_ERROR_EXCEPTION("Invalid wire type for column %Qv: expected %Qlv, found %Qlv",
                SparseColumnsName,
                EWireType::RepeatedVariant16,
                sparseColumnsField->GetWireType());
        }
    }

    // Dense fields.
    for (size_t i = 0; i != children.size(); ++i) {
        const auto& child = children[i];
        const auto& childName = child->GetName();
        if (childName.empty()) {
            THROW_ERROR_EXCEPTION("Element #%v of row Skiff schema must have a name",
                i);
        } else if (childName == OtherColumnsName || childName == SparseColumnsName) {
            THROW_ERROR_EXCEPTION("Invalid placement of special column %Qv",
                childName);
        }
        auto res = topLevelNames.emplace(childName);
        if (!res.second) {
            THROW_ERROR_EXCEPTION("Name %Qv is found multiple times",
                childName);
        }
        if (childName == KeySwitchColumnName) {
            if (child->GetWireType() != EWireType::Boolean) {
                ThrowInvalidSkiffTypeError(
                    childName,
                    CreateSimpleTypeSchema(EWireType::Boolean),
                    child);
            }
            result.KeySwitchFieldIndex = i;
        } else if (childName == rowIndexColumnName) {
            result.RowIndexFieldIndex = i;
            result.RowIndexMode = GetRowRangeIndexMode(child, childName);
        } else if (childName == rangeIndexColumnName) {
            result.RangeIndexFieldIndex = i;
            result.RangeIndexMode = GetRowRangeIndexMode(child, childName);
        }
        result.DenseFieldDescriptionList.emplace_back(childName, child);
    }

    // Sparse fields.
    if (sparseColumnsField) {
        for (const auto& child : sparseColumnsField->GetChildren()) {
            const auto& name = child->GetName();
            if (name.empty()) {
                THROW_ERROR_EXCEPTION("Children of %Qv must have nonempty name",
                    SparseColumnsName);
            }
            if (IsSkiffSpecialColumn(name, rangeIndexColumnName, rowIndexColumnName)) {
                THROW_ERROR_EXCEPTION("Skiff special column %Qv cannot be a child of %Qv",
                    name,
                    SparseColumnsName);
            }
            auto res = topLevelNames.emplace(name);
            if (!res.second) {
                THROW_ERROR_EXCEPTION("Name %Qv is found multiple times",
                    name);
            }
            result.SparseFieldDescriptionList.emplace_back(name, child);
        }
    }

    return result;
}

std::vector<TSkiffTableDescription> CreateTableDescriptionList(
    const std::vector<std::shared_ptr<TSkiffSchema>>& skiffSchemas,
    const TString& rangeIndexColumnName,
    const TString& rowIndexColumnName)
{
    std::vector<TSkiffTableDescription> result;
    for (ui16 index = 0; index < skiffSchemas.size(); ++index) {
        result.emplace_back(CreateTableDescription(
            skiffSchemas[index],
            rangeIndexColumnName,
            rowIndexColumnName));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

static constexpr char ReferencePrefix = '$';

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSkiffSchemaRepresentation)

class TSkiffSchemaRepresentation
    : public TYsonStruct
{
public:
    TString Name;
    EWireType WireType;
    std::optional<std::vector<INodePtr>> Children;

    REGISTER_YSON_STRUCT(TSkiffSchemaRepresentation);

    static void Register(TRegistrar registrar) {
        registrar.Parameter("name", &TThis::Name)
            .Default();
        registrar.Parameter("wire_type", &TThis::WireType);
        registrar.Parameter("children", &TThis::Children)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TSkiffSchemaRepresentation)

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<TSkiffSchema> ParseSchema(
    const INodePtr& schemaNode,
    const IMapNodePtr& registry,
    THashMap<TString, std::shared_ptr<TSkiffSchema>>* parsedRegistry,
    THashSet<TString>* parseInProgressNames)
{
    auto schemaNodeType = schemaNode->GetType();
    if (schemaNodeType == ENodeType::String) {
        auto name = schemaNode->AsString()->GetValue();
        if (!name.StartsWith(ReferencePrefix)) {
            THROW_ERROR_EXCEPTION(
                "Invalid reference %Qv, reference must start with %Qv",
                name,
                ReferencePrefix);
        }
        name = name.substr(1);
        auto it = parsedRegistry->find(name);
        if (it != parsedRegistry->end()) {
            return it->second;
        } else if (parseInProgressNames->contains(name)) {
            THROW_ERROR_EXCEPTION(
                "Type %Qv is recursive, recursive types are forbidden",
                name);
        } else {
            auto schemaFromRegistry = registry->FindChild(name);
            if (!schemaFromRegistry) {
                THROW_ERROR_EXCEPTION(
                    "Cannot resolve type reference %Qv",
                    name);
            }
            parseInProgressNames->insert(name);
            auto result = ParseSchema(schemaFromRegistry, registry, parsedRegistry, parseInProgressNames);
            parseInProgressNames->erase(name);
            parsedRegistry->emplace(name, result);
            return result;
        }
    } else if (schemaNodeType == ENodeType::Map) {
        auto schemaMapNode = schemaNode->AsMap();
        auto schemaRepresentation = ConvertTo<TSkiffSchemaRepresentationPtr>(schemaMapNode);
        if (IsSimpleType(schemaRepresentation->WireType)) {
            return CreateSimpleTypeSchema(schemaRepresentation->WireType)->SetName(schemaRepresentation->Name);
        } else {
            if (!schemaRepresentation->Children) {
                THROW_ERROR_EXCEPTION(
                    "Complex type %Qlv lacks children",
                    schemaRepresentation->WireType);
            }
            std::vector<std::shared_ptr<TSkiffSchema>> childSchemaList;
            for (const auto& childNode : *schemaRepresentation->Children) {
                auto childSchema = ParseSchema(childNode, registry, parsedRegistry, parseInProgressNames);
                childSchemaList.push_back(childSchema);
            }

            switch (schemaRepresentation->WireType) {
                case EWireType::Variant8:
                    return CreateVariant8Schema(childSchemaList)->SetName(schemaRepresentation->Name);
                case EWireType::Variant16:
                    return CreateVariant16Schema(childSchemaList)->SetName(schemaRepresentation->Name);
                case EWireType::RepeatedVariant8:
                    return CreateRepeatedVariant8Schema(childSchemaList)->SetName(schemaRepresentation->Name);
                case EWireType::RepeatedVariant16:
                    return CreateRepeatedVariant16Schema(childSchemaList)->SetName(schemaRepresentation->Name);
                case EWireType::Tuple:
                    return CreateTupleSchema(childSchemaList)->SetName(schemaRepresentation->Name);
                default:
                    YT_ABORT();
            }
        }
    } else {
        THROW_ERROR_EXCEPTION(
            "Invalid type for Skiff schema description; expected %Qlv or %Qlv, found %Qlv",
            ENodeType::Map,
            ENodeType::String,
            schemaNodeType);
    }
}

std::vector<std::shared_ptr<TSkiffSchema>> ParseSkiffSchemas(
    const NYTree::IMapNodePtr& skiffSchemaRegistry,
    const NYTree::IListNodePtr& tableSkiffSchemas)
{
    THashMap<TString, std::shared_ptr<TSkiffSchema>> parsedRegistry;
    std::vector<std::shared_ptr<TSkiffSchema>> result;
    for (const auto& node : tableSkiffSchemas->GetChildren()) {
        THashSet<TString> parseInProgressNames;
        auto skiffSchema = ParseSchema(node, skiffSchemaRegistry, &parsedRegistry, &parseInProgressNames);
        result.push_back(skiffSchema);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TFieldDescription::TFieldDescription(TString name, std::shared_ptr<TSkiffSchema> schema)
    : Name_(std::move(name))
    , Schema_(std::move(schema))
{ }

EWireType TFieldDescription::ValidatedSimplify() const
{
    auto result = Simplify();
    if (!result) {
        THROW_ERROR_EXCEPTION("Column %Qv cannot be represented with Skiff schema %Qv",
            Name_,
            GetShortDebugString(Schema_));
    }
    return *result;
}

bool TFieldDescription::IsNullable() const
{
    return !IsRequired();
}

bool TFieldDescription::IsRequired() const
{
    return DeoptionalizeSchema(Schema_).second;
}

std::optional<EWireType> TFieldDescription::Simplify() const
{
    const auto& [deoptionalized, required] = DeoptionalizeSchema(Schema_);
    auto wireType = deoptionalized->GetWireType();
    if (IsSimpleType(wireType)) {
        if (wireType != EWireType::Nothing || required) {
            return wireType;
        }
    }
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSkiffExt
