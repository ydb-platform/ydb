#include "skiff.h"

#include <yt/cpp/mapreduce/common/retry_lib.h>

#include <yt/cpp/mapreduce/http/retry_request.h>
#include <yt/cpp/mapreduce/http/requests.h>

#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/interface/common.h>
#include <yt/cpp/mapreduce/interface/serialize.h>

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <library/cpp/yson/node/node_builder.h>
#include <library/cpp/yson/node/node_io.h>

#include <yt/cpp/mapreduce/raw_client/raw_batch_request.h>
#include <yt/cpp/mapreduce/raw_client/raw_requests.h>

#include <yt/cpp/mapreduce/skiff/skiff_schema.h>

#include <library/cpp/yson/consumer.h>
#include <library/cpp/yson/writer.h>

#include <util/string/cast.h>
#include <util/stream/str.h>
#include <util/stream/file.h>
#include <util/folder/path.h>

namespace NYT {
namespace NDetail {

using namespace NRawClient;

using ::ToString;

////////////////////////////////////////////////////////////////////////////////

static NSkiff::TSkiffSchemaPtr ReadSkiffSchema(const TString& fileName)
{
    if (!TFsPath(fileName).Exists()) {
        return nullptr;
    }
    TIFStream input(fileName);
    NSkiff::TSkiffSchemaPtr schema;
    Deserialize(schema, NodeFromYsonStream(&input));
    return schema;
}

NSkiff::TSkiffSchemaPtr GetJobInputSkiffSchema()
{
    return ReadSkiffSchema("skiff_input");
}

NSkiff::EWireType ValueTypeToSkiffType(EValueType valueType)
{
    using NSkiff::EWireType;
    switch (valueType) {
        case VT_INT64:
        case VT_INT32:
        case VT_INT16:
        case VT_INT8:
            return EWireType::Int64;

        case VT_UINT64:
        case VT_UINT32:
        case VT_UINT16:
        case VT_UINT8:
            return EWireType::Uint64;

        case VT_DOUBLE:
        case VT_FLOAT:
            return EWireType::Double;

        case VT_BOOLEAN:
            return EWireType::Boolean;

        case VT_STRING:
        case VT_UTF8:
        case VT_JSON:
            return EWireType::String32;

        case VT_ANY:
            return EWireType::Yson32;

        case VT_NULL:
        case VT_VOID:
            return EWireType::Nothing;

        case VT_DATE:
        case VT_DATETIME:
        case VT_TIMESTAMP:
            return EWireType::Uint64;

        case VT_INTERVAL:
            return EWireType::Int64;

        case VT_DATE32:
        case VT_DATETIME64:
        case VT_TIMESTAMP64:
        case VT_INTERVAL64:
            return EWireType::Int64;
    };
    ythrow yexception() << "Cannot convert EValueType '" << valueType << "' to NSkiff::EWireType";
}

NSkiff::TSkiffSchemaPtr CreateSkiffSchema(
    const TTableSchema& schema,
    const TCreateSkiffSchemaOptions& options)
{
    using namespace NSkiff;

    Y_ENSURE(schema.Strict(), "Cannot create Skiff schema for non-strict table schema");
    TVector<TSkiffSchemaPtr> skiffColumns;
    for (const auto& column: schema.Columns()) {
        TSkiffSchemaPtr skiffColumn;
        if (column.Deleted().Defined() && *column.Deleted()) {
            continue;
        }
        if (column.Type() == VT_ANY && *column.TypeV3() != *NTi::Optional(NTi::Yson())) {
            // We ignore all complex types until YT-12717 is done.
            return nullptr;
        }
        if (column.TypeV3()->IsDecimal() ||
            column.TypeV3()->IsOptional() && column.TypeV3()->AsOptional()->GetItemType()->IsDecimal())
        {
            // Complex logic for decimal types, ignore them for now.
            return nullptr;
        }
        if (column.Required() || NTi::IsSingular(column.TypeV3()->GetTypeName())) {
            skiffColumn = CreateSimpleTypeSchema(ValueTypeToSkiffType(column.Type()));
        } else {
            skiffColumn = CreateVariant8Schema({
                CreateSimpleTypeSchema(EWireType::Nothing),
                CreateSimpleTypeSchema(ValueTypeToSkiffType(column.Type()))});
        }
        if (options.RenameColumns_) {
            auto maybeName = options.RenameColumns_->find(column.Name());
            skiffColumn->SetName(maybeName == options.RenameColumns_->end() ? column.Name() : maybeName->second);
        } else {
            skiffColumn->SetName(column.Name());
        }
        skiffColumns.push_back(skiffColumn);
    }

    if (options.HasKeySwitch_) {
        skiffColumns.push_back(
            CreateSimpleTypeSchema(EWireType::Boolean)->SetName("$key_switch"));
    }
    if (options.HasRangeIndex_) {
        skiffColumns.push_back(
            CreateVariant8Schema({
                CreateSimpleTypeSchema(EWireType::Nothing),
                CreateSimpleTypeSchema(EWireType::Int64)})
            ->SetName("$range_index"));
    }

    skiffColumns.push_back(
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Int64)})
        ->SetName("$row_index"));

    return CreateTupleSchema(std::move(skiffColumns));
}

NSkiff::TSkiffSchemaPtr CreateSkiffSchema(
    const TNode& schemaNode,
    const TCreateSkiffSchemaOptions& options)
{
    TTableSchema schema;
    Deserialize(schema, schemaNode);
    return CreateSkiffSchema(schema, options);
}

void Serialize(const NSkiff::TSkiffSchemaPtr& schema, NYson::IYsonConsumer* consumer)
{
    consumer->OnBeginMap();
    if (schema->GetName().size() > 0) {
        consumer->OnKeyedItem("name");
        consumer->OnStringScalar(schema->GetName());
    }
    consumer->OnKeyedItem("wire_type");
    consumer->OnStringScalar(ToString(schema->GetWireType()));
    if (schema->GetChildren().size() > 0) {
        consumer->OnKeyedItem("children");
        consumer->OnBeginList();
        for (const auto& child : schema->GetChildren()) {
            consumer->OnListItem();
            Serialize(child, consumer);
        }
        consumer->OnEndList();
    }
    consumer->OnEndMap();
}

void Deserialize(NSkiff::TSkiffSchemaPtr& schema, const TNode& node)
{
    using namespace NSkiff;

    static auto createSchema = [](EWireType wireType, TVector<TSkiffSchemaPtr>&& children) -> TSkiffSchemaPtr {
        switch (wireType) {
            case EWireType::Tuple:
                return CreateTupleSchema(std::move(children));
            case EWireType::Variant8:
                return CreateVariant8Schema(std::move(children));
            case EWireType::Variant16:
                return CreateVariant16Schema(std::move(children));
            case EWireType::RepeatedVariant8:
                return CreateRepeatedVariant8Schema(std::move(children));
            case EWireType::RepeatedVariant16:
                return CreateRepeatedVariant16Schema(std::move(children));
            default:
                return CreateSimpleTypeSchema(wireType);
        }
    };

    const auto& map = node.AsMap();
    const auto* wireTypePtr = map.FindPtr("wire_type");
    Y_ENSURE(wireTypePtr, "'wire_type' is a required key");
    auto wireType = FromString<NSkiff::EWireType>(wireTypePtr->AsString());

    const auto* childrenPtr = map.FindPtr("children");
    Y_ENSURE(NSkiff::IsSimpleType(wireType) || childrenPtr,
        "'children' key is required for complex node '" << wireType << "'");
    TVector<TSkiffSchemaPtr> children;
    if (childrenPtr) {
        for (const auto& childNode : childrenPtr->AsList()) {
            TSkiffSchemaPtr childSchema;
            Deserialize(childSchema, childNode);
            children.push_back(std::move(childSchema));
        }
    }

    schema = createSchema(wireType, std::move(children));

    const auto* namePtr = map.FindPtr("name");
    if (namePtr) {
        schema->SetName(namePtr->AsString());
    }
}

TFormat CreateSkiffFormat(const NSkiff::TSkiffSchemaPtr& schema) {
    Y_ENSURE(schema->GetWireType() == NSkiff::EWireType::Variant16,
        "Bad wire type for schema; expected 'variant16', got " << schema->GetWireType());

    THashMap<
        NSkiff::TSkiffSchemaPtr,
        size_t,
        NSkiff::TSkiffSchemaPtrHasher,
        NSkiff::TSkiffSchemaPtrEqual> schemasMap;
    size_t tableIndex = 0;
    auto config = TNode("skiff");
    config.Attributes()["table_skiff_schemas"] = TNode::CreateList();

    for (const auto& schemaChild : schema->GetChildren()) {
        auto [iter, inserted] = schemasMap.emplace(schemaChild, tableIndex);
        size_t currentIndex;
        if (inserted) {
            currentIndex = tableIndex;
            ++tableIndex;
        } else {
            currentIndex = iter->second;
        }
        config.Attributes()["table_skiff_schemas"].Add("$" + ToString(currentIndex));
    }

    config.Attributes()["skiff_schema_registry"] = TNode::CreateMap();

    for (const auto& [tableSchema, index] : schemasMap) {
        TNode node;
        TNodeBuilder nodeBuilder(&node);
        Serialize(tableSchema, &nodeBuilder);
        config.Attributes()["skiff_schema_registry"][ToString(index)] = std::move(node);
    }

    return TFormat(config);
}

NSkiff::TSkiffSchemaPtr CreateSkiffSchemaIfNecessary(
    const TClientContext& context,
    const IClientRetryPolicyPtr& clientRetryPolicy,
    const TTransactionId& transactionId,
    ENodeReaderFormat nodeReaderFormat,
    const TVector<TRichYPath>& tablePaths,
    const TCreateSkiffSchemaOptions& options)
{
    if (nodeReaderFormat == ENodeReaderFormat::Yson) {
        return nullptr;
    }

    for (const auto& path : tablePaths) {
        if (path.Columns_) {
            switch (nodeReaderFormat) {
                case ENodeReaderFormat::Skiff:
                    ythrow TApiUsageError() << "Cannot use Skiff format with column selectors";
                case ENodeReaderFormat::Auto:
                    return nullptr;
                default:
                    Y_ABORT("Unexpected node reader format: %d", static_cast<int>(nodeReaderFormat));
            }
        }
    }

    auto nodes = NRawClient::BatchTransform(
        clientRetryPolicy->CreatePolicyForGenericRequest(),
        context,
        NRawClient::CanonizeYPaths(clientRetryPolicy->CreatePolicyForGenericRequest(), context, tablePaths),
        [&] (TRawBatchRequest& batch, const TRichYPath& path) {
            auto getOptions = TGetOptions()
                .AttributeFilter(
                    TAttributeFilter()
                        .AddAttribute("schema")
                        .AddAttribute("dynamic")
                        .AddAttribute("type")
                );
            return batch.Get(transactionId, path.Path_, getOptions);
        });

    TVector<NSkiff::TSkiffSchemaPtr> schemas;
    for (size_t tableIndex = 0; tableIndex < nodes.size(); ++tableIndex) {
        const auto& tablePath = tablePaths[tableIndex].Path_;
        const auto& attributes = nodes[tableIndex].GetAttributes();
        Y_ENSURE_EX(attributes["type"] == TNode("table"),
            TApiUsageError() << "Operation input path " << tablePath << " is not a table");
        bool dynamic = attributes["dynamic"].AsBool();
        bool strict = attributes["schema"].GetAttributes()["strict"].AsBool();
        switch (nodeReaderFormat) {
            case ENodeReaderFormat::Skiff:
                Y_ENSURE_EX(strict,
                    TApiUsageError() << "Cannot use skiff format for table with non-strict schema '" << tablePath << "'");
                Y_ENSURE_EX(!dynamic,
                    TApiUsageError() << "Cannot use skiff format for dynamic table '" << tablePath << "'");
                break;
            case ENodeReaderFormat::Auto:
                if (dynamic || !strict) {
                    YT_LOG_DEBUG("Cannot use skiff format for table '%v' as it is dynamic or has non-strict schema",
                        tablePath);
                    return nullptr;
                }
                break;
            default:
                Y_ABORT("Unexpected node reader format: %d", static_cast<int>(nodeReaderFormat));
        }

        NSkiff::TSkiffSchemaPtr curSkiffSchema;
        if (tablePaths[tableIndex].RenameColumns_) {
            auto customOptions = options;
            customOptions.RenameColumns(*tablePaths[tableIndex].RenameColumns_);
            curSkiffSchema = CreateSkiffSchema(attributes["schema"], customOptions);
        } else {
            curSkiffSchema = CreateSkiffSchema(attributes["schema"], options);
        }

        if (!curSkiffSchema) {
            return nullptr;
        }
        schemas.push_back(curSkiffSchema);
    }
    return NSkiff::CreateVariant16Schema(std::move(schemas));
}

////////////////////////////////////////////////////////////////////////////////

NSkiff::TSkiffSchemaPtr CreateSkiffSchema(
    const TVector<NSkiff::TSkiffSchemaPtr>& tableSchemas,
    const TCreateSkiffSchemaOptions& options
) {
    constexpr auto KEY_SWITCH_COLUMN = "$key_switch";
    constexpr auto ROW_INDEX_COLUMN = "$row_index";
    constexpr auto RANGE_INDEX_COLUMN = "$range_index";

    TVector<NSkiff::TSkiffSchemaPtr> schemas;
    schemas.reserve(tableSchemas.size());

    for (const auto& tableSchema : tableSchemas) {
        Y_ENSURE(tableSchema->GetWireType() == NSkiff::EWireType::Tuple,
                "Expected 'tuple' wire type for table schema, got '" << tableSchema->GetWireType() << "'");

        const auto& children = tableSchema->GetChildren();
        NSkiff::TSkiffSchemaList columns;

        columns.reserve(children.size() + 3);
        if (options.HasKeySwitch_) {
            columns.push_back(
                CreateSimpleTypeSchema(NSkiff::EWireType::Boolean)->SetName(KEY_SWITCH_COLUMN));
        }
        columns.push_back(
            NSkiff::CreateVariant8Schema({
                CreateSimpleTypeSchema(NSkiff::EWireType::Nothing),
                CreateSimpleTypeSchema(NSkiff::EWireType::Int64)})
            ->SetName(ROW_INDEX_COLUMN));
        if (options.HasRangeIndex_) {
            columns.push_back(
                NSkiff::CreateVariant8Schema({
                    CreateSimpleTypeSchema(NSkiff::EWireType::Nothing),
                    CreateSimpleTypeSchema(NSkiff::EWireType::Int64)})
                ->SetName(RANGE_INDEX_COLUMN));
        }
        columns.insert(columns.end(), children.begin(), children.end());

        schemas.push_back(NSkiff::CreateTupleSchema(columns));
    }

    return NSkiff::CreateVariant16Schema(schemas);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
