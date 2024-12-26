#include "row_helpers.h"
#include "yson_helpers.h"
#include "yt/yt/client/table_client/public.h"

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/async_stream.h>
#include <yt/yt/core/json/json_parser.h>
#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/client/formats/config.h>
#include <yt/yt/client/formats/parser.h>
#include <yt/yt/client/formats/format.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/value_consumer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/library/formats/format.h>
#include <yt/yt/library/formats/lenval_control_constants.h>
#include <yt/yt/library/formats/protobuf_writer.h>
#include <yt/yt/library/formats/protobuf_parser.h>
#include <yt/yt/library/formats/protobuf.h>

#include <yt/yt/library/formats/unittests/protobuf_format_ut.pb.h>

#include <yt/yt/library/named_value/named_value.h>

#include <util/random/fast.h>

#include <google/protobuf/text_format.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>

using namespace std::string_view_literals;


namespace NYT {
namespace {

using namespace NYson;
using namespace NYTree;
using namespace NFormats;
using namespace NTableClient;
using namespace NConcurrency;
using namespace NProtobufFormatTest;

using ::google::protobuf::FileDescriptor;
using NNamedValue::MakeRow;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EProtoFormatType,
    (FileDescriptorLegacy)
    (FileDescriptor)
    (Structured)
);

////////////////////////////////////////////////////////////////////////////////

#define EXPECT_NODES_EQUAL(a, b) \
    EXPECT_TRUE(AreNodesEqual((a), (b))) \
        << #a ": " << ConvertToYsonString((a), EYsonFormat::Text).ToString() \
        << "\n\n" #b ": " << ConvertToYsonString((b), EYsonFormat::Text).ToString();

////////////////////////////////////////////////////////////////////////////////

TString ConvertToTextYson(const INodePtr& node)
{
    return ConvertToYsonString(node, EYsonFormat::Text).ToString();
}

// Hardcoded serialization of file descriptor used in old format description.
TString FileDescriptorLegacy = "\x0a\xb6\x03\x0a\x29\x6a\x75\x6e\x6b\x2f\x65\x72\x6d\x6f\x6c\x6f\x76\x64\x2f\x74\x65\x73\x74\x2d\x70\x72\x6f\x74\x6f\x62"
    "\x75\x66\x2f\x6d\x65\x73\x73\x61\x67\x65\x2e\x70\x72\x6f\x74\x6f\x22\x2d\x0a\x0f\x54\x45\x6d\x62\x65\x64\x65\x64\x4d\x65\x73\x73\x61\x67\x65\x12"
    "\x0b\x0a\x03\x4b\x65\x79\x18\x01\x20\x01\x28\x09\x12\x0d\x0a\x05\x56\x61\x6c\x75\x65\x18\x02\x20\x01\x28\x09\x22\xb3\x02\x0a\x08\x54\x4d\x65\x73"
    "\x73\x61\x67\x65\x12\x0e\x0a\x06\x44\x6f\x75\x62\x6c\x65\x18\x01\x20\x01\x28\x01\x12\x0d\x0a\x05\x46\x6c\x6f\x61\x74\x18\x02\x20\x01\x28\x02\x12"
    "\x0d\x0a\x05\x49\x6e\x74\x36\x34\x18\x03\x20\x01\x28\x03\x12\x0e\x0a\x06\x55\x49\x6e\x74\x36\x34\x18\x04\x20\x01\x28\x04\x12\x0e\x0a\x06\x53\x49"
    "\x6e\x74\x36\x34\x18\x05\x20\x01\x28\x12\x12\x0f\x0a\x07\x46\x69\x78\x65\x64\x36\x34\x18\x06\x20\x01\x28\x06\x12\x10\x0a\x08\x53\x46\x69\x78\x65"
    "\x64\x36\x34\x18\x07\x20\x01\x28\x10\x12\x0d\x0a\x05\x49\x6e\x74\x33\x32\x18\x08\x20\x01\x28\x05\x12\x0e\x0a\x06\x55\x49\x6e\x74\x33\x32\x18\x09"
    "\x20\x01\x28\x0d\x12\x0e\x0a\x06\x53\x49\x6e\x74\x33\x32\x18\x0a\x20\x01\x28\x11\x12\x0f\x0a\x07\x46\x69\x78\x65\x64\x33\x32\x18\x0b\x20\x01\x28"
    "\x07\x12\x10\x0a\x08\x53\x46\x69\x78\x65\x64\x33\x32\x18\x0c\x20\x01\x28\x0f\x12\x0c\x0a\x04\x42\x6f\x6f\x6c\x18\x0d\x20\x01\x28\x08\x12\x0e\x0a"
    "\x06\x53\x74\x72\x69\x6e\x67\x18\x0e\x20\x01\x28\x09\x12\x0d\x0a\x05\x42\x79\x74\x65\x73\x18\x0f\x20\x01\x28\x0c\x12\x14\x0a\x04\x45\x6e\x75\x6d"
    "\x18\x10\x20\x01\x28\x0e\x32\x06\x2e\x45\x45\x6e\x75\x6d\x12\x21\x0a\x07\x4d\x65\x73\x73\x61\x67\x65\x18\x11\x20\x01\x28\x0b\x32\x10\x2e\x54\x45"
    "\x6d\x62\x65\x64\x65\x64\x4d\x65\x73\x73\x61\x67\x65\x2a\x24\x0a\x05\x45\x45\x6e\x75\x6d\x12\x07\x0a\x03\x4f\x6e\x65\x10\x01\x12\x07\x0a\x03\x54"
    "\x77\x6f\x10\x02\x12\x09\x0a\x05\x54\x68\x72\x65\x65\x10\x03";

TString GenerateRandomLenvalString(TFastRng64& rng, ui32 size)
{
    TString result;
    result.append(reinterpret_cast<const char*>(&size), sizeof(size));

    size += sizeof(ui32);

    while (result.size() < size) {
        ui64 num = rng.GenRand();
        result.append(reinterpret_cast<const char*>(&num), sizeof(num));
    }
    if (result.size() > size) {
        result.resize(size);
    }
    return result;
}

static TProtobufFormatConfigPtr MakeProtobufFormatConfig(const std::vector<const ::google::protobuf::Descriptor*>& descriptorList)
{
    ::google::protobuf::FileDescriptorSet fileDescriptorSet;
    THashSet<const ::google::protobuf::FileDescriptor*> files;

    std::function<void(const ::google::protobuf::FileDescriptor*)> addFile;
    addFile = [&] (const ::google::protobuf::FileDescriptor* fileDescriptor) {
        if (!files.insert(fileDescriptor).second) {
            return;
        }

        // N.B. We want to write dependencies in fileDescriptorSet in topological order
        // so we traverse dependencies first and the add current fileDescriptor.
        for (int i = 0; i < fileDescriptor->dependency_count(); ++i) {
            addFile(fileDescriptor->dependency(i));
        }
        fileDescriptor->CopyTo(fileDescriptorSet.add_file());
    };
    std::vector<TString> typeNames;

    for (const auto* descriptor : descriptorList) {
        addFile(descriptor->file());
        typeNames.push_back(descriptor->full_name());
    }

    auto formatConfigYsonString = BuildYsonStringFluently()
        .BeginMap()
            .Item("file_descriptor_set_text").Value(fileDescriptorSet.ShortDebugString())
            .Item("type_names").Value(typeNames)
        .EndMap();

    return ConvertTo<TProtobufFormatConfigPtr>(formatConfigYsonString);
}

INodePtr ParseYson(TStringBuf data)
{
    return ConvertToNode(NYson::TYsonString(TString{data}));
}

TString LenvalBytes(const ::google::protobuf::Message& message)
{
    TStringStream out;
    ui32 messageSize = static_cast<ui32>(message.ByteSizeLong());
    out.Write(&messageSize, sizeof(messageSize));
    if (!message.SerializeToArcadiaStream(&out)) {
        THROW_ERROR_EXCEPTION("Can not serialize message");
    }
    return out.Str();
}

void EnsureTypesMatch(EValueType expected, EValueType actual)
{
    if (expected != actual) {
        THROW_ERROR_EXCEPTION("Mismatching type: expected %Qlv, actual %Qlv",
            expected,
            actual);
    }
}

double GetDouble(const TUnversionedValue& row)
{
    EnsureTypesMatch(EValueType::Double, row.Type);
    return row.Data.Double;
}

template <typename TMessage>
TCollectingValueConsumer ParseRows(
    const TMessage& message,
    const TProtobufFormatConfigPtr& config,
    const TTableSchemaPtr& schema = New<TTableSchema>(),
    int count = 1)
{
    TString lenvalBytes;
    TStringOutput out(lenvalBytes);
    auto messageSize = static_cast<ui32>(message.ByteSize());
    for (int i = 0; i < count; ++i) {
        out.Write(&messageSize, sizeof(messageSize));
        if (!message.SerializeToArcadiaStream(&out)) {
            THROW_ERROR_EXCEPTION("Failed to serialize message");
        }
    }

    TCollectingValueConsumer rowCollector(schema);
    auto parser = CreateParserForProtobuf(&rowCollector, config, 0);
    parser->Read(lenvalBytes);
    parser->Finish();
    if (static_cast<ssize_t>(rowCollector.Size()) != count) {
        THROW_ERROR_EXCEPTION("rowCollector has wrong size: expected %v, actual %v",
            count,
            rowCollector.Size());
    }
    return rowCollector;
}

template <typename TMessage>
TCollectingValueConsumer ParseRows(
    const TMessage& message,
    const INodePtr& config,
    const TTableSchemaPtr& schema = New<TTableSchema>(),
    int count = 1)
{
    return ParseRows(message, ConvertTo<TProtobufFormatConfigPtr>(config->Attributes().ToMap()), schema, count);
}


void AddDependencies(
    const FileDescriptor* fileDescriptor,
    std::vector<const FileDescriptor*>& fileDescriptors,
    THashSet<const FileDescriptor*>& fileDescriptorSet)
{
    if (fileDescriptorSet.contains(fileDescriptor)) {
        return;
    }
    fileDescriptorSet.insert(fileDescriptor);
    for (int i = 0; i < fileDescriptor->dependency_count(); ++i) {
        AddDependencies(fileDescriptor->dependency(i), fileDescriptors, fileDescriptorSet);
    }
    fileDescriptors.push_back(fileDescriptor);
}

template <typename ... Ts>
INodePtr CreateFileDescriptorConfig(std::optional<EComplexTypeMode> complexTypeMode = {})
{
    std::vector<const FileDescriptor*> fileDescriptors;
    THashSet<const FileDescriptor*> fileDescriptorSet;
    std::vector<const FileDescriptor*> originalFileDescriptors = {Ts::descriptor()->file()...};

    for (auto d : originalFileDescriptors) {
        AddDependencies(d, fileDescriptors, fileDescriptorSet);
    }

    ::google::protobuf::FileDescriptorSet fileDescriptorSetProto;
    for (auto fileDescriptor : fileDescriptors) {
        fileDescriptor->CopyTo(fileDescriptorSetProto.add_file());
    }
    TString fileDescriptorSetText;
    ::google::protobuf::TextFormat::Printer().PrintToString(fileDescriptorSetProto, &fileDescriptorSetText);
    std::vector<TString> typeNames = {Ts::descriptor()->full_name()...};
    return BuildYsonNodeFluently()
        .BeginAttributes()
            .Item("file_descriptor_set_text").Value(fileDescriptorSetText)
            .Item("type_names").Value(typeNames)
            .OptionalItem("complex_type_mode", complexTypeMode)
        .EndAttributes()
        .Value("protobuf");
}

static const auto EnumerationsConfig = BuildYsonNodeFluently()
    .BeginMap()
        .Item("EEnum")
        .BeginMap()
            .Item("One").Value(1)
            .Item("Two").Value(2)
            .Item("Three").Value(3)
            .Item("MinusFortyTwo").Value(-42)
            .Item("MaxInt32").Value(std::numeric_limits<int>::max())
            .Item("MinInt32").Value(std::numeric_limits<int>::min())
        .EndMap()
    .EndMap();

INodePtr CreateAllFieldsConfig(EProtoFormatType protoFormatType)
{
    switch (protoFormatType) {
        case EProtoFormatType::FileDescriptor:
            return CreateFileDescriptorConfig<TMessage>();
        case EProtoFormatType::FileDescriptorLegacy:
            return BuildYsonNodeFluently()
                .BeginAttributes()
                    .Item("file_descriptor_set")
                    .Value(FileDescriptorLegacy)
                    .Item("file_indices")
                    .BeginList()
                        .Item().Value(0)
                    .EndList()
                    .Item("message_indices")
                    .BeginList()
                        .Item().Value(1)
                    .EndList()
                .EndAttributes()
                .Value("protobuf");
        case EProtoFormatType::Structured:
            return BuildYsonNodeFluently()
                .BeginAttributes()
                    .Item("enumerations").Value(EnumerationsConfig)
                    .Item("tables")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("columns")
                            .BeginList()
                                .Item()
                                .BeginMap()
                                    .Item("name").Value("Double")
                                    .Item("field_number").Value(1)
                                    .Item("proto_type").Value("double")
                                .EndMap()

                                .Item()
                                .BeginMap()
                                    .Item("name").Value("Float")
                                    .Item("field_number").Value(2)
                                    .Item("proto_type").Value("float")
                                .EndMap()

                                .Item()
                                .BeginMap()
                                    .Item("name").Value("Int64")
                                    .Item("field_number").Value(3)
                                    .Item("proto_type").Value("int64")
                                .EndMap()

                                .Item()
                                .BeginMap()
                                    .Item("name").Value("UInt64")
                                    .Item("field_number").Value(4)
                                    .Item("proto_type").Value("uint64")
                                .EndMap()

                                .Item()
                                .BeginMap()
                                    .Item("name").Value("SInt64")
                                    .Item("field_number").Value(5)
                                    .Item("proto_type").Value("sint64")
                                .EndMap()

                                .Item()
                                .BeginMap()
                                    .Item("name").Value("Fixed64")
                                    .Item("field_number").Value(6)
                                    .Item("proto_type").Value("fixed64")
                                .EndMap()

                                .Item()
                                .BeginMap()
                                    .Item("name").Value("SFixed64")
                                    .Item("field_number").Value(7)
                                    .Item("proto_type").Value("sfixed64")
                                .EndMap()

                                .Item()
                                .BeginMap()
                                    .Item("name").Value("Int32")
                                    .Item("field_number").Value(8)
                                    .Item("proto_type").Value("int32")
                                .EndMap()

                                .Item()
                                .BeginMap()
                                    .Item("name").Value("UInt32")
                                    .Item("field_number").Value(9)
                                    .Item("proto_type").Value("uint32")
                                .EndMap()

                                .Item()
                                .BeginMap()
                                    .Item("name").Value("SInt32")
                                    .Item("field_number").Value(10)
                                    .Item("proto_type").Value("sint32")
                                .EndMap()

                                .Item()
                                .BeginMap()
                                    .Item("name").Value("Fixed32")
                                    .Item("field_number").Value(11)
                                    .Item("proto_type").Value("fixed32")
                                .EndMap()

                                .Item()
                                .BeginMap()
                                    .Item("name").Value("SFixed32")
                                    .Item("field_number").Value(12)
                                    .Item("proto_type").Value("sfixed32")
                                .EndMap()

                                .Item()
                                .BeginMap()
                                    .Item("name").Value("Bool")
                                    .Item("field_number").Value(13)
                                    .Item("proto_type").Value("bool")
                                .EndMap()

                                .Item()
                                .BeginMap()
                                    .Item("name").Value("String")
                                    .Item("field_number").Value(14)
                                    .Item("proto_type").Value("string")
                                .EndMap()

                                .Item()
                                .BeginMap()
                                    .Item("name").Value("Bytes")
                                    .Item("field_number").Value(15)
                                    .Item("proto_type").Value("bytes")
                                .EndMap()

                                .Item()
                                .BeginMap()
                                    .Item("name").Value("Enum")
                                    .Item("field_number").Value(16)
                                    .Item("proto_type").Value("enum_string")
                                    .Item("enumeration_name").Value("EEnum")
                                .EndMap()

                                .Item()
                                .BeginMap()
                                    .Item("name").Value("Message")
                                    .Item("field_number").Value(17)
                                    .Item("proto_type").Value("message")
                                .EndMap()

                                .Item()
                                .BeginMap()
                                    .Item("name").Value("AnyWithMap")
                                    .Item("field_number").Value(18)
                                    .Item("proto_type").Value("any")
                                .EndMap()

                                .Item()
                                .BeginMap()
                                    .Item("name").Value("AnyWithInt64")
                                    .Item("field_number").Value(19)
                                    .Item("proto_type").Value("any")
                                .EndMap()

                                .Item()
                                .BeginMap()
                                    .Item("name").Value("AnyWithString")
                                    .Item("field_number").Value(20)
                                    .Item("proto_type").Value("any")
                                .EndMap()

                                .Item()
                                .BeginMap()
                                    .Item("name").Value("OtherColumns")
                                    .Item("field_number").Value(21)
                                    .Item("proto_type").Value("other_columns")
                                .EndMap()

                                .Item()
                                .BeginMap()
                                    .Item("name").Value("MissingInt64")
                                    .Item("field_number").Value(22)
                                    .Item("proto_type").Value("int64")
                                .EndMap()
                            .EndList()
                        .EndMap()
                    .EndList()
                .EndAttributes()
                .Value("protobuf");
    }
    Y_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

struct TLenvalEntry
{
    TString RowData;
    ui32 TableIndex;
    ui64 TabletIndex;
};

////////////////////////////////////////////////////////////////////////////////

class TLenvalParser
{
public:
    explicit TLenvalParser(IInputStream* input)
        : Input_(input)
    { }

    explicit TLenvalParser(TStringBuf input)
        : StreamHolder_(std::make_unique<TMemoryInput>(input))
        , Input_(StreamHolder_.get())
    { }

    std::optional<TLenvalEntry> Next()
    {
        ui32 rowSize;
        size_t read = Input_->Load(&rowSize, sizeof(rowSize));
        if (read == 0) {
            return std::nullopt;
        } else if (read < sizeof(rowSize)) {
            THROW_ERROR_EXCEPTION("corrupted lenval: can't read row length");
        }
        switch (rowSize) {
            case LenvalTableIndexMarker: {
                ui32 tableIndex;
                read = Input_->Load(&tableIndex, sizeof(tableIndex));
                if (read != sizeof(tableIndex)) {
                    THROW_ERROR_EXCEPTION("corrupted lenval: can't read table index");
                }
                CurrentTableIndex_ = tableIndex;
                return Next();
            }
            case LenvalTabletIndexMarker: {
                ui64 tabletIndex;
                read = Input_->Load(&tabletIndex, sizeof(tabletIndex));
                if (read != sizeof(tabletIndex)) {
                    THROW_ERROR_EXCEPTION("corrupted lenval: can't read tablet index");
                }
                CurrentTabletIndex_ = tabletIndex;
                return Next();
            }
            case LenvalEndOfStream:
                EndOfStream_ = true;
                return std::nullopt;
            case LenvalKeySwitch:
            case LenvalRangeIndexMarker:
            case LenvalRowIndexMarker:
                THROW_ERROR_EXCEPTION("marker is unsupported");
            default: {
                TLenvalEntry result;
                result.RowData.resize(rowSize);
                result.TableIndex = CurrentTableIndex_;
                result.TabletIndex = CurrentTabletIndex_;
                Input_->Load(result.RowData.Detach(), rowSize);

                return result;
            }
        }
    }

    bool IsEndOfStream() const
    {
        return EndOfStream_;
    }

private:
    std::unique_ptr<IInputStream> StreamHolder_;
    IInputStream* Input_;
    ui32 CurrentTableIndex_ = 0;
    ui64 CurrentTabletIndex_ = 0;
    bool EndOfStream_ = false;
};

////////////////////////////////////////////////////////////////////////////////

namespace {

TProtobufFormatConfigPtr ParseAndValidateConfig(const INodePtr& node, std::vector<TTableSchemaPtr> schemas = {})
{
    auto config = ConvertTo<TProtobufFormatConfigPtr>(node);
    if (schemas.empty()) {
        schemas.assign(config->Tables.size(), New<TTableSchema>());
    }
    New<TProtobufParserFormatDescription>()->Init(config, schemas);
    New<TProtobufWriterFormatDescription>()->Init(config, schemas);
    return config;
}

} // namespace

INodePtr BuildEmbeddedConfig(EComplexTypeMode complexTypeMode, EProtoFormatType formatType) {
    if (formatType == EProtoFormatType::FileDescriptor) {
        return CreateFileDescriptorConfig<NYT::TEmbeddingMessage>(complexTypeMode);
    }

    auto config = BuildYsonNodeFluently()
        .BeginAttributes()
            .Item("tables").BeginList()
                .Item().BeginMap()
                    .Item("columns").BeginList()
                        .Item().BeginMap()
                            .Item("name").Value("*")
                            .Item("field_number").Value(2)
                            .Item("proto_type").Value("embedded_message")
                            .Item("fields").BeginList()
                                .Item().BeginMap()
                                    .Item("name").Value("other_columns_field")
                                    .Item("field_number").Value(15)
                                    .Item("proto_type").Value("other_columns")
                                .EndMap()
                                .Item().BeginMap()
                                    .Item("name").Value("embedded_num")
                                    .Item("field_number").Value(10)
                                    .Item("proto_type").Value("uint64")
                                .EndMap()
                                .Item().BeginMap()
                                    .Item("name").Value("embedded_extra_field")
                                    .Item("field_number").Value(11)
                                    .Item("proto_type").Value("string")
                                .EndMap()
                                .Item().BeginMap()
                                    .Item("name").Value("variant")
                                    .Item("proto_type").Value("oneof")
                                    .Item("fields").BeginList()
                                        .Item().BeginMap()
                                            .Item("name").Value("str_variant")
                                            .Item("field_number").Value(101)
                                            .Item("proto_type").Value("string")
                                        .EndMap()
                                        .Item().BeginMap()
                                            .Item("name").Value("uint_variant")
                                            .Item("field_number").Value(102)
                                            .Item("proto_type").Value("uint64")
                                        .EndMap()
                                    .EndList()
                                .EndMap()
                                .Item().BeginMap()
                                    .Item("name").Value("*")
                                    .Item("field_number").Value(1)
                                    .Item("proto_type").Value("embedded_message")
                                    .Item("fields").BeginList()
                                        .Item().BeginMap()
                                            .Item("name").Value("embedded2_num")
                                            .Item("field_number").Value(10)
                                            .Item("proto_type").Value("uint64")
                                        .EndMap()
                                        .Item().BeginMap()
                                            .Item("name").Value("embedded2_struct")
                                            .Item("field_number").Value(17)
                                            .Item("proto_type").Value("structured_message")
                                            .Item("fields").BeginList()
                                                .Item().BeginMap()
                                                    .Item("name").Value("float1")
                                                    .Item("field_number").Value(1)
                                                    .Item("proto_type").Value("float")
                                                .EndMap()
                                                .Item().BeginMap()
                                                    .Item("name").Value("string1")
                                                    .Item("field_number").Value(2)
                                                    .Item("proto_type").Value("string")
                                                .EndMap()
                                            .EndList()
                                        .EndMap()
                                        .Item().BeginMap()
                                            .Item("name").Value("embedded2_repeated")
                                            .Item("field_number").Value(42)
                                            .Item("proto_type").Value("string")
                                            .Item("repeated").Value(true)
                                        .EndMap()
                                    .EndList()
                                .EndMap()
                            .EndList()
                        .EndMap()
                        .Item().BeginMap()
                            .Item("name").Value("num")
                            .Item("field_number").Value(12)
                            .Item("proto_type").Value("uint64")
                        .EndMap()
                        .Item().BeginMap()
                            .Item("name").Value("extra_field")
                            .Item("field_number").Value(13)
                            .Item("proto_type").Value("string")
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
            .Item("complex_type_mode").Value(complexTypeMode)
        .EndAttributes()
        .Value("protobuf");
    return config;
}

TTableSchemaPtr BuildEmbeddedSchema()
{
    auto schema = New<TTableSchema>(std::vector<TColumnSchema>{
        {"num", SimpleLogicalType(ESimpleLogicalValueType::Uint64)},
        {"embedded_num", SimpleLogicalType(ESimpleLogicalValueType::Uint64)},
        {"variant", VariantStructLogicalType({
            {"str_variant", SimpleLogicalType(ESimpleLogicalValueType::String)},
            {"uint_variant", SimpleLogicalType(ESimpleLogicalValueType::Uint64)},
        })},
        {"extra_column", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Uint64))},
        {"embedded2_num", SimpleLogicalType(ESimpleLogicalValueType::Uint64)},
        {"embedded2_struct", StructLogicalType({
            {"float1", SimpleLogicalType(ESimpleLogicalValueType::Float)},
            {"string1", SimpleLogicalType(ESimpleLogicalValueType::String)},
        })},
        {"embedded2_repeated", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
        {"other_complex_field", StructLogicalType({
            {"one", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"two", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"three", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        })},
        {"extra_int", SimpleLogicalType(ESimpleLogicalValueType::Int64)},

    });
    return schema;
}

TEST(TProtobufFormat, TestConfigParsingEmbedded) {
    auto config = BuildEmbeddedConfig(EComplexTypeMode::Positional, EProtoFormatType::Structured);
    auto schema = BuildEmbeddedSchema();

    EXPECT_NO_THROW(
        ParseAndValidateConfig(config->Attributes().ToMap(), {schema}));
}

TEST(TProtobufFormat, TestConfigParsing)
{
    // Empty config.
    EXPECT_THROW_WITH_SUBSTRING(
        ParseAndValidateConfig(ParseYson("{}")),
        "one of \"tables\", \"file_descriptor_set\" and \"file_descriptor_set_text\" must be specified");

    // Broken protobuf.
    EXPECT_THROW_WITH_SUBSTRING(
        ParseAndValidateConfig(ParseYson(R"({file_descriptor_set="dfgxx"; file_indices=[0]; message_indices=[0]})")),
        "Error parsing \"file_descriptor_set\" in protobuf config");

    EXPECT_NO_THROW(ParseAndValidateConfig(
        CreateAllFieldsConfig(EProtoFormatType::Structured)->Attributes().ToMap()));

    EXPECT_NO_THROW(ParseAndValidateConfig(
        CreateAllFieldsConfig(EProtoFormatType::FileDescriptorLegacy)->Attributes().ToMap()));

    EXPECT_NO_THROW(ParseAndValidateConfig(
        CreateAllFieldsConfig(EProtoFormatType::FileDescriptor)->Attributes().ToMap()));

    auto embeddedInsideNonembeddedConfig = BuildYsonNodeFluently()
        .BeginMap()
            .Item("tables").BeginList()
                .Item().BeginMap()
                    .Item("columns").BeginList()
                        .Item().BeginMap()
                            .Item("name").Value("embedded_message1")
                            .Item("field_number").Value(1)
                            .Item("proto_type").Value("embedded_message")
                            .Item("fields").BeginList()
                                .Item().BeginMap()
                                    .Item("name").Value("field1")
                                    .Item("field_number").Value(2)
                                    .Item("proto_type").Value("structured_message")
                                    .Item("fields").BeginList()
                                        .Item().BeginMap()
                                            .Item("name").Value("embedded_message2")
                                            .Item("field_number").Value(3)
                                            .Item("proto_type").Value("embedded_message")
                                            .Item("fields").BeginList()
                                                .Item().BeginMap()
                                                    .Item("name").Value("field2")
                                                    .Item("field_number").Value(4)
                                                    .Item("proto_type").Value("string")
                                                .EndMap()
                                            .EndList()
                                        .EndMap()
                                    .EndList()
                                .EndMap()
                            .EndList()
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
        .EndMap();

    auto schemaForEmbedded = New<TTableSchema>(std::vector{
        TColumnSchema("field1", StructLogicalType({
            {"embedded_message2", StructLogicalType({
                {"field2", SimpleLogicalType(ESimpleLogicalValueType::String)},
            })},
        }))
    });

    EXPECT_THROW_WITH_SUBSTRING(
        ParseAndValidateConfig(embeddedInsideNonembeddedConfig, {schemaForEmbedded}),
        "embedded_message inside of structured_message is not allowed");

    auto repeatedEmbeddedConfig = BuildYsonNodeFluently()
        .BeginMap()
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("*")
                            .Item("field_number").Value(1)
                            .Item("proto_type").Value("embedded_message")
                            .Item("repeated").Value(true)
                            .Item("fields").BeginList()
                                .Item().BeginMap()
                                    .Item("name").Value("field1")
                                    .Item("field_number").Value(1)
                                    .Item("proto_type").Value("uint64")
                                .EndMap()
                            .EndList()
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
        .EndMap();

    EXPECT_THROW_WITH_SUBSTRING(
        ParseAndValidateConfig(repeatedEmbeddedConfig),
        R"(type "embedded_message" can not be repeated)");

    auto multipleOtherColumnsConfig = BuildYsonNodeFluently()
        .BeginMap()
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("Other1")
                            .Item("field_number").Value(1)
                            .Item("proto_type").Value("other_columns")
                        .EndMap()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("Other2")
                            .Item("field_number").Value(2)
                            .Item("proto_type").Value("other_columns")
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
        .EndMap();

    EXPECT_THROW_WITH_SUBSTRING(
        ParseAndValidateConfig(multipleOtherColumnsConfig),
        "Multiple \"other_columns\" in protobuf config are not allowed");

    auto duplicateColumnNamesConfig = BuildYsonNodeFluently()
        .BeginMap()
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("SomeColumn")
                            .Item("field_number").Value(1)
                            .Item("proto_type").Value("int64")
                        .EndMap()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("SomeColumn")
                            .Item("field_number").Value(2)
                            .Item("proto_type").Value("string")
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
        .EndMap();

    EXPECT_THROW_WITH_SUBSTRING(
        ParseAndValidateConfig(duplicateColumnNamesConfig),
        "Multiple fields with same column name \"SomeColumn\" are forbidden in protobuf format");

    auto anyCorrespondsToStruct = BuildYsonNodeFluently()
        .BeginMap()
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("SomeColumn")
                            .Item("field_number").Value(1)
                            .Item("proto_type").Value("any")
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
        .EndMap();

    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("SomeColumn", StructLogicalType({})),
    });

    EXPECT_THROW_WITH_SUBSTRING(
        ParseAndValidateConfig(anyCorrespondsToStruct, {schema}),
        "Table schema and protobuf format config mismatch");

    auto configWithBytes = BuildYsonNodeFluently()
        .BeginMap()
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("SomeColumn")
                            .Item("field_number").Value(1)
                            .Item("proto_type").Value("bytes")
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
        .EndMap();

    auto schemaWithUtf8 = New<TTableSchema>(std::vector{
        TColumnSchema("SomeColumn", SimpleLogicalType(ESimpleLogicalValueType::Utf8)),
    });

    EXPECT_THROW_WITH_SUBSTRING(
        ParseAndValidateConfig(configWithBytes, {schemaWithUtf8}),
        "mismatch: expected logical type to be one of");

    auto configWithPackedNonRepeated = BuildYsonNodeFluently()
        .BeginMap()
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("SomeColumn")
                            .Item("field_number").Value(1)
                            .Item("proto_type").Value("int64")
                            .Item("packed").Value(true)
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
        .EndMap();

    auto schemaWithInt64List = New<TTableSchema>(std::vector<TColumnSchema>{
        {"SomeColumn", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
    });
    EXPECT_THROW_WITH_SUBSTRING(
        ParseAndValidateConfig(configWithPackedNonRepeated, {schemaWithInt64List}),
        "Field \"SomeColumn\" is marked \"packed\" but is not marked \"repeated\"");

    auto configWithPackedRepeatedString = BuildYsonNodeFluently()
        .BeginMap()
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("SomeColumn")
                            .Item("field_number").Value(1)
                            .Item("proto_type").Value("string")
                            .Item("packed").Value(true)
                            .Item("repeated").Value(true)
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
        .EndMap();

    auto schemaWithStringList = New<TTableSchema>(std::vector{
        TColumnSchema("SomeColumn", ListLogicalType(
            SimpleLogicalType(ESimpleLogicalValueType::String)))
    });

    EXPECT_THROW_WITH_SUBSTRING(
        ParseAndValidateConfig(configWithPackedRepeatedString, {schemaWithStringList}),
        "packed protobuf field must have primitive numeric type, got \"string\"");

    auto configWithMissingFieldNumber = BuildYsonNodeFluently()
        .BeginMap()
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("SomeColumn")
                            .Item("proto_type").Value("string")
                            .Item("repeated").Value(true)
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
        .EndMap();

    EXPECT_THROW_WITH_SUBSTRING(
        ParseAndValidateConfig(configWithMissingFieldNumber, {schemaWithStringList}),
        "\"field_number\" is required");
}

TEST(TProtobufFormat, TestParseBigZigZag)
{
    constexpr i32 value = Min<i32>();
    TMessage message;
    message.set_int32_field(value);
    auto config = ConvertTo<TProtobufFormatConfigPtr>(CreateAllFieldsConfig(EProtoFormatType::Structured)->Attributes().ToMap());
    auto rowCollector = ParseRows(message, config);
    EXPECT_EQ(GetInt64(rowCollector.GetRowValue(0, "Int32")), value);
}

TEST(TProtobufFormat, TestParseEnumerationString)
{
    auto config = ConvertTo<TProtobufFormatConfigPtr>(CreateAllFieldsConfig(EProtoFormatType::Structured)->Attributes().ToMap());
    {
        TMessage message;
        message.set_enum_field(EEnum::One);
        auto rowCollector = ParseRows(message, config);
        EXPECT_EQ(GetString(rowCollector.GetRowValue(0, "Enum")), "One");
    }
    {
        TMessage message;
        message.set_enum_field(EEnum::Two);
        auto rowCollector = ParseRows(message, config);
        EXPECT_EQ(GetString(rowCollector.GetRowValue(0, "Enum")), "Two");
    }
    {
        TMessage message;
        message.set_enum_field(EEnum::Three);
        auto rowCollector = ParseRows(message, config);
        EXPECT_EQ(GetString(rowCollector.GetRowValue(0, "Enum")), "Three");
    }
    {
        TMessage message;
        message.set_enum_field(EEnum::MinusFortyTwo);
        auto rowCollector = ParseRows(message, config);
        EXPECT_EQ(GetString(rowCollector.GetRowValue(0, "Enum")), "MinusFortyTwo");
    }
}

TEST(TProtobufFormat, TestParseWrongEnumeration)
{
    auto config = ConvertTo<TProtobufFormatConfigPtr>(CreateAllFieldsConfig(EProtoFormatType::Structured)->Attributes().ToMap());
    TMessage message;
    auto enumTag = TMessage::descriptor()->FindFieldByName("enum_field")->number();
    message.mutable_unknown_fields()->AddVarint(enumTag, 30);
    EXPECT_ANY_THROW(ParseRows(message, config));
}

TEST(TProtobufFormat, TestParseEnumerationInt)
{
    TCollectingValueConsumer rowCollector;

    auto config = BuildYsonNodeFluently()
        .BeginMap()
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("Enum")
                            .Item("field_number").Value(16)
                            .Item("proto_type").Value("enum_int")
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
        .EndMap();

    auto parser = CreateParserForProtobuf(&rowCollector, ConvertTo<TProtobufFormatConfigPtr>(config), 0);

    {
        TMessage message;
        message.set_enum_field(EEnum::One);
        parser->Read(LenvalBytes(message));
    }
    {
        TMessage message;
        message.set_enum_field(EEnum::Two);
        parser->Read(LenvalBytes(message));
    }
    {
        TMessage message;
        message.set_enum_field(EEnum::Three);
        parser->Read(LenvalBytes(message));
    }
    {
        TMessage message;
        message.set_enum_field(EEnum::MinusFortyTwo);
        parser->Read(LenvalBytes(message));
    }
    {
        TMessage message;
        auto enumTag = TMessage::descriptor()->FindFieldByName("enum_field")->number();
        message.mutable_unknown_fields()->AddVarint(enumTag, 100500);
        parser->Read(LenvalBytes(message));
    }

    parser->Finish();

    EXPECT_EQ(GetInt64(rowCollector.GetRowValue(0, "Enum")), 1);
    EXPECT_EQ(GetInt64(rowCollector.GetRowValue(1, "Enum")), 2);
    EXPECT_EQ(GetInt64(rowCollector.GetRowValue(2, "Enum")), 3);
    EXPECT_EQ(GetInt64(rowCollector.GetRowValue(3, "Enum")), -42);
    EXPECT_EQ(GetInt64(rowCollector.GetRowValue(4, "Enum")), 100500);
}

TEST(TProtobufFormat, TestParseRandomGarbage)
{
    // Check that we never crash.

    TFastRng64 rng(42);
    for (int i = 0; i != 1000; ++i) {
        auto bytes = GenerateRandomLenvalString(rng, 8);

        TCollectingValueConsumer rowCollector;
        auto parser = CreateParserForProtobuf(
            &rowCollector,
            ConvertTo<TProtobufFormatConfigPtr>(CreateAllFieldsConfig(EProtoFormatType::Structured)->Attributes().ToMap()),
            0);
        try {
            parser->Read(bytes);
            parser->Finish();
        } catch (...) {
        }
    }
}

TEST(TProtobufFormat, TestParseZeroColumns)
{
    auto config = BuildYsonNodeFluently()
        .BeginMap()
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                    .EndList()
                .EndMap()
            .EndList()
        .EndMap();

    TCollectingValueConsumer rowCollector;
    auto parser = CreateParserForProtobuf(
        &rowCollector,
        ConvertTo<TProtobufFormatConfigPtr>(config),
        0);

    // Empty lenval values.
    parser->Read("\0\0\0\0"sv);
    parser->Read("\0\0\0\0"sv);

    parser->Finish();

    ASSERT_EQ(static_cast<ssize_t>(rowCollector.Size()), 2);
    EXPECT_EQ(static_cast<int>(rowCollector.GetRow(0).GetCount()), 0);
    EXPECT_EQ(static_cast<int>(rowCollector.GetRow(1).GetCount()), 0);
}

TEST(TProtobufFormat, TestWriteEnumerationString)
{
    auto config = CreateAllFieldsConfig(EProtoFormatType::Structured);

    auto nameTable = New<TNameTable>();

    TString result;
    TStringOutput resultStream(result);
    auto writer = CreateWriterForProtobuf(
        config->Attributes(),
        {New<TTableSchema>()},
        nameTable,
        CreateAsyncAdapter(&resultStream),
        true,
        New<TControlAttributesConfig>(),
        0);

    EXPECT_EQ(true, writer->Write({
        MakeRow(nameTable, {
            {"Enum", "MinusFortyTwo"}
        }).Get()
    }));
    EXPECT_EQ(true, writer->Write({
        MakeRow(nameTable, {
            {"Enum", "Three"},
        }).Get()
    }));

    writer->Close()
        .Get()
        .ThrowOnError();

    TStringInput si(result);
    TLenvalParser parser(&si);
    {
        auto row = parser.Next();
        ASSERT_TRUE(row);
        NYT::TMessage message;
        ASSERT_TRUE(message.ParseFromString(row->RowData));
        ASSERT_EQ(message.enum_field(), NYT::EEnum::MinusFortyTwo);
    }
    {
        auto row = parser.Next();
        ASSERT_TRUE(row);
        NYT::TMessage message;
        ASSERT_TRUE(message.ParseFromString(row->RowData));
        ASSERT_EQ(message.enum_field(), NYT::EEnum::Three);
    }
    {
        auto row = parser.Next();
        ASSERT_FALSE(row);
    }
}

TEST(TProtobufFormat, TestWriteEnumerationInt)
{
    auto config = BuildYsonNodeFluently()
        .BeginAttributes()
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("Enum")
                            .Item("field_number").Value(16)
                            .Item("proto_type").Value("enum_int")
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
        .EndAttributes()
        .Value("protobuf");

    auto nameTable = New<TNameTable>();

    auto writeAndParseRow = [&] (TUnversionedRow row, TMessage* message) {
        TString result;
        TStringOutput resultStream(result);
        auto writer = CreateWriterForProtobuf(
            config->Attributes(),
            {New<TTableSchema>()},
            nameTable,
            CreateAsyncAdapter(&resultStream),
            true,
            New<TControlAttributesConfig>(),
            0);
        Y_UNUSED(writer->Write({row}));
        writer->Close()
            .Get()
            .ThrowOnError();

        TStringInput si(result);
        TLenvalParser parser(&si);
        auto protoRow = parser.Next();
        ASSERT_TRUE(protoRow);

        ASSERT_TRUE(message->ParseFromString(protoRow->RowData));

        auto nextProtoRow = parser.Next();
        ASSERT_FALSE(nextProtoRow);
    };

    {
        TMessage message;
        writeAndParseRow(
            MakeRow(nameTable, {
                {"Enum", -42},
            }).Get(),
            &message);
        ASSERT_EQ(message.enum_field(), EEnum::MinusFortyTwo);
    }
    {
        TMessage message;
        writeAndParseRow(
            MakeRow(nameTable, {
                {"Enum", static_cast<ui64>(std::numeric_limits<i32>::max())},
            }).Get(),
            &message);
        ASSERT_EQ(message.enum_field(), EEnum::MaxInt32);
    }
    {
        TMessage message;
        writeAndParseRow(
            MakeRow(nameTable, {
                {"Enum", std::numeric_limits<i32>::max()},
            }).Get(),
            &message);
        ASSERT_EQ(message.enum_field(), EEnum::MaxInt32);
    }
    {
        TMessage message;
        writeAndParseRow(
            MakeRow(nameTable, {
                {"Enum", std::numeric_limits<i32>::min()},
            }).Get(),
            &message);
        ASSERT_EQ(message.enum_field(), EEnum::MinInt32);
    }

    TMessage message;
    ASSERT_THROW(
        writeAndParseRow(
            MakeRow(nameTable, {
                {"Enum", static_cast<i64>(std::numeric_limits<i32>::max()) + 1},
            }).Get(),
            &message),
        TErrorException);

    ASSERT_THROW(
        writeAndParseRow(
            MakeRow(nameTable, {
                {"Enum", static_cast<i64>(std::numeric_limits<i32>::min()) - 1},
            }).Get(),
            &message),
        TErrorException);

    ASSERT_THROW(
        writeAndParseRow(
            MakeRow(nameTable, {
                {"Enum", static_cast<ui64>(std::numeric_limits<i32>::max()) + 1},
            }).Get(),
            &message),
        TErrorException);
}


TEST(TProtobufFormat, TestWriteZeroColumns)
{
    auto config = BuildYsonNodeFluently()
        .BeginAttributes()
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                    .EndList()
                .EndMap()
            .EndList()
        .EndAttributes()
        .Value("protobuf");

    auto nameTable = New<TNameTable>();

    TString result;
    TStringOutput resultStream(result);
    auto writer = CreateWriterForProtobuf(
        config->Attributes(),
        {New<TTableSchema>()},
        nameTable,
        CreateAsyncAdapter(&resultStream),
        true,
        New<TControlAttributesConfig>(),
        0);

    EXPECT_EQ(true, writer->Write({
        MakeRow(nameTable, {
            {"Int64", -1},
            {"String", "this_is_string"},
        }).Get()
    }));
    EXPECT_EQ(true, writer->Write({MakeRow(nameTable, { }).Get()}));

    writer->Close()
        .Get()
        .ThrowOnError();

    ASSERT_EQ(result, "\0\0\0\0\0\0\0\0"sv);
}

TEST(TProtobufFormat, TestTabletIndex)
{
    auto config = ConvertTo<TProtobufFormatConfigPtr>(BuildYsonNodeFluently()
        .BeginMap()
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("int64_field")
                            .Item("field_number").Value(3)
                            .Item("proto_type").Value("int64")
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
        .EndMap());

    auto nameTable = New<TNameTable>();

    TString result;
    TStringOutput resultStream(result);
    auto controlAttributesConfig = New<TControlAttributesConfig>();
    controlAttributesConfig->EnableTabletIndex = true;

    auto writer = CreateWriterForProtobuf(
        config,
        {New<TTableSchema>()},
        nameTable,
        CreateAsyncAdapter(&resultStream),
        true,
        controlAttributesConfig,
        0);

    EXPECT_EQ(true, writer->Write({
        MakeRow(nameTable, {
            {TString(TabletIndexColumnName), 1LL << 50},
            {"int64_field", -2345},
        }).Get(),
        MakeRow(nameTable, {
            {TString(TabletIndexColumnName), 12},
            {"int64_field", 2345},
        }).Get(),
    }));

    writer->Close()
        .Get()
        .ThrowOnError();

    TStringInput si(result);
    TLenvalParser parser(&si);
    {
        auto row = parser.Next();
        ASSERT_TRUE(row);
        ASSERT_EQ(row->TabletIndex, 1ULL << 50);
        NYT::TMessage message;
        ASSERT_TRUE(message.ParseFromString(row->RowData));
        ASSERT_EQ(message.int64_field(), -2345);
    }
    {
        auto row = parser.Next();
        ASSERT_TRUE(row);
        ASSERT_EQ(static_cast<int>(row->TabletIndex), 12);
        NYT::TMessage message;
        ASSERT_TRUE(message.ParseFromString(row->RowData));
        ASSERT_EQ(message.int64_field(), 2345);
    }
    {
        auto row = parser.Next();
        ASSERT_FALSE(row);
    }
}

TEST(TProtobufFormat, TestContext)
{
    auto config = BuildYsonNodeFluently()
        .BeginMap()
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                    .EndList()
                .EndMap()
            .EndList()
        .EndMap();

    TCollectingValueConsumer rowCollector;
    auto parser = CreateParserForProtobuf(
        &rowCollector,
        ConvertTo<TProtobufFormatConfigPtr>(config),
        0);

    TString context;
    try {
        TMessage message;
        message.set_string_field("PYSHCH-PYSHCH");
        parser->Read(LenvalBytes(message));
        parser->Finish();
        GTEST_FATAL_FAILURE_("expected to throw");
    } catch (const NYT::TErrorException& e) {
        context = *e.Error().Attributes().Find<TString>("context");
    }
    ASSERT_NE(context.find("PYSHCH-PYSHCH"), TString::npos);
}

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr CreateSchemaWithStructuredMessage()
{
    auto keyValueStruct = StructLogicalType({
        {"key", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
        {"value", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
    });

    return New<TTableSchema>(std::vector<TColumnSchema>{
        {"first", StructLogicalType({
            {"field_missing_from_proto1", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int32))},
            {"enum_field", SimpleLogicalType(ESimpleLogicalValueType::String)},
            {"int64_field", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"repeated_int64_field", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"another_repeated_int64_field", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"message_field", keyValueStruct},
            {"repeated_message_field", ListLogicalType(keyValueStruct)},
            {"any_int64_field", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"any_map_field", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any))},
            {"optional_int64_field", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"repeated_optional_any_field", ListLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any)))},
            {"packed_repeated_enum_field", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
            {"optional_repeated_bool_field", OptionalLogicalType(ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Boolean)))},
            {"oneof_field", VariantStructLogicalType({
                {"oneof_string_field_1", SimpleLogicalType(ESimpleLogicalValueType::String)},
                {"oneof_string_field", SimpleLogicalType(ESimpleLogicalValueType::String)},
                {"oneof_message_field", keyValueStruct},
            })},
            {"optional_oneof_field", OptionalLogicalType(VariantStructLogicalType({
                {"oneof_string_field_1", SimpleLogicalType(ESimpleLogicalValueType::String)},
                {"oneof_string_field", SimpleLogicalType(ESimpleLogicalValueType::String)},
                {"oneof_message_field", keyValueStruct},
            }))},
            {"map_field", DictLogicalType(
                SimpleLogicalType(ESimpleLogicalValueType::Int64),
                OptionalLogicalType(keyValueStruct))
            },
            {"field_missing_from_proto2", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int32))},
        })},
        {"repeated_int64_field", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        {"another_repeated_int64_field", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        {"repeated_message_field", ListLogicalType(keyValueStruct)},
        {"second", StructLogicalType({
            {"one", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"two", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"three", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        })},
        {"any_field", SimpleLogicalType(ESimpleLogicalValueType::Any)},

        {"int64_field", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        {"uint64_field", SimpleLogicalType(ESimpleLogicalValueType::Uint64)},
        {"int32_field", SimpleLogicalType(ESimpleLogicalValueType::Int32)},
        {"uint32_field", SimpleLogicalType(ESimpleLogicalValueType::Uint32)},

        {"enum_int_field", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        {"enum_string_string_field", SimpleLogicalType(ESimpleLogicalValueType::String)},
        {"enum_string_int64_field", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},

        {"repeated_optional_any_field", ListLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any)))},

        {"other_complex_field", StructLogicalType({
            {"one", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"two", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"three", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        })},

        {"utf8_field", SimpleLogicalType(ESimpleLogicalValueType::Utf8)},

        {"packed_repeated_int64_field", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},

        {"optional_repeated_int64_field", OptionalLogicalType(ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)))},

        {"oneof_field", VariantStructLogicalType({
            {"oneof_string_field_1", SimpleLogicalType(ESimpleLogicalValueType::String)},
            {"oneof_string_field", SimpleLogicalType(ESimpleLogicalValueType::String)},
            {"oneof_message_field", keyValueStruct},
        })},

        {"optional_oneof_field", OptionalLogicalType(VariantStructLogicalType({
            {"oneof_string_field_1", SimpleLogicalType(ESimpleLogicalValueType::String)},
            {"oneof_string_field", SimpleLogicalType(ESimpleLogicalValueType::String)},
            {"oneof_message_field", keyValueStruct},
        }))},

        {"map_field", DictLogicalType(
            SimpleLogicalType(ESimpleLogicalValueType::Int64),
            OptionalLogicalType(keyValueStruct))
        },
    });
}

INodePtr CreateConfigWithStructuredMessage(EComplexTypeMode complexTypeMode, EProtoFormatType formatType)
{
    if (formatType == EProtoFormatType::FileDescriptor) {
        return CreateFileDescriptorConfig<TMessageWithStructuredEmbedded>(complexTypeMode);
    }
    YT_VERIFY(formatType == EProtoFormatType::Structured);

    auto buildOneofConfig = [] (TString prefix, int fieldNumberOffset) {
        return BuildYsonNodeFluently()
            .BeginMap()
                .Item("name").Value(prefix + "oneof_field")
                .Item("proto_type").Value("oneof")
                .Item("fields").BeginList()
                    .Item().BeginMap()
                        .Item("name").Value(prefix + "oneof_string_field_1")
                        .Item("field_number").Value(101 + fieldNumberOffset)
                        .Item("proto_type").Value("string")
                    .EndMap()
                    .Item().BeginMap()
                        .Item("name").Value(prefix + "oneof_string_field")
                        .Item("field_number").Value(102 + fieldNumberOffset)
                        .Item("proto_type").Value("string")
                    .EndMap()
                    .Item().BeginMap()
                        .Item("name").Value(prefix + "oneof_message_field")
                        .Item("field_number").Value(1000 + fieldNumberOffset)
                        .Item("proto_type").Value("structured_message")
                        .Item("fields").BeginList()
                            .Item().BeginMap()
                                .Item("name").Value("key")
                                .Item("field_number").Value(1)
                                .Item("proto_type").Value("string")
                            .EndMap()
                            .Item().BeginMap()
                                .Item("name").Value("value")
                                .Item("field_number").Value(2)
                                .Item("proto_type").Value("string")
                            .EndMap()
                        .EndList()
                    .EndMap()
                .EndList()
            .EndMap();
    };
    auto oneofConfig = buildOneofConfig("", 0);
    auto optionalOneofConfig = buildOneofConfig("optional_", 1000);

    auto keyValueFields = BuildYsonStringFluently()
        .BeginList()
            .Item().BeginMap()
                .Item("name").Value("key")
                .Item("field_number").Value(1)
                .Item("proto_type").Value("string")
            .EndMap()
            .Item().BeginMap()
                .Item("name").Value("value")
                .Item("field_number").Value(2)
                .Item("proto_type").Value("string")
            .EndMap()
        .EndList();

    return BuildYsonNodeFluently()
        .BeginAttributes()
            .Item("enumerations").Value(EnumerationsConfig)
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("first")
                            .Item("field_number").Value(1)
                            .Item("proto_type").Value("structured_message")
                            .Item("fields")
                            .BeginList()
                                .Item()
                                .BeginMap()
                                    .Item("name").Value("int64_field")
                                    .Item("field_number").Value(2)
                                    .Item("proto_type").Value("int64")
                                .EndMap()
                                .Item()
                                .BeginMap()
                                    .Item("name").Value("enum_field")
                                    .Item("field_number").Value(1)
                                    .Item("proto_type").Value("enum_string")
                                    .Item("enumeration_name").Value("EEnum")
                                .EndMap()
                                .Item()
                                .BeginMap()
                                    .Item("name").Value("packed_repeated_enum_field")
                                    .Item("field_number").Value(11)
                                    .Item("proto_type").Value("enum_string")
                                    .Item("enumeration_name").Value("EEnum")
                                    .Item("repeated").Value(true)
                                    .Item("packed").Value(true)
                                .EndMap()
                                .Item().BeginMap()
                                    .Item("name").Value("message_field")
                                    .Item("field_number").Value(4)
                                    .Item("proto_type").Value("structured_message")
                                    .Item("fields").Value(keyValueFields)
                                .EndMap()
                                .Item().BeginMap()
                                    .Item("name").Value("repeated_int64_field")
                                    .Item("field_number").Value(3)
                                    .Item("proto_type").Value("int64")
                                    .Item("repeated").Value(true)
                                .EndMap()
                                .Item().BeginMap()
                                    .Item("name").Value("another_repeated_int64_field")
                                    .Item("field_number").Value(9)
                                    .Item("proto_type").Value("int64")
                                    .Item("repeated").Value(true)
                                .EndMap()
                                .Item().BeginMap()
                                    .Item("name").Value("repeated_message_field")
                                    .Item("field_number").Value(5)
                                    .Item("proto_type").Value("structured_message")
                                    .Item("repeated").Value(true)
                                    .Item("fields").Value(keyValueFields)
                                .EndMap()
                                .Item()
                                .BeginMap()
                                    .Item("name").Value("any_int64_field")
                                    .Item("field_number").Value(6)
                                    .Item("proto_type").Value("any")
                                .EndMap()
                                .Item()
                                .BeginMap()
                                    .Item("name").Value("any_map_field")
                                    .Item("field_number").Value(7)
                                    .Item("proto_type").Value("any")
                                .EndMap()
                                .Item()
                                .BeginMap()
                                    .Item("name").Value("optional_int64_field")
                                    .Item("field_number").Value(8)
                                    .Item("proto_type").Value("int64")
                                .EndMap()
                                .Item()
                                .BeginMap()
                                    .Item("name").Value("repeated_optional_any_field")
                                    .Item("field_number").Value(10)
                                    .Item("proto_type").Value("any")
                                    .Item("repeated").Value(true)
                                .EndMap()
                                .Item()
                                .BeginMap()
                                    .Item("name").Value("optional_repeated_bool_field")
                                    .Item("field_number").Value(12)
                                    .Item("proto_type").Value("bool")
                                    .Item("repeated").Value(true)
                                .EndMap()
                                .Item().Value(oneofConfig)
                                .Item().Value(optionalOneofConfig)
                                .Item()
                                .BeginMap()
                                    .Item("name").Value("map_field")
                                    .Item("field_number").Value(13)
                                    .Item("proto_type").Value("structured_message")
                                    .Item("repeated").Value(true)
                                    .Item("fields")
                                    .BeginList()
                                        .Item()
                                        .BeginMap()
                                            .Item("name").Value("key")
                                            .Item("field_number").Value(1)
                                            .Item("proto_type").Value("int64")
                                        .EndMap()
                                        .Item()
                                        .BeginMap()
                                            .Item("name").Value("value")
                                            .Item("field_number").Value(2)
                                            .Item("proto_type").Value("structured_message")
                                            .Item("fields").Value(keyValueFields)
                                        .EndMap()
                                    .EndList()
                                .EndMap()
                            .EndList()
                        .EndMap()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("second")
                            .Item("field_number").Value(2)
                            .Item("proto_type").Value("structured_message")
                            .Item("fields")
                            .BeginList()
                                .Item().BeginMap()
                                    .Item("name").Value("one")
                                    .Item("field_number").Value(2)
                                    .Item("proto_type").Value("int64")
                                .EndMap()
                                .Item().BeginMap()
                                    .Item("name").Value("two")
                                    .Item("field_number").Value(500000000)
                                    .Item("proto_type").Value("int64")
                                .EndMap()
                                .Item().BeginMap()
                                    .Item("name").Value("three")
                                    .Item("field_number").Value(100500)
                                    .Item("proto_type").Value("int64")
                                .EndMap()
                            .EndList()
                        .EndMap()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("repeated_message_field")
                            .Item("field_number").Value(3)
                            .Item("proto_type").Value("structured_message")
                            .Item("repeated").Value(true)
                            .Item("fields")
                            .BeginList()
                                .Item().BeginMap()
                                    .Item("name").Value("key")
                                    .Item("field_number").Value(1)
                                    .Item("proto_type").Value("string")
                                .EndMap()
                                .Item().BeginMap()
                                    .Item("name").Value("value")
                                    .Item("field_number").Value(2)
                                    .Item("proto_type").Value("string")
                                .EndMap()
                            .EndList()
                        .EndMap()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("repeated_int64_field")
                            .Item("field_number").Value(4)
                            .Item("proto_type").Value("int64")
                            .Item("repeated").Value(true)
                        .EndMap()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("another_repeated_int64_field")
                            .Item("field_number").Value(13)
                            .Item("proto_type").Value("int64")
                            .Item("repeated").Value(true)
                        .EndMap()
                        .Item()
                        .BeginMap()
                            // In schema it is of type "any".
                            .Item("name").Value("any_field")
                            .Item("field_number").Value(5)
                            .Item("proto_type").Value("int64")
                        .EndMap()
                        // The next fields are for type casting testing
                        .Item()
                        .BeginMap()
                            // In schema it is of type "int64".
                            .Item("name").Value("int64_field")
                            .Item("field_number").Value(6)
                            .Item("proto_type").Value("int32")
                        .EndMap()
                        .Item()
                        .BeginMap()
                            // In schema it is of type "uint64".
                            .Item("name").Value("uint64_field")
                            .Item("field_number").Value(7)
                            .Item("proto_type").Value("uint32")
                        .EndMap()
                        .Item()
                        .BeginMap()
                            // In schema it is of type "int32".
                            .Item("name").Value("int32_field")
                            .Item("field_number").Value(8)
                            .Item("proto_type").Value("int64")
                        .EndMap()
                        .Item()
                        .BeginMap()
                            // In schema it is of type "uint32".
                            .Item("name").Value("uint32_field")
                            .Item("field_number").Value(9)
                            .Item("proto_type").Value("uint64")
                        .EndMap()

                        // Enums.
                        .Item()
                        .BeginMap()
                            .Item("name").Value("enum_int_field")
                            .Item("field_number").Value(10)
                            .Item("proto_type").Value("enum_int")
                            .Item("enumeration_name").Value("EEnum")
                        .EndMap()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("enum_string_string_field")
                            .Item("field_number").Value(11)
                            .Item("proto_type").Value("enum_string")
                            .Item("enumeration_name").Value("EEnum")
                        .EndMap()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("enum_string_int64_field")
                            .Item("field_number").Value(12)
                            .Item("proto_type").Value("enum_string")
                            .Item("enumeration_name").Value("EEnum")
                        .EndMap()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("utf8_field")
                            .Item("field_number").Value(16)
                            .Item("proto_type").Value("string")
                        .EndMap()

                        // list<optional<any>>.
                        .Item()
                        .BeginMap()
                            .Item("name").Value("repeated_optional_any_field")
                            .Item("field_number").Value(14)
                            .Item("proto_type").Value("any")
                            .Item("repeated").Value(true)
                        .EndMap()

                        // Other columns.
                        .Item()
                        .BeginMap()
                            .Item("name").Value("other_columns_field")
                            .Item("field_number").Value(15)
                            .Item("proto_type").Value("other_columns")
                        .EndMap()

                        .Item()
                        .BeginMap()
                            .Item("name").Value("packed_repeated_int64_field")
                            .Item("field_number").Value(17)
                            .Item("proto_type").Value("int64")
                            .Item("repeated").Value(true)
                            .Item("packed").Value(true)
                        .EndMap()

                        .Item()
                        .BeginMap()
                            .Item("name").Value("optional_repeated_int64_field")
                            .Item("field_number").Value(18)
                            .Item("proto_type").Value("int64")
                            .Item("repeated").Value(true)
                        .EndMap()

                        .Item().Value(oneofConfig)
                        .Item().Value(optionalOneofConfig)

                        .Item()
                        .BeginMap()
                            .Item("name").Value("map_field")
                            .Item("field_number").Value(19)
                            .Item("proto_type").Value("structured_message")
                            .Item("repeated").Value(true)
                            .Item("fields")
                            .BeginList()
                                .Item()
                                .BeginMap()
                                    .Item("name").Value("key")
                                    .Item("field_number").Value(1)
                                    .Item("proto_type").Value("int64")
                                .EndMap()
                                .Item()
                                .BeginMap()
                                    .Item("name").Value("value")
                                    .Item("field_number").Value(2)
                                    .Item("proto_type").Value("structured_message")
                                    .Item("fields").Value(keyValueFields)
                                .EndMap()
                            .EndList()
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
            .Item("complex_type_mode").Value(complexTypeMode)
        .EndAttributes()
        .Value("protobuf");
}

using TProtobufFormatStructuredMessageParameter = std::tuple<EComplexTypeMode, int, EProtoFormatType>;

class TProtobufFormatStructuredMessage
    : public ::testing::TestWithParam<TProtobufFormatStructuredMessageParameter>
{ };

INSTANTIATE_TEST_SUITE_P(
    FileDescriptor,
    TProtobufFormatStructuredMessage,
    ::testing::Values(TProtobufFormatStructuredMessageParameter{
        EComplexTypeMode::Positional,
        1,
        EProtoFormatType::FileDescriptor}));

INSTANTIATE_TEST_SUITE_P(
    Positional,
    TProtobufFormatStructuredMessage,
    ::testing::Values(TProtobufFormatStructuredMessageParameter{
        EComplexTypeMode::Positional,
        1,
        EProtoFormatType::Structured}));

INSTANTIATE_TEST_SUITE_P(
    Named,
    TProtobufFormatStructuredMessage,
    ::testing::Values(TProtobufFormatStructuredMessageParameter{
        EComplexTypeMode::Named,
        1,
        EProtoFormatType::Structured}));

INSTANTIATE_TEST_SUITE_P(
    ManyRows,
    TProtobufFormatStructuredMessage,
    ::testing::Values(TProtobufFormatStructuredMessageParameter{
        EComplexTypeMode::Named,
        30000,
        EProtoFormatType::Structured}));

TEST_P(TProtobufFormatStructuredMessage, EmbeddedWrite)
{
    auto [complexTypeMode, rowCount, protoFormatType] = GetParam();

    auto nameTable = New<TNameTable>();
    auto numId = nameTable->RegisterName("num");
    auto embeddedNumId = nameTable->RegisterName("embedded_num");
    auto variantId = nameTable->RegisterName("variant");
    auto embedded2NumId = nameTable->RegisterName("embedded2_num");
    auto embedded2StructId = nameTable->RegisterName("embedded2_struct");
    auto embedded2RepeatedId = nameTable->RegisterName("embedded2_repeated");
    auto extraIntId = nameTable->RegisterName("extra_int");
    auto otherComplexFieldId = nameTable->RegisterName("other_complex_field");

    //message T2 {
    //    optional ui64 embedded2_num;
    //};
    //message T1 {
    //  required T2 t2 [embedded];
    //  optional ui64 embedded_num;
    //};
    //
    //message T {
    //   required T1 t1 [embedded];
    //   optional ui64 num;
    //};

    auto schema = BuildEmbeddedSchema();
    auto config = BuildEmbeddedConfig(complexTypeMode, protoFormatType);

    TString result;
    TStringOutput resultStream(result);
    auto writer = CreateWriterForProtobuf(
        ConvertTo<TProtobufFormatConfigPtr>(config->Attributes()),
        {schema},
        nameTable,
        CreateAsyncAdapter(&resultStream),
        true,
        New<TControlAttributesConfig>(),
        0);

    TUnversionedRowBuilder builder;
    builder.AddValue(MakeUnversionedUint64Value(789, numId));
    builder.AddValue(MakeUnversionedUint64Value(123, embeddedNumId));
    builder.AddValue(MakeUnversionedUint64Value(456, embedded2NumId));
    builder.AddValue(MakeUnversionedCompositeValue("[1; 555u]", variantId));
    auto embeddedYson = BuildYsonStringFluently()
        .BeginList()
            // float1
            .Item().Value(1.5f)
            // string1
            .Item().Value("abc")
        .EndList();
    auto embeddedYsonStr = embeddedYson.ToString();
    builder.AddValue(MakeUnversionedCompositeValue(embeddedYsonStr, embedded2StructId));
    auto repeatedYsonStr = BuildYsonStringFluently()
        .BeginList()
            .Item().Value("a")
            .Item().Value("b")
        .EndList()
        .ToString();
    builder.AddValue(MakeUnversionedCompositeValue(repeatedYsonStr, embedded2RepeatedId));
    builder.AddValue(MakeUnversionedInt64Value(111, extraIntId));
    auto otherComplexFieldYson = BuildYsonStringFluently()
        .BeginList()
            .Item().Value(22)
            .Item().Value(23)
            .Item().Value(24)
        .EndList();
    auto otherComplexFieldYsonStr = otherComplexFieldYson.ToString();
    builder.AddValue(MakeUnversionedCompositeValue(otherComplexFieldYsonStr, otherComplexFieldId));


    auto rows = std::vector<TUnversionedRow>(rowCount, builder.GetRow());
    EXPECT_EQ(true, writer->Write(rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    TStringInput input(result);
    TLenvalParser lenvalParser(&input);

    for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
        auto entry = lenvalParser.Next();
        ASSERT_TRUE(entry);

        NYT::TEmbeddingMessage message;
        ASSERT_TRUE(message.ParseFromString(entry->RowData));

        EXPECT_EQ(message.num(), 789UL);
        EXPECT_EQ(message.t1().embedded_num(), 123UL);
        EXPECT_EQ(message.t1().t2().embedded2_num(), 456UL);

        EXPECT_FALSE(message.t1().has_str_variant());
        EXPECT_TRUE(message.t1().has_uint_variant());
        EXPECT_EQ(message.t1().uint_variant(), 555UL);

        EXPECT_EQ(message.t1().t2().embedded2_struct().float1(), 1.5f);
        EXPECT_EQ(message.t1().t2().embedded2_struct().string1(), "abc");

        ASSERT_EQ(message.t1().t2().embedded2_repeated_size(), 2);
        EXPECT_EQ(message.t1().t2().embedded2_repeated(0), "a");
        EXPECT_EQ(message.t1().t2().embedded2_repeated(1), "b");

        {
            auto otherColumns = ConvertToNode(TYsonString(message.other_columns_field()))->AsMap();
            auto mode = complexTypeMode;
            auto expected = ([&] {
                switch (mode) {
                    case EComplexTypeMode::Named:
                        return BuildYsonNodeFluently()
                            .BeginMap()
                                .Item("one").Value(22)
                                .Item("two").Value(23)
                                .Item("three").Value(24)
                            .EndMap();
                    case EComplexTypeMode::Positional:
                        return ConvertToNode(otherComplexFieldYson);
                }
                YT_ABORT();
            })();

            EXPECT_NODES_EQUAL(expected, otherColumns->GetChildOrThrow("other_complex_field"));
            EXPECT_EQ(ConvertTo<i64>(otherColumns->GetChildOrThrow("extra_int")), 111);
        }

        ASSERT_FALSE(message.has_extra_field());
        ASSERT_FALSE(message.t1().has_embedded_extra_field());
    }

    ASSERT_FALSE(lenvalParser.Next());
}

TEST_P(TProtobufFormatStructuredMessage, Write)
{
    auto [complexTypeMode, rowCount, protoFormatType] = GetParam();

    auto nameTable = New<TNameTable>();
    auto firstId = nameTable->RegisterName("first");
    auto secondId = nameTable->RegisterName("second");
    auto repeatedMessageId = nameTable->RegisterName("repeated_message_field");
    auto repeatedInt64Id = nameTable->RegisterName("repeated_int64_field");
    auto anotherRepeatedInt64Id = nameTable->RegisterName("another_repeated_int64_field");
    auto anyFieldId = nameTable->RegisterName("any_field");
    auto int64FieldId = nameTable->RegisterName("int64_field");
    auto uint64FieldId = nameTable->RegisterName("uint64_field");
    auto int32FieldId = nameTable->RegisterName("int32_field");
    auto uint32FieldId = nameTable->RegisterName("uint32_field");
    auto enumIntFieldId = nameTable->RegisterName("enum_int_field");
    auto enumStringStringFieldId = nameTable->RegisterName("enum_string_string_field");
    auto enumStringInt64FieldId = nameTable->RegisterName("enum_string_int64_field");
    auto utf8FieldId = nameTable->RegisterName("utf8_field");
    auto repeatedOptionalAnyFieldId = nameTable->RegisterName("repeated_optional_any_field");
    auto otherComplexFieldId = nameTable->RegisterName("other_complex_field");
    auto packedRepeatedInt64FieldId = nameTable->RegisterName("packed_repeated_int64_field");
    auto optionalRepeatedInt64FieldId = nameTable->RegisterName("optional_repeated_int64_field");
    auto oneofFieldId = nameTable->RegisterName("oneof_field");
    auto optionalOneofFieldId = nameTable->RegisterName("optional_oneof_field");
    auto mapFieldId = nameTable->RegisterName("map_field");

    auto schema = CreateSchemaWithStructuredMessage();
    auto config = CreateConfigWithStructuredMessage(complexTypeMode, protoFormatType);

    TString result;
    TStringOutput resultStream(result);
    auto writer = CreateWriterForProtobuf(
        ConvertTo<TProtobufFormatConfigPtr>(config->Attributes()),
        {schema},
        nameTable,
        CreateAsyncAdapter(&resultStream),
        true,
        New<TControlAttributesConfig>(),
        0);

    auto firstYsonStr = BuildYsonStringFluently()
        .BeginList()
            // field_missing_from_proto1
            .Item().Value(11111)
            // enum_field
            .Item().Value("Two")
            // int64_field
            .Item().Value(44)
            // repeated_int64_field
            .Item()
                .BeginList()
                    .Item().Value(55)
                    .Item().Value(56)
                    .Item().Value(57)
                .EndList()
            // another_repeated_int64_field
            .Item()
                .BeginList()
                .EndList()
            // message_field
            .Item()
                .BeginList()
                    .Item().Value("key")
                    .Item().Value("value")
                .EndList()
            // repeated_message_field
            .Item()
                .BeginList()
                    .Item()
                    .BeginList()
                        .Item().Value("key1")
                        .Item().Value("value1")
                    .EndList()
                    .Item()
                    .BeginList()
                        .Item().Value("key2")
                        .Item().Value("value2")
                    .EndList()
                .EndList()
            // any_int64_field
            .Item().Value(45)
            // any_map_field
            .Item()
                .BeginMap()
                    .Item("key").Value("value")
                .EndMap()
            // optional_int64_field
            .Item().Entity()
            // repeated_optional_any_field
            .Item()
                .BeginList()
                    .Item().Value(2)
                    .Item().Entity()
                    .Item().Value("foo")
                .EndList()
            // packed_repeated_enum_field
            .Item()
                .BeginList()
                    .Item().Value("MinusFortyTwo")
                    .Item().Value("Two")
                .EndList()
            // optional_repeated_bool_field
            .Item()
                .BeginList()
                    .Item().Value(false)
                    .Item().Value(true)
                    .Item().Value(false)
                .EndList()
            // oneof_field
            .Item()
                .BeginList()
                    // message_field
                    .Item().Value(2)
                    .Item().BeginList()
                        .Item().Value("foo")
                        .Item().Entity()
                    .EndList()
                .EndList()
            // optional_oneof_field
            .Item()
                .Entity()
            // map_field
            .Item()
                .BeginList()
                    .Item().BeginList()
                        .Item().Value(13)
                        .Item().BeginList()
                            .Item().Value("bac")
                            .Item().Value("cab")
                        .EndList()
                    .EndList()
                    .Item().BeginList()
                        .Item().Value(15)
                        .Item().BeginList()
                            .Item().Value("ya")
                            .Item().Value("make")
                        .EndList()
                    .EndList()
                .EndList()
        .EndList()
        .ToString();

    auto secondYsonStr = BuildYsonStringFluently()
        .BeginList()
            .Item().Value(101)
            .Item().Value(102)
            .Item().Value(103)
        .EndList()
        .ToString();

    auto repeatedMessageYsonStr = BuildYsonStringFluently()
        .BeginList()
            .Item()
            .BeginList()
                .Item().Value("key11")
                .Item().Value("value11")
            .EndList()
            .Item()
            .BeginList()
                .Item().Value("key21")
                .Item().Value("value21")
            .EndList()
        .EndList()
        .ToString();

    auto repeatedInt64Yson = BuildYsonStringFluently()
        .BeginList()
            .Item().Value(31)
            .Item().Value(32)
            .Item().Value(33)
        .EndList();
    auto repeatedInt64YsonStr = repeatedInt64Yson.ToString();

    auto anotherRepeatedInt64YsonStr = BuildYsonStringFluently()
        .BeginList()
        .EndList()
        .ToString();

    auto repeatedOptionalAnyYson = BuildYsonStringFluently()
        .BeginList()
            .Item().Value(1)
            .Item().Value("abc")
            .Item().Entity()
            .Item().Value(true)
        .EndList();
    auto repeatedOptionalAnyYsonStr = repeatedOptionalAnyYson.ToString();

    auto otherComplexFieldYson = BuildYsonStringFluently()
        .BeginList()
            .Item().Value(22)
            .Item().Value(23)
            .Item().Value(24)
        .EndList();
    auto otherComplexFieldYsonStr = otherComplexFieldYson.ToString();

    TUnversionedRowBuilder builder;
    builder.AddValue(MakeUnversionedCompositeValue(firstYsonStr, firstId));
    builder.AddValue(MakeUnversionedCompositeValue(secondYsonStr, secondId));
    builder.AddValue(MakeUnversionedCompositeValue(repeatedMessageYsonStr, repeatedMessageId));
    builder.AddValue(MakeUnversionedCompositeValue(repeatedInt64YsonStr, repeatedInt64Id));
    builder.AddValue(MakeUnversionedCompositeValue(anotherRepeatedInt64YsonStr, anotherRepeatedInt64Id));
    builder.AddValue(MakeUnversionedInt64Value(4321, anyFieldId));

    builder.AddValue(MakeUnversionedInt64Value(-64, int64FieldId));
    builder.AddValue(MakeUnversionedUint64Value(64, uint64FieldId));
    builder.AddValue(MakeUnversionedInt64Value(-32, int32FieldId));
    builder.AddValue(MakeUnversionedUint64Value(32, uint32FieldId));

    builder.AddValue(MakeUnversionedInt64Value(-42, enumIntFieldId));
    builder.AddValue(MakeUnversionedStringValue("Three", enumStringStringFieldId));
    builder.AddValue(MakeUnversionedInt64Value(1, enumStringInt64FieldId));

    const auto HelloWorldInRussian = "\xd0\x9f\xd1\x80\xd0\xb8\xd0\xb2\xd0\xb5\xd1\x82, \xd0\xbc\xd0\xb8\xd1\x80!";
    builder.AddValue(MakeUnversionedStringValue(HelloWorldInRussian, utf8FieldId));

    builder.AddValue(MakeUnversionedCompositeValue(repeatedOptionalAnyYsonStr, repeatedOptionalAnyFieldId));

    builder.AddValue(MakeUnversionedCompositeValue(otherComplexFieldYsonStr, otherComplexFieldId));

    builder.AddValue(MakeUnversionedCompositeValue("[12;-10;123456789000;]", packedRepeatedInt64FieldId));

    builder.AddValue(MakeUnversionedCompositeValue("[1;2;3]", optionalRepeatedInt64FieldId));

    builder.AddValue(MakeUnversionedCompositeValue("[0; foobaz]", oneofFieldId));
    builder.AddValue(MakeUnversionedNullValue(optionalOneofFieldId));

    builder.AddValue(MakeUnversionedCompositeValue("[[2; [x; y]]; [5; [z; w]]]", mapFieldId));

    auto rows = std::vector<TUnversionedRow>(rowCount, builder.GetRow());
    EXPECT_EQ(true, writer->Write(rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    TStringInput input(result);
    TLenvalParser lenvalParser(&input);

    for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
        auto entry = lenvalParser.Next();
        ASSERT_TRUE(entry);

        NYT::TMessageWithStructuredEmbedded message;
        ASSERT_TRUE(message.ParseFromString(entry->RowData));

        const auto& first = message.first();
        EXPECT_EQ(first.enum_field(), EEnum::Two);
        EXPECT_EQ(first.int64_field(), 44);
        std::vector<i64> firstRepeatedInt64Field(
            first.repeated_int64_field().begin(),
            first.repeated_int64_field().end());
        EXPECT_EQ(firstRepeatedInt64Field, (std::vector<i64>{55, 56, 57}));
        std::vector<i64> firstAnotherRepeatedInt64Field(
            first.another_repeated_int64_field().begin(),
            first.another_repeated_int64_field().end());
        EXPECT_EQ(firstAnotherRepeatedInt64Field, (std::vector<i64>{}));
        EXPECT_EQ(first.message_field().key(), "key");
        EXPECT_EQ(first.message_field().value(), "value");
        ASSERT_EQ(first.repeated_message_field_size(), 2);
        EXPECT_EQ(first.repeated_message_field(0).key(), "key1");
        EXPECT_EQ(first.repeated_message_field(0).value(), "value1");
        EXPECT_EQ(first.repeated_message_field(1).key(), "key2");
        EXPECT_EQ(first.repeated_message_field(1).value(), "value2");

        EXPECT_NODES_EQUAL(
            ConvertToNode(TYsonString(first.any_int64_field())),
            BuildYsonNodeFluently().Value(45));

        EXPECT_NODES_EQUAL(
            ConvertToNode(TYsonString(first.any_map_field())),
            BuildYsonNodeFluently().BeginMap()
                .Item("key").Value("value")
            .EndMap());

        std::vector<TYsonString> firstRepeatedOptionalAnyField(
            first.repeated_optional_any_field().begin(),
            first.repeated_optional_any_field().end());

        EXPECT_NODES_EQUAL(
            ConvertToNode(firstRepeatedOptionalAnyField),
            BuildYsonNodeFluently()
                .BeginList()
                    .Item().Value(2)
                    .Item().Entity()
                    .Item().Value("foo")
                .EndList());

        EXPECT_FALSE(first.has_optional_int64_field());

        std::vector<EEnum> actualFirstPackedRepeatedEnumField;
        for (auto x : first.packed_repeated_enum_field()) {
            actualFirstPackedRepeatedEnumField.push_back(static_cast<EEnum>(x));
        }
        auto expectedFirstPackedRepeatedEnumField = std::vector<EEnum>{EEnum::MinusFortyTwo, EEnum::Two};
        EXPECT_EQ(expectedFirstPackedRepeatedEnumField, actualFirstPackedRepeatedEnumField);

        std::vector<bool> firstOptionalRepeatedBoolField(
            first.optional_repeated_bool_field().begin(),
            first.optional_repeated_bool_field().end());
        auto expectedFirstOptionalRepeatedBoolField = std::vector<bool>{false, true, false};
        EXPECT_EQ(expectedFirstOptionalRepeatedBoolField, firstOptionalRepeatedBoolField);

        EXPECT_FALSE(first.has_oneof_string_field_1());
        EXPECT_FALSE(first.has_oneof_string_field());
        EXPECT_TRUE(first.has_oneof_message_field());
        EXPECT_EQ(first.oneof_message_field().key(), "foo");
        EXPECT_FALSE(first.oneof_message_field().has_value());

        EXPECT_FALSE(first.has_optional_oneof_string_field_1());
        EXPECT_FALSE(first.has_optional_oneof_string_field());
        EXPECT_FALSE(first.has_optional_oneof_message_field());

        EXPECT_EQ(std::ssize(first.map_field()), 2);
        ASSERT_EQ(static_cast<int>(first.map_field().count(13)), 1);
        EXPECT_EQ(first.map_field().at(13).key(), "bac");
        EXPECT_EQ(first.map_field().at(13).value(), "cab");
        ASSERT_EQ(static_cast<int>(first.map_field().count(15)), 1);
        EXPECT_EQ(first.map_field().at(15).key(), "ya");
        EXPECT_EQ(first.map_field().at(15).value(), "make");

        const auto& second = message.second();
        EXPECT_EQ(second.one(), 101);
        EXPECT_EQ(second.two(), 102);
        EXPECT_EQ(second.three(), 103);

        ASSERT_EQ(message.repeated_message_field_size(), 2);
        EXPECT_EQ(message.repeated_message_field(0).key(), "key11");
        EXPECT_EQ(message.repeated_message_field(0).value(), "value11");
        EXPECT_EQ(message.repeated_message_field(1).key(), "key21");
        EXPECT_EQ(message.repeated_message_field(1).value(), "value21");

        std::vector<i64> repeatedInt64Field(
            message.repeated_int64_field().begin(),
            message.repeated_int64_field().end());
        EXPECT_EQ(repeatedInt64Field, (std::vector<i64>{31, 32, 33}));

        std::vector<i64> anotherRepeatedInt64Field(
            message.another_repeated_int64_field().begin(),
            message.another_repeated_int64_field().end());
        EXPECT_EQ(anotherRepeatedInt64Field, (std::vector<i64>{}));

        EXPECT_EQ(message.int64_any_field(), 4321);

        // Note the reversal of 32 <-> 64.
        EXPECT_EQ(message.int32_field(), -64);
        EXPECT_EQ(message.uint32_field(), 64u);
        EXPECT_EQ(message.int64_field(), -32);
        EXPECT_EQ(message.uint64_field(), 32u);

        EXPECT_EQ(message.enum_int_field(), EEnum::MinusFortyTwo);
        EXPECT_EQ(message.enum_string_string_field(), EEnum::Three);
        EXPECT_EQ(message.enum_string_int64_field(), EEnum::One);

        EXPECT_EQ(message.utf8_field(), HelloWorldInRussian);

        std::vector<TYsonString> repeatedOptionalAnyField(
            message.repeated_optional_any_field().begin(),
            message.repeated_optional_any_field().end());
        EXPECT_NODES_EQUAL(ConvertToNode(repeatedOptionalAnyField), ConvertToNode(repeatedOptionalAnyYson));

        {
            auto otherColumns = ConvertToNode(TYsonString(message.other_columns_field()))->AsMap();
            auto mode = complexTypeMode;
            auto expected = ([&] {
                switch (mode) {
                    case EComplexTypeMode::Named:
                        return BuildYsonNodeFluently()
                            .BeginMap()
                                .Item("one").Value(22)
                                .Item("two").Value(23)
                                .Item("three").Value(24)
                            .EndMap();
                    case EComplexTypeMode::Positional:
                        return ConvertToNode(otherComplexFieldYson);
                }
                YT_ABORT();
            })();

            EXPECT_NODES_EQUAL(expected, otherColumns->GetChildOrThrow("other_complex_field"));
        }

        std::vector<i64> actualPackedRepeatedInt64Field(
            message.packed_repeated_int64_field().begin(),
            message.packed_repeated_int64_field().end());
        auto expectedPackedRepeatedInt64Field = std::vector<i64>{12, -10, 123456789000LL};
        EXPECT_EQ(expectedPackedRepeatedInt64Field, actualPackedRepeatedInt64Field);

        std::vector<i64> actualOptionalRepeatedInt64Field(
            message.optional_repeated_int64_field().begin(),
            message.optional_repeated_int64_field().end());
        auto expectedOptionalRepeatedInt64Field = std::vector<i64>{1, 2, 3};
        EXPECT_EQ(expectedOptionalRepeatedInt64Field, actualOptionalRepeatedInt64Field);

        EXPECT_TRUE(message.has_oneof_string_field_1());
        EXPECT_EQ(message.oneof_string_field_1(), "foobaz");
        EXPECT_FALSE(message.has_oneof_string_field());
        EXPECT_FALSE(message.has_oneof_message_field());

        EXPECT_FALSE(message.has_optional_oneof_string_field_1());
        EXPECT_FALSE(message.has_optional_oneof_string_field());
        EXPECT_FALSE(message.has_optional_oneof_message_field());

        EXPECT_EQ(std::ssize(message.map_field()), 2);
        ASSERT_EQ(static_cast<int>(message.map_field().count(2)), 1);
        EXPECT_EQ(message.map_field().at(2).key(), "x");
        EXPECT_EQ(message.map_field().at(2).value(), "y");
        ASSERT_EQ(static_cast<int>(message.map_field().count(5)), 1);
        EXPECT_EQ(message.map_field().at(5).key(), "z");
        EXPECT_EQ(message.map_field().at(5).value(), "w");
    }

    ASSERT_FALSE(lenvalParser.Next());
}

INodePtr SortMapByKey(const INodePtr& node)
{
    auto keyValuePairs = ConvertTo<std::vector<std::pair<i64, INodePtr>>>(node);
    std::sort(std::begin(keyValuePairs), std::end(keyValuePairs));
    return ConvertTo<INodePtr>(keyValuePairs);
}

TEST_P(TProtobufFormatStructuredMessage, EmbeddedParse)
{
    auto [complexTypeMode, rowCount, protoFormatType] = GetParam();

    auto schema = BuildEmbeddedSchema();
    auto config = BuildEmbeddedConfig(complexTypeMode, protoFormatType);

    NYT::TEmbeddingMessage message;

    message.set_num(789);
    auto* t1 = message.mutable_t1();
    t1->set_embedded_num(123);
    auto* t2 = t1->mutable_t2();
    t2->set_embedded2_num(456);
    t1->set_uint_variant(555);
    t2->add_embedded2_repeated("a");
    t2->add_embedded2_repeated("b");
    t2->add_embedded2_repeated("c");
    auto* embedded2_struct = t2->mutable_embedded2_struct();
    embedded2_struct->set_float1(1.5f);
    embedded2_struct->set_string1("abc");

    //message.set_extra_field("*");
    //t1->set_embedded_extra_field("*");

    auto rowCollector = ParseRows(message, config, schema, rowCount);
    for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
        EXPECT_EQ(GetUint64(rowCollector.GetRowValue(rowIndex, "num")), 789u);
        EXPECT_EQ(GetUint64(rowCollector.GetRowValue(rowIndex, "embedded_num")), 123u);
        EXPECT_EQ(GetUint64(rowCollector.GetRowValue(rowIndex, "embedded2_num")), 456u);
        EXPECT_NODES_EQUAL(
            GetComposite(rowCollector.GetRowValue(rowIndex, "variant")),
            ConvertToNode(TYsonString(TStringBuf("[1; 555u]"))));

        auto embedded2_repeatedNode = GetComposite(rowCollector.GetRowValue(rowIndex, "embedded2_repeated"));
        ASSERT_EQ(embedded2_repeatedNode->GetType(), ENodeType::List);
        const auto& embedded2_repeatedList = embedded2_repeatedNode->AsList();
        ASSERT_EQ(embedded2_repeatedList->GetChildCount(), 3);
        EXPECT_EQ(embedded2_repeatedList->GetChildValueOrThrow<TString>(0), "a");
        EXPECT_EQ(embedded2_repeatedList->GetChildValueOrThrow<TString>(1), "b");
        EXPECT_EQ(embedded2_repeatedList->GetChildValueOrThrow<TString>(2), "c");

        auto embedded2_structNode = GetComposite(rowCollector.GetRowValue(rowIndex, "embedded2_struct"));
        ASSERT_EQ(embedded2_structNode->GetType(), ENodeType::List);
        const auto& embedded2_structList = embedded2_structNode->AsList();
        ASSERT_EQ(embedded2_structList->GetChildCount(), 2);
        EXPECT_EQ(embedded2_structList->GetChildValueOrThrow<double>(0), 1.5f);
        EXPECT_EQ(embedded2_structList->GetChildValueOrThrow<TString>(1), "abc");
    }
}

TEST_P(TProtobufFormatStructuredMessage, Parse)
{
    auto [complexTypeMode, rowCount, protoFormatType] = GetParam();

    auto schema = CreateSchemaWithStructuredMessage();
    auto config = CreateConfigWithStructuredMessage(complexTypeMode, protoFormatType);

    NYT::TMessageWithStructuredEmbedded message;

    auto* first = message.mutable_first();
    first->set_enum_field(EEnum::Two);
    first->set_int64_field(44);

    first->add_repeated_int64_field(55);
    first->add_repeated_int64_field(56);
    first->add_repeated_int64_field(57);

    // another_repeated_int64_field is intentionally empty.

    first->mutable_message_field()->set_key("key");
    first->mutable_message_field()->set_value("value");
    auto* firstSubfield1 = first->add_repeated_message_field();
    firstSubfield1->set_key("key1");
    firstSubfield1->set_value("value1");
    auto* firstSubfield2 = first->add_repeated_message_field();
    firstSubfield2->set_key("key2");
    firstSubfield2->set_value("value2");

    first->set_any_int64_field(BuildYsonStringFluently().Value(4422).ToString());
    first->set_any_map_field(
        BuildYsonStringFluently()
            .BeginMap()
                .Item("key").Value("value")
            .EndMap()
        .ToString());

    first->add_repeated_optional_any_field("%false");
    first->add_repeated_optional_any_field("42");
    first->add_repeated_optional_any_field("#");

    first->add_packed_repeated_enum_field(EEnum::MaxInt32);
    first->add_packed_repeated_enum_field(EEnum::MinusFortyTwo);

    // optional_repeated_bool_field is intentionally empty.

    first->mutable_oneof_message_field()->set_key("KEY");

    // optional_oneof_field is intentionally empty.

    (*first->mutable_map_field())[111].set_key("key111");
    (*first->mutable_map_field())[111].set_value("value111");
    (*first->mutable_map_field())[222].set_key("key222");
    (*first->mutable_map_field())[222].set_value("value222");

    auto* second = message.mutable_second();
    second->set_one(101);
    second->set_two(102);
    second->set_three(103);

    message.add_repeated_int64_field(31);
    message.add_repeated_int64_field(32);
    message.add_repeated_int64_field(33);

    // another_repeated_int64_field is intentionally empty.

    auto* subfield1 = message.add_repeated_message_field();
    subfield1->set_key("key11");
    subfield1->set_value("value11");
    auto* subfield2 = message.add_repeated_message_field();
    subfield2->set_key("key21");
    subfield2->set_value("value21");

    message.set_int64_any_field(4321);

    // Note the reversal of 32 <-> 64.
    message.set_int64_field(-32);
    message.set_uint64_field(32);
    message.set_int32_field(-64);
    message.set_uint32_field(64);

    // Note that we don't set the "enum_string_int64_field" as it would fail during parsing.
    message.set_enum_int_field(EEnum::MinusFortyTwo);
    message.set_enum_string_string_field(EEnum::Three);

    const auto HelloWorldInChinese = "\xe4\xbd\xa0\xe5\xa5\xbd\xef\xbc\x8c\xe4\xb8\x96\xe7\x95\x8c";
    message.set_utf8_field(HelloWorldInChinese);

    message.add_repeated_optional_any_field("#");
    message.add_repeated_optional_any_field("1");
    message.add_repeated_optional_any_field("\"qwe\"");
    message.add_repeated_optional_any_field("%true");

    auto otherComplexFieldPositional = BuildYsonNodeFluently()
        .BeginList()
            .Item().Value(301)
            .Item().Value(302)
            .Item().Value(303)
        .EndList();

    auto mode = complexTypeMode;
    auto otherComplexField = ([&] {
        switch (mode) {
            case EComplexTypeMode::Named:
                return BuildYsonNodeFluently()
                    .BeginMap()
                        .Item("one").Value(301)
                        .Item("two").Value(302)
                        .Item("three").Value(303)
                    .EndMap();
            case EComplexTypeMode::Positional:
                return otherComplexFieldPositional;
        }
        YT_ABORT();
    })();
    auto otherColumnsYson = BuildYsonStringFluently()
        .BeginMap()
            .Item("other_complex_field").Value(otherComplexField)
        .EndMap();
    message.set_other_columns_field(otherColumnsYson.ToString());

    message.add_packed_repeated_int64_field(-123456789000LL);
    message.add_packed_repeated_int64_field(0);

    message.add_optional_repeated_int64_field(-4242);

    // optional_oneof_field is intentionally empty.

    message.set_oneof_string_field("spam");

    (*message.mutable_map_field())[777].set_key("key777");
    (*message.mutable_map_field())[777].set_value("value777");
    (*message.mutable_map_field())[888].set_key("key888");
    (*message.mutable_map_field())[888].set_value("value888");

    auto rowCollector = ParseRows(message, config, schema, rowCount);
    for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
        auto firstNode = GetComposite(rowCollector.GetRowValue(rowIndex, "first"));
        ASSERT_EQ(firstNode->GetType(), ENodeType::List);
        const auto& firstList = firstNode->AsList();
        ASSERT_EQ(firstList->GetChildCount(), 17);

        EXPECT_EQ(firstList->GetChildOrThrow(0)->GetType(), ENodeType::Entity);
        EXPECT_EQ(firstList->GetChildValueOrThrow<TString>(1), "Two");
        EXPECT_EQ(firstList->GetChildValueOrThrow<i64>(2), 44);

        ASSERT_EQ(firstList->GetChildOrThrow(3)->GetType(), ENodeType::List);
        EXPECT_EQ(ConvertTo<std::vector<i64>>(firstList->GetChildOrThrow(3)), (std::vector<i64>{55, 56, 57}));

        ASSERT_EQ(firstList->GetChildOrThrow(4)->GetType(), ENodeType::List);
        EXPECT_EQ(ConvertTo<std::vector<i64>>(firstList->GetChildOrThrow(4)), (std::vector<i64>{}));

        ASSERT_EQ(firstList->GetChildOrThrow(5)->GetType(), ENodeType::List);
        EXPECT_EQ(firstList->GetChildOrThrow(5)->AsList()->GetChildValueOrThrow<TString>(0), "key");
        EXPECT_EQ(firstList->GetChildOrThrow(5)->AsList()->GetChildValueOrThrow<TString>(1), "value");

        ASSERT_EQ(firstList->GetChildOrThrow(6)->GetType(), ENodeType::List);
        ASSERT_EQ(firstList->GetChildOrThrow(6)->AsList()->GetChildCount(), 2);

        const auto& firstSubNode1 = firstList->GetChildOrThrow(6)->AsList()->GetChildOrThrow(0);
        ASSERT_EQ(firstSubNode1->GetType(), ENodeType::List);
        ASSERT_EQ(firstSubNode1->AsList()->GetChildCount(), 2);
        EXPECT_EQ(firstSubNode1->AsList()->GetChildValueOrThrow<TString>(0), "key1");
        EXPECT_EQ(firstSubNode1->AsList()->GetChildValueOrThrow<TString>(1), "value1");

        const auto& firstSubNode2 = firstList->GetChildOrThrow(6)->AsList()->GetChildOrThrow(1);
        ASSERT_EQ(firstSubNode2->GetType(), ENodeType::List);
        ASSERT_EQ(firstSubNode2->AsList()->GetChildCount(), 2);
        EXPECT_EQ(firstSubNode2->AsList()->GetChildValueOrThrow<TString>(0), "key2");
        EXPECT_EQ(firstSubNode2->AsList()->GetChildValueOrThrow<TString>(1), "value2");

        ASSERT_EQ(firstList->GetChildOrThrow(7)->GetType(), ENodeType::Int64);
        EXPECT_EQ(firstList->GetChildValueOrThrow<i64>(7), 4422);

        ASSERT_EQ(firstList->GetChildOrThrow(8)->GetType(), ENodeType::Map);
        EXPECT_NODES_EQUAL(
            firstList->GetChildOrThrow(8),
            BuildYsonNodeFluently()
                .BeginMap()
                    .Item("key").Value("value")
                .EndMap());

        ASSERT_EQ(firstList->GetChildOrThrow(9)->GetType(), ENodeType::Entity);

        EXPECT_NODES_EQUAL(
            firstList->GetChildOrThrow(10),
            BuildYsonNodeFluently()
                .BeginList()
                    .Item().Value(false)
                    .Item().Value(42)
                    .Item().Entity()
                .EndList());

        EXPECT_NODES_EQUAL(
            firstList->GetChildOrThrow(11),
            BuildYsonNodeFluently()
                .BeginList()
                    .Item().Value("MaxInt32")
                    .Item().Value("MinusFortyTwo")
                .EndList());

        // optional_repeated_bool_field.
        ASSERT_EQ(firstList->GetChildOrThrow(12)->GetType(), ENodeType::Entity);

        // oneof_field.
        EXPECT_NODES_EQUAL(
            firstList->GetChildOrThrow(13),
            BuildYsonNodeFluently()
                .BeginList()
                    .Item().Value(2)
                    .Item().BeginList()
                        .Item().Value("KEY")
                        .Item().Entity()
                    .EndList()
                .EndList());

        // optional_oneof_field.
        ASSERT_EQ(firstList->GetChildOrThrow(14)->GetType(), ENodeType::Entity);

        // map_field.
        EXPECT_NODES_EQUAL(
            SortMapByKey(firstList->GetChildOrThrow(15)),
            BuildYsonNodeFluently()
                .BeginList()
                    .Item().BeginList()
                        .Item().Value(111)
                        .Item().BeginList()
                            .Item().Value("key111")
                            .Item().Value("value111")
                        .EndList()
                    .EndList()
                    .Item().BeginList()
                        .Item().Value(222)
                        .Item().BeginList()
                            .Item().Value("key222")
                            .Item().Value("value222")
                        .EndList()
                    .EndList()
                .EndList());

        // field_missing_from_proto2.
        ASSERT_EQ(firstList->GetChildOrThrow(16)->GetType(), ENodeType::Entity);

        auto secondNode = GetComposite(rowCollector.GetRowValue(rowIndex, "second"));
        ASSERT_EQ(secondNode->GetType(), ENodeType::List);
        EXPECT_EQ(ConvertTo<std::vector<i64>>(secondNode), (std::vector<i64>{101, 102, 103}));

        auto repeatedMessageNode = GetComposite(rowCollector.GetRowValue(rowIndex, "repeated_message_field"));
        ASSERT_EQ(repeatedMessageNode->GetType(), ENodeType::List);
        ASSERT_EQ(repeatedMessageNode->AsList()->GetChildCount(), 2);

        const auto& subNode1 = repeatedMessageNode->AsList()->GetChildOrThrow(0);
        ASSERT_EQ(subNode1->GetType(), ENodeType::List);
        ASSERT_EQ(subNode1->AsList()->GetChildCount(), 2);
        EXPECT_EQ(subNode1->AsList()->GetChildValueOrThrow<TString>(0), "key11");
        EXPECT_EQ(subNode1->AsList()->GetChildValueOrThrow<TString>(1), "value11");

        const auto& subNode2 = repeatedMessageNode->AsList()->GetChildOrThrow(1);
        ASSERT_EQ(subNode2->GetType(), ENodeType::List);
        ASSERT_EQ(subNode2->AsList()->GetChildCount(), 2);
        EXPECT_EQ(subNode2->AsList()->GetChildValueOrThrow<TString>(0), "key21");
        EXPECT_EQ(subNode2->AsList()->GetChildValueOrThrow<TString>(1), "value21");

        auto repeatedInt64Node = GetComposite(rowCollector.GetRowValue(rowIndex, "repeated_int64_field"));
        EXPECT_EQ(ConvertTo<std::vector<i64>>(repeatedInt64Node), (std::vector<i64>{31, 32, 33}));

        auto anotherRepeatedInt64Node = GetComposite(rowCollector.GetRowValue(rowIndex, "another_repeated_int64_field"));
        EXPECT_EQ(ConvertTo<std::vector<i64>>(anotherRepeatedInt64Node), (std::vector<i64>{}));

        auto anyValue = rowCollector.GetRowValue(rowIndex, "any_field");
        ASSERT_EQ(anyValue.Type, EValueType::Int64);
        EXPECT_EQ(anyValue.Data.Int64, 4321);

        EXPECT_EQ(GetInt64(rowCollector.GetRowValue(rowIndex, "int64_field")), -64);
        EXPECT_EQ(GetUint64(rowCollector.GetRowValue(rowIndex, "uint64_field")), 64u);
        EXPECT_EQ(GetInt64(rowCollector.GetRowValue(rowIndex, "int32_field")), -32);
        EXPECT_EQ(GetUint64(rowCollector.GetRowValue(rowIndex, "uint32_field")), 32u);

        EXPECT_EQ(GetInt64(rowCollector.GetRowValue(rowIndex, "enum_int_field")), -42);
        EXPECT_EQ(GetString(rowCollector.GetRowValue(rowIndex, "enum_string_string_field")), "Three");

        EXPECT_EQ(GetString(rowCollector.GetRowValue(rowIndex, "utf8_field")), HelloWorldInChinese);

        auto repeatedRepeatedOptionalAnyNode = GetComposite(rowCollector.GetRowValue(rowIndex, "repeated_optional_any_field"));
        auto expectedRepeatedOptionalAnyNode = BuildYsonNodeFluently()
            .BeginList()
                .Item().Entity()
                .Item().Value(1)
                .Item().Value("qwe")
                .Item().Value(true)
            .EndList();
        EXPECT_NODES_EQUAL(repeatedRepeatedOptionalAnyNode, expectedRepeatedOptionalAnyNode);

        auto actualOtherComplexField = GetComposite(rowCollector.GetRowValue(rowIndex, "other_complex_field"));
        EXPECT_NODES_EQUAL(actualOtherComplexField, otherComplexFieldPositional);

        EXPECT_NODES_EQUAL(
            GetComposite(rowCollector.GetRowValue(rowIndex, "packed_repeated_int64_field")),
            ConvertToNode(TYsonString(TStringBuf("[-123456789000;0]"))));

        EXPECT_NODES_EQUAL(
            GetComposite(rowCollector.GetRowValue(rowIndex, "optional_repeated_int64_field")),
            ConvertToNode(TYsonString(TStringBuf("[-4242]"))));

        EXPECT_NODES_EQUAL(
            GetComposite(rowCollector.GetRowValue(rowIndex, "oneof_field")),
            ConvertToNode(TYsonString(TStringBuf("[1; \"spam\"]"))));

        EXPECT_FALSE(rowCollector.FindRowValue(rowIndex, "optional_oneof_field"));

        // map_field.
        EXPECT_NODES_EQUAL(
            SortMapByKey(GetComposite(rowCollector.GetRowValue(rowIndex, "map_field"))),
            ConvertToNode(TYsonString(TStringBuf("[[777; [key777; value777]]; [888; [key888; value888]]]"))));
    }
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TTableSchemaPtr> CreateSeveralTablesSchemas()
{
    return {
        New<TTableSchema>(std::vector<TColumnSchema>{
            {"embedded", StructLogicalType({
                {"enum_field", SimpleLogicalType(ESimpleLogicalValueType::String)},
                {"int64_field", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            })},
            {"repeated_int64_field", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"any_field", SimpleLogicalType(ESimpleLogicalValueType::Any)},
        }),
        New<TTableSchema>(std::vector<TColumnSchema>{
            {"enum_field", SimpleLogicalType(ESimpleLogicalValueType::String)},
            {"int64_field", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        }),
        // Empty schema.
        New<TTableSchema>(),
    };
}

INodePtr CreateSeveralTablesConfig(EProtoFormatType protoFormatType)
{
    if (protoFormatType == EProtoFormatType::FileDescriptor) {
        return CreateFileDescriptorConfig<TSeveralTablesMessageFirst, TSeveralTablesMessageSecond, TSeveralTablesMessageThird>();
    }
    YT_VERIFY(protoFormatType == EProtoFormatType::Structured);

    return BuildYsonNodeFluently()
        .BeginAttributes()
            .Item("enumerations").Value(EnumerationsConfig)
            .Item("tables")
            .BeginList()
                // Table #1.
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("embedded")
                            .Item("field_number").Value(1)
                            .Item("proto_type").Value("structured_message")
                            .Item("fields")
                            .BeginList()
                                .Item()
                                .BeginMap()
                                    .Item("name").Value("int64_field")
                                    .Item("field_number").Value(2)
                                    .Item("proto_type").Value("int64")
                                .EndMap()
                                .Item()
                                .BeginMap()
                                    .Item("name").Value("enum_field")
                                    .Item("field_number").Value(1)
                                    .Item("proto_type").Value("enum_string")
                                    .Item("enumeration_name").Value("EEnum")
                                .EndMap()
                            .EndList()
                        .EndMap()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("repeated_int64_field")
                            .Item("field_number").Value(2)
                            .Item("proto_type").Value("int64")
                            .Item("repeated").Value(true)
                        .EndMap()
                        .Item()
                        .BeginMap()
                            // In schema it is of type "any".
                            .Item("name").Value("any_field")
                            .Item("field_number").Value(3)
                            .Item("proto_type").Value("int64")
                        .EndMap()
                    .EndList()
                .EndMap()

                // Table #2.
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("int64_field")
                            .Item("field_number").Value(2)
                            .Item("proto_type").Value("int64")
                        .EndMap()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("enum_field")
                            .Item("field_number").Value(1)
                            .Item("proto_type").Value("enum_string")
                            .Item("enumeration_name").Value("EEnum")
                        .EndMap()
                    .EndList()
                .EndMap()

                // Table #3.
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("string_field")
                            .Item("field_number").Value(1)
                            .Item("proto_type").Value("string")
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
        .EndAttributes()
        .Value("protobuf");
}

using TProtobufFormatSeveralTablesParam = std::tuple<EProtoFormatType>;

class TProtobufFormatSeveralTables
    : public ::testing::TestWithParam<TProtobufFormatSeveralTablesParam>
{ };

INSTANTIATE_TEST_SUITE_P(
    FileDescriptor,
    TProtobufFormatSeveralTables,
    ::testing::Values(TProtobufFormatSeveralTablesParam{
        EProtoFormatType::FileDescriptor}));

INSTANTIATE_TEST_SUITE_P(
    Structured,
    TProtobufFormatSeveralTables,
    ::testing::Values(TProtobufFormatSeveralTablesParam{
        EProtoFormatType::Structured}));

TEST_P(TProtobufFormatSeveralTables, Write)
{
    auto [protoFormatType] = GetParam();

    auto schemas = CreateSeveralTablesSchemas();
    auto configNode = CreateSeveralTablesConfig(protoFormatType);

    auto config = ConvertTo<TProtobufFormatConfigPtr>(configNode->Attributes().ToMap());

    auto nameTable = New<TNameTable>();
    auto embeddedId = nameTable->RegisterName("embedded");
    auto anyFieldId = nameTable->RegisterName("any_field");
    auto int64FieldId = nameTable->RegisterName("int64_field");
    auto repeatedInt64Id = nameTable->RegisterName("repeated_int64_field");
    auto enumFieldId = nameTable->RegisterName("enum_field");
    auto stringFieldId = nameTable->RegisterName("string_field");
    auto tableIndexId = nameTable->RegisterName(TableIndexColumnName);

    TString result;
    TStringOutput resultStream(result);
    auto controlAttributesConfig = New<TControlAttributesConfig>();
    controlAttributesConfig->EnableTableIndex = true;
    controlAttributesConfig->EnableEndOfStream = true;
    auto writer = CreateWriterForProtobuf(
        std::move(config),
        schemas,
        nameTable,
        CreateAsyncAdapter(&resultStream),
        true,
        std::move(controlAttributesConfig),
        0);

    auto embeddedYson = BuildYsonStringFluently()
        .BeginList()
            .Item().Value("Two")
            .Item().Value(44)
        .EndList()
        .ToString();

    auto repeatedInt64Yson = ConvertToYsonString(std::vector<i64>{31, 32, 33}).ToString();

    {
        TUnversionedRowBuilder builder;
        builder.AddValue(MakeUnversionedCompositeValue(embeddedYson, embeddedId));
        builder.AddValue(MakeUnversionedCompositeValue(repeatedInt64Yson, repeatedInt64Id));
        builder.AddValue(MakeUnversionedInt64Value(4321, anyFieldId));
        EXPECT_EQ(true, writer->Write({builder.GetRow()}));
    }
    {
        TUnversionedRowBuilder builder;
        builder.AddValue(MakeUnversionedStringValue("Two", enumFieldId));
        builder.AddValue(MakeUnversionedInt64Value(999, int64FieldId));
        builder.AddValue(MakeUnversionedInt64Value(1, tableIndexId));
        EXPECT_EQ(true, writer->Write({builder.GetRow()}));
    }
    {
        TUnversionedRowBuilder builder;
        builder.AddValue(MakeUnversionedStringValue("blah", stringFieldId));
        builder.AddValue(MakeUnversionedInt64Value(2, tableIndexId));
        EXPECT_EQ(true, writer->Write({builder.GetRow()}));
    }

    writer->Close()
        .Get()
        .ThrowOnError();

    TStringInput input(result);
    TLenvalParser lenvalParser(&input);

    {
        auto entry = lenvalParser.Next();
        ASSERT_TRUE(entry);

        NYT::TSeveralTablesMessageFirst message;
        ASSERT_TRUE(message.ParseFromString(entry->RowData));

        const auto& embedded = message.embedded();
        EXPECT_EQ(embedded.enum_field(), EEnum::Two);
        EXPECT_EQ(embedded.int64_field(), 44);

        std::vector<i64> repeatedInt64Field(
            message.repeated_int64_field().begin(),
            message.repeated_int64_field().end());
        EXPECT_EQ(repeatedInt64Field, (std::vector<i64>{31, 32, 33}));
        EXPECT_EQ(message.int64_field(), 4321);
    }
    {
        auto entry = lenvalParser.Next();
        ASSERT_TRUE(entry);

        NYT::TSeveralTablesMessageSecond message;
        ASSERT_TRUE(message.ParseFromString(entry->RowData));

        EXPECT_EQ(message.enum_field(), EEnum::Two);
        EXPECT_EQ(message.int64_field(), 999);
    }
    {
        auto entry = lenvalParser.Next();
        ASSERT_TRUE(entry);

        NYT::TSeveralTablesMessageThird message;
        ASSERT_TRUE(message.ParseFromString(entry->RowData));

        EXPECT_EQ(message.string_field(), "blah");
    }
    ASSERT_FALSE(lenvalParser.IsEndOfStream());
    ASSERT_FALSE(lenvalParser.Next());
    ASSERT_TRUE(lenvalParser.IsEndOfStream());
    ASSERT_FALSE(lenvalParser.Next());
}

TEST_P(TProtobufFormatSeveralTables, Parse)
{
    auto [protoFormatType] = GetParam();

    auto schemas = CreateSeveralTablesSchemas();
    auto configNode = CreateSeveralTablesConfig(protoFormatType);
    auto config = ConvertTo<TProtobufFormatConfigPtr>(configNode->Attributes().ToMap());

    std::vector<TCollectingValueConsumer> rowCollectors;
    std::vector<std::unique_ptr<IParser>> parsers;
    for (const auto& schema : schemas) {
        rowCollectors.emplace_back(schema);
    }
    for (int tableIndex = 0; tableIndex < static_cast<int>(schemas.size()); ++tableIndex) {
        parsers.push_back(CreateParserForProtobuf(
            &rowCollectors[tableIndex],
            config,
            tableIndex));
    }

    NYT::TSeveralTablesMessageFirst firstMessage;
    auto* embedded = firstMessage.mutable_embedded();
    embedded->set_enum_field(EEnum::Two);
    embedded->set_int64_field(44);

    firstMessage.add_repeated_int64_field(55);
    firstMessage.add_repeated_int64_field(56);
    firstMessage.add_repeated_int64_field(57);

    firstMessage.set_int64_field(4444);

    NYT::TSeveralTablesMessageSecond secondMessage;
    secondMessage.set_enum_field(EEnum::Two);
    secondMessage.set_int64_field(44);

    NYT::TSeveralTablesMessageThird thirdMessage;
    thirdMessage.set_string_field("blah");

    auto parse = [] (auto& parser, const auto& message) {
        TString lenvalBytes;
        {
            TStringOutput out(lenvalBytes);
            auto messageSize = static_cast<ui32>(message.ByteSizeLong());
            out.Write(&messageSize, sizeof(messageSize));
            ASSERT_TRUE(message.SerializeToArcadiaStream(&out));
        }
        parser->Read(lenvalBytes);
        parser->Finish();
    };

    parse(parsers[0], firstMessage);
    parse(parsers[1], secondMessage);
    parse(parsers[2], thirdMessage);

    {
        const auto& rowCollector = rowCollectors[0];
        ASSERT_EQ(static_cast<int>(rowCollector.Size()), 1);

        auto embeddedNode = GetComposite(rowCollector.GetRowValue(0, "embedded"));
        ASSERT_EQ(ConvertToTextYson(embeddedNode), "[\"Two\";44;]");

        auto repeatedInt64Node = GetComposite(rowCollector.GetRowValue(0, "repeated_int64_field"));
        ASSERT_EQ(ConvertToTextYson(repeatedInt64Node), "[55;56;57;]");

        auto int64Field = GetInt64(rowCollector.GetRowValue(0, "any_field"));
        EXPECT_EQ(int64Field, 4444);
    }

    {
        const auto& rowCollector = rowCollectors[1];
        ASSERT_EQ(static_cast<int>(rowCollector.Size()), 1);

        EXPECT_EQ(GetString(rowCollector.GetRowValue(0, "enum_field")), "Two");
        EXPECT_EQ(GetInt64(rowCollector.GetRowValue(0, "int64_field")), 44);
    }

    {
        const auto& rowCollector = rowCollectors[2];
        ASSERT_EQ(static_cast<int>(rowCollector.Size()), 1);

        EXPECT_EQ(GetString(rowCollector.GetRowValue(0, "string_field")), "blah");
    }
}

TEST(TProtobufFormat, SchemaConfigMismatch)
{
    auto createParser = [] (const TTableSchemaPtr& schema, const INodePtr& configNode) {
        TCollectingValueConsumer rowCollector(schema);
        return CreateParserForProtobuf(
            &rowCollector,
            ConvertTo<TProtobufFormatConfigPtr>(configNode),
            0);
    };
    auto createSeveralTableWriter = [] (const std::vector<TTableSchemaPtr>& schemas, const INodePtr& configNode) {
        TString result;
        TStringOutput resultStream(result);
        return CreateWriterForProtobuf(
            ConvertTo<TProtobufFormatConfigPtr>(configNode),
            schemas,
            New<TNameTable>(),
            CreateAsyncAdapter(&resultStream),
            true,
            New<TControlAttributesConfig>(),
            0);
    };
    auto createWriter = [&] (const TTableSchemaPtr& schema, const INodePtr& configNode) {
        createSeveralTableWriter({schema}, configNode);
    };

    auto schema_struct_with_int64 = New<TTableSchema>(std::vector<TColumnSchema>{
        {"struct", StructLogicalType({
            {"int64_field", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        })},
    });

    auto schema_struct_with_uint64 = New<TTableSchema>(std::vector<TColumnSchema>{
        {"struct", StructLogicalType({
            {"int64_field", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Uint64))},
        })},
    });

    auto config_struct_with_int64 = BuildYsonNodeFluently()
        .BeginMap()
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("struct")
                            .Item("field_number").Value(1)
                            .Item("proto_type").Value("structured_message")
                            .Item("fields")
                            .BeginList()
                                .Item().BeginMap()
                                    .Item("name").Value("int64_field")
                                    .Item("field_number").Value(2)
                                    // Wrong type.
                                    .Item("proto_type").Value("int64")
                                .EndMap()
                            .EndList()
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
        .EndMap();

    // OK.
    EXPECT_NO_THROW(createParser(schema_struct_with_int64, config_struct_with_int64));
    EXPECT_NO_THROW(createWriter(schema_struct_with_int64, config_struct_with_int64));

    // Types mismatch.
    EXPECT_THROW_WITH_SUBSTRING(
        createParser(schema_struct_with_uint64, config_struct_with_int64),
        "signedness of both types must be the same");
    EXPECT_THROW_WITH_SUBSTRING(
        createWriter(schema_struct_with_uint64, config_struct_with_int64),
        "signedness of both types must be the same");

    // No schema for structured field is Ok.
    EXPECT_NO_THROW(createParser(New<TTableSchema>(), config_struct_with_int64));
    EXPECT_NO_THROW(createWriter(New<TTableSchema>(), config_struct_with_int64));

    auto schema_list_int64 = New<TTableSchema>(std::vector<TColumnSchema>{
        {
            "repeated",
            ListLogicalType(
                SimpleLogicalType(ESimpleLogicalValueType::Int64)),
        },
    });

    auto schema_list_optional_int64 = New<TTableSchema>(std::vector<TColumnSchema>{
        {
            "repeated",
            ListLogicalType(
                OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))),
        },
    });

    auto config_repeated_int64 = BuildYsonNodeFluently()
        .BeginMap()
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("repeated")
                            .Item("field_number").Value(1)
                            .Item("repeated").Value(true)
                            .Item("proto_type").Value("int64")
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
        .EndMap();

    // OK.
    EXPECT_NO_THROW(createParser(schema_list_int64, config_repeated_int64));
    EXPECT_NO_THROW(createWriter(schema_list_int64, config_repeated_int64));

    // No schema for repeated field is Ok.
    EXPECT_NO_THROW(createParser(New<TTableSchema>(), config_repeated_int64));
    EXPECT_NO_THROW(createWriter(New<TTableSchema>(), config_repeated_int64));

    // List of optional is not allowed.
    EXPECT_THROW_WITH_SUBSTRING(
        createParser(schema_list_optional_int64, config_repeated_int64),
        "unexpected logical metatype \"optional\"");
    EXPECT_THROW_WITH_SUBSTRING(
        createWriter(schema_list_optional_int64, config_repeated_int64),
        "unexpected logical metatype \"optional\"");

    auto schema_optional_list_int64 = New<TTableSchema>(std::vector<TColumnSchema>{
        {"repeated", OptionalLogicalType(
            ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)))},
    });

    // Optional list is OK.
    EXPECT_NO_THROW(createParser(schema_optional_list_int64, config_repeated_int64));
    EXPECT_NO_THROW(createWriter(schema_optional_list_int64, config_repeated_int64));

    auto schema_optional_optional_int64 = New<TTableSchema>(std::vector<TColumnSchema>{
        {"field", OptionalLogicalType(
            OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)))},
    });

    auto config_int64 = BuildYsonNodeFluently()
        .BeginMap()
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("field")
                            .Item("field_number").Value(1)
                            .Item("proto_type").Value("int64")
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
        .EndMap();

    // Optional of optional is not allowed.
    EXPECT_THROW_WITH_SUBSTRING(
        createParser(schema_optional_optional_int64, config_int64),
        "unexpected logical metatype \"optional\"");
    EXPECT_THROW_WITH_SUBSTRING(
        createWriter(schema_optional_optional_int64, config_int64),
        "unexpected logical metatype \"optional\"");

    auto schema_struct_with_both = New<TTableSchema>(std::vector<TColumnSchema>{
        {"struct", StructLogicalType({
            {"required_field", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"optional_field", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        })},
    });

    auto config_struct_with_required = BuildYsonNodeFluently()
        .BeginMap()
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("struct")
                            .Item("field_number").Value(1)
                            .Item("proto_type").Value("structured_message")
                            .Item("fields")
                            .BeginList()
                                .Item().BeginMap()
                                    .Item("name").Value("required_field")
                                    .Item("field_number").Value(2)
                                    .Item("proto_type").Value("int64")
                                .EndMap()
                            .EndList()
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
        .EndMap();

    auto config_struct_with_optional = BuildYsonNodeFluently()
        .BeginMap()
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("struct")
                            .Item("field_number").Value(1)
                            .Item("proto_type").Value("structured_message")
                            .Item("fields")
                            .BeginList()
                                .Item().BeginMap()
                                    .Item("name").Value("optional_field")
                                    .Item("field_number").Value(2)
                                    .Item("proto_type").Value("int64")
                                .EndMap()
                            .EndList()
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
        .EndMap();

    auto config_struct_with_unknown = BuildYsonNodeFluently()
        .BeginMap()
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("struct")
                            .Item("field_number").Value(1)
                            .Item("proto_type").Value("structured_message")
                            .Item("fields")
                            .BeginList()
                                .Item().BeginMap()
                                    .Item("name").Value("required_field")
                                    .Item("field_number").Value(1)
                                    .Item("proto_type").Value("int64")
                                .EndMap()
                                .Item().BeginMap()
                                    .Item("name").Value("optional_field")
                                    .Item("field_number").Value(2)
                                    .Item("proto_type").Value("int64")
                                .EndMap()
                                .Item().BeginMap()
                                    .Item("name").Value("unknown_field")
                                    .Item("field_number").Value(3)
                                    .Item("proto_type").Value("int64")
                                .EndMap()
                            .EndList()
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
        .EndMap();

    // Schema has more fields, non-optional field is missing in protobuf config.
    // Parser should fail.
    EXPECT_THROW_WITH_SUBSTRING(
        createParser(schema_struct_with_both, config_struct_with_optional),
        "non-optional field \"required_field\" in schema is missing from protobuf config");
    // Writer feels OK.
    EXPECT_NO_THROW(createWriter(schema_struct_with_both, config_struct_with_optional));

    // Schema has more fields, optional field is missing in protobuf config.
    // It's OK for both the writer and the parser.
    EXPECT_NO_THROW(createParser(schema_struct_with_both, config_struct_with_required));
    EXPECT_NO_THROW(createWriter(schema_struct_with_both, config_struct_with_required));

    // Protobuf config has more fields, it is always OK.
    EXPECT_NO_THROW(createParser(schema_struct_with_both, config_struct_with_unknown));
    EXPECT_NO_THROW(createWriter(schema_struct_with_both, config_struct_with_unknown));

    auto schema_int64 = New<TTableSchema>(std::vector<TColumnSchema>{
        {"int64_field", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
    });

    auto config_two_tables = BuildYsonNodeFluently()
        .BeginMap()
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("int64_field")
                            .Item("field_number").Value(1)
                            .Item("proto_type").Value("int64")
                        .EndMap()
                    .EndList()
                .EndMap()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("int64_field")
                            .Item("field_number").Value(1)
                            .Item("proto_type").Value("int64")
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
        .EndMap();

    EXPECT_NO_THROW(createWriter(schema_int64, config_two_tables));
    EXPECT_THROW_WITH_SUBSTRING(
        createSeveralTableWriter({schema_int64, schema_int64, schema_int64}, config_two_tables),
        "Number of schemas is greater than number of tables in protobuf config: 3 > 2");

    auto schema_variant_with_int = New<TTableSchema>(std::vector<TColumnSchema>{
        {"variant", VariantStructLogicalType({
            {"a", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        })},
    });
    auto schema_variant_with_optional_int = New<TTableSchema>(std::vector<TColumnSchema>{
        {"variant", VariantStructLogicalType({
            {"a", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        })},
    });

    auto config_with_oneof = BuildYsonNodeFluently()
        .BeginMap()
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("variant")
                            .Item("proto_type").Value("oneof")
                            .Item("fields").BeginList()
                                .Item()
                                .BeginMap()
                                    .Item("name").Value("a")
                                    .Item("field_number").Value(1)
                                    .Item("proto_type").Value("int64")
                                .EndMap()
                            .EndList()
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
        .EndMap();

    // Oneof fields require schematized columns.
    EXPECT_THROW_WITH_SUBSTRING(
        createParser(New<TTableSchema>(), config_with_oneof),
        "requires a corresponding schematized column");
    EXPECT_THROW_WITH_SUBSTRING(
        createWriter(New<TTableSchema>(), config_with_oneof),
        "requires a corresponding schematized column");

    EXPECT_THROW_WITH_SUBSTRING(
        createParser(schema_variant_with_optional_int, config_with_oneof),
        "Optional variant field \"variant.a\"");
    EXPECT_THROW_WITH_SUBSTRING(
        createWriter(schema_variant_with_optional_int, config_with_oneof),
        "Optional variant field \"variant.a\"");
    EXPECT_NO_THROW(createParser(schema_variant_with_int, config_with_oneof));
    EXPECT_NO_THROW(createWriter(schema_variant_with_int, config_with_oneof));
}

TEST(TProtobufFormat, MultipleOtherColumns)
{
    auto nameTable = New<TNameTable>();

    TString data;
    TStringOutput resultStream(data);

    auto controlAttributesConfig = New<TControlAttributesConfig>();
    controlAttributesConfig->EnableTableIndex = true;
    controlAttributesConfig->EnableEndOfStream = true;

    auto protoWriter = CreateWriterForProtobuf(
        MakeProtobufFormatConfig({TOtherColumnsMessage::descriptor(), TOtherColumnsMessage::descriptor()}),
        std::vector<TTableSchemaPtr>(2, New<TTableSchema>()),
        nameTable,
        CreateAsyncAdapter(&resultStream),
        true,
        controlAttributesConfig,
        0);

    EXPECT_EQ(true, protoWriter->Write(
        std::vector<TUnversionedRow>{
            NNamedValue::MakeRow(nameTable, {
                {TString(TableIndexColumnName), 0},
                {"field1", "foo"},
            }),
            NNamedValue::MakeRow(nameTable, {
                {TString(TableIndexColumnName), 1},
                {"field2", "bar"},
            }),
        }));

    WaitFor(protoWriter->Close())
        .ThrowOnError();

    std::vector<TString> otherColumnsValue;
    auto parser = TLenvalParser(data);
    while (auto item = parser.Next()) {
        TOtherColumnsMessage message;
        bool parsed = message.ParseFromString(item->RowData);
        EXPECT_TRUE(parsed);
        otherColumnsValue.push_back(CanonizeYson(message.other_columns_field()));
    }

    EXPECT_EQ(
        otherColumnsValue,
        std::vector<TString>({
            CanonizeYson("{field1=foo}"),
            CanonizeYson("{field2=bar}"),
        }));
}

////////////////////////////////////////////////////////////////////////////////

using TProtobufFormatAllFieldsParameter = std::tuple<int, EProtoFormatType>;
class TProtobufFormatAllFields
    : public ::testing::TestWithParam<TProtobufFormatAllFieldsParameter>
{
public:
    bool IsLegacyFormat() const
    {
        auto [rowCount, protoFormatType] = GetParam();
        return protoFormatType == EProtoFormatType::FileDescriptorLegacy;
    }
};

INSTANTIATE_TEST_SUITE_P(
    Specification,
    TProtobufFormatAllFields,
    ::testing::Values(TProtobufFormatAllFieldsParameter{1, EProtoFormatType::Structured}));

INSTANTIATE_TEST_SUITE_P(
    FileDescriptorLegacy,
    TProtobufFormatAllFields,
    ::testing::Values(TProtobufFormatAllFieldsParameter{1, EProtoFormatType::FileDescriptorLegacy}));

INSTANTIATE_TEST_SUITE_P(
    FileDescriptor,
    TProtobufFormatAllFields,
    ::testing::Values(TProtobufFormatAllFieldsParameter{1, EProtoFormatType::FileDescriptor}));

INSTANTIATE_TEST_SUITE_P(
    ManyRows,
    TProtobufFormatAllFields,
    ::testing::Values(TProtobufFormatAllFieldsParameter{50000, EProtoFormatType::Structured}));

TEST_P(TProtobufFormatAllFields, Writer)
{
    auto [rowCount, protoFormatType] = GetParam();
    auto config = CreateAllFieldsConfig(protoFormatType);

    auto nameTable = New<TNameTable>();

    auto doubleId = nameTable->RegisterName("Double");
    auto floatId = nameTable->RegisterName("Float");

    auto int64Id = nameTable->RegisterName("Int64");
    auto uint64Id = nameTable->RegisterName("UInt64");
    auto sint64Id = nameTable->RegisterName("SInt64");
    auto fixed64Id = nameTable->RegisterName("Fixed64");
    auto sfixed64Id = nameTable->RegisterName("SFixed64");

    auto int32Id = nameTable->RegisterName("Int32");
    auto uint32Id = nameTable->RegisterName("UInt32");
    auto sint32Id = nameTable->RegisterName("SInt32");
    auto fixed32Id = nameTable->RegisterName("Fixed32");
    auto sfixed32Id = nameTable->RegisterName("SFixed32");

    auto boolId = nameTable->RegisterName("Bool");
    auto stringId = nameTable->RegisterName("String");
    auto bytesId = nameTable->RegisterName("Bytes");

    auto enumId = nameTable->RegisterName("Enum");

    auto messageId = nameTable->RegisterName("Message");

    auto anyWithMapId = nameTable->RegisterName("AnyWithMap");
    auto anyWithInt64Id = nameTable->RegisterName("AnyWithInt64");
    auto anyWithStringId = nameTable->RegisterName("AnyWithString");

    auto otherInt64ColumnId = nameTable->RegisterName("OtherInt64Column");
    auto otherDoubleColumnId = nameTable->RegisterName("OtherDoubleColumn");
    auto otherStringColumnId = nameTable->RegisterName("OtherStringColumn");
    auto otherNullColumnId = nameTable->RegisterName("OtherNullColumn");
    auto otherBooleanColumnId = nameTable->RegisterName("OtherBooleanColumn");
    auto otherAnyColumnId = nameTable->RegisterName("OtherAnyColumn");

    auto tableIndexColumnId = nameTable->RegisterName(TableIndexColumnName);
    auto rowIndexColumnId = nameTable->RegisterName(RowIndexColumnName);
    auto rangeIndexColumnId = nameTable->RegisterName(RangeIndexColumnName);

    auto missintInt64Id = nameTable->RegisterName("MissingInt64");

    TString result;
    TStringOutput resultStream(result);
    auto writer = CreateWriterForProtobuf(
        config->Attributes(),
        {New<TTableSchema>()},
        nameTable,
        CreateAsyncAdapter(&resultStream),
        true,
        New<TControlAttributesConfig>(),
        0);

    TEmbeddedMessage embeddedMessage;
    embeddedMessage.set_key("embedded_key");
    embeddedMessage.set_value("embedded_value");
    TString embeddedMessageBytes;
    ASSERT_TRUE(embeddedMessage.SerializeToString(&embeddedMessageBytes));

    auto mapNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("Key").Value("Value")
            .Item("Another")
            .BeginList()
                .Item().Value(1)
                .Item().Value("two")
            .EndList()
        .EndMap();
    auto ysonString = ConvertToYsonString(mapNode).ToString();

    TUnversionedRowBuilder builder;
    for (const auto& value : {
        MakeUnversionedDoubleValue(3.14159, doubleId),
        MakeUnversionedDoubleValue(2.71828, floatId),

        MakeUnversionedInt64Value(-1, int64Id),
        MakeUnversionedUint64Value(2, uint64Id),
        MakeUnversionedInt64Value(-3, sint64Id),
        MakeUnversionedUint64Value(4, fixed64Id),
        MakeUnversionedInt64Value(-5, sfixed64Id),

        MakeUnversionedInt64Value(-6, int32Id),
        MakeUnversionedUint64Value(7, uint32Id),
        MakeUnversionedInt64Value(-8, sint32Id),
        MakeUnversionedUint64Value(9, fixed32Id),
        MakeUnversionedInt64Value(-10, sfixed32Id),

        MakeUnversionedBooleanValue(true, boolId),
        MakeUnversionedStringValue("this_is_string", stringId),
        MakeUnversionedStringValue("this_is_bytes", bytesId),

        MakeUnversionedStringValue("Two", enumId),

        MakeUnversionedStringValue(embeddedMessageBytes, messageId),

        MakeUnversionedNullValue(missintInt64Id),

        MakeUnversionedInt64Value(12, tableIndexColumnId),
        MakeUnversionedInt64Value(42, rowIndexColumnId),
        MakeUnversionedInt64Value(333, rangeIndexColumnId),
    }) {
        builder.AddValue(value);
    }

    if (!IsLegacyFormat()) {
        builder.AddValue(MakeUnversionedAnyValue(ysonString, anyWithMapId));
        builder.AddValue(MakeUnversionedInt64Value(22, anyWithInt64Id));
        builder.AddValue(MakeUnversionedStringValue("some_string", anyWithStringId));

        builder.AddValue(MakeUnversionedInt64Value(-123, otherInt64ColumnId));
        builder.AddValue(MakeUnversionedDoubleValue(-123.456, otherDoubleColumnId));
        builder.AddValue(MakeUnversionedStringValue("some_string", otherStringColumnId));
        builder.AddValue(MakeUnversionedBooleanValue(true, otherBooleanColumnId));
        builder.AddValue(MakeUnversionedAnyValue(ysonString, otherAnyColumnId));
        builder.AddValue(MakeUnversionedNullValue(otherNullColumnId));
    }

    auto row = builder.GetRow();
    std::vector<TUnversionedRow> rows(rowCount, row);
    EXPECT_EQ(true, writer->Write(rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    TStringInput input(result);
    TLenvalParser lenvalParser(&input);

    for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
        auto entry = lenvalParser.Next();
        ASSERT_TRUE(entry);

        NYT::TMessage message;
        ASSERT_TRUE(message.ParseFromString(entry->RowData));

        EXPECT_DOUBLE_EQ(message.double_field(), 3.14159);
        EXPECT_FLOAT_EQ(message.float_field(), 2.71828);
        EXPECT_EQ(message.int64_field(), -1);
        EXPECT_EQ(message.uint64_field(), 2u);
        EXPECT_EQ(message.sint64_field(), -3);
        EXPECT_EQ(message.fixed64_field(), 4u);
        EXPECT_EQ(message.sfixed64_field(), -5);

        EXPECT_EQ(message.int32_field(), -6);
        EXPECT_EQ(message.uint32_field(), 7u);
        EXPECT_EQ(message.sint32_field(), -8);
        EXPECT_EQ(message.fixed32_field(), 9u);
        EXPECT_EQ(message.sfixed32_field(), -10);

        EXPECT_EQ(message.bool_field(), true);
        EXPECT_EQ(message.string_field(), "this_is_string");
        EXPECT_EQ(message.bytes_field(), "this_is_bytes");

        EXPECT_EQ(message.enum_field(), EEnum::Two);

        EXPECT_EQ(message.message_field().key(), "embedded_key");
        EXPECT_EQ(message.message_field().value(), "embedded_value");

        if (!IsLegacyFormat()) {
            EXPECT_TRUE(AreNodesEqual(ConvertToNode(TYsonString(message.any_field_with_map())), mapNode));
            EXPECT_TRUE(AreNodesEqual(
                ConvertToNode(TYsonString(message.any_field_with_int64())),
                BuildYsonNodeFluently().Value(22)));
            EXPECT_TRUE(AreNodesEqual(
                ConvertToNode(TYsonString(message.any_field_with_string())),
                BuildYsonNodeFluently().Value("some_string")));

            auto otherColumnsMap = ConvertToNode(TYsonString(message.other_columns_field()))->AsMap();
            EXPECT_EQ(otherColumnsMap->GetChildValueOrThrow<i64>("OtherInt64Column"), -123);
            EXPECT_DOUBLE_EQ(otherColumnsMap->GetChildValueOrThrow<double>("OtherDoubleColumn"), -123.456);
            EXPECT_EQ(otherColumnsMap->GetChildValueOrThrow<TString>("OtherStringColumn"), "some_string");
            EXPECT_EQ(otherColumnsMap->GetChildValueOrThrow<bool>("OtherBooleanColumn"), true);
            EXPECT_TRUE(AreNodesEqual(otherColumnsMap->GetChildOrThrow("OtherAnyColumn"), mapNode));
            EXPECT_EQ(otherColumnsMap->GetChildOrThrow("OtherNullColumn")->GetType(), ENodeType::Entity);

            auto keys = otherColumnsMap->GetKeys();
            std::sort(keys.begin(), keys.end());
            std::vector<std::string> expectedKeys = {
                "OtherInt64Column",
                "OtherDoubleColumn",
                "OtherStringColumn",
                "OtherBooleanColumn",
                "OtherAnyColumn",
                "OtherNullColumn"
            };
            std::sort(expectedKeys.begin(), expectedKeys.end());
            EXPECT_EQ(expectedKeys, keys);
        }
    }

    ASSERT_FALSE(lenvalParser.Next());
}

TEST_P(TProtobufFormatAllFields, Parser)
{
    auto [rowCount, protoFormatType] = GetParam();

    auto config = CreateAllFieldsConfig(protoFormatType);

    TMessage message;
    message.set_double_field(3.14159);
    message.set_float_field(2.71828);

    message.set_int64_field(-1);
    message.set_uint64_field(2);
    message.set_sint64_field(-3);
    message.set_fixed64_field(4);
    message.set_sfixed64_field(-5);

    message.set_int32_field(-6);
    message.set_uint32_field(7);
    message.set_sint32_field(-8);
    message.set_fixed32_field(9);
    message.set_sfixed32_field(-10);

    message.set_bool_field(true);
    message.set_string_field("this_is_string");
    message.set_bytes_field("this_is_bytes");
    message.set_enum_field(EEnum::Three);

    message.mutable_message_field()->set_key("embedded_key");
    message.mutable_message_field()->set_value("embedded_value");

    auto mapNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("Key").Value("Value")
            .Item("Another")
            .BeginList()
                .Item().Value(1)
                .Item().Value("two")
            .EndList()
        .EndMap();

    auto otherColumnsNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("OtherInt64Column").Value(-123)
            .Item("OtherDoubleColumn").Value(-123.456)
            .Item("OtherStringColumn").Value("some_string")
            .Item("OtherBooleanColumn").Value(true)
            .Item("OtherAnyColumn").Value(mapNode)
            .Item("OtherNullColumn").Entity()
        .EndMap();

    if (!IsLegacyFormat()) {
        message.set_any_field_with_map(ConvertToYsonString(mapNode).ToString());
        message.set_any_field_with_int64(BuildYsonStringFluently().Value(22).ToString());
        message.set_any_field_with_string(BuildYsonStringFluently().Value("some_string").ToString());
        message.set_other_columns_field(ConvertToYsonString(otherColumnsNode).ToString());
    }

    auto rowCollector = ParseRows(
        message,
        ConvertTo<TProtobufFormatConfigPtr>(config->Attributes().ToMap()),
        New<TTableSchema>(),
        rowCount);

    for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
        int expectedSize = IsLegacyFormat() ? 17 : 26;
        ASSERT_EQ(static_cast<int>(rowCollector.GetRow(rowIndex).GetCount()), expectedSize);

        ASSERT_DOUBLE_EQ(GetDouble(rowCollector.GetRowValue(rowIndex, "Double")), 3.14159);
        ASSERT_NEAR(GetDouble(rowCollector.GetRowValue(rowIndex, "Float")), 2.71828, 1e-5);

        ASSERT_EQ(GetInt64(rowCollector.GetRowValue(rowIndex, "Int64")), -1);
        ASSERT_EQ(GetUint64(rowCollector.GetRowValue(rowIndex, "UInt64")), 2u);
        ASSERT_EQ(GetInt64(rowCollector.GetRowValue(rowIndex, "SInt64")), -3);
        ASSERT_EQ(GetUint64(rowCollector.GetRowValue(rowIndex, "Fixed64")), 4u);
        ASSERT_EQ(GetInt64(rowCollector.GetRowValue(rowIndex, "SFixed64")), -5);

        ASSERT_EQ(GetInt64(rowCollector.GetRowValue(rowIndex, "Int32")), -6);
        ASSERT_EQ(GetUint64(rowCollector.GetRowValue(rowIndex, "UInt32")), 7u);
        ASSERT_EQ(GetInt64(rowCollector.GetRowValue(rowIndex, "SInt32")), -8);
        ASSERT_EQ(GetUint64(rowCollector.GetRowValue(rowIndex, "Fixed32")), 9u);
        ASSERT_EQ(GetInt64(rowCollector.GetRowValue(rowIndex, "SFixed32")), -10);

        ASSERT_EQ(GetBoolean(rowCollector.GetRowValue(rowIndex, "Bool")), true);
        ASSERT_EQ(GetString(rowCollector.GetRowValue(rowIndex, "String")), "this_is_string");
        ASSERT_EQ(GetString(rowCollector.GetRowValue(rowIndex, "Bytes")), "this_is_bytes");

        if (IsLegacyFormat()) {
            ASSERT_EQ(GetInt64(rowCollector.GetRowValue(rowIndex, "Enum")), 3);
        } else {
            ASSERT_EQ(GetString(rowCollector.GetRowValue(rowIndex, "Enum")), "Three");
        }

        TEmbeddedMessage embeddedMessage;
        ASSERT_TRUE(embeddedMessage.ParseFromString(GetString(rowCollector.GetRowValue(rowIndex, "Message"))));
        ASSERT_EQ(embeddedMessage.key(), "embedded_key");
        ASSERT_EQ(embeddedMessage.value(), "embedded_value");

        if (!IsLegacyFormat()) {
            ASSERT_TRUE(AreNodesEqual(GetAny(rowCollector.GetRowValue(rowIndex, "AnyWithMap")), mapNode));
            ASSERT_EQ(GetInt64(rowCollector.GetRowValue(rowIndex, "AnyWithInt64")), 22);
            ASSERT_EQ(GetString(rowCollector.GetRowValue(rowIndex, "AnyWithString")), "some_string");

            ASSERT_EQ(GetInt64(rowCollector.GetRowValue(rowIndex, "OtherInt64Column")), -123);
            ASSERT_DOUBLE_EQ(GetDouble(rowCollector.GetRowValue(rowIndex, "OtherDoubleColumn")), -123.456);
            ASSERT_EQ(GetString(rowCollector.GetRowValue(rowIndex, "OtherStringColumn")), "some_string");
            ASSERT_EQ(GetBoolean(rowCollector.GetRowValue(rowIndex, "OtherBooleanColumn")), true);
            ASSERT_TRUE(AreNodesEqual(GetAny(rowCollector.GetRowValue(rowIndex, "OtherAnyColumn")), mapNode));
            ASSERT_EQ(rowCollector.GetRowValue(rowIndex, "OtherNullColumn").Type, EValueType::Null);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TProtobufFormatCompat
    : public ::testing::Test
{
public:
    static TTableSchemaPtr GetEarlySchema()
    {
        static const auto schema = New<TTableSchema>(std::vector<TColumnSchema>{
            {"a", OptionalLogicalType(VariantStructLogicalType({
                {"f1", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            }))},
        });
        return schema;
    }

    static TTableSchemaPtr GetFirstMiddleSchema()
    {
        static const auto schema = New<TTableSchema>(std::vector<TColumnSchema>{
            {"a", OptionalLogicalType(VariantStructLogicalType({
                {"f1", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
                {"f2", SimpleLogicalType(ESimpleLogicalValueType::String)},
            }))},
            {"b", OptionalLogicalType(StructLogicalType({
                {"x", SimpleLogicalType(ESimpleLogicalValueType::String)},
            }))},
        });
        return schema;
    }

    static TTableSchemaPtr GetSecondMiddleSchema()
    {
        static const auto schema = New<TTableSchema>(std::vector<TColumnSchema>{
            {"a", OptionalLogicalType(VariantStructLogicalType({
                {"f1", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
                {"f2", SimpleLogicalType(ESimpleLogicalValueType::String)},
            }))},
            {"b", OptionalLogicalType(StructLogicalType({
                {"x", SimpleLogicalType(ESimpleLogicalValueType::String)},
                {"y", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
            }))},
        });
        return schema;
    }

    static TTableSchemaPtr GetThirdMiddleSchema()
    {
        static const auto schema = New<TTableSchema>(std::vector<TColumnSchema>{
            {"a", OptionalLogicalType(VariantStructLogicalType({
                {"f1", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
                {"f2", SimpleLogicalType(ESimpleLogicalValueType::String)},
            }))},
            {"b", OptionalLogicalType(StructLogicalType({
                {"x", SimpleLogicalType(ESimpleLogicalValueType::String)},
                {"y", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
                {"z", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
            }))},
        });
        return schema;
    }

    static TTableSchemaPtr GetLateSchema()
    {
        static const auto schema = New<TTableSchema>(std::vector<TColumnSchema>{
            {"a", OptionalLogicalType(VariantStructLogicalType({
                {"f1", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
                {"f2", SimpleLogicalType(ESimpleLogicalValueType::String)},
                {"f3", SimpleLogicalType(ESimpleLogicalValueType::Boolean)},
            }))},
            {"c", OptionalLogicalType(ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Boolean)))},
            {"b", OptionalLogicalType(StructLogicalType({
                {"x", SimpleLogicalType(ESimpleLogicalValueType::String)},
                {"y", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
                {"z", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
            }))},
        });
        return schema;
    }

    static TProtobufFormatConfigPtr GetFirstMiddleConfig()
    {
        static const auto config = ConvertTo<TProtobufFormatConfigPtr>(BuildYsonNodeFluently()
            .BeginMap().Item("tables").BeginList().Item().BeginMap().Item("columns").BeginList()
                .Item().BeginMap()
                    .Item("name").Value("a")
                    .Item("field_number").Value(0)
                    .Item("proto_type").Value("oneof")
                    .Item("fields").BeginList()
                        .Item().BeginMap()
                            .Item("name").Value("f1")
                            .Item("field_number").Value(1)
                            .Item("proto_type").Value("int64")
                        .EndMap()
                    .EndList()
                .EndMap()
                .Item().BeginMap()
                    .Item("name").Value("b")
                    .Item("field_number").Value(2)
                    .Item("proto_type").Value("structured_message")
                    .Item("fields")
                    .BeginList()
                        .Item().BeginMap()
                            .Item("name").Value("x")
                            .Item("field_number").Value(1)
                            .Item("proto_type").Value("string")
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList().EndMap().EndList().EndMap());
        return config;
    }

    static TProtobufFormatConfigPtr GetSecondMiddleConfig()
    {
        static const auto config = ConvertTo<TProtobufFormatConfigPtr>(BuildYsonNodeFluently()
            .BeginMap().Item("tables").BeginList().Item().BeginMap().Item("columns").BeginList()
                .Item().BeginMap()
                    .Item("name").Value("a")
                    .Item("field_number").Value(0)
                    .Item("proto_type").Value("oneof")
                    .Item("fields").BeginList()
                        .Item().BeginMap()
                            .Item("name").Value("f1")
                            .Item("field_number").Value(1)
                            .Item("proto_type").Value("int64")
                        .EndMap()
                        .Item().BeginMap()
                            .Item("name").Value("f2")
                            .Item("field_number").Value(101)
                            .Item("proto_type").Value("string")
                        .EndMap()
                    .EndList()
                .EndMap()
                .Item().BeginMap()
                    .Item("name").Value("b")
                    .Item("field_number").Value(2)
                    .Item("proto_type").Value("structured_message")
                    .Item("fields")
                    .BeginList()
                        .Item().BeginMap()
                            .Item("name").Value("x")
                            .Item("field_number").Value(1)
                            .Item("proto_type").Value("string")
                        .EndMap()
                        .Item().BeginMap()
                            .Item("name").Value("y")
                            .Item("field_number").Value(2)
                            .Item("proto_type").Value("string")
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList().EndMap().EndList().EndMap());
        return config;
    }
};

template <typename TMessage>
TMessage WriteRow(
    TUnversionedRow row,
    const TProtobufFormatConfigPtr& config,
    const TTableSchemaPtr& schema,
    const TNameTablePtr& nameTable)
{
    TString result;
    TStringOutput resultStream(result);

    auto writer = CreateWriterForProtobuf(
        config,
        {schema},
        nameTable,
        CreateAsyncAdapter(&resultStream),
        true,
        New<TControlAttributesConfig>(),
        0);
    Y_UNUSED(writer->Write(std::vector<TUnversionedRow>{row}));
    writer->Close().Get().ThrowOnError();

    TStringInput input(result);
    TLenvalParser lenvalParser(&input);
    auto entry = lenvalParser.Next();
    if (!entry) {
        THROW_ERROR_EXCEPTION("Unexpected end of stream in lenval parser");
    }
    TMessage message;
    if (!message.ParseFromString(entry->RowData)) {
        THROW_ERROR_EXCEPTION("Failed to parse message");
    }
    if (lenvalParser.Next()) {
        THROW_ERROR_EXCEPTION("Unexpected entry in lenval parser");
    }
    return message;
}

TEST_F(TProtobufFormatCompat, Write)
{
    auto nameTable = TNameTable::FromSchema(*GetLateSchema());
    auto config = GetSecondMiddleConfig();

    auto writeRow = [&] (TUnversionedRow row, const TTableSchemaPtr& schema) {
        return WriteRow<NYT::TCompatMessage>(row, config, schema, nameTable);
    };

    {
        auto earlyRow = MakeRow(nameTable, {
            {"a", EValueType::Composite, "[0; -24]"}
        });

        SCOPED_TRACE("early");
        auto message = writeRow(earlyRow, GetEarlySchema());
        EXPECT_EQ(message.f1(), -24);
        EXPECT_FALSE(message.has_f2());
        EXPECT_EQ(message.has_b(), false);
    }
    {
        auto firstMiddleRow = MakeRow(nameTable, {
            {"a", EValueType::Composite, "[1; foobar]"},
            {"b", EValueType::Composite, "[foo]"},
        });

        SCOPED_TRACE("firstMiddle");
        auto message = writeRow(firstMiddleRow, GetFirstMiddleSchema());
        EXPECT_FALSE(message.has_f1());
        EXPECT_EQ(message.f2(), "foobar");
        EXPECT_EQ(message.b().x(), "foo");
        EXPECT_EQ(message.b().has_y(), false);
    }
    {
        auto secondMiddleRow = MakeRow(nameTable, {
            {"a", EValueType::Composite, "[1; foobar]"},
            {"b", EValueType::Composite, "[foo; bar]"},
        });

        SCOPED_TRACE("secondMiddle");
        auto message = writeRow(secondMiddleRow, GetSecondMiddleSchema());
        EXPECT_FALSE(message.has_f1());
        EXPECT_EQ(message.f2(), "foobar");
        EXPECT_EQ(message.b().x(), "foo");
        EXPECT_EQ(message.b().y(), "bar");
    }
    {
        auto thirdMiddleRow = MakeRow(nameTable, {
            {"a", EValueType::Composite, "[1; foobar]"},
            {"b", EValueType::Composite, "[foo; bar; spam]"},
        });

        SCOPED_TRACE("thirdMiddle");
        auto message = writeRow(thirdMiddleRow, GetThirdMiddleSchema());
        EXPECT_FALSE(message.has_f1());
        EXPECT_EQ(message.f2(), "foobar");
        EXPECT_EQ(message.b().x(), "foo");
        EXPECT_EQ(message.b().y(), "bar");
    }
    {
        auto lateRow = MakeRow(nameTable, {
            {"a", EValueType::Composite, "[2; %true]"},
            {"c", EValueType::Composite, "[%false; %true; %false]"},
            {"b", EValueType::Composite, "[foo; bar; spam]"},
        });

        SCOPED_TRACE("late");
        auto message = writeRow(lateRow, GetLateSchema());
        EXPECT_FALSE(message.has_f1());
        EXPECT_FALSE(message.has_f2());
        EXPECT_EQ(message.b().x(), "foo");
        EXPECT_EQ(message.b().y(), "bar");
    }
}

TEST_F(TProtobufFormatCompat, Parse)
{
    auto config = GetSecondMiddleConfig();

    NYT::TCompatMessage message;
    message.set_f2("Sandiego");
    message.mutable_b()->set_x("foo");
    message.mutable_b()->set_y("bar");

    {
        SCOPED_TRACE("early");
        auto collector = ParseRows(message, config, GetEarlySchema());
        EXPECT_FALSE(collector.FindRowValue(0, "a"));
        EXPECT_FALSE(collector.GetNameTable()->FindId("b"));
        EXPECT_FALSE(collector.GetNameTable()->FindId("c"));
    }
    {
        SCOPED_TRACE("firstMiddle");
        auto collector = ParseRows(message, config, GetFirstMiddleSchema());
        EXPECT_NODES_EQUAL(
            GetComposite(collector.GetRowValue(0, "a")),
            ConvertToNode(TYsonString(TStringBuf("[1;Sandiego]"))));
        EXPECT_NODES_EQUAL(GetComposite(collector.GetRowValue(0, "b")), ConvertToNode(TYsonString(TStringBuf("[foo]"))));
        EXPECT_FALSE(collector.GetNameTable()->FindId("c"));
    }
    {
        SCOPED_TRACE("secondMiddle");
        auto collector = ParseRows(message, config, GetSecondMiddleSchema());
        EXPECT_NODES_EQUAL(
            GetComposite(collector.GetRowValue(0, "a")),
            ConvertToNode(TYsonString(TStringBuf("[1;Sandiego]"))));
        EXPECT_NODES_EQUAL(GetComposite(collector.GetRowValue(0, "b")), ConvertToNode(TYsonString(TStringBuf("[foo;bar]"))));
        EXPECT_FALSE(collector.GetNameTable()->FindId("c"));
    }
    {
        SCOPED_TRACE("thirdMiddle");
        auto collector = ParseRows(message, config, GetThirdMiddleSchema());
        EXPECT_NODES_EQUAL(
            GetComposite(collector.GetRowValue(0, "a")),
            ConvertToNode(TYsonString(TStringBuf("[1;Sandiego]"))));
        EXPECT_NODES_EQUAL(GetComposite(collector.GetRowValue(0, "b")), ConvertToNode(TYsonString(TStringBuf("[foo;bar;#]"))));
        EXPECT_FALSE(collector.GetNameTable()->FindId("c"));
    }
    {
        SCOPED_TRACE("late");
        auto collector = ParseRows(message, config, GetLateSchema());
        EXPECT_NODES_EQUAL(
            GetComposite(collector.GetRowValue(0, "a")),
            ConvertToNode(TYsonString(TStringBuf("[1;Sandiego]"))));
        EXPECT_NODES_EQUAL(GetComposite(collector.GetRowValue(0, "b")), ConvertToNode(TYsonString(TStringBuf("[foo;bar;#]"))));
        EXPECT_TRUE(collector.GetNameTable()->FindId("c"));
    }
}

TEST_F(TProtobufFormatCompat, ParseWrong)
{
    NYT::TCompatMessage message;
    message.set_f1(42);
    message.mutable_b()->set_x("foo");
    message.mutable_b()->set_y("bar");

    EXPECT_THROW_WITH_SUBSTRING(
        ParseRows(message, GetFirstMiddleConfig(), GetFirstMiddleSchema()),
        "Unexpected field number 2");
}

////////////////////////////////////////////////////////////////////////////////

class TProtobufFormatEnumCompat
    : public ::testing::Test
{
public:
    static TTableSchemaPtr CreateTableSchema()
    {
        static const auto schema = New<TTableSchema>(std::vector<TColumnSchema>{
            {"optional_enum", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
            {"required_enum", SimpleLogicalType(ESimpleLogicalValueType::String)},
            {"repeated_enum", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
            {"packed_repeated_enum", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
            {"inner", OptionalLogicalType(StructLogicalType({
                {"optional_enum", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
                {"required_enum", SimpleLogicalType(ESimpleLogicalValueType::String)},
                {"repeated_enum", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
                {"packed_repeated_enum", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
            }))},
        });
        return schema;
    }
    static TProtobufFormatConfigPtr CreateProtobufFormatConfig()
    {
        static const auto config = ConvertTo<TProtobufFormatConfigPtr>(BuildYsonNodeFluently()
            .BeginMap()
            .Item("enumerations").BeginMap()
                .Item("ECompatEnum")
                .BeginMap()
                    .Item("One").Value(1)
                    .Item("Two").Value(2)
                    .Item("Three").Value(3)
                .EndMap()
            .EndMap()
            .Item("tables").BeginList().Item().BeginMap().Item("columns").BeginList()
                .Item().BeginMap()
                    .Item("name").Value("optional_enum")
                    .Item("field_number").Value(1)
                    .Item("proto_type").Value("enum_string")
                    .Item("enum_writing_mode").Value("skip_unknown_values")
                    .Item("enumeration_name").Value("ECompatEnum")
                .EndMap()
                .Item().BeginMap()
                    .Item("name").Value("required_enum")
                    .Item("field_number").Value(2)
                    .Item("proto_type").Value("enum_string")
                    .Item("enum_writing_mode").Value("skip_unknown_values")
                    .Item("enumeration_name").Value("ECompatEnum")
                .EndMap()
                .Item().BeginMap()
                    .Item("name").Value("repeated_enum")
                    .Item("field_number").Value(3)
                    .Item("proto_type").Value("enum_string")
                    .Item("repeated").Value(true)
                    .Item("enum_writing_mode").Value("skip_unknown_values")
                    .Item("enumeration_name").Value("ECompatEnum")
                .EndMap()
                .Item().BeginMap()
                    .Item("name").Value("packed_repeated_enum")
                    .Item("field_number").Value(4)
                    .Item("proto_type").Value("enum_string")
                    .Item("repeated").Value(true)
                    .Item("packed").Value(true)
                    .Item("enum_writing_mode").Value("skip_unknown_values")
                    .Item("enumeration_name").Value("ECompatEnum")
                .EndMap()
                .Item().BeginMap()
                    .Item("name").Value("inner")
                    .Item("field_number").Value(100)
                    .Item("proto_type").Value("structured_message")
                    .Item("fields").BeginList()
                        .Item().BeginMap()
                            .Item("name").Value("optional_enum")
                            .Item("field_number").Value(1)
                            .Item("proto_type").Value("enum_string")
                            .Item("enum_writing_mode").Value("skip_unknown_values")
                            .Item("enumeration_name").Value("ECompatEnum")
                        .EndMap()
                        .Item().BeginMap()
                            .Item("name").Value("required_enum")
                            .Item("field_number").Value(2)
                            .Item("proto_type").Value("enum_string")
                            .Item("enum_writing_mode").Value("skip_unknown_values")
                            .Item("enumeration_name").Value("ECompatEnum")
                        .EndMap()
                        .Item().BeginMap()
                            .Item("name").Value("repeated_enum")
                            .Item("field_number").Value(3)
                            .Item("proto_type").Value("enum_string")
                            .Item("repeated").Value(true)
                            .Item("enum_writing_mode").Value("skip_unknown_values")
                            .Item("enumeration_name").Value("ECompatEnum")
                        .EndMap()
                        .Item().BeginMap()
                            .Item("name").Value("packed_repeated_enum")
                            .Item("field_number").Value(4)
                            .Item("proto_type").Value("enum_string")
                            .Item("repeated").Value(true)
                            .Item("packed").Value(true)
                            .Item("enum_writing_mode").Value("skip_unknown_values")
                            .Item("enumeration_name").Value("ECompatEnum")
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList().EndMap().EndList().EndMap());
        return config;
    }

};

TEST_F(TProtobufFormatEnumCompat, WriteCanSkipUnknownEnumValues)
{
    auto schema = CreateTableSchema();
    auto config = CreateProtobufFormatConfig();

    auto nameTable = TNameTable::FromSchema(*schema);

    auto row = MakeRow(nameTable, {
        {"optional_enum", "MinusFortyTwo"},
        {"required_enum", "One"},
        {"repeated_enum", EValueType::Composite, "[MinusFortyTwo;One;MinusFortyTwo]"},
        {"packed_repeated_enum", EValueType::Composite, "[MinusFortyTwo;Two;MinusFortyTwo]"},
        {"inner", EValueType::Composite, "[MinusFortyTwo;Two;[MinusFortyTwo;Two];[One;MinusFortyTwo]]"},
    });

    auto collectRepeated = [] (const auto& repeated) {
        std::vector<TEnumCompat::ECompatEnum> values;
        for (auto value : repeated) {
            values.push_back(static_cast<TEnumCompat::ECompatEnum>(value));
        }
        return values;
    };

    auto message = WriteRow<TEnumCompat>(row, config, schema, nameTable);

    EXPECT_FALSE(message.has_optional_enum());
    EXPECT_EQ(message.required_enum(), TEnumCompat::One);
    EXPECT_EQ(collectRepeated(message.repeated_enum()), std::vector{TEnumCompat::One});
    EXPECT_EQ(collectRepeated(message.packed_repeated_enum()), std::vector{TEnumCompat::Two});

    ASSERT_TRUE(message.has_inner());
    EXPECT_FALSE(message.inner().has_optional_enum());
    EXPECT_EQ(message.inner().required_enum(), TEnumCompat::Two);
    EXPECT_EQ(collectRepeated(message.inner().repeated_enum()), std::vector{TEnumCompat::Two});
    EXPECT_EQ(collectRepeated(message.inner().packed_repeated_enum()), std::vector{TEnumCompat::One});
}

TEST_F(TProtobufFormatEnumCompat, WriteDoesntSkipRequiredFields)
{
    auto schema = CreateTableSchema();
    auto config = CreateProtobufFormatConfig();

    auto nameTable = TNameTable::FromSchema(*schema);

    {
        auto row = MakeRow(nameTable, {{"required_enum", "MinusFortyTwo"}});
        EXPECT_THROW_WITH_SUBSTRING(WriteRow<TEnumCompat>(row, config, schema, nameTable), "Invalid value for enum");
    }
    {
        auto row = MakeRow(nameTable, {{"inner", EValueType::Composite, "[#;MinusFortyTwo;#;#]"},});
        EXPECT_THROW_WITH_SUBSTRING(WriteRow<TEnumCompat>(row, config, schema, nameTable), "Invalid value for enum");
    }
}

////////////////////////////////////////////////////////////////////////////////

class TProtobufFormatRuntimeErrors
    : public ::testing::Test
{
public:
    static TTableSchemaPtr GetSchemaWithVariant(bool optional = false)
    {
        auto variantType = VariantStructLogicalType({
            {"f1", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"f2", SimpleLogicalType(ESimpleLogicalValueType::String)},
        });
        return New<TTableSchema>(std::vector<TColumnSchema>{
            {"a", optional ? OptionalLogicalType(variantType) : variantType},
        });
    }

    static TTableSchemaPtr GetSchemaWithStruct(bool optional = false)
    {
        auto structType = StructLogicalType({
            {"f1", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"f2", SimpleLogicalType(ESimpleLogicalValueType::String)},
        });
        return New<TTableSchema>(std::vector<TColumnSchema>{
            {"a", optional ? OptionalLogicalType(structType) : structType},
        });
    }

    static TProtobufFormatConfigPtr GetConfigWithVariant()
    {
        static const auto config = ConvertTo<TProtobufFormatConfigPtr>(BuildYsonNodeFluently()
            .BeginMap().Item("tables").BeginList().Item().BeginMap().Item("columns").BeginList()
                .Item().BeginMap()
                    .Item("name").Value("a")
                    .Item("proto_type").Value("oneof")
                    .Item("fields").BeginList()
                        .Item().BeginMap()
                            .Item("name").Value("f1")
                            .Item("field_number").Value(1)
                            .Item("proto_type").Value("int64")
                        .EndMap()
                        .Item().BeginMap()
                            .Item("name").Value("f2")
                            .Item("field_number").Value(2)
                            .Item("proto_type").Value("string")
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList().EndMap().EndList().EndMap());
        return config;
    }

    static TProtobufFormatConfigPtr GetConfigWithStruct()
    {
        static const auto config = ConvertTo<TProtobufFormatConfigPtr>(BuildYsonNodeFluently()
            .BeginMap().Item("tables").BeginList().Item().BeginMap().Item("columns").BeginList()
                .Item().BeginMap()
                    .Item("name").Value("a")
                    .Item("field_number").Value(1)
                    .Item("proto_type").Value("structured_message")
                    .Item("fields").BeginList()
                        .Item().BeginMap()
                            .Item("name").Value("f1")
                            .Item("field_number").Value(1)
                            .Item("proto_type").Value("int64")
                        .EndMap()
                        .Item().BeginMap()
                            .Item("name").Value("f2")
                            .Item("field_number").Value(2)
                            .Item("proto_type").Value("string")
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList().EndMap().EndList().EndMap());
        return config;
    }
};

TEST_F(TProtobufFormatRuntimeErrors, ParseVariant)
{
    {
        SCOPED_TRACE("Optional variant, all missing");
        TMessageWithOneof message;
        auto collector = ParseRows(message, GetConfigWithVariant(), GetSchemaWithVariant(/* optional */ true));
        EXPECT_FALSE(collector.FindRowValue(0, "a"));
    }
    {
        SCOPED_TRACE("All missing");
        TMessageWithOneof message;
        EXPECT_THROW_WITH_SUBSTRING(
            ParseRows(message, GetConfigWithVariant(), GetSchemaWithVariant()),
            "required field \"<root>.a\" is missing");
    }
    {
        SCOPED_TRACE("two alternatives");
        TMessageWithStruct::TStruct message;
        message.set_f1(5);
        message.set_f2("boo");
        EXPECT_THROW_WITH_SUBSTRING(
            ParseRows(message, GetConfigWithVariant(), GetSchemaWithVariant()),
            "multiple entries for oneof field \"<root>.a\"");
    }
}

TEST_F(TProtobufFormatRuntimeErrors, ParseStruct)
{
    {
        SCOPED_TRACE("Optional submessage missing");
        TMessageWithStruct message;
        auto collector = ParseRows(message, GetConfigWithStruct(), GetSchemaWithStruct(/* optional */ true));
        EXPECT_FALSE(collector.FindRowValue(0, "a"));
    }
    {
        SCOPED_TRACE("Required submessage missing");
        TMessageWithStruct message;
        EXPECT_THROW_WITH_SUBSTRING(
            ParseRows(message, GetConfigWithStruct(), GetSchemaWithStruct()),
            "required field \"<root>.a\" is missing");
    }
    {
        SCOPED_TRACE("All fields missing");
        TMessageWithStruct message;
        message.mutable_a();
        EXPECT_THROW_WITH_SUBSTRING(
            ParseRows(message, GetConfigWithStruct(), GetSchemaWithStruct()),
            "required field \"<root>.a.f1\" is missing");
    }
    {
        SCOPED_TRACE("Second field missing");
        TMessageWithStruct message;
        message.mutable_a()->set_f1(17);
        EXPECT_THROW_WITH_SUBSTRING(
            ParseRows(message, GetConfigWithStruct(), GetSchemaWithStruct()),
            "required field \"<root>.a.f2\" is missing");
    }
    {
        SCOPED_TRACE("All present");
        TMessageWithStruct message;
        message.mutable_a()->set_f1(17);
        message.mutable_a()->set_f2("foobar");
        auto collector = ParseRows(message, GetConfigWithStruct(), GetSchemaWithStruct());
        EXPECT_NODES_EQUAL(
            GetComposite(collector.GetRowValue(0, "a")),
            ConvertToNode(TYsonString(TStringBuf("[17;foobar]"))));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
