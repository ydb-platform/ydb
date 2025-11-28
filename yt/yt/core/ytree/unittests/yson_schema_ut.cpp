#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/tree_builder.h>
#include <yt/yt/core/ytree/yson_struct.h>
#include <yt/yt/core/ytree/yson_schema.h>

#include <yt/yt/core/ytree/unittests/proto/test.pb.h>

#include <yt/yt/client/table_client/logical_type.h>

namespace NYT::NYTree {
namespace {

using namespace NYson;
using NTableClient::TTypeV3LogicalTypeWrapper;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETestEnum,
    (Value0)
    (Value1)
);

////////////////////////////////////////////////////////////////////////////////

struct TTestSubStruct
    : public TYsonStruct
{
    ui32 MyUint;

    REGISTER_YSON_STRUCT(TTestSubStruct);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("my_uint", &TThis::MyUint)
            .Default();
    }
};

using TTestSubStructPtr = TIntrusivePtr<TTestSubStruct>;

////////////////////////////////////////////////////////////////////////////////

struct TTestSubStructLite
    : public TYsonStructLite
{
    i32 MyInt;

    REGISTER_YSON_STRUCT_LITE(TTestSubStructLite);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("my_int", &TThis::MyInt)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRefCountedEntity final
{ };

using TRefCountedEntityPtr = TIntrusivePtr<TRefCountedEntity>;

void Serialize(const TRefCountedEntityPtr& /*entity*/, NYson::IYsonConsumer* /*consumer*/)
{ }

void Deserialize(const TRefCountedEntity& /*entity*/, NYTree::INodePtr /*node*/)
{ }

void Deserialize(const TRefCountedEntity& /*entity*/, TYsonPullParserCursor* /*pullParser*/)
{ }

struct TTestStructWithRequiredParameters
    : public virtual TYsonStruct
{
    TRefCountedEntityPtr MyPtr;
    TTestSubStructPtr Sub;
    REGISTER_YSON_STRUCT(TTestStructWithRequiredParameters);
    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("my_ptr", &TThis::MyPtr);
        registrar.Parameter("sub", &TThis::Sub);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestSubStructWithPtrAndString
    : public virtual TYsonStruct
{
    TRefCountedEntityPtr MyPtr;
    TString MyStr;

    REGISTER_YSON_STRUCT(TTestSubStructWithPtrAndString);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("my_ptr", &TThis::MyPtr);
        registrar.Parameter("my_str", &TThis::MyStr)
            .Default("Default");
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestYsonStruct
    : public TYsonStruct
{
    TString MyString;
    std::string MyStdString;
    TTestSubStructPtr Sub;
    std::vector<TTestSubStructLite> SubList;
    std::vector<TString> MyStringList;
    std::unordered_map<TString, int> IntMap;
    std::optional<i64> NullableInt;
    ETestEnum MyEnum;
    unsigned int MyUint;
    bool MyBool;
    char MyChar;
    i8 MyByte;
    ui8 MyUbyte;
    i16 MyShort;
    ui16 MyUshort;

    REGISTER_YSON_STRUCT(TTestYsonStruct);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("my_string", &TThis::MyString);
        registrar.Parameter("my_std_string", &TThis::MyStdString);
        registrar.Parameter("sub", &TThis::Sub)
            .DefaultNew();
        registrar.Parameter("sub_list", &TThis::SubList)
            .Default();
        registrar.Parameter("int_map", &TThis::IntMap)
            .Default();
        registrar.Parameter("my_string_list", &TThis::MyStringList)
            .Default();
        registrar.Parameter("nullable_int", &TThis::NullableInt)
            .Default();
        registrar.Parameter("my_uint", &TThis::MyUint)
            .Default();
        registrar.Parameter("my_bool", &TThis::MyBool)
            .Default();
        registrar.Parameter("my_char", &TThis::MyChar)
            .Default();
        registrar.Parameter("my_byte", &TThis::MyByte)
            .Default();
        registrar.Parameter("my_ubyte", &TThis::MyUbyte)
            .Default();
        registrar.Parameter("my_short", &TThis::MyShort)
            .Default();
        registrar.Parameter("my_ushort", &TThis::MyUshort)
            .Default();
        registrar.Parameter("my_enum", &TThis::MyEnum)
            .Default(ETestEnum::Value1);

        registrar.Preprocessor([] (TThis* ysonStruct) {
            ysonStruct->Sub->MyUint = 8;
        });
    }
};

using TTestYsonStructPtr = TIntrusivePtr<TTestYsonStruct>;

////////////////////////////////////////////////////////////////////////////////

struct TTestStructWithProtobuf
    : public TYsonStruct
{
    std::optional<NProto::TTestMessage> MyMessage;

    REGISTER_YSON_STRUCT(TTestStructWithProtobuf);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("my_message", &TThis::MyMessage)
            .Optional();
    }
};

struct TTestStructWithProtobufAsYson
    : public TYsonStruct
{
    std::optional<TProtoSerializedAsYson<NProto::TTestMessage>> MyMessage;

    REGISTER_YSON_STRUCT(TTestStructWithProtobufAsYson);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("my_message", &TThis::MyMessage)
            .Optional();
    }
};

struct TTestStructWithProtobufAsString
    : public TYsonStruct
{
    std::optional<TProtoSerializedAsString<NProto::TTestMessage>> MyMessage;

    REGISTER_YSON_STRUCT(TTestStructWithProtobufAsString);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("my_message", &TThis::MyMessage)
            .Optional();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TCustomType
{
    int Value;
};

void Serialize(TCustomType value, NYT::NYson::IYsonConsumer* consumer)
{
    consumer->OnInt64Scalar(value.Value);
}

void Deserialize(TCustomType& value, NYT::NYTree::INodePtr node)
{
    value.Value = node->GetValue<int>();
}

struct TTestStructWithCustomType
    : public TYsonStruct
{
    TCustomType MyType;

    REGISTER_YSON_STRUCT(TTestStructWithCustomType);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("my_type", &TThis::MyType)
            .Optional();
    }
};

struct TTestStructWithUndefinedType
    : public TYsonStruct
{
    NYT::NYTree::TYsonStructPtr UndefinedTypeField;

    REGISTER_YSON_STRUCT(TTestStructWithUndefinedType);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("undefined_type_field", &TThis::UndefinedTypeField)
            .Optional();
    }
};

struct TTestStructWithTuples
    : public TYsonStruct
{
    std::tuple<TString, ui64, double> Tuple;
    std::pair<TString, TString> Pair;

    REGISTER_YSON_STRUCT(TTestStructWithTuples)

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("tuple", &TThis::Tuple);
        registrar.Parameter("pair", &TThis::Pair);
    }
};

struct TTestStructWithArray
    : public TYsonStruct
{
    std::array<TString, 3> StringArray;

    REGISTER_YSON_STRUCT(TTestStructWithArray)

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("string_array", &TThis::StringArray);
    }
};

YT_DEFINE_STRONG_TYPEDEF(TStringTypedef, TString);
YT_DEFINE_STRONG_TYPEDEF(TIntTypedef, i64);

struct TTestStructWithStrongTypedef
    : public TYsonStruct
{
    TStringTypedef StringTypedef;
    TString String;
    TIntTypedef IntTypedef;
    i64 Int;

    REGISTER_YSON_STRUCT(TTestStructWithStrongTypedef)

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("string_typedef", &TThis::StringTypedef)
            .Default();
        registrar.Parameter("string", &TThis::String)
            .Default();
        registrar.Parameter("int_typedef", &TThis::IntTypedef)
            .Default();
        registrar.Parameter("int", &TThis::Int)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

IMapNodePtr GetSchema(const TYsonStructPtr& ysonStruct, const TYsonStructWriteSchemaOptions& options = {})
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    ysonStruct->WriteSchema(builder.get(), options);
    return builder->EndTree()->AsMap();
}

void CheckSchema(const TYsonStructPtr& ysonStruct, TStringBuf expected, const TYsonStructWriteSchemaOptions& options = {})
{
    auto actualNode = GetSchema(ysonStruct, options);
    auto expectedNode = ConvertToNode(TYsonStringBuf(expected), GetEphemeralNodeFactory());
    EXPECT_TRUE(AreNodesEqual(expectedNode, actualNode))
        << "Expected: " << ConvertToYsonString(expectedNode, EYsonFormat::Text, 4).AsStringBuf() << "\n\n"
        << "Actual: " << ConvertToYsonString(actualNode, EYsonFormat::Text, 4).AsStringBuf() << "\n\n";

    EXPECT_NO_THROW(ConvertTo<TTypeV3LogicalTypeWrapper>(actualNode));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonStructSchemaTest, TestYsonStruct)
{
    CheckSchema(
        New<TTestYsonStruct>(),
        R"({type_name="struct";
            members=[
                {name="my_string";type="string";required=%true;};
                {name="my_std_string";type="string";required=%true;};
                {
                    name="sub";
                    type={type_name="optional";item={type_name="struct";members=[{name="my_uint";type="uint32";}]}};
                };
                {
                    name="sub_list";
                    type={type_name="list";item={type_name="struct";members=[{name="my_int";type="int32";}]}}
                };
                {name="int_map";type={type_name="dict";key="string";value="int32";}};
                {name="my_string_list";type={type_name="list";item="string";}};
                {name="nullable_int";type={type_name="optional";item="int64";}};
                {name="my_uint";type="uint32";};
                {name="my_bool";type="bool";};
                {name="my_char";type="int8";};
                {name="my_byte";type="int8";};
                {name="my_ubyte";type="uint8";};
                {name="my_short";type="int16";};
                {name="my_ushort";type="uint16";};
                {name="my_enum";type={type_name="tagged";tag="enum/ETestEnum";item="string";enum=["value0";"value1";]}};
            ];})");
}

TEST(TYsonStructSchemaTest, TestSchemaForProtobuf)
{
    CheckSchema(
        New<TTestStructWithProtobuf>(),
        R"({
            type_name="struct";
            members=[
                {
                    name="my_message";
                    type={
                        type_name="optional";
                        item={
                            type_name="struct";
                            members=[
                                {name="int32_field";type="int32";};
                                {name="string_field";"type"="utf8";};
                            ];
                        };
                    };
                };
            ];})");
}

TEST(TYsonStructSchemaTest, TestSchemaForProtobufAsYson)
{
    CheckSchema(
        New<TTestStructWithProtobufAsYson>(),
        R"({
            type_name="struct";
            members=[
                {
                    name="my_message";
                    type={
                        type_name="optional";
                        item={
                            type_name="struct";
                            members=[
                                {name="int32_field";type="int32";};
                                {name="string_field";"type"="utf8";};
                            ];
                        };
                    };
                };
            ];})");
}

TEST(TYsonStructSchemaTest, TestSchemaForProtobufAsString)
{
    CheckSchema(
        New<TTestStructWithProtobufAsString>(),
        R"({
            type_name="struct";
            members=[
                {
                    name="my_message";
                    type={
                        "type_name"="optional";
                        "item"="string";
                    };
                };
            ];})");
}

TEST(TYsonStructSchemaTest, TestYsonStructWithCustomType)
{
    CheckSchema(
        New<TTestStructWithCustomType>(),
        R"({type_name="struct";
            members=[
                {name="my_type";type="int64";};
            ]})");
}

TEST(TYsonStructSchemaTest, TestYsonStructWithTuples)
{
    CheckSchema(
        New<TTestStructWithTuples>(),
        R"({type_name="struct";
            members=[
                {
                    name="tuple";
                    required=%true;
                    type={
                        type_name="tuple";
                        elements=[{"type"="string"};{"type"="uint64"};{"type"="double"};];
                    };
                };
                {
                    name="pair";
                    required=%true;
                    type={
                        type_name="tuple";
                        elements=[{"type"="string"};{"type"="string"};];
                    };
                };
            ]})");
}

TEST(TYsonStructSchemaTest, TestYsonStructWithArray)
{
    CheckSchema(
        New<TTestStructWithArray>(),
        R"({type_name="struct";
            members=[
                {
                    name="string_array";
                    required=%true;
                    type={
                        type_name="tuple";
                        elements=[{"type"="string"};{"type"="string"};{"type"="string"};];
                    };
                };
            ]})");
}

////////////////////////////////////////////////////////////////////////////////

IMapNodePtr GetMember(IListNodePtr members, TStringBuf name)
{
    for (auto child : members->GetChildren()) {
        if (child->AsMap()->template GetChildValueOrThrow<TString>("name") == name) {
            return child->AsMap();
        }
    }
    return nullptr;
}

INodePtr GetDefault(IMapNodePtr member, TStringBuf name)
{
    return GetMember(member->GetChildValueOrThrow<IListNodePtr>("members"), name)
        ->FindChild("default_value");
}

bool SourceLocationContains(IMapNodePtr member, TStringBuf substring)
{
    return member->template GetChildValueOrThrow<TString>("source_location_file_name").Contains(substring);
}

bool CppTypeNameContains(IMapNodePtr member, TStringBuf substring)
{
    return member->template GetChildValueOrThrow<TString>("cpp_type_name").Contains(substring);
}

IMapNodePtr UnwrapMember(IMapNodePtr member)
{
    return member
        ->template GetChildValueOrThrow<IMapNodePtr>("type")
        ->template GetChildValueOrThrow<IMapNodePtr>("item");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonStructSchemaTest, TestDefaultValues)
{
    {
        auto schema = GetSchema(New<TTestYsonStruct>(), {.AddDefaultValues = true});
        auto description = Format("Schema: %v", ConvertToYsonString(schema, EYsonFormat::Pretty));

        EXPECT_EQ(GetDefault(schema, "my_enum")->GetValue<TString>(), "value1") << description;
        EXPECT_FALSE(GetDefault(schema, "my_std_string")) << description;
        EXPECT_FALSE(GetDefault(schema, "nullable_int")) << description;
        EXPECT_EQ(GetDefault(schema, "my_string_list")->GetType(), ENodeType::List) << description;

        EXPECT_EQ(GetDefault(UnwrapMember(GetMember(schema->FindChild("members")->AsList(), "sub")), "my_uint")->GetValue<ui64>(), 0u) << description;
        EXPECT_EQ(GetDefault(schema, "sub")->AsMap()->template GetChildValueOrThrow<ui64>("my_uint"), 8u) << description;
    }

    {
        auto schema = GetSchema(New<TTestStructWithProtobuf>(), {.AddDefaultValues = true});
        auto description = Format("Schema: %v", ConvertToYsonString(schema, EYsonFormat::Pretty));
        auto members = schema->FindChild("members")->AsList();

        EXPECT_EQ(GetDefault(UnwrapMember(GetMember(members, "my_message")), "string_field")->GetValue<TString>(), "string_field_default") << description;
    }
}

TEST(TYsonStructSchemaTest, TestDefaultValuesWithRequiredParameter)
{
    auto schema = GetSchema(New<TTestStructWithRequiredParameters>(), {.AddDefaultValues = true});
    auto description = Format("Schema: %v", ConvertToYsonString(schema, EYsonFormat::Pretty));
    EXPECT_FALSE(GetDefault(schema, "my_ptr")) << description;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonStructSchemaTest, TestSourceLocation)
{
    {
        auto schema = GetSchema(New<TTestYsonStruct>(), {.AddSourceLocation = true});
        auto description = Format("Schema: %v", ConvertToYsonString(schema, EYsonFormat::Pretty));
        auto members = schema->FindChild("members")->AsList();

        EXPECT_TRUE(SourceLocationContains(schema, "yson_schema_ut.cpp")) << description;

        auto subMember = GetMember(members, "sub")
            ->template GetChildValueOrThrow<IMapNodePtr>("type")
            ->template GetChildValueOrThrow<IMapNodePtr>("item");
        EXPECT_TRUE(SourceLocationContains(subMember, "yson_schema_ut.cpp")) << description;
    }

    {
        auto schema = GetSchema(New<TTestStructWithProtobuf>(), {.AddSourceLocation = true});
        auto description = Format("Schema: %v", ConvertToYsonString(schema, EYsonFormat::Pretty));
        auto members = schema->FindChild("members")->AsList();

        auto subMember = GetMember(members, "my_message")
            ->template GetChildValueOrThrow<IMapNodePtr>("type")
            ->template GetChildValueOrThrow<IMapNodePtr>("item");
        EXPECT_TRUE(SourceLocationContains(subMember, "test.proto")) << description;
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonStructSchemaTest, CppTypeName)
{
    {
        auto schema = GetSchema(New<TTestYsonStruct>(), {.AddCppTypeNames = true});
        auto description = Format("Schema: %v", ConvertToYsonString(schema, EYsonFormat::Pretty));
        auto members = schema->FindChild("members")->AsList();

        EXPECT_TRUE(CppTypeNameContains(schema, "TTestYsonStruct")) << description;

        // Cpp type name is duplicated when struct field is not trivial.
        // Upper level is required for trivial types. Lower level is required for nested arrays/maps/etc.
        EXPECT_TRUE(CppTypeNameContains(GetMember(members, "my_enum"), "ETestEnum")) << description;
        EXPECT_TRUE(
            CppTypeNameContains(
                GetMember(members, "my_enum")->template GetChildValueOrThrow<IMapNodePtr>("type"),
                "ETestEnum")) << description;

        EXPECT_TRUE(
            GetMember(members, "my_enum")
                ->template GetChildValueOrThrow<TString>("containing_struct_cpp_type_name")
                .Contains("TTestYsonStruct")) << description;

        EXPECT_TRUE(
            CppTypeNameContains(
                GetMember(members, "nullable_int")->template GetChildValueOrThrow<IMapNodePtr>("type"),
                "optional")) << description;

        EXPECT_TRUE(
            CppTypeNameContains(
                GetMember(members, "sub_list")->template GetChildValueOrThrow<IMapNodePtr>("type"),
                "vector")) << description;

        EXPECT_TRUE(
            CppTypeNameContains(
                GetMember(members, "sub")->template GetChildValueOrThrow<IMapNodePtr>("type"),
                "TIntrusivePtr")) << description;

    }

    {
        auto schema = GetSchema(New<TTestStructWithProtobuf>(), {.AddCppTypeNames = true});
        auto description = Format("Schema: %v", ConvertToYsonString(schema, EYsonFormat::Pretty));
        auto members = schema->FindChild("members")->AsList();

        auto subMember = GetMember(members, "my_message")
            ->template GetChildValueOrThrow<IMapNodePtr>("type")
            ->template GetChildValueOrThrow<IMapNodePtr>("item");
        EXPECT_TRUE(CppTypeNameContains(subMember, "TTestMessage")) << description;
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonStructSchemaTest, StrongTypedef)
{
    CheckSchema(
        New<TTestStructWithStrongTypedef>(),
        R"({
            type_name="struct";
            members=[
                {
                    name="string_typedef";
                    type="string";
                };
                {
                    name="string";
                    type="string";
                };
                {
                    name="int_typedef";
                    type="int64";
                };
                {
                    name="int";
                    type="int64";
                };
            ];})");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

template <>
void WriteSchema<TRefCountedEntity>(NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& /*options*/)
{
    BuildYsonFluently(consumer)
        .Value("null");
}

////////////////////////////////////////////////////////////////////////////////

template <>
void WriteSchema<TCustomType>(NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& /*options*/)
{
    BuildYsonFluently(consumer)
        .Value("int64");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
