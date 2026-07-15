#include <yt/yt/client/table_client/logical_type.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/tree_builder.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTableClient {
namespace {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETestEnum,
    (Value0)
    (Value1)
);

////////////////////////////////////////////////////////////////////////////////

struct TSimpleYsonStruct
    : public TYsonStruct
{
    TString MyString;
    i64 MyInt;
    std::optional<double> MyOptional;
    std::vector<TString> MyList;

    REGISTER_YSON_STRUCT(TSimpleYsonStruct);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("my_string", &TThis::MyString)
            .Default();
        registrar.Parameter("my_int", &TThis::MyInt)
            .Default();
        registrar.Parameter("my_optional", &TThis::MyOptional)
            .Default();
        registrar.Parameter("my_list", &TThis::MyList)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TSubYsonStruct
    : public TYsonStruct
{
    ui32 MyUint;
    TString MyString;

    REGISTER_YSON_STRUCT(TSubYsonStruct);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("my_uint", &TThis::MyUint)
            .Default();
        registrar.Parameter("my_string", &TThis::MyString)
            .Default();
    }
};

using TSubYsonStructPtr = TIntrusivePtr<TSubYsonStruct>;

////////////////////////////////////////////////////////////////////////////////

struct TRichYsonStruct
    : public TYsonStruct
{
    TString MyString;
    std::string MyStdString;
    bool MyBool;
    double MyDouble;
    i8 MyByte;
    ui8 MyUbyte;
    i16 MyShort;
    ui16 MyUshort;
    i32 MyInt32;
    ui32 MyUint32;
    i64 MyInt64;
    ui64 MyUint64;
    ETestEnum MyEnum;

    std::optional<TString> OptionalString;
    std::optional<i64> OptionalInt;

    std::vector<TString> StringList;
    THashMap<TString, i64> IntMap;

    TSubYsonStructPtr Sub;
    std::optional<TSubYsonStructPtr> OptionalSub;
    std::vector<TSubYsonStructPtr> SubList;
    THashMap<TString, TSubYsonStructPtr> SubMap;

    REGISTER_YSON_STRUCT(TRichYsonStruct);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("my_string", &TThis::MyString)
            .Default();
        registrar.Parameter("my_std_string", &TThis::MyStdString)
            .Default();
        registrar.Parameter("my_bool", &TThis::MyBool)
            .Default();
        registrar.Parameter("my_double", &TThis::MyDouble)
            .Default();
        registrar.Parameter("my_byte", &TThis::MyByte)
            .Default();
        registrar.Parameter("my_ubyte", &TThis::MyUbyte)
            .Default();
        registrar.Parameter("my_short", &TThis::MyShort)
            .Default();
        registrar.Parameter("my_ushort", &TThis::MyUshort)
            .Default();
        registrar.Parameter("my_int32", &TThis::MyInt32)
            .Default();
        registrar.Parameter("my_uint32", &TThis::MyUint32)
            .Default();
        registrar.Parameter("my_int64", &TThis::MyInt64)
            .Default();
        registrar.Parameter("my_uint64", &TThis::MyUint64)
            .Default();
        registrar.Parameter("my_enum", &TThis::MyEnum)
            .Default(ETestEnum::Value0);

        registrar.Parameter("optional_string", &TThis::OptionalString)
            .Default();
        registrar.Parameter("optional_int", &TThis::OptionalInt)
            .Default();

        registrar.Parameter("string_list", &TThis::StringList)
            .Default();
        registrar.Parameter("int_map", &TThis::IntMap)
            .Default();

        registrar.Parameter("sub", &TThis::Sub)
            .DefaultNew();
        registrar.Parameter("optional_sub", &TThis::OptionalSub)
            .Default();
        registrar.Parameter("sub_list", &TThis::SubList)
            .Default();
        registrar.Parameter("sub_map", &TThis::SubMap)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

INodePtr GetSchema(const TYsonStructPtr& ysonStruct)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    ysonStruct->WriteSchema(builder.get(), {});
    return builder->EndTree();
}

TEST(TYsonStructV3SchemaTest, SimpleStructConvertsToTypeV3)
{
    auto schema = GetSchema(New<TSimpleYsonStruct>());
    EXPECT_NO_THROW(ConvertTo<TTypeV3LogicalTypeWrapper>(schema));
}

TEST(TYsonStructV3SchemaTest, RichStructConvertsToTypeV3)
{
    auto schema = GetSchema(New<TRichYsonStruct>());
    EXPECT_NO_THROW(ConvertTo<TTypeV3LogicalTypeWrapper>(schema));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
