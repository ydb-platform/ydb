#include <yt/yt/library/logical_type_shortcuts/logical_type_shortcuts.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/resource/resource.h>

#include <util/string/split.h>

namespace NYT::NTableClient {
namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TLogicalTypeTest, TestCastToV1Type)
{
    using namespace NLogicalTypeShortcuts;

    EXPECT_EQ(
        CastToV1Type(Int64()),
        std::pair(ESimpleLogicalValueType::Int64, true));

    EXPECT_EQ(
        CastToV1Type(Optional(Int64())),
        std::pair(ESimpleLogicalValueType::Int64, false));

    EXPECT_EQ(
        CastToV1Type(Optional(Optional(Int64()))),
        std::pair(ESimpleLogicalValueType::Any, false));

    EXPECT_EQ(
        CastToV1Type(Null()),
        std::pair(ESimpleLogicalValueType::Null, false));

    EXPECT_EQ(
        CastToV1Type(Optional(Null())),
        std::pair(ESimpleLogicalValueType::Any, false));

    EXPECT_EQ(
        CastToV1Type(List(Int64())),
        std::pair(ESimpleLogicalValueType::Any, true));

    EXPECT_EQ(
        CastToV1Type(Struct("value", Int64())),
        std::pair(ESimpleLogicalValueType::Any, true));

    EXPECT_EQ(
        CastToV1Type(Tuple(Int64(), Uint64())),
        std::pair(ESimpleLogicalValueType::Any, true));

    EXPECT_EQ(
        CastToV1Type(Dict(String(), Uint64())),
        std::pair(ESimpleLogicalValueType::Any, true));

    EXPECT_EQ(
        CastToV1Type(Tagged("foo", String())),
        std::pair(ESimpleLogicalValueType::String, true));

    EXPECT_EQ(
        CastToV1Type(Tagged("foo", Optional(String()))),
        std::pair(ESimpleLogicalValueType::String, false));

    EXPECT_EQ(
        CastToV1Type(Tagged("foo", Optional(Optional(String())))),
        std::pair(ESimpleLogicalValueType::Any, false));

    EXPECT_EQ(
        CastToV1Type(TaggedLogicalType("foo", List(String()))),
        std::pair(ESimpleLogicalValueType::Any, true));

    EXPECT_EQ(
        CastToV1Type(DecimalLogicalType(3, 2)),
        std::pair(ESimpleLogicalValueType::String, true));
}

TEST(TLogicalTypeTest, DictValidationTest)
{
    EXPECT_NO_THROW(DictLogicalType(
        SimpleLogicalType(ESimpleLogicalValueType::String),
        OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any))));

    EXPECT_THROW_WITH_SUBSTRING(
        ValidateLogicalType(TComplexTypeFieldDescriptor("example_column", DictLogicalType(
            OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any)),
            OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))))),
        "is not allowed in dict key");

    EXPECT_THROW_WITH_SUBSTRING(
        ValidateLogicalType(TComplexTypeFieldDescriptor("example_column", DictLogicalType(
            ListLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any))),
            OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))))),
        "is not allowed in dict key");

    EXPECT_THROW_WITH_SUBSTRING(
        ValidateLogicalType(TComplexTypeFieldDescriptor("example_column", DictLogicalType(
            StructLogicalType({
                {"foo", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
                {"bar", SimpleLogicalType(ESimpleLogicalValueType::Any)},
            }),
            OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))))),
        "is not allowed in dict key");

    EXPECT_THROW_WITH_SUBSTRING(
        ValidateLogicalType(TComplexTypeFieldDescriptor("example_column", DictLogicalType(
            VariantStructLogicalType({
                {"foo", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
                {"bar", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any))},
            }),
            OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))))),
        "is not allowed in dict key");

    EXPECT_THROW_WITH_SUBSTRING(
        ValidateLogicalType(TComplexTypeFieldDescriptor("example_column", DictLogicalType(
            TaggedLogicalType("bar", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any))),
            OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))))),
        "is not allowed in dict key");
    EXPECT_NO_THROW(
        ValidateLogicalType(TComplexTypeFieldDescriptor("example_column", DictLogicalType(
            TaggedLogicalType("bar", SimpleLogicalType(ESimpleLogicalValueType::Int64)),
            OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))))));

    EXPECT_NO_THROW(
        ValidateLogicalType(
            TComplexTypeFieldDescriptor("example_column",
                DictLogicalType(
                    DecimalLogicalType(3, 2),
                    OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))))));
}

TEST(TLogicalTypeTest, TestDetag)
{
    EXPECT_EQ(
        *DetagLogicalType(
            TaggedLogicalType("tag", SimpleLogicalType(ESimpleLogicalValueType::String))),
        *SimpleLogicalType(ESimpleLogicalValueType::String));
    EXPECT_EQ(
        *DetagLogicalType(
            TaggedLogicalType("tag",
                StructLogicalType({
                    {"list", TaggedLogicalType("tag2", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int8)))},
                    {"tuple", TupleLogicalType({
                            SimpleLogicalType(ESimpleLogicalValueType::Double),
                            TaggedLogicalType("tag3", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))),
                        })
                    },
                }))),
        *StructLogicalType({
            {"list", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int8))},
            {"tuple", TupleLogicalType({
                    SimpleLogicalType(ESimpleLogicalValueType::Double),
                    OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String)),
                })
            },
        }));
}


static const std::vector<TLogicalTypePtr> ComplexTypeExampleList = {
    // Simple types
    SimpleLogicalType(ESimpleLogicalValueType::Null),
    SimpleLogicalType(ESimpleLogicalValueType::Int64),
    SimpleLogicalType(ESimpleLogicalValueType::String),
    SimpleLogicalType(ESimpleLogicalValueType::Utf8),
    OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)),


    // Decimals
    DecimalLogicalType(1, 0),
    DecimalLogicalType(1, 1),
    DecimalLogicalType(3, 2),
    DecimalLogicalType(35, 0),
    DecimalLogicalType(35, 35),
    OptionalLogicalType(DecimalLogicalType(1, 0)),
    OptionalLogicalType(DecimalLogicalType(1, 1)),
    OptionalLogicalType(DecimalLogicalType(3, 2)),
    OptionalLogicalType(DecimalLogicalType(35, 0)),
    OptionalLogicalType(DecimalLogicalType(35, 35)),

    // Optionals
    OptionalLogicalType(
        OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Utf8))),
    OptionalLogicalType(
        ListLogicalType(
            OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Utf8)))),

    // Lists
    ListLogicalType(
        SimpleLogicalType(ESimpleLogicalValueType::Utf8)),
    ListLogicalType(
        ListLogicalType(
            SimpleLogicalType(ESimpleLogicalValueType::Utf8))),
    ListLogicalType(
        ListLogicalType(
            SimpleLogicalType(ESimpleLogicalValueType::String))),

    // Structs
    StructLogicalType({
        {"key", SimpleLogicalType(ESimpleLogicalValueType::Utf8)},
        {"value", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Utf8))},
    }),
    StructLogicalType({
        {"value", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Utf8))},
        {"key", SimpleLogicalType(ESimpleLogicalValueType::Utf8)},
    }),
    StructLogicalType({
        {"key", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        {"value", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
    }),

    // Tuples
    TupleLogicalType({
        SimpleLogicalType(ESimpleLogicalValueType::Int64),
        OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)),
    }),
    TupleLogicalType({
        OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)),
        SimpleLogicalType(ESimpleLogicalValueType::Int64),
    }),
    TupleLogicalType({
        SimpleLogicalType(ESimpleLogicalValueType::Int64),
        SimpleLogicalType(ESimpleLogicalValueType::Int64),
    }),

    // VariantTuple
    VariantTupleLogicalType(
        {
            SimpleLogicalType(ESimpleLogicalValueType::String),
            SimpleLogicalType(ESimpleLogicalValueType::Int64),
        }),
    VariantTupleLogicalType(
        {
            SimpleLogicalType(ESimpleLogicalValueType::Int64),
            SimpleLogicalType(ESimpleLogicalValueType::String),
        }),
    VariantTupleLogicalType(
        {
            SimpleLogicalType(ESimpleLogicalValueType::String),
            SimpleLogicalType(ESimpleLogicalValueType::String),
        }),

    // VariantStruct
    VariantStructLogicalType(
        {
            {"string", SimpleLogicalType(ESimpleLogicalValueType::String)},
            {"int",    SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        }),
    VariantStructLogicalType(
        {
            {"int",    SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"string", SimpleLogicalType(ESimpleLogicalValueType::String)},
        }),
    VariantStructLogicalType(
        {
            {"string", SimpleLogicalType(ESimpleLogicalValueType::String)},
            {"string", SimpleLogicalType(ESimpleLogicalValueType::String)},
        }),

    // Dict
    DictLogicalType(
        SimpleLogicalType(ESimpleLogicalValueType::String),
        SimpleLogicalType(ESimpleLogicalValueType::Int64)),
    DictLogicalType(
        OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String)),
        SimpleLogicalType(ESimpleLogicalValueType::Int64)),
    DictLogicalType(
        OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Null)),
        OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))),

    // Tagged
    TaggedLogicalType("foo", SimpleLogicalType(ESimpleLogicalValueType::Int64)),
    TaggedLogicalType("bar", SimpleLogicalType(ESimpleLogicalValueType::Int64)),
    TaggedLogicalType("foo", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))),
    TaggedLogicalType("foo", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))),
};

TEST(TLogicalTypeTest, TestAllTypesAreInExamples)
{
    auto allMetatypes = TEnumTraits<ELogicalMetatype>::GetDomainValues();
    std::set<ELogicalMetatype> actualMetatypes;
    for (const auto& example : ComplexTypeExampleList) {
        actualMetatypes.insert(example->GetMetatype());
    }
    // This test checks that we have all top level metatypes in our ComplexTypeExampleList
    // and therefore all the tests that use this example list cover all all complex metatypes.
    EXPECT_EQ(actualMetatypes, std::set<ELogicalMetatype>(allMetatypes.begin(), allMetatypes.end()));
}

TEST(TLogicalTypeTest, TestAllExamplesHaveDifferentHash)
{
    std::map<int, TLogicalTypePtr> hashToType;
    auto hashFunction = THash<TLogicalType>();
    for (const auto& example : ComplexTypeExampleList) {
        auto hash = hashFunction(*example);
        auto it = hashToType.find(hash);
        if (it != hashToType.end()) {
            ADD_FAILURE() << Format(
                "Type %Qv and %Qv have the same hash %v",
                *it->second,
                *example,
                hash);
        } else {
            hashToType[hash] = example;
        }
    }
}

TEST(TLogicalTypeTest, TestAllExamplesAreNotEqual)
{
    // Hopefully our example list will never be so big that n^2 complexity is a problem.
    for (const auto& lhs : ComplexTypeExampleList) {
        for (const auto& rhs : ComplexTypeExampleList) {
            if (&lhs == &rhs) {
                EXPECT_EQ(*lhs, *rhs);
            } else {
                EXPECT_NE(*lhs, *rhs);
            }
        }
    }
}

TEST(TLogicalTypeTest, TestBadProtobufDeserialization)
{
    NProto::TLogicalType proto;

    TLogicalTypePtr deserializedType;
    EXPECT_THROW_WITH_SUBSTRING(
        FromProto(&deserializedType, proto),
        "Cannot parse unknown logical type from proto");
}

TEST(TLogicalTypeTest, TestIsComparable) {
    EXPECT_TRUE(IsComparable(
        OptionalLogicalType(
            OptionalLogicalType(
                SimpleLogicalType(ESimpleLogicalValueType::Int64)))));

    EXPECT_TRUE(IsComparable(
        ListLogicalType(
            SimpleLogicalType(ESimpleLogicalValueType::Int64))));

    EXPECT_TRUE(IsComparable(
        TupleLogicalType({
            SimpleLogicalType(ESimpleLogicalValueType::Int64),
            SimpleLogicalType(ESimpleLogicalValueType::String),
        })));

    EXPECT_FALSE(IsComparable(
        StructLogicalType({
            {"foo", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"bar", SimpleLogicalType(ESimpleLogicalValueType::String)},
        })));

    EXPECT_FALSE(IsComparable(
        DictLogicalType(
            SimpleLogicalType(ESimpleLogicalValueType::Int64),
            SimpleLogicalType(ESimpleLogicalValueType::String))));

    EXPECT_FALSE(IsComparable(
        VariantStructLogicalType({
            {"foo", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"bar", SimpleLogicalType(ESimpleLogicalValueType::String)},
        })));

    EXPECT_TRUE(IsComparable(
        VariantTupleLogicalType({
            SimpleLogicalType(ESimpleLogicalValueType::Int64),
            SimpleLogicalType(ESimpleLogicalValueType::String),
        })));

    EXPECT_TRUE(IsComparable(DecimalLogicalType(3, 2)));
}

TString CanonizeYsonString(TString input)
{
    auto node = ConvertToNode(TYsonString(input));
    auto binaryYson = ConvertToYsonString(node);

    TStringStream out;
    {
        TYsonWriter writer(&out, EYsonFormat::Pretty);
        ParseYsonStringBuffer(binaryYson.AsStringBuf(), EYsonType::Node, &writer);
    }
    return out.Str();
}

TString ToTypeV3(const TLogicalTypePtr& logicalType)
{
    TTypeV3LogicalTypeWrapper w{logicalType};
    auto ysonString = ConvertToYsonString(w);
    return CanonizeYsonString(ysonString.ToString());
}

class TLogicalTypeYson
    : public ::testing::TestWithParam<bool>
{
public:
    TLogicalTypePtr FromTypeV3(TString yson)
    {
        const auto parseFromNode = GetParam();
        const auto ysonStr = TYsonStringBuf(yson, EYsonType::Node);
        if (parseFromNode) {
            auto wrapper = ConvertTo<TTypeV3LogicalTypeWrapper>(ConvertTo<INodePtr>(ysonStr));
            return wrapper.LogicalType;
        } else {
            auto wrapper = ConvertTo<TTypeV3LogicalTypeWrapper>(ysonStr);
            return wrapper.LogicalType;
        }
    }
};

INSTANTIATE_TEST_SUITE_P(
    ParseFromNode,
    TLogicalTypeYson,
    ::testing::Values(true));

INSTANTIATE_TEST_SUITE_P(
    ParseFromCursor,
    TLogicalTypeYson,
    ::testing::Values(false));

#define CHECK_TYPE_V3(type, expectedYson) \
    do { \
        EXPECT_EQ(ToTypeV3(type), CanonizeYsonString(expectedYson)); \
        EXPECT_EQ(*type, *FromTypeV3(expectedYson)); \
} while(0)

TEST_P(TLogicalTypeYson, TestTypeV3Yson)
{
    // Ill formed.
    EXPECT_THROW_THAT(FromTypeV3("{}"), ::testing::AnyOf(
        ::testing::HasSubstr("has no child with key"),
        ::testing::HasSubstr("is required")));

    // Simple types.
    CHECK_TYPE_V3(SimpleLogicalType(ESimpleLogicalValueType::Int64), "int64");
    EXPECT_EQ(*SimpleLogicalType(ESimpleLogicalValueType::Int64), *FromTypeV3("{type_name=int64;}"));

    CHECK_TYPE_V3(SimpleLogicalType(ESimpleLogicalValueType::Uint64), "uint64");
    EXPECT_EQ(*SimpleLogicalType(ESimpleLogicalValueType::Uint64), *FromTypeV3("{type_name=uint64;}"));

    CHECK_TYPE_V3(SimpleLogicalType(ESimpleLogicalValueType::Double), "double");
    EXPECT_EQ(*SimpleLogicalType(ESimpleLogicalValueType::Double), *FromTypeV3("{type_name=double;}"));

    CHECK_TYPE_V3(SimpleLogicalType(ESimpleLogicalValueType::Float), "float");
    EXPECT_EQ(*SimpleLogicalType(ESimpleLogicalValueType::Float), *FromTypeV3("{type_name=float;}"));

    CHECK_TYPE_V3(SimpleLogicalType(ESimpleLogicalValueType::Boolean), "bool");
    EXPECT_EQ(*SimpleLogicalType(ESimpleLogicalValueType::Boolean), *FromTypeV3("{type_name=bool;}"));
    EXPECT_THROW_WITH_SUBSTRING(FromTypeV3("boolean"), "is not valid type_v3 simple type");
    EXPECT_THROW_WITH_SUBSTRING(FromTypeV3("{type_name=boolean}"), "is not valid type_v3 simple type");

    CHECK_TYPE_V3(SimpleLogicalType(ESimpleLogicalValueType::String), "string");
    EXPECT_EQ(*SimpleLogicalType(ESimpleLogicalValueType::String), *FromTypeV3("{type_name=string;}"));

    CHECK_TYPE_V3(SimpleLogicalType(ESimpleLogicalValueType::Null), "null");
    EXPECT_EQ(*SimpleLogicalType(ESimpleLogicalValueType::Null), *FromTypeV3("{type_name=null;}"));

    CHECK_TYPE_V3(SimpleLogicalType(ESimpleLogicalValueType::Any), "yson");
    EXPECT_EQ(*SimpleLogicalType(ESimpleLogicalValueType::Any), *FromTypeV3("{type_name=yson;}"));
    EXPECT_THROW_WITH_SUBSTRING(FromTypeV3("{type_name=any}"), "is not valid type_v3 simple type");

    CHECK_TYPE_V3(SimpleLogicalType(ESimpleLogicalValueType::Int32), "int32");
    EXPECT_EQ(*SimpleLogicalType(ESimpleLogicalValueType::Int32), *FromTypeV3("{type_name=int32;}"));

    CHECK_TYPE_V3(SimpleLogicalType(ESimpleLogicalValueType::Int16), "int16");
    EXPECT_EQ(*SimpleLogicalType(ESimpleLogicalValueType::Int16), *FromTypeV3("{type_name=int16;}"));

    CHECK_TYPE_V3(SimpleLogicalType(ESimpleLogicalValueType::Int8), "int8");
    EXPECT_EQ(*SimpleLogicalType(ESimpleLogicalValueType::Int8),  *FromTypeV3("{type_name=int8;}"));

    CHECK_TYPE_V3(SimpleLogicalType(ESimpleLogicalValueType::Uint32), "uint32");
    EXPECT_EQ(*SimpleLogicalType(ESimpleLogicalValueType::Uint32), *FromTypeV3("{type_name=uint32;}"));

    CHECK_TYPE_V3(SimpleLogicalType(ESimpleLogicalValueType::Uint16), "uint16");
    EXPECT_EQ(*SimpleLogicalType(ESimpleLogicalValueType::Uint16), *FromTypeV3("{type_name=uint16;}"));

    CHECK_TYPE_V3(SimpleLogicalType(ESimpleLogicalValueType::Uint8), "uint8");
    EXPECT_EQ(*SimpleLogicalType(ESimpleLogicalValueType::Uint8),  *FromTypeV3("{type_name=uint8;}"));

    CHECK_TYPE_V3(SimpleLogicalType(ESimpleLogicalValueType::Utf8), "utf8");
    EXPECT_EQ(*SimpleLogicalType(ESimpleLogicalValueType::Utf8), *FromTypeV3("{type_name=utf8;}"));

    CHECK_TYPE_V3(SimpleLogicalType(ESimpleLogicalValueType::Date), "date");
    EXPECT_EQ(*SimpleLogicalType(ESimpleLogicalValueType::Date), *FromTypeV3("{type_name=date;}"));

    CHECK_TYPE_V3(SimpleLogicalType(ESimpleLogicalValueType::Datetime), "datetime");
    EXPECT_EQ(*SimpleLogicalType(ESimpleLogicalValueType::Datetime), *FromTypeV3("{type_name=datetime;}"));

    CHECK_TYPE_V3(SimpleLogicalType(ESimpleLogicalValueType::Timestamp), "timestamp");
    EXPECT_EQ(*SimpleLogicalType(ESimpleLogicalValueType::Timestamp), *FromTypeV3("{type_name=timestamp;}"));

    CHECK_TYPE_V3(SimpleLogicalType(ESimpleLogicalValueType::Interval), "interval");
    EXPECT_EQ(*SimpleLogicalType(ESimpleLogicalValueType::Interval), *FromTypeV3("{type_name=interval;}"));

    CHECK_TYPE_V3(SimpleLogicalType(ESimpleLogicalValueType::Json), "json");
    EXPECT_EQ(*SimpleLogicalType(ESimpleLogicalValueType::Json), *FromTypeV3("{type_name=json;}"));

    // Optional.
    CHECK_TYPE_V3(
        OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Boolean)),
        "{type_name=optional; item=bool}");

    CHECK_TYPE_V3(
        OptionalLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any))),
        R"(
        {
            type_name=optional;
            item={
                type_name=optional;
                item=yson;
            }
        }
        )");

    EXPECT_THROW_THAT(FromTypeV3("{type_name=optional}"), ::testing::AnyOf(
        ::testing::HasSubstr("has no child with key"),
        ::testing::HasSubstr("is required")));

    // List
    CHECK_TYPE_V3(
        ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Boolean)),
        "{type_name=list; item=bool}");

    CHECK_TYPE_V3(
        ListLogicalType(ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any))),
        R"(
        {
            type_name=list;
            item={
                type_name=list;
                item=yson;
            }
        }
        )");

    EXPECT_THROW_THAT(FromTypeV3("{type_name=list}"), ::testing::AnyOf(
        ::testing::HasSubstr("has no child with key"),
        ::testing::HasSubstr("is required")));

    // Struct
    CHECK_TYPE_V3(
        StructLogicalType({
            {"a", SimpleLogicalType(ESimpleLogicalValueType::Boolean)},
            {"b", StructLogicalType({
                { "foo", SimpleLogicalType(ESimpleLogicalValueType::Boolean) },
            })},
        }),
        R"(
        {
            type_name=struct;
            members=[
                {name=a; type=bool};
                {
                    name=b;
                    type={
                        type_name=struct;
                        members=[{name=foo;type=bool}];
                    };
                };
            ];
        }
        )");

    // Tuple
    CHECK_TYPE_V3(
        TupleLogicalType({
            SimpleLogicalType(ESimpleLogicalValueType::Boolean),
            TupleLogicalType({
                SimpleLogicalType(ESimpleLogicalValueType::Boolean)
            })
        }),
        R"(
        {
            type_name=tuple;
            elements=[
                {type=bool};
                {
                    type={
                        type_name=tuple;
                        elements=[{type=bool}];
                    };
                };
            ];
        }
        )");

    // VariantStruct
    CHECK_TYPE_V3(
        VariantStructLogicalType({
            {"a", SimpleLogicalType(ESimpleLogicalValueType::Boolean)},
            {"b", VariantStructLogicalType({
                { "foo", SimpleLogicalType(ESimpleLogicalValueType::Boolean) },
            })},
        }),
        R"(
        {
            type_name=variant;
            members=[
                {name=a; type=bool};
                {
                    name=b;
                    type={
                        type_name=variant;
                        members=[{name=foo;type=bool}];
                    };
                };
            ];
        }
        )");

    // Tuple
    CHECK_TYPE_V3(
        VariantTupleLogicalType({
            SimpleLogicalType(ESimpleLogicalValueType::Boolean),
            VariantTupleLogicalType({
                SimpleLogicalType(ESimpleLogicalValueType::Boolean)
            })
        }),
        R"(
        {
            type_name=variant;
            elements=[
                {type=bool};
                {
                    type={
                        type_name=variant;
                        elements=[{type=bool}];
                    };
                };
            ];
        }
        )");

    // Dict
    CHECK_TYPE_V3(
        DictLogicalType(
            SimpleLogicalType(ESimpleLogicalValueType::Boolean),
            DictLogicalType(
                OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Boolean)),
                SimpleLogicalType(ESimpleLogicalValueType::Boolean))),
        R"(
        {
            type_name=dict;
            key=bool;
            value={
                type_name=dict;
                key={type_name=optional; item=bool};
                value=bool;
            }
        }
        )");

    // Tagged
    CHECK_TYPE_V3(
        TaggedLogicalType(
            "foo",
            TaggedLogicalType(
                "bar",
                SimpleLogicalType(ESimpleLogicalValueType::Boolean))),
        R"(
        {
            type_name=tagged;
            tag=foo;
            item={
                type_name=tagged;
                tag=bar;
                item=bool;
            }
        }
        )");

    // Decimal
    CHECK_TYPE_V3(
        DecimalLogicalType(3, 2),
        R"(
        {
            type_name=decimal;
            precision=3;
            scale=2;
        }
        )");
}

class TLogicalTypeTestExamples
    : public ::testing::TestWithParam<TLogicalTypePtr>
{ };

TEST_P(TLogicalTypeTestExamples, TestProtoSerialization)
{
    auto type = GetParam();

    NProto::TLogicalType proto;
    ToProto(&proto, type);

    TLogicalTypePtr deserializedType;
    FromProto(&deserializedType, proto);

    EXPECT_EQ(*type, *deserializedType);
}

TEST_P(TLogicalTypeTestExamples, TestYsonSerialization)
{
    auto type = GetParam();
    auto yson = ConvertToYsonString(type);
    auto deserializedType = ConvertTo<TLogicalTypePtr>(yson);
    EXPECT_EQ(*type, *deserializedType);
}

INSTANTIATE_TEST_SUITE_P(
    Examples,
    TLogicalTypeTestExamples,
    ::testing::ValuesIn(ComplexTypeExampleList));

using TCombineTypeFunc = std::function<TLogicalTypePtr(const TLogicalTypePtr&)>;

std::vector<std::pair<TString, TCombineTypeFunc>> CombineFunctions = {
    {
        "optional",
        [] (const TLogicalTypePtr& type) {
            return OptionalLogicalType(type);
        },
    },
    {
        "list",
        [] (const TLogicalTypePtr& type) {
            return ListLogicalType(type);
        },
    },
    {
        "struct",
        [] (const TLogicalTypePtr& type) {
            return StructLogicalType({{"field", type}});
        },
    },
    {
        "tuple",
        [] (const TLogicalTypePtr& type) {
            return TupleLogicalType({type});
        },
    },
    {
        "variant_struct",
        [] (const TLogicalTypePtr& type) {
            return VariantStructLogicalType({{"field", type}});
        },
    },
    {
        "variant_tuple",
        [] (const TLogicalTypePtr& type) {
            return VariantTupleLogicalType({type});
        },
    },
    {
        "dict",
        [] (const TLogicalTypePtr& type) {
            return DictLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String), type);
        },
    },
    {
        "dict-key",
        [] (const TLogicalTypePtr& type) {
            return DictLogicalType(type, SimpleLogicalType(ESimpleLogicalValueType::String));
        },
    },
    {
        "dict-value",
        [] (const TLogicalTypePtr& type) {
            return DictLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String), type);
        }
    },
    {
        "tagged",
        [] (const TLogicalTypePtr& type) {
            return TaggedLogicalType("foo", type);
        }
    }
};

TEST(TLogicalTypeTest, TestAllTypesInCombineFunctions)
{
    const auto allMetatypes = TEnumTraits<ELogicalMetatype>::GetDomainValues();
    std::set<ELogicalMetatype> actualMetatypes;
    for (const auto& [name, function] : CombineFunctions) {
        auto combined = function(SimpleLogicalType(ESimpleLogicalValueType::Int64));
        actualMetatypes.insert(combined->GetMetatype());
    }
    std::set<ELogicalMetatype> expectedMetatypes(allMetatypes.begin(), allMetatypes.end());
    expectedMetatypes.erase(ELogicalMetatype::Simple);
    expectedMetatypes.erase(ELogicalMetatype::Decimal);
    EXPECT_EQ(actualMetatypes, expectedMetatypes);
}

class TCombineLogicalMetatypeTests
    : public ::testing::TestWithParam<std::pair<TString, TCombineTypeFunc>>
{ };

INSTANTIATE_TEST_SUITE_P(
    CombineFunctions,
    TCombineLogicalMetatypeTests,
    ::testing::ValuesIn(CombineFunctions));

TEST_P(TCombineLogicalMetatypeTests, TestValidateStruct)
{
    auto badType = StructLogicalType({{"", SimpleLogicalType(ESimpleLogicalValueType::Int64)}});
    EXPECT_THROW_WITH_SUBSTRING(
        ValidateLogicalType(TComplexTypeFieldDescriptor("test-column", badType)),
        "Name of struct field #0 is empty");

    const auto& [combineName, combineFunc] = GetParam();
    const auto combinedType1 = combineFunc(badType);
    const auto combinedType2 = combineFunc(combinedType1);
    EXPECT_NE(*combinedType1, *badType);
    EXPECT_NE(*combinedType1, *combinedType2);

    EXPECT_THROW_WITH_SUBSTRING(
        ValidateLogicalType(TComplexTypeFieldDescriptor("test-column", combinedType1)),
        "Name of struct field #0 is empty");

    EXPECT_THROW_WITH_SUBSTRING(
        ValidateLogicalType(TComplexTypeFieldDescriptor("test-column", combinedType2)),
        "Name of struct field #0 is empty");
}

TEST_P(TCombineLogicalMetatypeTests, TestValidateAny)
{
    const auto& [combineName, combineFunc] = GetParam();

    if (combineName == "optional" || combineName == "dict-key") {
        // Skip tests for these combiners.
        return;
    }

    auto badType = SimpleLogicalType(ESimpleLogicalValueType::Any);
    auto goodType = OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any));
    EXPECT_THROW_WITH_SUBSTRING(
        ValidateLogicalType(TComplexTypeFieldDescriptor("test-column", badType)),
        "is disallowed outside of optional");
    EXPECT_NO_THROW(ValidateLogicalType(TComplexTypeFieldDescriptor("test-column", goodType)));

    EXPECT_THROW_WITH_SUBSTRING(
        ValidateLogicalType(TComplexTypeFieldDescriptor("test-column", combineFunc(badType))),
        "is disallowed outside of optional");
    EXPECT_NO_THROW(ValidateLogicalType(TComplexTypeFieldDescriptor("test-column", combineFunc(goodType))));
}

TEST_P(TCombineLogicalMetatypeTests, TestTrivialDetag)
{
    using namespace NLogicalTypeShortcuts;

    const auto& [combineName, combineFunc] = GetParam();
    if (combineName == "tagged") {
        // Skip test for this combiner.
        return;
    }
    const auto& logicalType = Utf8();

    const auto combinedType = combineFunc(logicalType);

    const auto detaggedType = DetagLogicalType(combinedType);
    EXPECT_EQ(*detaggedType, *combinedType);
    EXPECT_EQ(detaggedType.Get(), combinedType.Get());
}

TEST_P(TCombineLogicalMetatypeTests, TestNonTrivialDetag)
{
    using namespace NLogicalTypeShortcuts;

    const auto& [combineName, combineFunc] = GetParam();
    if (combineName == "tagged") {
        // Skip test for this combiner.
        return;
    }
    const auto& logicalType = TaggedLogicalType("foo", Utf8());

    const auto combinedType = combineFunc(logicalType);

    const auto detaggedType = DetagLogicalType(combinedType);
    const auto expectedDetaggedType = combineFunc(Utf8());

    EXPECT_EQ(*expectedDetaggedType, *detaggedType);
}

////////////////////////////////////////////////////////////////////////////////

class TStructValidationTest
    : public ::testing::TestWithParam<ELogicalMetatype>
{ };

INSTANTIATE_TEST_SUITE_P(
    Tests,
    TStructValidationTest,
    ::testing::ValuesIn({ELogicalMetatype::Struct, ELogicalMetatype::VariantStruct}));

TEST_P(TStructValidationTest, Test)
{
    auto metatype = GetParam();
    auto validate = [&] (const std::vector<TStructField>& fields) {
        std::function<TLogicalTypePtr(std::vector<TStructField> fields)> typeConstructor;
        if (metatype == ELogicalMetatype::Struct) {
            typeConstructor = StructLogicalType;
        } else {
            YT_VERIFY(metatype == ELogicalMetatype::VariantStruct);
            typeConstructor = VariantStructLogicalType;
        }
        ValidateLogicalType(TComplexTypeFieldDescriptor("test-column", typeConstructor(fields)));
    };

    EXPECT_NO_THROW(validate({}));

    EXPECT_THROW_WITH_SUBSTRING(
        validate({{"", SimpleLogicalType(ESimpleLogicalValueType::Int64)}}),
        "Name of struct field #0 is empty");

    EXPECT_THROW_WITH_SUBSTRING(
        validate({
            {"a", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"a", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        }),
        "Struct field name \"a\" is used twice");

    EXPECT_THROW_WITH_SUBSTRING(
        validate({
            {TString(257, 'a'), SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        }),
        "Name of struct field #0 exceeds limit");
    EXPECT_THROW_WITH_SUBSTRING(
        validate({
            {"\xFF", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        }),
        "Name of struct field #0 is not valid utf8");
}

////////////////////////////////////////////////////////////////////////////////

std::vector<std::vector<TString>> ParseData(TStringBuf data, int expectedFieldsCount) {
    TString noComments;
    {
        TMemoryInput in(data);
        TString line;
        while (in.ReadLine(line)) {
            if (StripString(line).StartsWith('#')) {
                continue;
            }
            noComments += line;
        }
    }

    std::vector<std::vector<TString>> result;
    for (TStringBuf record : StringSplitter(noComments).SplitByString(";;")) {
        record = StripString(record);
        if (record.Empty()) {
            continue;
        }
        std::vector<TString> fields;
        for (TStringBuf field : StringSplitter(record).SplitByString("::")) {
            fields.emplace_back(StripString(field));
        }
        if (static_cast<int>(fields.size()) != expectedFieldsCount) {
            ythrow yexception() << "Unexpected field count expected: " << expectedFieldsCount << " actual: " << fields.size();
        }
        result.push_back(fields);
    }
    return result;
}

TEST(TTestLogicalTypesWithData, GoodTypes)
{
    auto records = ParseData(NResource::Find("/types/good"), 2);

    for (const auto& record : records) {
        const auto& typeYson = record.at(0);
        // TODO(levysotsky): Remove when tz_* types are supported.
        if (typeYson.Contains("tz_")) {
            continue;
        }
        const auto& typeText = record.at(1);
        TString context = Format("text: %v\nyson: %v\n", typeText, typeYson);
        auto wrapError = [&] (const std::exception& ex) {
            return yexception() << "Unexpected error: " << ex.what() << '\n' << context;
        };

        TLogicalTypePtr type;
        try {
            type = ConvertTo<TLogicalTypePtr>(TYsonStringBuf(typeYson));
        } catch (const std::exception& ex) {
            ythrow wrapError(ex);
        }

        auto serialized = ConvertToYsonString(type);
        TLogicalTypePtr type2;
        try {
            type2 = ConvertTo<TLogicalTypePtr>(serialized);
        } catch (const std::exception& ex) {
            ythrow wrapError(ex);
        }

        EXPECT_EQ(*type, *type2);
    }
}

TEST(TTestLogicalTypesWithData, BadTypes)
{
    auto records = ParseData(NResource::Find("/types/bad"), 3);

    for (const auto& record : records) {
        const auto& typeYson = record.at(0);
        // TODO(levysotsky): Remove when tz_* types are supported.
        if (typeYson.Contains("tz_")) {
            continue;
        }

        EXPECT_THROW(ConvertTo<TLogicalTypePtr>(TYsonStringBuf(typeYson)), TErrorException);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
