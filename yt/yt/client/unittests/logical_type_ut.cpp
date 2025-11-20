#include <yt/yt/library/logical_type_shortcuts/logical_type_shortcuts.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/resource/resource.h>

#include <util/string/split.h>

namespace NYT::NTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NLogicalTypeShortcuts;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void ValidateComplexLogicalType(TLogicalTypePtr type)
{
    return ValidateLogicalType(TComplexTypeFieldDescriptor("example_column", std::move(type)));
}

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
        CastToV1Type(Tagged("foo", List(String()))),
        std::pair(ESimpleLogicalValueType::Any, true));

    EXPECT_EQ(
        CastToV1Type(Decimal(3, 2)),
        std::pair(ESimpleLogicalValueType::String, true));
}

TEST(TLogicalTypeTest, DictValidationTest)
{
    EXPECT_NO_THROW(Dict(String(), Optional(Yson())));

    EXPECT_THROW_WITH_SUBSTRING(
        ValidateComplexLogicalType(Dict(Optional(Yson()), Optional(String()))),
        "is not allowed in dict key");

    EXPECT_THROW_WITH_SUBSTRING(
        ValidateComplexLogicalType(Dict(
            List(Optional(Yson())),
            Optional(String()))),
        "is not allowed in dict key");

    EXPECT_THROW_WITH_SUBSTRING(
        ValidateComplexLogicalType(Dict(
            StructLogicalType({
                {"foo", Int64()},
                {"bar", Yson()},
            }),
            Optional(String()))),
        "is not allowed in dict key");

    EXPECT_THROW_WITH_SUBSTRING(
        ValidateComplexLogicalType(Dict(
            VariantStructLogicalType({
                {"foo", Int64()},
                {"bar", Optional(Yson())},
            }),
            Optional(String()))),
        "is not allowed in dict key");

    EXPECT_THROW_WITH_SUBSTRING(
        ValidateComplexLogicalType(Dict(
            Tagged("bar", Optional(Yson())),
            Optional(String()))),
        "is not allowed in dict key");
    EXPECT_NO_THROW(
        ValidateComplexLogicalType(Dict(
            Tagged("bar", Int64()),
            Optional(String()))));

    EXPECT_NO_THROW(ValidateComplexLogicalType(Dict(
        Decimal(3, 2),
        Optional(String()))));
}

TEST(TLogicalTypeTest, TestDetag)
{
    EXPECT_EQ(
        *DetagLogicalType(
            Tagged("tag", String())),
        *String());
    EXPECT_EQ(
        *DetagLogicalType(
            Tagged("tag",
                StructLogicalType({
                    {"list", Tagged("tag2", List(Int8()))},
                    {"tuple", Tuple(Double(), Tagged("tag3", Optional(String())))},
                }))),
        *StructLogicalType({
            {"list", List(Int8())},
            {"tuple", Tuple(Double(), Optional(String()))},
        }));
}

const std::vector<TLogicalTypePtr> ComplexTypeExampleList = {
    // Simple types
    Null(),
    Int64(),
    String(),
    Utf8(),
    Optional(Int64()),

    // Decimals
    Decimal(1, 0),
    Decimal(1, 1),
    Decimal(3, 2),
    Decimal(35, 0),
    Decimal(35, 35),
    Optional(Decimal(1, 0)),
    Optional(Decimal(1, 1)),
    Optional(Decimal(3, 2)),
    Optional(Decimal(35, 0)),
    Optional(Decimal(35, 35)),

    // Optionals
    Optional(Optional(Utf8())),
    Optional(List(Optional(Utf8()))),

    // Lists
    List(Utf8()),
    List(List(Utf8())),
    List(List(String())),

    // Structs
    StructLogicalType({
        {"key", Utf8()},
        {"value", List(Utf8())},
    }),
    StructLogicalType({
        {"value", List(Utf8())},
        {"key", Utf8()},
    }),
    StructLogicalType({
        {"key", Int64()},
        {"value", List(Int64())},
    }),

    // Tuples
    Tuple(Int64(), Optional(Int64())),
    Tuple(Optional(Int64()), Int64()),
    Tuple(Int64(), Int64()),

    // VariantTuple
    VariantTuple(String(), Int64()),
    VariantTuple(Int64(), String()),
    VariantTuple(String(), String()),

    // VariantStruct
    VariantStructLogicalType({
        {"string", String()},
        {"int", Int64()},
    }),
    VariantStructLogicalType({
        {"int", Int64()},
        {"string", String()},
    }),
    VariantStructLogicalType({
        {"string", String()},
        {"string", String()},
    }),

    // Dict
    Dict(String(), Int64()),
    Dict(Optional(String()), Int64()),
    Dict(Optional(Null()), Optional(String())),

    // Tagged
    Tagged("foo", Int64()),
    Tagged("bar", Int64()),
    Tagged("foo", Optional(Int64())),
    Tagged("foo", List(Int64())),
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
    EXPECT_TRUE(IsComparable(Optional(Optional(Int64()))));

    EXPECT_TRUE(IsComparable(List(Int64())));
    EXPECT_FALSE(IsComparable(Dict(Int64(), String())));

    EXPECT_TRUE(IsComparable(Tuple(Int64(), String())));
    EXPECT_TRUE(IsComparable(VariantTuple(Int64(), String())));

    EXPECT_FALSE(IsComparable(Struct("foo", Int64(), "bar", String())));
    EXPECT_FALSE(IsComparable(VariantStruct("foo", Int64(), "bar", String())));

    EXPECT_TRUE(IsComparable(Decimal(3, 2)));
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
    CHECK_TYPE_V3(Int64(), "int64");
    EXPECT_EQ(*Int64(), *FromTypeV3("{type_name=int64;}"));

    CHECK_TYPE_V3(Uint64(), "uint64");
    EXPECT_EQ(*Uint64(), *FromTypeV3("{type_name=uint64;}"));

    CHECK_TYPE_V3(Double(), "double");
    EXPECT_EQ(*Double(), *FromTypeV3("{type_name=double;}"));

    CHECK_TYPE_V3(Float(), "float");
    EXPECT_EQ(*Float(), *FromTypeV3("{type_name=float;}"));

    CHECK_TYPE_V3(Bool(), "bool");
    EXPECT_EQ(*Bool(), *FromTypeV3("{type_name=bool;}"));
    EXPECT_THROW_WITH_SUBSTRING(FromTypeV3("boolean"), "is not valid type_v3 simple type");
    EXPECT_THROW_WITH_SUBSTRING(FromTypeV3("{type_name=boolean}"), "is not valid type_v3 simple type");

    CHECK_TYPE_V3(String(), "string");
    EXPECT_EQ(*String(), *FromTypeV3("{type_name=string;}"));

    CHECK_TYPE_V3(Null(), "null");
    EXPECT_EQ(*Null(), *FromTypeV3("{type_name=null;}"));

    CHECK_TYPE_V3(Yson(), "yson");
    EXPECT_EQ(*Yson(), *FromTypeV3("{type_name=yson;}"));
    EXPECT_THROW_WITH_SUBSTRING(FromTypeV3("{type_name=any}"), "is not valid type_v3 simple type");

    CHECK_TYPE_V3(Int32(), "int32");
    EXPECT_EQ(*Int32(), *FromTypeV3("{type_name=int32;}"));

    CHECK_TYPE_V3(Int16(), "int16");
    EXPECT_EQ(*Int16(), *FromTypeV3("{type_name=int16;}"));

    CHECK_TYPE_V3(Int8(), "int8");
    EXPECT_EQ(*Int8(),  *FromTypeV3("{type_name=int8;}"));

    CHECK_TYPE_V3(Uint32(), "uint32");
    EXPECT_EQ(*Uint32(), *FromTypeV3("{type_name=uint32;}"));

    CHECK_TYPE_V3(Uint16(), "uint16");
    EXPECT_EQ(*Uint16(), *FromTypeV3("{type_name=uint16;}"));

    CHECK_TYPE_V3(Uint8(), "uint8");
    EXPECT_EQ(*Uint8(),  *FromTypeV3("{type_name=uint8;}"));

    CHECK_TYPE_V3(Utf8(), "utf8");
    EXPECT_EQ(*Utf8(), *FromTypeV3("{type_name=utf8;}"));

    CHECK_TYPE_V3(Date(), "date");
    EXPECT_EQ(*Date(), *FromTypeV3("{type_name=date;}"));

    CHECK_TYPE_V3(Datetime(), "datetime");
    EXPECT_EQ(*Datetime(), *FromTypeV3("{type_name=datetime;}"));

    CHECK_TYPE_V3(Timestamp(), "timestamp");
    EXPECT_EQ(*Timestamp(), *FromTypeV3("{type_name=timestamp;}"));

    CHECK_TYPE_V3(Interval(), "interval");
    EXPECT_EQ(*Interval(), *FromTypeV3("{type_name=interval;}"));

    CHECK_TYPE_V3(Json(), "json");
    EXPECT_EQ(*Json(), *FromTypeV3("{type_name=json;}"));

    // Optional.
    CHECK_TYPE_V3(
        Optional(Bool()),
        "{type_name=optional; item=bool}");

    CHECK_TYPE_V3(
        Optional(Optional(Yson())),
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
        List(Bool()),
        "{type_name=list; item=bool}");

    CHECK_TYPE_V3(
        List(List(Yson())),
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
            {"a", Bool()},
            {"b", StructLogicalType({
                { "foo", Bool() },
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
    CHECK_TYPE_V3(Tuple(Bool(), Tuple(Bool())),
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
            {"a", Bool()},
            {"b", VariantStructLogicalType({
                { "foo", Bool() },
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
        VariantTuple(Bool(), VariantTuple(Bool())),
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
    CHECK_TYPE_V3(Dict(Bool(), Dict(Optional(Bool()), Bool())),
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
        Tagged("foo", Tagged("bar", Bool())),
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
        Decimal(3, 2),
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
            return Optional(type);
        },
    },
    {
        "list",
        [] (const TLogicalTypePtr& type) {
            return List(type);
        },
    },
    {
        "struct",
        [] (const TLogicalTypePtr& type) {
            return Struct("field", type);
        },
    },
    {
        "tuple",
        [] (const TLogicalTypePtr& type) {
            return Tuple(type);
        },
    },
    {
        "variant_struct",
        [] (const TLogicalTypePtr& type) {
            return VariantStruct("field", type);
        },
    },
    {
        "variant_tuple",
        [] (const TLogicalTypePtr& type) {
            return VariantTuple(type);
        },
    },
    {
        "dict",
        [] (const TLogicalTypePtr& type) {
            return Dict(String(), type);
        },
    },
    {
        "dict-key",
        [] (const TLogicalTypePtr& type) {
            return Dict(type, String());
        },
    },
    {
        "dict-value",
        [] (const TLogicalTypePtr& type) {
            return Dict(String(), type);
        }
    },
    {
        "tagged",
        [] (const TLogicalTypePtr& type) {
            return Tagged("foo", type);
        }
    }
};

TEST(TLogicalTypeTest, TestAllTypesInCombineFunctions)
{
    const auto allMetatypes = TEnumTraits<ELogicalMetatype>::GetDomainValues();
    std::set<ELogicalMetatype> actualMetatypes;
    for (const auto& [name, function] : CombineFunctions) {
        auto combined = function(Int64());
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
    auto badType = StructLogicalType({{"", Int64()}});

    EXPECT_THROW_WITH_SUBSTRING(
        ValidateComplexLogicalType(badType),
        Format("Name of struct field #0 is empty"));

    const auto& [combineName, combineFunc] = GetParam();
    const auto combinedType1 = combineFunc(badType);
    const auto combinedType2 = combineFunc(combinedType1);
    EXPECT_NE(*combinedType1, *badType);
    EXPECT_NE(*combinedType1, *combinedType2);

    EXPECT_THROW_WITH_SUBSTRING(
        ValidateComplexLogicalType(combinedType1),
        Format("Name of struct field #0 is empty"));

    EXPECT_THROW_WITH_SUBSTRING(
        ValidateComplexLogicalType(combinedType2),
        Format("Name of struct field #0 is empty"));
}

TEST_P(TCombineLogicalMetatypeTests, TestValidateAny)
{
    const auto& [combineName, combineFunc] = GetParam();
    if (combineName == "optional" || combineName == "dict-key") {
        // Skip tests for these combiners.
        return;
    }

    auto badType = Yson();
    auto goodType = Optional(Yson());
    EXPECT_THROW_WITH_SUBSTRING(
        ValidateComplexLogicalType(badType),
        "is disallowed outside of optional");
    EXPECT_NO_THROW(ValidateComplexLogicalType(goodType));

    EXPECT_THROW_WITH_SUBSTRING(
        ValidateComplexLogicalType(combineFunc(badType)),
        "is disallowed outside of optional");
    EXPECT_NO_THROW(ValidateComplexLogicalType(combineFunc(goodType)));
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
    const auto& logicalType = Tagged("foo", Utf8());

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
        if (record.empty()) {
            continue;
        }
        std::vector<TString> fields;
        for (TStringBuf field : StringSplitter(record).SplitByString("::")) {
            fields.emplace_back(StripString(field));
        }
        if (std::ssize(fields) != expectedFieldsCount) {
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
