#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>
#include <ydb/public/sdk/cpp/client/ydb_types/exceptions/exceptions.h>
#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <google/protobuf/messagext.h>
#include <google/protobuf/text_format.h>

namespace NYdb {

using TExpectedErrorException = yexception;

Y_UNIT_TEST_SUITE(YdbValue) {
    Y_UNIT_TEST(ParseType1) {
        auto protoTypeStr = R"(
            struct_type {
                members {
                    name: "Member1"
                    type {
                        type_id: UINT32
                    }
                }
                members {
                    name: "Member2"
                    type {
                        list_type {
                            item {
                                type_id: STRING
                            }
                        }
                    }
                }
                members {
                    name: "Member3"
                    type {
                        tuple_type {
                            elements {
                                optional_type {
                                    item {
                                        type_id: UTF8
                                    }
                                }
                            }
                            elements {
                                decimal_type {
                                    precision: 8
                                    scale: 13
                                }
                            }
                            elements {
                                void_type: NULL_VALUE
                            }
                        }
                    }
                }
            }
        )";

        Ydb::Type protoType;
        NProtoBuf::TextFormat::ParseFromString(protoTypeStr, &protoType);

        UNIT_ASSERT_NO_DIFF(FormatType(protoType),
            R"(Struct<'Member1':Uint32,'Member2':List<String>,'Member3':Tuple<Utf8?,Decimal(8,13),Void>>)");
    }

    Y_UNIT_TEST(ParseType2) {
        auto protoTypeStr = R"(
            dict_type {
                key {
                    type_id: UINT32
                }
                payload {
                    struct_type {
                        members {
                            name: "Member1"
                            type {
                                type_id: DATE
                            }
                        }
                    }
                }
            }
        )";

        Ydb::Type protoType;
        NProtoBuf::TextFormat::ParseFromString(protoTypeStr, &protoType);

        UNIT_ASSERT_NO_DIFF(FormatType(protoType),
            R"(Dict<Uint32,Struct<'Member1':Date>>)");
    }

    Y_UNIT_TEST(ParseTaggedType) {
        auto protoTypeStr = R"(
            tagged_type {
                tag: "my_tag"
                type {
                    type_id: STRING
                }
            }
        )";

        Ydb::Type protoType;
        NProtoBuf::TextFormat::ParseFromString(protoTypeStr, &protoType);

        UNIT_ASSERT_NO_DIFF(FormatType(protoType),
            R"(Tagged<String,'my_tag'>)");
    }

    Y_UNIT_TEST(BuildTaggedType) {
        auto type = TTypeBuilder()
            .BeginTagged("my_tag")
                .BeginList()
                    .BeginOptional()
                        .Primitive(EPrimitiveType::Uint32)
                    .EndOptional()
                .EndList()
            .EndTagged()
            .Build();

        UNIT_ASSERT_NO_DIFF(FormatType(type),
            R"(Tagged<List<Uint32?>,'my_tag'>)");
    }

    Y_UNIT_TEST(BuildType) {
        auto type = TTypeBuilder()
            .BeginStruct()
            .AddMember("Member1")
                .BeginList()
                    .BeginOptional()
                        .Primitive(EPrimitiveType::Uint32)
                    .EndOptional()
                .EndList()
            .AddMember("Member2")
                .BeginDict()
                .DictKey().Primitive(EPrimitiveType::Int64)
                .DictPayload()
                    .BeginTuple()
                    .AddElement()
                        .Decimal(TDecimalType(8, 13))
                    .AddElement()
                        .Pg(TPgType("pgint2"))
                    .AddElement()
                        .BeginOptional()
                            .Primitive(EPrimitiveType::Utf8)
                        .EndOptional()
                    .EndTuple()
                .EndDict()
            .EndStruct()
            .Build();

        UNIT_ASSERT_NO_DIFF(FormatType(type),
            R"(Struct<'Member1':List<Uint32?>,'Member2':Dict<Int64,Tuple<Decimal(8,13),Pg('pgint2','',0,0,0),Utf8?>>>)");
    }

    Y_UNIT_TEST(BuildTypeReuse) {
        auto intType = TTypeBuilder()
            .Primitive(EPrimitiveType::Int32)
            .Build();

        UNIT_ASSERT_NO_DIFF(FormatType(intType),
            R"(Int32)");

        auto optIntType = TTypeBuilder()
            .Optional(intType)
            .Build();

        UNIT_ASSERT_NO_DIFF(FormatType(optIntType),
            R"(Int32?)");

        auto structType = TTypeBuilder()
            .BeginStruct()
            .AddMember("Member1", intType)
            .AddMember("Member2")
                .List(optIntType)
            .EndStruct()
            .Build();

        UNIT_ASSERT_NO_DIFF(FormatType(structType),
            R"(Struct<'Member1':Int32,'Member2':List<Int32?>>)");

        auto tupleType = TTypeBuilder()
            .BeginTuple()
            .AddElement(optIntType)
            .AddElement()
                .Primitive(EPrimitiveType::String)
            .EndTuple()
            .Build();

        UNIT_ASSERT_NO_DIFF(FormatType(tupleType),
            R"(Tuple<Int32?,String>)");

        auto type = TTypeBuilder()
            .BeginDict()
            .DictKey(tupleType)
            .DictPayload(structType)
            .EndDict()
            .Build();

        UNIT_ASSERT_NO_DIFF(FormatType(type),
            R"(Dict<Tuple<Int32?,String>,Struct<'Member1':Int32,'Member2':List<Int32?>>>)");
    }

    Y_UNIT_TEST(BuildTypeIncomplete) {
        try {
            auto value = TTypeBuilder()
                .BeginTuple()
                .AddElement()
                    .Primitive(EPrimitiveType::Uint32)
                .AddElement()
                    .Primitive(EPrimitiveType::String)
                .Build();
        } catch (const TExpectedErrorException& e) {
            return;
        }

        UNIT_ASSERT(false);
    }

    Y_UNIT_TEST(ParseValue1) {
        auto protoTypeStr = R"(
            struct_type {
                members {
                    name: "Member1"
                    type {
                        type_id: UINT32
                    }
                }
                members {
                    name: "Member2"
                    type {
                        list_type {
                            item {
                                type_id: STRING
                            }
                        }
                    }
                }
                members {
                    name: "Member3"
                    type {
                        tuple_type {
                            elements {
                                optional_type {
                                    item {
                                        type_id: UTF8
                                    }
                                }
                            }
                            elements {
                                type_id: UTF8
                            }
                            elements {
                                void_type: NULL_VALUE
                            }
                        }
                    }
                }
            }
        )";

        auto protoValueStr = R"(
            items {
                uint32_value: 137
            }
            items {
                items {
                    bytes_value: "String1"
                }
                items {
                    bytes_value: "String2"
                }
            }
            items {
                items {
                    null_flag_value: NULL_VALUE
                }
                items {
                    text_value: "UtfString"
                }
                items {
                }
            }
        )";

        Ydb::Type protoType;
        NProtoBuf::TextFormat::ParseFromString(protoTypeStr, &protoType);

        Ydb::Value protoValue;
        NProtoBuf::TextFormat::ParseFromString(protoValueStr, &protoValue);

        TValue value(TType(protoType), protoValue);

        UNIT_ASSERT_NO_DIFF(FormatValueYson(value),
            R"([137u;["String1";"String2"];[#;"UtfString";"Void"]])");
    }

    Y_UNIT_TEST(ParseValue2) {
        auto protoTypeStr = R"(
            dict_type {
                key {
                    type_id: UINT32
                }
                payload {
                    struct_type {
                        members {
                            name: "Member1"
                            type {
                                type_id: DATE
                            }
                        }
                    }
                }
            }
        )";

        auto protoValueStr = R"(
            pairs {
                key {
                    uint32_value: 10
                }
                payload {
                    items {
                        uint32_value: 1000
                    }
                }
            }
            pairs {
                key {
                    uint32_value: 20
                }
                payload {
                    items {
                        uint32_value: 2000
                    }
                }
            }
        )";

        Ydb::Type protoType;
        NProtoBuf::TextFormat::ParseFromString(protoTypeStr, &protoType);

        Ydb::Value protoValue;
        NProtoBuf::TextFormat::ParseFromString(protoValueStr, &protoValue);

        TValue value(TType(protoType), protoValue);

        UNIT_ASSERT_NO_DIFF(FormatValueYson(value),
            R"([[10u;[1000u]];[20u;[2000u]]])");
        UNIT_ASSERT_NO_DIFF(FormatValueJson(value, EBinaryStringEncoding::Unicode),
            R"([[10,{"Member1":"1972-09-27"}],[20,{"Member1":"1975-06-24"}]])");
    }

    Y_UNIT_TEST(ParseValuePg) {
        auto protoTypeStr = R"(
            struct_type {
                members {
                    name: "A"
                    type {
                        pg_type {
                            oid: 123
                            typlen: 345
                            typmod: -321
                        }
                    }
                }
                members {
                    name: "B"
                    type {
                        pg_type {
                            oid: 123
                            typlen: -345
                            typmod: 321
                        }
                    }
                }
                members {
                    name: "C"
                    type {
                        pg_type {
                            oid: 123
                            typlen: -345
                            typmod: -321
                        }
                    }
                }
                members {
                    name: "D"
                    type {
                        pg_type {
                            oid: 123
                            typlen: -1
                            typmod: -1
                        }
                    }
                }
                members {
                    name: "E"
                    type {
                        pg_type {
                            oid: 123
                            typlen: -1
                            typmod: -1
                        }
                    }
                }
            }
            )";

        auto protoValueStr = R"(
            items {
                text_value: "my_text_value"
            }
            items {
                bytes_value: "my_binary_value"
            }
            items {
                text_value: ""
            }
            items {
                bytes_value: ""
            }
            items {
            }
        )";

        Ydb::Type protoType;
        NProtoBuf::TextFormat::ParseFromString(protoTypeStr, &protoType);

        Ydb::Value protoValue;
        NProtoBuf::TextFormat::ParseFromString(protoValueStr, &protoValue);

        TValue value(TType(protoType), protoValue);

        UNIT_ASSERT_NO_DIFF(FormatValueYson(value),
            R"(["my_text_value";["my_binary_value"];"";[""];#])");
        UNIT_ASSERT_NO_DIFF(FormatValueJson(value, EBinaryStringEncoding::Unicode),
            R"({"A":"my_text_value","B":["my_binary_value"],"C":"","D":[""],"E":null})");
        UNIT_ASSERT_NO_DIFF(FormatValueJson(value, EBinaryStringEncoding::Base64),
            R"({"A":"my_text_value","B":["bXlfYmluYXJ5X3ZhbHVl"],"C":"","D":[""],"E":null})");
    }

    Y_UNIT_TEST(ParseValueMaybe) {
        auto protoTypeStr = R"(
            tuple_type {
                elements {
                    optional_type {
                        item {
                            type_id: UTF8
                        }
                    }
                }
                elements {
                    optional_type {
                        item {
                            type_id: INT8
                        }
                    }
                }
                elements {
                    optional_type {
                        item {
                            type_id: DOUBLE
                        }
                    }
                }
                elements {
                    optional_type {
                        item {
                            type_id: UINT64
                        }
                    }
                }
                elements {
                    optional_type {
                        item {
                            type_id: DYNUMBER
                        }
                    }
                }
            }
        )";

        auto protoValueStr = R"(
            items {
                text_value: "SomeUtf"
            }
            items {
                int32_value: -5
            }
            items {
                null_flag_value: NULL_VALUE
            }
            items {
                nested_value {
                    uint64_value: 7
                }
            }
            items {
                nested_value {
                    text_value: "12.345"
                }
            }
        )";

        Ydb::Type protoType;
        NProtoBuf::TextFormat::ParseFromString(protoTypeStr, &protoType);

        Ydb::Value protoValue;
        NProtoBuf::TextFormat::ParseFromString(protoValueStr, &protoValue);

        TValue value(TType(protoType), protoValue);
        TValueParser parser(value);

        parser.OpenTuple();
        UNIT_ASSERT(parser.TryNextElement());
        UNIT_ASSERT_VALUES_EQUAL(parser.GetOptionalUtf8(), "SomeUtf");
        UNIT_ASSERT(parser.TryNextElement());
        UNIT_ASSERT_VALUES_EQUAL(parser.GetOptionalInt8(), -5);
        UNIT_ASSERT(parser.TryNextElement());
        UNIT_ASSERT_VALUES_EQUAL(parser.GetOptionalDouble(), TMaybe<double>());
        UNIT_ASSERT(parser.TryNextElement());
        UNIT_ASSERT_VALUES_EQUAL(parser.GetOptionalUint64(), (ui64)7);
        UNIT_ASSERT(parser.TryNextElement());
        UNIT_ASSERT_VALUES_EQUAL(parser.GetOptionalDyNumber(), "12.345");
        parser.CloseTuple();
    }

    Y_UNIT_TEST(BuildValueIncomplete) {
        try {
            auto value = TValueBuilder()
                .BeginTuple()
                .AddElement()
                    .Uint32(10)
                .AddElement()
                    .String("test")
                .Build();
        } catch (const TExpectedErrorException& e) {
            return;
        }

        UNIT_ASSERT(false);
    }

    Y_UNIT_TEST(BuildValueListItemMismatch1) {
        try {
            auto value = TValueBuilder()
                .BeginList()
                .AddListItem()
                    .Int32(17)
                .AddListItem()
                    .Uint32(19)
                .EndList()
                .Build();
        } catch (const TExpectedErrorException& e) {
            return;
        }

        UNIT_ASSERT(false);
    }

    Y_UNIT_TEST(BuildValueListItemMismatch2) {
        auto itemValue = TValueBuilder()
            .String("Test")
            .Build();

        try {
            auto value = TValueBuilder()
                .BeginList()
                .AddListItem(itemValue)
                .AddListItem()
                    .Int32(17)
                .EndList()
                .Build();
        } catch (const TExpectedErrorException& e) {
            return;
        }

        UNIT_ASSERT(false);
    }

    Y_UNIT_TEST(BuildValueListItemMismatch3) {
        auto itemValue = TValueBuilder()
            .String("Test")
            .Build();

        try {
            auto value = TValueBuilder()
                .BeginList()
                .AddListItem()
                    .Int32(17)
                .AddListItem(itemValue)
                .EndList()
                .Build();
        } catch (const TExpectedErrorException& e) {
            return;
        }

        UNIT_ASSERT(false);
    }

    Y_UNIT_TEST(BuildValueListItemMismatch4) {
        try {
            auto value = TValueBuilder()
                .BeginList()
                .AddListItem()
                    .BeginList()
                    .AddListItem()
                        .Uint32(10)
                    .EndList()
                .AddListItem()
                    .EmptyList(TTypeBuilder().Primitive(EPrimitiveType::Uint64).Build())
                .EndList()
                .Build();
        } catch (const TExpectedErrorException& e) {
            return;
        }

        UNIT_ASSERT(false);
    }


    Y_UNIT_TEST(BuildValueEmptyListUnknown) {
        try {
            auto value = TValueBuilder()
                .EmptyList()
                .Build();
        } catch (const TExpectedErrorException& e) {
            return;
        }

        UNIT_ASSERT(false);
    }

    Y_UNIT_TEST(BuildDyNumberValue) {
        auto value = TValueBuilder()
            .DyNumber("12.345")
            .Build();

        UNIT_ASSERT_NO_DIFF(FormatValueYson(value),
            R"("12.345")");
        UNIT_ASSERT_NO_DIFF(FormatValueJson(value, EBinaryStringEncoding::Unicode),
            R"("12.345")");
    }

    Y_UNIT_TEST(BuildTaggedValue) {
        auto value = TValueBuilder()
            .BeginTagged("my_tag")
                .DyNumber("12.345")
            .EndTagged()
            .Build();

        UNIT_ASSERT_NO_DIFF(FormatValueYson(value),
            R"("12.345")");
        UNIT_ASSERT_NO_DIFF(FormatValueJson(value, EBinaryStringEncoding::Unicode),
            R"("12.345")");
    }

    Y_UNIT_TEST(BuildValueList) {
        auto intValue = TValueBuilder()
            .Int32(21)
            .Build();

        auto value = TValueBuilder()
            .BeginList()
            .AddListItem()
                .Int32(17)
            .AddListItem()
                .Int32(19)
            .AddListItem(intValue)
            .EndList()
            .Build();

        UNIT_ASSERT_NO_DIFF(FormatValueYson(value),
            R"([17;19;21])");
        UNIT_ASSERT_NO_DIFF(FormatValueJson(value, EBinaryStringEncoding::Unicode),
            R"([17,19,21])");
    }

    Y_UNIT_TEST(BuildValueListEmpty) {
        auto value = TValueBuilder()
            .EmptyList(TTypeBuilder().Primitive(EPrimitiveType::Uint32).Build())
            .Build();

        UNIT_ASSERT_NO_DIFF(FormatType(value.GetType()),
            R"(List<Uint32>)");
        UNIT_ASSERT_NO_DIFF(FormatValueYson(value),
            R"([])");
        UNIT_ASSERT_NO_DIFF(FormatValueJson(value, EBinaryStringEncoding::Unicode),
            R"([])");
    }

    Y_UNIT_TEST(BuildValueListEmpty2) {
        auto value = TValueBuilder()
            .BeginList()
            .AddListItem()
                .BeginList()
                .AddListItem()
                    .Uint32(10)
                .EndList()
            .AddListItem()
                .EmptyList()
            .EndList()
            .Build();

        UNIT_ASSERT_NO_DIFF(FormatValueYson(value),
            R"([[10u];[]])");
        UNIT_ASSERT_NO_DIFF(FormatValueJson(value, EBinaryStringEncoding::Unicode),
            R"([[10],[]])");
    }

    Y_UNIT_TEST(BuildValueListEmpty3) {
        auto value = TValueBuilder()
            .BeginList()
            .AddListItem()
                .EmptyList(TTypeBuilder().Primitive(EPrimitiveType::Uint32).Build())
            .AddListItem()
                .BeginList()
                .AddListItem()
                    .Uint32(10)
                .EndList()
            .EndList()
            .Build();

        UNIT_ASSERT_NO_DIFF(FormatValueYson(value),
            R"([[];[10u]])");
        UNIT_ASSERT_NO_DIFF(FormatValueJson(value, EBinaryStringEncoding::Unicode),
            R"([[],[10]])");
    }

    Y_UNIT_TEST(BuildValueBadCall) {
        try {
            auto value = TValueBuilder()
                .AddListItem()
                .EndList()
                .Build();
        } catch (const TExpectedErrorException& e) {
            return;
        }

        UNIT_ASSERT(false);
    }

    Y_UNIT_TEST(BuildValueOptional) {
        auto value = TValueBuilder()
            .BeginList()
            .AddListItem()
                .OptionalInt32(1)
            .AddListItem()
                .EmptyOptional()
            .AddListItem()
                .BeginOptional()
                    .Int32(57)
                .EndOptional()
            .AddListItem()
                .EmptyOptional(TTypeBuilder().Primitive(EPrimitiveType::Int32).Build())
            .EndList()
            .Build();

        UNIT_ASSERT_NO_DIFF(FormatType(value.GetType()),
            R"(List<Int32?>)");
        UNIT_ASSERT_NO_DIFF(FormatValueYson(value),
            R"([[1];#;[57];#])");
        UNIT_ASSERT_NO_DIFF(FormatValueJson(value, EBinaryStringEncoding::Unicode),
            R"([1,null,57,null])");

        auto expectedProtoValueStr =
            "items {\n"
            "  int32_value: 1\n"
            "}\n"
            "items {\n"
            "  null_flag_value: NULL_VALUE\n"
            "}\n"
            "items {\n"
            "  int32_value: 57\n"
            "}\n"
            "items {\n"
            "  null_flag_value: NULL_VALUE\n"
            "}\n";

        TString protoValueStr;
        NProtoBuf::TextFormat::PrintToString(value.GetProto(), &protoValueStr);
        UNIT_ASSERT_NO_DIFF(protoValueStr, expectedProtoValueStr);
    }

    Y_UNIT_TEST(BuildValueNestedOptional) {
        auto value = TValueBuilder()
            .BeginTuple()
            .AddElement()
                .BeginOptional()
                    .BeginOptional()
                        .Int32(10)
                    .EndOptional()
                .EndOptional()
            .AddElement()
                .BeginOptional()
                    .BeginOptional()
                        .BeginOptional()
                            .Int64(-1)
                        .EndOptional()
                    .EndOptional()
                .EndOptional()
            .AddElement()
                .BeginOptional()
                    .EmptyOptional(TTypeBuilder().Primitive(EPrimitiveType::String).Build())
                .EndOptional()
            .AddElement()
                .BeginOptional()
                    .BeginOptional()
                        .EmptyOptional(EPrimitiveType::Utf8)
                    .EndOptional()
                .EndOptional()
            .EndTuple()
            .Build();

        UNIT_ASSERT_NO_DIFF(FormatType(value.GetType()),
            R"(Tuple<Int32??,Int64???,String??,Utf8???>)");
        UNIT_ASSERT_NO_DIFF(FormatValueYson(value),
            R"([[[10]];[[[-1]]];[#];[[#]]])");
        UNIT_ASSERT_NO_DIFF(FormatValueJson(value, EBinaryStringEncoding::Unicode),
            R"([10,-1,null,null])");

        auto expectedProtoValueStr =
            "items {\n"
            "  int32_value: 10\n"
            "}\n"
            "items {\n"
            "  int64_value: -1\n"
            "}\n"
            "items {\n"
            "  nested_value {\n"
            "    null_flag_value: NULL_VALUE\n"
            "  }\n"
            "}\n"
            "items {\n"
            "  nested_value {\n"
            "    nested_value {\n"
            "      null_flag_value: NULL_VALUE\n"
            "    }\n"
            "  }\n"
            "}\n";

        TString protoValueStr;
        NProtoBuf::TextFormat::PrintToString(value.GetProto(), &protoValueStr);
        UNIT_ASSERT_NO_DIFF(protoValueStr, expectedProtoValueStr);
    }

    Y_UNIT_TEST(BuildValueOptionalMismatch1) {
        try {
            auto value = TValueBuilder()
                .BeginList()
                .AddListItem()
                    .BeginOptional()
                        .Uint32(10)
                    .EndOptional()
                .AddListItem()
                    .BeginOptional()
                        .Int32(20)
                    .EndOptional()
                .EndList()
                .Build();
        } catch (const TExpectedErrorException& e) {
            return;
        }

        UNIT_ASSERT(false);
    }

    Y_UNIT_TEST(BuildValueOptionalMismatch2) {
        try {
            auto value = TValueBuilder()
                .BeginList()
                .AddListItem()
                    .BeginOptional()
                        .Uint32(10)
                    .EndOptional()
                .AddListItem()
                    .BeginOptional()
                        .Int32(57)
                    .EndOptional()
                .EndList()
                .Build();
        } catch (const TExpectedErrorException& e) {
            return;
        }

        UNIT_ASSERT(false);
    }

    Y_UNIT_TEST(BuildValueStruct) {
        auto value = TValueBuilder()
            .BeginList()
            .AddListItem()
                .BeginStruct()
                .AddMember("Id")
                    .Uint32(1)
                .AddMember("Name")
                    .String("Anna")
                .AddMember("Value")
                    .Int32(-100)
                .AddMember("Description")
                    .EmptyOptional(EPrimitiveType::Utf8)
                .EndStruct()
            .AddListItem()
                .BeginStruct()
                .AddMember("Name")
                    .String("Paul")
                .AddMember("Value", TValueBuilder().Int32(-200).Build())
                .AddMember("Id")
                    .Uint32(2)
                .AddMember("Description")
                    .OptionalUtf8("Some details")
                .EndStruct()
            .EndList()
            .Build();

        UNIT_ASSERT_NO_DIFF(FormatValueYson(value),
            R"([[1u;"Anna";-100;#];[2u;"Paul";-200;["Some details"]]])");
        UNIT_ASSERT_NO_DIFF(
            FormatValueJson(value, EBinaryStringEncoding::Unicode),
            R"([{"Id":1,"Name":"Anna","Value":-100,"Description":null},)"
            R"({"Id":2,"Name":"Paul","Value":-200,"Description":"Some details"}])"
        );
    }

    Y_UNIT_TEST(BuildValueStructMissingMember) {
        try {
            auto value = TValueBuilder()
                .BeginList()
                .AddListItem()
                    .BeginStruct()
                    .AddMember("Id")
                        .Uint32(1)
                    .AddMember("Name")
                        .String("Anna")
                    .AddMember("Value")
                        .Int32(-100)
                    .EndStruct()
                .AddListItem()
                    .BeginStruct()
                    .AddMember("Value")
                        .Int32(-200)
                    .AddMember("Id")
                        .Uint32(2)
                    .EndStruct()
                .EndList()
                .Build();
        } catch (const TExpectedErrorException& e) {
            return;
        }

        UNIT_ASSERT(false);
    }

    Y_UNIT_TEST(BuildValueTuplePrimitives) {
        auto value = TValueBuilder()
            .BeginTuple()
            .AddElement().Bool(true)
            .AddElement().Int8(-1)
            .AddElement().Uint8(1)
            .AddElement().Int16(-2)
            .AddElement().Uint16(2)
            .AddElement().Int32(-3)
            .AddElement().Uint32(3)
            .AddElement().Int64(-4)
            .AddElement().Uint64(4)
            .AddElement().Float(-5.5)
            .AddElement().Double(6.6)
            .AddElement().Date(TInstant::Days(7))
            .AddElement().Datetime(TInstant::Seconds(8))
            .AddElement().Timestamp(TInstant::MicroSeconds(9))
            .AddElement().Interval(-10)
            .AddElement().TzDate("2018-02-02,Europe/Moscow")
            .AddElement().TzDatetime("2018-02-03T15:00:00,Europe/Moscow")
            .AddElement().TzTimestamp("2018-02-07T15:00:00,Europe/Moscow")
            .AddElement().String("TestString")
            .AddElement().Utf8("TestUtf8")
            .AddElement().Yson("[]")
            .AddElement().Json("{}")
            .EndTuple()
            .Build();

        UNIT_ASSERT_NO_DIFF(FormatType(value.GetType()),
            R"(Tuple<Bool,Int8,Uint8,Int16,Uint16,Int32,Uint32,Int64,Uint64,Float,Double,Date,Datetime,Timestamp,Interval,TzDate,TzDatetime,TzTimestamp,String,Utf8,Yson,Json>)");
        UNIT_ASSERT_NO_DIFF(FormatValueYson(value),
            R"([%true;-1;1u;-2;2u;-3;3u;-4;4u;-5.5;6.6;7u;8u;9u;-10;"2018-02-02,Europe/Moscow";"2018-02-03T15:00:00,Europe/Moscow";"2018-02-07T15:00:00,Europe/Moscow";"TestString";"TestUtf8";"[]";"{}"])");
        UNIT_ASSERT_NO_DIFF(FormatValueJson(value, EBinaryStringEncoding::Unicode),
            R"([true,-1,1,-2,2,-3,3,-4,4,-5.5,6.6,"1970-01-08","1970-01-01T00:00:08Z",)"
            R"("1970-01-01T00:00:00.000009Z",-10,"2018-02-02,Europe/Moscow",)"
            R"("2018-02-03T15:00:00,Europe/Moscow","2018-02-07T15:00:00,Europe/Moscow",)"
            R"("TestString","TestUtf8","[]","{}"])");
    }

    Y_UNIT_TEST(BuildValueTuple1) {
        auto value = TValueBuilder()
            .BeginList()
            .AddListItem()
                .BeginTuple()
                .AddElement()
                    .Int32(10)
                .AddElement()
                    .String("Str1")
                .EndTuple()
            .AddListItem()
                .BeginTuple()
                .AddElement(TValueBuilder().Int32(20).Build())
                .AddElement()
                    .String("Str2")
                .EndTuple()
            .EndList()
            .Build();

        UNIT_ASSERT_NO_DIFF(FormatValueYson(value),
            R"([[10;"Str1"];[20;"Str2"]])");
        UNIT_ASSERT_NO_DIFF(FormatValueJson(value, EBinaryStringEncoding::Unicode),
            R"([[10,"Str1"],[20,"Str2"]])");
    }

    Y_UNIT_TEST(BuildValueTuple2) {
        auto value = TValueBuilder()
            .BeginList()
            .AddListItem()
                .BeginTuple()
                .AddElement(TValueBuilder().Int32(-10).Build())
                .AddElement(TValueBuilder().Utf8("Utf1").Build())
                .EndTuple()
            .AddListItem()
                .BeginTuple()
                .AddElement()
                    .Int32(-20)
                .AddElement()
                    .Utf8("Utf2")
                .EndTuple()
            .EndList()
            .Build();

        UNIT_ASSERT_NO_DIFF(FormatValueYson(value),
            R"([[-10;"Utf1"];[-20;"Utf2"]])");
        UNIT_ASSERT_NO_DIFF(FormatValueJson(value, EBinaryStringEncoding::Unicode),
            R"([[-10,"Utf1"],[-20,"Utf2"]])");
    }

    Y_UNIT_TEST(BuildValueTupleElementsMismatch1) {
        try {
            auto value = TValueBuilder()
                .BeginList()
                .AddListItem()
                    .BeginTuple()
                    .AddElement()
                        .Int32(10)
                    .AddElement()
                        .String("Str1")
                    .EndTuple()
                .AddListItem()
                    .BeginTuple()
                    .AddElement()
                        .Int32(-20)
                    .EndTuple()
                .EndList()
                .Build();
        } catch (const TExpectedErrorException& e) {
            return;
        }

        UNIT_ASSERT(false);
    }

    Y_UNIT_TEST(BuildValueTupleElementsMismatch2) {
        try {
            auto value = TValueBuilder()
                .BeginList()
                .AddListItem()
                    .BeginTuple()
                    .AddElement()
                        .Int32(10)
                    .AddElement()
                        .String("Str1")
                    .EndTuple()
                .AddListItem()
                    .BeginTuple()
                    .AddElement()
                        .Int32(20)
                    .AddElement()
                        .String("Str2")
                    .AddElement()
                        .Uint64(1)
                    .EndTuple()
                .EndList()
                .Build();
        } catch (const TExpectedErrorException& e) {
            return;
        }

        UNIT_ASSERT(false);
    }

    Y_UNIT_TEST(BuildValueTupleTypeMismatch) {
        try {
            auto value = TValueBuilder()
                .BeginList()
                .AddListItem()
                    .BeginTuple()
                    .AddElement()
                        .Int32(10)
                    .AddElement()
                        .String("Str1")
                    .EndTuple()
                .AddListItem()
                    .BeginTuple()
                    .AddElement()
                        .Int32(20)
                    .AddElement()
                        .Utf8("Utf2")
                    .EndTuple()
                .EndList()
                .Build();
        } catch (const TExpectedErrorException& e) {
            return;
        }

        UNIT_ASSERT(false);
    }

    Y_UNIT_TEST(BuildValueDict1) {
        auto value = TValueBuilder()
            .BeginDict()
            .AddDictItem()
                .DictKey(TValueBuilder().Int32(1).Build())
                .DictPayload(TValueBuilder().String("Str1").Build())
            .AddDictItem()
                .DictKey()
                    .Int32(2)
                .DictPayload()
                    .String("Str2")
            .EndDict()
            .Build();

        UNIT_ASSERT_NO_DIFF(FormatValueYson(value),
            R"([[1;"Str1"];[2;"Str2"]])");
        UNIT_ASSERT_NO_DIFF(FormatValueJson(value, EBinaryStringEncoding::Unicode),
            R"([[1,"Str1"],[2,"Str2"]])");
    }

    Y_UNIT_TEST(BuildValueDict2) {
        auto value = TValueBuilder()
            .BeginDict()
            .AddDictItem()
                .DictKey()
                    .Int32(1)
                .DictPayload()
                    .String("Str1")
            .AddDictItem()
                .DictKey(TValueBuilder().Int32(2).Build())
                .DictPayload(TValueBuilder().String("Str2").Build())
            .EndDict()
            .Build();

        UNIT_ASSERT_NO_DIFF(FormatValueYson(value),
            R"([[1;"Str1"];[2;"Str2"]])");
        UNIT_ASSERT_NO_DIFF(FormatValueJson(value, EBinaryStringEncoding::Unicode),
            R"([[1,"Str1"],[2,"Str2"]])");
    }

    Y_UNIT_TEST(BuildValueDictTypeMismatch1) {
        try {
            auto value = TValueBuilder()
                .BeginDict()
                .AddDictItem()
                    .DictKey(TValueBuilder().Int32(1).Build())
                    .DictPayload(TValueBuilder().String("Str1").Build())
                .AddDictItem()
                    .DictKey()
                        .Int32(2)
                    .DictPayload()
                        .Utf8("Utf2")
                .EndDict()
                .Build();
        } catch (const TExpectedErrorException& e) {
            return;
        }

        UNIT_ASSERT(false);
    }

    Y_UNIT_TEST(BuildValueDictTypeMismatch2) {
        try {
            auto value = TValueBuilder()
                .BeginDict()
                .AddDictItem()
                    .DictKey()
                        .Int32(1)
                    .DictPayload()
                        .String("Str1")
                .AddDictItem()
                    .DictKey(TValueBuilder().Uint32(2).Build())
                    .DictPayload(TValueBuilder().String("Str2").Build())
                .EndDict()
                .Build();
        } catch (const TExpectedErrorException& e) {
            return;
        }

        UNIT_ASSERT(false);
    }

    Y_UNIT_TEST(BuildValueDictEmpty1) {
        auto value = TValueBuilder()
            .EmptyDict(
                TTypeBuilder().Primitive(EPrimitiveType::Uint32).Build(),
                TTypeBuilder().Primitive(EPrimitiveType::String).Build())
            .Build();

        UNIT_ASSERT_NO_DIFF(FormatType(value.GetType()),
            R"(Dict<Uint32,String>)");
        UNIT_ASSERT_NO_DIFF(FormatValueYson(value),
            R"([])");
        UNIT_ASSERT_NO_DIFF(FormatValueJson(value, EBinaryStringEncoding::Unicode),
            R"([])");
    }

    Y_UNIT_TEST(BuildValueDictEmpty2) {
        auto value = TValueBuilder()
            .BeginList()
            .AddListItem()
                .BeginDict()
                .AddDictItem()
                    .DictKey()
                        .Int32(1)
                    .DictPayload()
                        .String("Str1")
                .EndDict()
            .AddListItem()
                .EmptyDict()
            .AddListItem()
                .EmptyDict(
                    TTypeBuilder().Primitive(EPrimitiveType::Int32).Build(),
                    TTypeBuilder().Primitive(EPrimitiveType::String).Build())
            .EndList()
            .Build();

        UNIT_ASSERT_NO_DIFF(FormatType(value.GetType()),
            R"(List<Dict<Int32,String>>)");
        UNIT_ASSERT_NO_DIFF(FormatValueYson(value),
            R"([[[1;"Str1"]];[];[]])");
        UNIT_ASSERT_NO_DIFF(FormatValueJson(value, EBinaryStringEncoding::Unicode),
            R"([[[1,"Str1"]],[],[]])");
    }

    Y_UNIT_TEST(BuildValueDictEmptyNoType) {
        try {
            auto value = TValueBuilder()
                .BeginList()
                .AddListItem()
                    .EmptyDict()
                .EndList()
                .Build();
        } catch (const TExpectedErrorException& e) {
            return;
        }

        UNIT_ASSERT(false);
    }

    Y_UNIT_TEST(BuildValueDictEmptyTypeMismatch) {
        try {
            auto value = TValueBuilder()
                .BeginList()
                .AddListItem()
                    .EmptyDict(
                        TTypeBuilder().Primitive(EPrimitiveType::Int32).Build(),
                        TTypeBuilder().Primitive(EPrimitiveType::String).Build())
                .AddListItem()
                    .EmptyDict(
                        TTypeBuilder().Primitive(EPrimitiveType::Int32).Build(),
                        TTypeBuilder().Primitive(EPrimitiveType::Utf8).Build())
                .EndList()
                .Build();
        } catch (const TExpectedErrorException& e) {
            return;
        }

        UNIT_ASSERT(false);
    }

    Y_UNIT_TEST(BuildValueWithType) {
        auto type = TTypeBuilder()
            .BeginTuple()
            .AddElement()
                .BeginStruct()
                .AddMember("Name")
                    .Primitive(EPrimitiveType::String)
                .AddMember("Value")
                    .Primitive(EPrimitiveType::Uint64)
                .EndStruct()
            .AddElement()
                .BeginOptional()
                    .Primitive(EPrimitiveType::Utf8)
                .EndOptional()
            .AddElement()
                .BeginList()
                    .Primitive(EPrimitiveType::Bool)
                .EndList()
            .AddElement()
                .BeginDict()
                .DictKey()
                    .Primitive(EPrimitiveType::Int32)
                .DictPayload()
                    .BeginOptional()
                        .Primitive(EPrimitiveType::Uint8)
                    .EndOptional()
                .EndDict()
            .AddElement()
                .BeginOptional()
                    .Primitive(EPrimitiveType::DyNumber)
                .EndOptional()
            .EndTuple()
            .Build();

        auto value = TValueBuilder(type)
            .BeginTuple()
            .AddElement()
                .BeginStruct()
                .AddMember("Value")
                    .Uint64(1)
                .AddMember("Name")
                    .String("Sergey")
                .EndStruct()
            .AddElement()
                .EmptyOptional()
            .AddElement()
                .BeginList()
                .AddListItem()
                    .Bool(true)
                .EndList()
            .AddElement()
                .BeginDict()
                .AddDictItem()
                    .DictKey()
                        .Int32(10)
                    .DictPayload()
                        .EmptyOptional()
                .EndDict()
            .AddElement()
                .BeginOptional()
                    .DyNumber("12.345")
                .EndOptional()
            .EndTuple()
            .Build();

        UNIT_ASSERT_NO_DIFF(FormatType(value.GetType()),
            R"(Tuple<Struct<'Name':String,'Value':Uint64>,Utf8?,List<Bool>,Dict<Int32,Uint8?>,DyNumber?>)");
        UNIT_ASSERT_NO_DIFF(FormatValueYson(value),
            R"([["Sergey";1u];#;[%true];[[10;#]];["12.345"]])");
        UNIT_ASSERT_NO_DIFF(FormatValueJson(value, EBinaryStringEncoding::Unicode),
            R"([{"Name":"Sergey","Value":1},null,[true],[[10,null]],"12.345"])");
    }

    Y_UNIT_TEST(CorrectUuid) {
        TString uuidStr = "5ca32c22-841b-11e8-adc0-fa7ae01bbebc";
        TUuidValue uuid(uuidStr);
        UNIT_ASSERT_VALUES_EQUAL(uuidStr, uuid.ToString());
    }

    Y_UNIT_TEST(IncorrectUuid) {
        UNIT_ASSERT_EXCEPTION(TUuidValue(""), TContractViolation);
        UNIT_ASSERT_EXCEPTION(TUuidValue("0123456789abcdef0123456789abcdef0123456789abcdef"), TContractViolation);
        UNIT_ASSERT_EXCEPTION(TUuidValue("5ca32c22+841b-11e8-adc0-fa7ae01bbebc"), TContractViolation);
        UNIT_ASSERT_EXCEPTION(TUuidValue("5ca32-c22841b-11e8-adc0-fa7ae01bbebc"), TContractViolation);
    }

}

} // namespace NYdb
