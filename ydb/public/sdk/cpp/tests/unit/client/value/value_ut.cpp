#include <gtest/gtest.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/type_switcher.h>
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/exceptions/exceptions.h>
#include <google/protobuf/text_format.h>

namespace NYdb {

using TExpectedErrorException = yexception;

void CheckProtoValue(const Ydb::Value& value, const TString& expected) {
    TStringType protoStr;
    google::protobuf::TextFormat::PrintToString(value, &protoStr);
    ASSERT_EQ(protoStr, expected);
}

TEST(YdbValue, ParseType1) {
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
    google::protobuf::TextFormat::ParseFromString(protoTypeStr, &protoType);

    ASSERT_EQ(FormatType(protoType),
        R"(Struct<'Member1':Uint32,'Member2':List<String>,'Member3':Tuple<Utf8?,Decimal(8,13),Void>>)");
}

TEST(YdbValue, ParseType2) {
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
    google::protobuf::TextFormat::ParseFromString(protoTypeStr, &protoType);

    ASSERT_EQ(FormatType(protoType),
        R"(Dict<Uint32,Struct<'Member1':Date>>)");
}

TEST(YdbValue, ParseTaggedType) {
    auto protoTypeStr = R"(
        tagged_type {
            tag: "my_tag"
            type {
                type_id: STRING
            }
        }
    )";

    Ydb::Type protoType;
    google::protobuf::TextFormat::ParseFromString(protoTypeStr, &protoType);

    ASSERT_EQ(FormatType(protoType),
        R"(Tagged<String,'my_tag'>)");
}

TEST(YdbValue, BuildTaggedType) {
    auto type = TTypeBuilder()
        .BeginTagged("my_tag")
            .BeginList()
                .BeginOptional()
                    .Primitive(EPrimitiveType::Uint32)
                .EndOptional()
            .EndList()
        .EndTagged()
        .Build();

    ASSERT_EQ(FormatType(type),
        R"(Tagged<List<Uint32?>,'my_tag'>)");
}

TEST(YdbValue, BuildType) {
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

    ASSERT_EQ(FormatType(type),
        R"(Struct<'Member1':List<Uint32?>,'Member2':Dict<Int64,Tuple<Decimal(8,13),Pg('pgint2','',0,0,0),Utf8?>>>)");
}

TEST(YdbValue, BuildTypeReuse) {
    auto intType = TTypeBuilder()
        .Primitive(EPrimitiveType::Int32)
        .Build();

    ASSERT_EQ(FormatType(intType),
        R"(Int32)");

    auto optIntType = TTypeBuilder()
        .Optional(intType)
        .Build();

    ASSERT_EQ(FormatType(optIntType),
        R"(Int32?)");

    auto structType = TTypeBuilder()
        .BeginStruct()
        .AddMember("Member1", intType)
        .AddMember("Member2")
            .List(optIntType)
        .EndStruct()
        .Build();

    ASSERT_EQ(FormatType(structType),
        R"(Struct<'Member1':Int32,'Member2':List<Int32?>>)");

    auto tupleType = TTypeBuilder()
        .BeginTuple()
        .AddElement(optIntType)
        .AddElement()
            .Primitive(EPrimitiveType::String)
        .EndTuple()
        .Build();

    ASSERT_EQ(FormatType(tupleType),
        R"(Tuple<Int32?,String>)");

    auto type = TTypeBuilder()
        .BeginDict()
        .DictKey(tupleType)
        .DictPayload(structType)
        .EndDict()
        .Build();

    ASSERT_EQ(FormatType(type),
        R"(Dict<Tuple<Int32?,String>,Struct<'Member1':Int32,'Member2':List<Int32?>>>)");
}

TEST(YdbValue, BuildTypeIncomplete) {
    ASSERT_THROW({
        auto value = TTypeBuilder()
            .BeginTuple()
            .AddElement()
                .Primitive(EPrimitiveType::Uint32)
            .AddElement()
                .Primitive(EPrimitiveType::String)
            .Build();
    }, TExpectedErrorException);
}

TEST(YdbValue, ParseValue1) {
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

    auto protoValueStr = 
        "items {\n"
        "  uint32_value: 137\n"
        "}\n"
        "items {\n"
        "  items {\n"
        "    bytes_value: \"String1\"\n"
        "  }\n"
        "  items {\n"
        "    bytes_value: \"String2\"\n"
        "  }\n"
        "}\n"
        "items {\n"
        "  items {\n"
        "    null_flag_value: NULL_VALUE\n"
        "  }\n"
        "  items {\n"
        "    text_value: \"UtfString\"\n"
        "  }\n"
        "  items {\n"
        "  }\n"
        "}\n";

    Ydb::Type protoType;
    google::protobuf::TextFormat::ParseFromString(protoTypeStr, &protoType);

    Ydb::Value protoValue;
    google::protobuf::TextFormat::ParseFromString(protoValueStr, &protoValue);

    TValue value(TType(protoType), protoValue);

    CheckProtoValue(value.GetProto(), protoValueStr);
}

TEST(YdbValue, ParseValue2) {
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

    auto protoValueStr = 
        "pairs {\n"
        "  key {\n"
        "    uint32_value: 10\n"
        "  }\n"
        "  payload {\n"
        "    items {\n"
        "      uint32_value: 1000\n"
        "    }\n"
        "  }\n"
        "}\n"
        "pairs {\n"
        "  key {\n"
        "    uint32_value: 20\n"
        "  }\n"
        "  payload {\n"
        "    items {\n"
        "      uint32_value: 2000\n"
        "    }\n"
        "  }\n"
        "}\n";

    Ydb::Type protoType;
    google::protobuf::TextFormat::ParseFromString(protoTypeStr, &protoType);

    Ydb::Value protoValue;
    google::protobuf::TextFormat::ParseFromString(protoValueStr, &protoValue);

    TValue value(TType(protoType), protoValue);

    CheckProtoValue(value.GetProto(), protoValueStr);
}

TEST(YdbValue, ParseValuePg) {
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

    auto protoValueStr = 
        "items {\n"
        "  text_value: \"my_text_value\"\n"
        "}\n"
        "items {\n"
        "  bytes_value: \"my_binary_value\"\n"
        "}\n"
        "items {\n"
        "  text_value: \"\"\n"
        "}\n"
        "items {\n"
        "  bytes_value: \"\"\n"
        "}\n";

    Ydb::Type protoType;
    google::protobuf::TextFormat::ParseFromString(protoTypeStr, &protoType);

    Ydb::Value protoValue;
    google::protobuf::TextFormat::ParseFromString(protoValueStr, &protoValue);

    TValue value(TType(protoType), protoValue);

    CheckProtoValue(value.GetProto(), protoValueStr);
}

TEST(YdbValue, ParseValueMaybe) {
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
    google::protobuf::TextFormat::ParseFromString(protoTypeStr, &protoType);

    Ydb::Value protoValue;
    google::protobuf::TextFormat::ParseFromString(protoValueStr, &protoValue);

    TValue value(TType(protoType), protoValue);
    TValueParser parser(value);

    parser.OpenTuple();
    ASSERT_TRUE(parser.TryNextElement());
    ASSERT_EQ(parser.GetOptionalUtf8(), "SomeUtf");
    ASSERT_TRUE(parser.TryNextElement());
    ASSERT_EQ(parser.GetOptionalInt8(), -5);
    ASSERT_TRUE(parser.TryNextElement());
    ASSERT_EQ(parser.GetOptionalDouble(), std::optional<double>());
    ASSERT_TRUE(parser.TryNextElement());
    ASSERT_EQ(parser.GetOptionalUint64(), (ui64)7);
    ASSERT_TRUE(parser.TryNextElement());
    ASSERT_EQ(parser.GetOptionalDyNumber(), "12.345");
    parser.CloseTuple();
}

TEST(YdbValue, BuildValueIncomplete) {
    ASSERT_THROW({
        auto value = TValueBuilder()
            .BeginTuple()
            .AddElement()
                .Uint32(10)
            .AddElement()
                .String("test")
            .Build();
    }, TExpectedErrorException);
}

TEST(YdbValue, BuildValueListItemMismatch1) {
    ASSERT_THROW({
        auto value = TValueBuilder()
            .BeginList()
            .AddListItem()
                .Int32(17)
            .AddListItem()
                .Uint32(19)
            .EndList()
            .Build();
    }, TExpectedErrorException);
}

TEST(YdbValue, BuildValueListItemMismatch2) {
    auto itemValue = TValueBuilder()
        .String("Test")
        .Build();

    ASSERT_THROW({
        auto value = TValueBuilder()
            .BeginList()
            .AddListItem(itemValue)
            .AddListItem()
                .Int32(17)
            .EndList()
            .Build();
    }, TExpectedErrorException);
}

TEST(YdbValue, BuildValueListItemMismatch3) {
    auto itemValue = TValueBuilder()
        .String("Test")
        .Build();

    ASSERT_THROW({
        auto value = TValueBuilder()
            .BeginList()
            .AddListItem()
                .Int32(17)
            .AddListItem(itemValue)
            .EndList()
            .Build();
    }, TExpectedErrorException);
}

TEST(YdbValue, BuildValueListItemMismatch4) {
    ASSERT_THROW({
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
    }, TExpectedErrorException);
}

TEST(YdbValue, BuildValueEmptyListUnknown) {
    ASSERT_THROW({
        auto value = TValueBuilder()
            .EmptyList()
            .Build();
    }, TExpectedErrorException);
}

TEST(YdbValue, BuildDyNumberValue) {
    auto value = TValueBuilder()
        .DyNumber("12.345")
        .Build();

    CheckProtoValue(value.GetProto(), "text_value: \"12.345\"\n");
}

TEST(YdbValue, BuildTaggedValue) {
    auto value = TValueBuilder()
        .BeginTagged("my_tag")
            .DyNumber("12.345")
        .EndTagged()
        .Build();

    CheckProtoValue(value.GetProto(), "text_value: \"12.345\"\n");
}

TEST(YdbValue, BuildValueList) {
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

    CheckProtoValue(value.GetProto(),
        "items {\n"
        "  int32_value: 17\n"
        "}\n"
        "items {\n"
        "  int32_value: 19\n"
        "}\n"
        "items {\n"
        "  int32_value: 21\n"
        "}\n");
}

TEST(YdbValue, BuildValueListEmpty) {
    auto value = TValueBuilder()
        .EmptyList(TTypeBuilder().Primitive(EPrimitiveType::Uint32).Build())
        .Build();

    ASSERT_EQ(FormatType(value.GetType()),
        R"(List<Uint32>)");
    CheckProtoValue(value.GetProto(), "");
}

TEST(YdbValue, BuildValueListEmpty2) {
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

    CheckProtoValue(value.GetProto(),
        "items {\n"
        "  items {\n"
        "    uint32_value: 10\n"
        "  }\n"
        "}\n"
        "items {\n"
        "}\n");
}

TEST(YdbValue, BuildValueListEmpty3) {
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

    CheckProtoValue(value.GetProto(),
        "items {\n"
        "}\n"
        "items {\n"
        "  items {\n"
        "    uint32_value: 10\n"
        "  }\n"
        "}\n");
}

TEST(YdbValue, BuildValueBadCall) {
    ASSERT_THROW({
        auto value = TValueBuilder()
            .AddListItem()
            .EndList()
            .Build();
    }, TExpectedErrorException);
}

TEST(YdbValue, BuildValueOptional) {
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

    ASSERT_EQ(FormatType(value.GetType()),
        R"(List<Int32?>)");

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

    CheckProtoValue(value.GetProto(), expectedProtoValueStr);
}

TEST(YdbValue, BuildValueNestedOptional) {
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

    ASSERT_EQ(FormatType(value.GetType()),
        R"(Tuple<Int32??,Int64???,String??,Utf8???>)");

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

    CheckProtoValue(value.GetProto(), expectedProtoValueStr);
}

TEST(YdbValue, BuildValueOptionalMismatch1) {
    ASSERT_THROW({
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
    }, TExpectedErrorException);
}

TEST(YdbValue, BuildValueOptionalMismatch2) {
    ASSERT_THROW({
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
    }, TExpectedErrorException);
}

TEST(YdbValue, BuildValueStruct) {
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

    auto expectedProtoValueStr =
        "items {\n"
        "  items {\n"
        "    uint32_value: 1\n"
        "  }\n"
        "  items {\n"
        "    bytes_value: \"Anna\"\n"
        "  }\n"
        "  items {\n"
        "    int32_value: -100\n"
        "  }\n"
        "  items {\n"
        "    null_flag_value: NULL_VALUE\n"
        "  }\n"
        "}\n"
        "items {\n"
        "  items {\n"
        "    uint32_value: 2\n"
        "  }\n"
        "  items {\n"
        "    bytes_value: \"Paul\"\n"
        "  }\n"
        "  items {\n"
        "    int32_value: -200\n"
        "  }\n"
        "  items {\n"
        "    text_value: \"Some details\"\n"
        "  }\n"
        "}\n";

    CheckProtoValue(value.GetProto(), expectedProtoValueStr);
}

TEST(YdbValue, BuildValueStructMissingMember) {
    ASSERT_THROW({
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
    }, TExpectedErrorException);
}

TEST(YdbValue, BuildValueTuplePrimitives) {
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

    ASSERT_EQ(FormatType(value.GetType()),
        R"(Tuple<Bool,Int8,Uint8,Int16,Uint16,Int32,Uint32,Int64,Uint64,Float,Double,Date,Datetime,Timestamp,Interval,TzDate,TzDatetime,TzTimestamp,String,Utf8,Yson,Json>)");

    auto expectedProtoValueStr =
        "items {\n"
        "  bool_value: true\n"
        "}\n"
        "items {\n"
        "  int32_value: -1\n"
        "}\n"
        "items {\n"
        "  uint32_value: 1\n"
        "}\n"
        "items {\n"
        "  int32_value: -2\n"
        "}\n"
        "items {\n"
        "  uint32_value: 2\n"
        "}\n"
        "items {\n"
        "  int32_value: -3\n"
        "}\n"
        "items {\n"
        "  uint32_value: 3\n"
        "}\n"
        "items {\n"
        "  int64_value: -4\n"
        "}\n"
        "items {\n"
        "  uint64_value: 4\n"
        "}\n"
        "items {\n"
        "  float_value: -5.5\n"
        "}\n"
        "items {\n"
        "  double_value: 6.6\n"
        "}\n"
        "items {\n"
        "  uint32_value: 7\n"
        "}\n"
        "items {\n"
        "  uint32_value: 8\n"
        "}\n"
        "items {\n"
        "  uint64_value: 9\n"
        "}\n"
        "items {\n"
        "  int64_value: -10\n"
        "}\n"
        "items {\n"
        "  text_value: \"2018-02-02,Europe/Moscow\"\n"
        "}\n"
        "items {\n"
        "  text_value: \"2018-02-03T15:00:00,Europe/Moscow\"\n"
        "}\n"
        "items {\n"
        "  text_value: \"2018-02-07T15:00:00,Europe/Moscow\"\n"
        "}\n"
        "items {\n"
        "  bytes_value: \"TestString\"\n"
        "}\n"
        "items {\n"
        "  text_value: \"TestUtf8\"\n"
        "}\n"
        "items {\n"
        "  bytes_value: \"[]\"\n"
        "}\n"
        "items {\n"
        "  text_value: \"{}\"\n"
        "}\n";

    CheckProtoValue(value.GetProto(), expectedProtoValueStr);
}

TEST(YdbValue, BuildValueTuple1) {
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

    auto expectedProtoValueStr =
        "items {\n"
        "  items {\n"
        "    int32_value: 10\n"
        "  }\n"
        "  items {\n"
        "    bytes_value: \"Str1\"\n"
        "  }\n"
        "}\n"
        "items {\n"
        "  items {\n"
        "    int32_value: 20\n"
        "  }\n"
        "  items {\n"
        "    bytes_value: \"Str2\"\n"
        "  }\n"
        "}\n";

    CheckProtoValue(value.GetProto(), expectedProtoValueStr);
}

TEST(YdbValue, BuildValueTuple2) {
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

    auto expectedProtoValueStr =
        "items {\n"
        "  items {\n"
        "    int32_value: -10\n"
        "  }\n"
        "  items {\n"
        "    text_value: \"Utf1\"\n"
        "  }\n"
        "}\n"
        "items {\n"
        "  items {\n"
        "    int32_value: -20\n"
        "  }\n"
        "  items {\n"
        "    text_value: \"Utf2\"\n"
        "  }\n"
        "}\n";

    CheckProtoValue(value.GetProto(), expectedProtoValueStr);
}

TEST(YdbValue, BuildValueTupleElementsMismatch1) {
    ASSERT_THROW({
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
    }, TExpectedErrorException);
}

TEST(YdbValue, BuildValueTupleElementsMismatch2) {
    ASSERT_THROW({
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
    }, TExpectedErrorException);
}

TEST(YdbValue, BuildValueTupleTypeMismatch) {
    ASSERT_THROW({
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
    }, TExpectedErrorException);
}

TEST(YdbValue, BuildValueDict1) {
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

    auto expectedProtoValueStr =
        "pairs {\n"
        "  key {\n"
        "    int32_value: 1\n"
        "  }\n"
        "  payload {\n"
        "    bytes_value: \"Str1\"\n"
        "  }\n"
        "}\n"
        "pairs {\n"
        "  key {\n"
        "    int32_value: 2\n"
        "  }\n"
        "  payload {\n"
        "    bytes_value: \"Str2\"\n"
        "  }\n"
        "}\n";

    CheckProtoValue(value.GetProto(), expectedProtoValueStr);
}

TEST(YdbValue, BuildValueDict2) {
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

    auto expectedProtoValueStr =
        "pairs {\n"
        "  key {\n"
        "    int32_value: 1\n"
        "  }\n"
        "  payload {\n"
        "    bytes_value: \"Str1\"\n"
        "  }\n"
        "}\n"
        "pairs {\n"
        "  key {\n"
        "    int32_value: 2\n"
        "  }\n"
        "  payload {\n"
        "    bytes_value: \"Str2\"\n"
        "  }\n"
        "}\n";

    CheckProtoValue(value.GetProto(), expectedProtoValueStr);
}

TEST(YdbValue, BuildValueDictTypeMismatch1) {
    ASSERT_THROW({
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
    }, TExpectedErrorException);
}

TEST(YdbValue, BuildValueDictTypeMismatch2) {
    ASSERT_THROW({
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
    }, TExpectedErrorException);
}

TEST(YdbValue, BuildValueDictEmpty1) {
    auto value = TValueBuilder()
        .EmptyDict(
            TTypeBuilder().Primitive(EPrimitiveType::Uint32).Build(),
            TTypeBuilder().Primitive(EPrimitiveType::String).Build())
        .Build();

    ASSERT_EQ(FormatType(value.GetType()),
        R"(Dict<Uint32,String>)");

    auto expectedProtoValueStr = "";
    CheckProtoValue(value.GetProto(), expectedProtoValueStr);
}

TEST(YdbValue, BuildValueDictEmpty2) {
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

    ASSERT_EQ(FormatType(value.GetType()),
        R"(List<Dict<Int32,String>>)");

    auto expectedProtoValueStr =
        "items {\n"
        "  pairs {\n"
        "    key {\n"
        "      int32_value: 1\n"
        "    }\n"
        "    payload {\n"
        "      bytes_value: \"Str1\"\n"
        "    }\n"
        "  }\n"
        "}\n"
        "items {\n"
        "}\n"
        "items {\n"
        "}\n";

    CheckProtoValue(value.GetProto(), expectedProtoValueStr);
}

TEST(YdbValue, BuildValueDictEmptyNoType) {
    ASSERT_THROW({
        auto value = TValueBuilder()
            .BeginList()
            .AddListItem()
                .EmptyDict()
            .EndList()
            .Build();
    }, TExpectedErrorException);
}

TEST(YdbValue, BuildValueDictEmptyTypeMismatch) {
    ASSERT_THROW({
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
    }, TExpectedErrorException);
}

TEST(YdbValue, BuildValueWithType) {
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

    ASSERT_EQ(FormatType(value.GetType()),
        R"(Tuple<Struct<'Name':String,'Value':Uint64>,Utf8?,List<Bool>,Dict<Int32,Uint8?>,DyNumber?>)");

    auto expectedProtoValueStr =
        "items {\n"
        "  items {\n"
        "    bytes_value: \"Sergey\"\n"
        "  }\n"
        "  items {\n"
        "    uint64_value: 1\n"
        "  }\n"
        "}\n"
        "items {\n"
        "  null_flag_value: NULL_VALUE\n"
        "}\n"
        "items {\n"
        "  items {\n"
        "    bool_value: true\n"
        "  }\n"
        "}\n"
        "items {\n"
        "  pairs {\n"
        "    key {\n"
        "      int32_value: 10\n"
        "    }\n"
        "    payload {\n"
        "      null_flag_value: NULL_VALUE\n"
        "    }\n"
        "  }\n"
        "}\n"
        "items {\n"
        "  text_value: \"12.345\"\n"
        "}\n";

    CheckProtoValue(value.GetProto(), expectedProtoValueStr);
}

TEST(YdbValue, CorrectUuid) {
    std::string uuidStr = "5ca32c22-841b-11e8-adc0-fa7ae01bbebc";
    TUuidValue uuid(uuidStr);
    ASSERT_EQ(uuidStr, uuid.ToString());
}

TEST(YdbValue, IncorrectUuid) {
    ASSERT_THROW(TUuidValue(""), TContractViolation);
    ASSERT_THROW(TUuidValue("0123456789abcdef0123456789abcdef0123456789abcdef"), TContractViolation);
    ASSERT_THROW(TUuidValue("5ca32c22+841b-11e8-adc0-fa7ae01bbebc"), TContractViolation);
    ASSERT_THROW(TUuidValue("5ca32-c22841b-11e8-adc0-fa7ae01bbebc"), TContractViolation);
}

} // namespace NYdb
