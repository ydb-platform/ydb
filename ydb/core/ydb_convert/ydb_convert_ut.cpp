#include "ydb_convert.h"

#include <google/protobuf/text_format.h>

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/aclib/aclib.h>

namespace NKikimr {

static void TestConvertTypeToYdb(const TString& input, const TString& expected) {
    NKikimrMiniKQL::TType typeFrom;
    google::protobuf::TextFormat::ParseFromString(input, &typeFrom);
    Ydb::Type typeTo;
    ConvertMiniKQLTypeToYdbType(typeFrom, typeTo);
    TString result;
    google::protobuf::TextFormat::PrintToString(typeTo, &result);
    UNIT_ASSERT_NO_DIFF(result, expected);
}

static void TestConvertTypeFromYdb(const TString& input, const TString& expected) {
    Ydb::Type typeFrom;
    google::protobuf::TextFormat::ParseFromString(input, &typeFrom);
    NKikimrMiniKQL::TType typeTo;
    ConvertYdbTypeToMiniKQLType(typeFrom, typeTo);
    TString result;
    google::protobuf::TextFormat::PrintToString(typeTo, &result);
    UNIT_ASSERT_NO_DIFF(result, expected);
}

static void TestConvertValueToYdb(const TString& inputType, const TString& input, const TString& expected) {
    NKikimrMiniKQL::TValue valueFrom;
    google::protobuf::TextFormat::ParseFromString(input, &valueFrom);
    NKikimrMiniKQL::TType typeFrom;
    google::protobuf::TextFormat::ParseFromString(inputType, &typeFrom);
    Ydb::Value valueTo;
    ConvertMiniKQLValueToYdbValue(typeFrom, valueFrom, valueTo);
    TString result;
    google::protobuf::TextFormat::PrintToString(valueTo, &result);
    UNIT_ASSERT_NO_DIFF(result, expected);
}

static void TestConvertValueFromYdb(const TString& inputType, const TString& input, const TString& expected) {
    Ydb::Value valueFrom;
    google::protobuf::TextFormat::ParseFromString(input, &valueFrom);
    Ydb::Type typeFrom;
    google::protobuf::TextFormat::ParseFromString(inputType, &typeFrom);
    NKikimrMiniKQL::TValue valueTo;
    ConvertYdbValueToMiniKQLValue(typeFrom, valueFrom, valueTo);
    TString result;
    google::protobuf::TextFormat::PrintToString(valueTo, &result);
    UNIT_ASSERT_NO_DIFF(result, expected);
}

Y_UNIT_TEST_SUITE(ConvertMiniKQLTypeToYdbTypeTest) {
    Y_UNIT_TEST(SimpleType) {
        const TString input =
            "Kind: Data\n"
            "Data {\n"
            "  Scheme: 1\n"
            "}\n";
        const TString expected =
            "type_id: INT32\n";
        TestConvertTypeToYdb(input, expected);
        TestConvertTypeFromYdb(expected, input);
    }

    Y_UNIT_TEST(TTzDate) {
        const TString input =
            "Kind: Data\n"
            "Data {\n"
            "  Scheme: 52\n"
            "}\n";
        const TString expected =
            "type_id: TZ_DATE\n";
        TestConvertTypeToYdb(input, expected);
        TestConvertTypeFromYdb(expected, input);
    }

    Y_UNIT_TEST(TTzDateTime) {
        const TString input =
            "Kind: Data\n"
            "Data {\n"
            "  Scheme: 53\n"
            "}\n";
        const TString expected =
            "type_id: TZ_DATETIME\n";
        TestConvertTypeToYdb(input, expected);
        TestConvertTypeFromYdb(expected, input);
    }

    Y_UNIT_TEST(TTzTimeStamp) {
        const TString input =
            "Kind: Data\n"
            "Data {\n"
            "  Scheme: 54\n"
            "}\n";
        const TString expected =
            "type_id: TZ_TIMESTAMP\n";
        TestConvertTypeToYdb(input, expected);
        TestConvertTypeFromYdb(expected, input);
    }

    Y_UNIT_TEST(DecimalType) {
        const TString input =
            "Kind: Data\n"
            "Data {\n"
            "  Scheme: 4865\n"
            "  DecimalParams {\n"
            "    Precision: 21\n"
            "    Scale: 8\n"
            "  }\n"
            "}\n";
        const TString expected =
            "decimal_type {\n"
            "  precision: 21\n"
            "  scale: 8\n"
            "}\n";
        TestConvertTypeToYdb(input, expected);
        TestConvertTypeFromYdb(expected, input);
    }

    Y_UNIT_TEST(UuidType) {
        const TString input =
            "Kind: Data\n"
            "Data {\n"
            "  Scheme: 4611\n"
            "}\n";
        const TString expected =
            "type_id: UUID\n";
        TestConvertTypeToYdb(input, expected);
        TestConvertTypeFromYdb(expected, input);
    }

    Y_UNIT_TEST(VariantTuple) {
        const TString a = R"___(Kind: Variant
Variant {
  TupleItems {
    Element {
      Kind: Data
      Data {
        Scheme: 4
      }
    }
    Element {
      Kind: Data
      Data {
        Scheme: 2
      }
    }
  }
}
)___";

        const TString b = R"___(variant_type {
  tuple_items {
    elements {
      type_id: UINT64
    }
    elements {
      type_id: UINT32
    }
  }
}
)___";
        TestConvertTypeToYdb(a, b);
        TestConvertTypeFromYdb(b, a);
    }

    Y_UNIT_TEST(VariantStruct) {
        const TString a = R"___(Kind: Variant
Variant {
  StructItems {
    Member {
      Name: "a"
      Type {
        Kind: Data
        Data {
          Scheme: 4
        }
      }
    }
    Member {
      Name: "b"
      Type {
        Kind: Data
        Data {
          Scheme: 2
        }
      }
    }
  }
}
)___";

        const TString b = R"___(variant_type {
  struct_items {
    members {
      name: "a"
      type {
        type_id: UINT64
      }
    }
    members {
      name: "b"
      type {
        type_id: UINT32
      }
    }
  }
}
)___";
        TestConvertTypeToYdb(a, b);
        TestConvertTypeFromYdb(b, a);
    }


    Y_UNIT_TEST(Void) {
        const TString input = "Kind: Void\n";
        const TString expected = "void_type: NULL_VALUE\n";
        TestConvertTypeToYdb(input, expected);
        TestConvertTypeFromYdb(expected, input);
    }

    Y_UNIT_TEST(Optional) {
        const TString input =
            "Kind: Optional\n"
            "Optional {\n"
            "  Item {\n"
            "    Kind: Data\n"
            "    Data {\n"
            "      Scheme: 1\n"
            "    }\n"
            "  }\n"
            "}\n";
        const TString expected =
            "optional_type {\n"
            "  item {\n"
            "    type_id: INT32\n"
            "  }\n"
            "}\n";
        TestConvertTypeToYdb(input, expected);
        TestConvertTypeFromYdb(expected, input);
    }

    Y_UNIT_TEST(List) {
        const TString input =
            "Kind: List\n"
            "List {\n"
            "  Item {\n"
            "    Kind: Data\n"
            "    Data {\n"
            "      Scheme: 1\n"
            "    }\n"
            "  }\n"
            "}\n";
        const TString expected = "list_type {\n"
            "  item {\n"
            "    type_id: INT32\n"
            "  }\n"
            "}\n";
        TestConvertTypeToYdb(input, expected);
        TestConvertTypeFromYdb(expected, input);
    }

    Y_UNIT_TEST(Tuple) {
        const TString input =
            "Kind: Tuple\n"
            "Tuple {\n"
            "  Element {\n"
            "    Kind: Data\n"
            "    Data {\n"
            "      Scheme: 1\n"
            "    }\n"
            "  }\n"
            "  Element {\n"
            "    Kind: Data\n"
            "    Data {\n"
            "      Scheme: 4097\n"
            "    }\n"
            "  }\n"
            "}\n";
        const TString expected =
            "tuple_type {\n"
            "  elements {\n"
            "    type_id: INT32\n"
            "  }\n"
            "  elements {\n"
            "    type_id: STRING\n"
            "  }\n"
            "}\n";
        TestConvertTypeToYdb(input, expected);
        TestConvertTypeFromYdb(expected, input);
    }

    Y_UNIT_TEST(Struct) {
        const TString input =
            "Kind: Struct\n"
            "Struct {\n"
            "  Member {\n"
            "    Name: \"x\"\n"
            "    Type {\n"
            "      Kind: Data\n"
            "      Data {\n"
            "        Scheme: 1\n"
            "      }\n"
            "    }\n"
            "  }\n"
            "  Member {\n"
            "    Name: \"y\"\n"
            "    Type {\n"
            "      Kind: Data\n"
            "      Data {\n"
            "        Scheme: 4097\n"
            "      }\n"
            "    }\n"
            "  }\n"
            "}\n";
        const TString expected =
            "struct_type {\n"
            "  members {\n"
            "    name: \"x\"\n"
            "    type {\n"
            "      type_id: INT32\n"
            "    }\n"
            "  }\n"
            "  members {\n"
            "    name: \"y\"\n"
            "    type {\n"
            "      type_id: STRING\n"
            "    }\n"
            "  }\n"
            "}\n";
        TestConvertTypeToYdb(input, expected);
        TestConvertTypeFromYdb(expected, input);
    }

    Y_UNIT_TEST(Dict) {
        const TString input =
            "Kind: Dict\n"
            "Dict {\n"
            "  Key {\n"
            "    Kind: Data\n"
            "    Data {\n"
            "      Scheme: 1\n"
            "    }\n"
            "  }\n"
            "  Payload {\n"
            "    Kind: Data\n"
            "    Data {\n"
            "      Scheme: 4097\n"
            "    }\n"
            "  }\n"
            "}\n";
        const TString expected =
            "dict_type {\n"
            "  key {\n"
            "    type_id: INT32\n"
            "  }\n"
            "  payload {\n"
            "    type_id: STRING\n"
            "  }\n"
            "}\n";
        TestConvertTypeToYdb(input, expected);
        TestConvertTypeFromYdb(expected, input);
    }

    Y_UNIT_TEST(PgType) {
        const TString input =
            "Kind: Pg\n"
            "Pg {\n"
            "  oid: 16\n"
            "}\n";
        const TString expected =
            "pg_type {\n"
            "  oid: 16\n"
            "}\n";
        TestConvertTypeToYdb(input, expected);
    }
} // ConvertMiniKQLTypeToYdbTypeTest

Y_UNIT_TEST_SUITE(ConvertMiniKQLValueToYdbValueTest) {
    Y_UNIT_TEST(Void) {
        TestConvertValueToYdb("Kind: Void\n", "", "");
    }

    Y_UNIT_TEST(SimpleBool) {
        const TString inputType =
            "Kind: Data\n"
            "Data {\n"
            "  Scheme: 6\n"
            "}\n";
        TestConvertValueToYdb(inputType, "Bool: true\n", "bool_value: true\n");
    }

    Y_UNIT_TEST(SimpleInt32) {
        const TString inputType =
            "Kind: Data\n"
            "Data {\n"
            "  Scheme: 1\n"
            "}\n";
        TestConvertValueToYdb(inputType, "Int32: -42\n", "int32_value: -42\n");
    }

    Y_UNIT_TEST(SimpleInt64) {
         const TString inputType =
            "Kind: Data\n"
            "Data {\n"
            "  Scheme: 3\n"
            "}\n";
        TestConvertValueToYdb(inputType, "Int64: -42000000000\n", "int64_value: -42000000000\n");
    }

    Y_UNIT_TEST(SimpleTzDate) {
         const TString inputType =
            "Kind: Data\n"
            "Data {\n"
            "  Scheme: 52\n"
            "}\n";
        TestConvertValueToYdb(inputType, "Text: \"2020-09-22,Europe/Moscow\"\n", "text_value: \"2020-09-22,Europe/Moscow\"\n");
    }

    Y_UNIT_TEST(SimpleTzDateTime) {
        const TString inputType =
            "Kind: Data\n"
            "Data {\n"
            "  Scheme: 53\n"
            "}\n";

        TestConvertValueToYdb(inputType, "Text: \"2018-02-03T15:00:00,Europe/Moscow\"\n", "text_value: \"2018-02-03T15:00:00,Europe/Moscow\"\n");
    }

    Y_UNIT_TEST(SimpleTzTimeStamp) {
        const TString inputType =
            "Kind: Data\n"
            "Data {\n"
            "  Scheme: 54\n"
            "}\n";

        TestConvertValueToYdb(inputType, "Text: \"2018-02-03T15:00:00,Europe/Moscow\"\n", "text_value: \"2018-02-03T15:00:00,Europe/Moscow\"\n");
    }

    Y_UNIT_TEST(SimpleDecimal) {
         const TString inputType =
            "Kind: Data\n"
            "Data {\n"
            "  Scheme: 4865\n"
            "  DecimalParams {\n"
            "    Precision: 21\n"
            "    Scale: 8\n"
            "  }\n"
            "}\n";
        TestConvertValueToYdb(inputType, "Low128: 123\nHi128: 456\n", "low_128: 123\nhigh_128: 456\n");
    }

    Y_UNIT_TEST(SimpleUuid) {
         const TString inputType =
            "Kind: Data\n"
            "Data {\n"
            "  Scheme: 4611\n"
            "}\n";
         const TString uuidStr = R"__(Low128: 1
Hi128: 2
)__";
        TestConvertValueToYdb(inputType, uuidStr, "low_128: 1\nhigh_128: 2\n");
    }

    Y_UNIT_TEST(OptionalString) {
        const TString inputType =
            "Kind: Optional\n"
            "Optional {\n"
            "  Item {\n"
            "    Kind: Data\n"
            "    Data {\n"
            "      Scheme: 4097\n"
            "    }\n"
            "  }\n"
            "}\n";
        const TString inputValue =
            "Optional {\n"
            "  Bytes: \"abc\"\n"
            "}\n";
        TestConvertValueToYdb(inputType, inputValue, "bytes_value: \"abc\"\n");
    }

    Y_UNIT_TEST(OptionalEmpty) {
        const TString inputType =
            "Kind: Optional\n"
            "Optional {\n"
            "  Item {\n"
            "    Kind: Data\n"
            "    Data {\n"
            "      Scheme: 4097\n"
            "    }\n"
            "  }\n"
            "}\n";
        const TString inputValue = "";
        TestConvertValueToYdb(inputType, inputValue, "null_flag_value: NULL_VALUE\n");
    }

    Y_UNIT_TEST(OptionalOptionalEmpty) {
        const TString inputType =
            "Kind: Optional\n"
            "Optional {\n"
            "  Item {\n"
            "    Kind: Optional\n"
            "    Optional {\n"
            "      Item {\n"
            "        Kind: Data\n"
            "        Data {\n"
            "          Scheme: 4097\n"
            "        }\n"
            "      }\n"
            "    }\n"
            "  }\n"
            "}\n";
        const TString inputValue =
            "Optional {\n"
            "}\n";
        TestConvertValueToYdb(inputType, inputValue,
            "nested_value {\n"
            "  null_flag_value: NULL_VALUE\n"
            "}\n");
    }

    Y_UNIT_TEST(OptionalOptionalEmpty2) {
        const TString inputType =
            "Kind: Optional\n"
            "Optional {\n"
            "  Item {\n"
            "    Kind: Optional\n"
            "    Optional {\n"
            "      Item {\n"
            "        Kind: Data\n"
            "        Data {\n"
            "          Scheme: 4097\n"
            "        }\n"
            "      }\n"
            "    }\n"
            "  }\n"
            "}\n";
        const TString inputValue = "";
        TestConvertValueToYdb(inputType, inputValue,
            "null_flag_value: NULL_VALUE\n");
    }

    Y_UNIT_TEST(List) {
        const TString inputType =
            "Kind: List\n"
            "List {\n"
            "  Item {\n"
            "    Kind: Data\n"
            "    Data {\n"
            "      Scheme: 4097\n"
            "    }\n"
            "  }\n"
            "}\n";
        const TString inputValue =
            "List {\n"
            "  Bytes: \"abc\"\n"
            "}\n"
            "List {\n"
            "  Bytes: \"zxc\"\n"
            "}\n";
        TestConvertValueToYdb(inputType, inputValue,
            "items {\n"
            "  bytes_value: \"abc\"\n"
            "}\n"
            "items {\n"
            "  bytes_value: \"zxc\"\n"
            "}\n");
    }

    Y_UNIT_TEST(Struct) {
        const TString inputType =
            "Kind: Struct\n"
            "Struct {\n"
            "  Member {\n"
            "    Name: \"x\"\n"
            "    Type {\n"
            "      Kind: Data\n"
            "      Data {\n"
            "        Scheme: 32\n"
            "      }\n"
            "    }\n"
            "  }\n"
            "  Member {\n"
            "    Name: \"y\"\n"
            "    Type {\n"
            "      Kind: Data\n"
            "      Data {\n"
            "        Scheme: 4097\n"
            "      }\n"
            "    }\n"
            "  }\n"
            "}\n";
        const TString inputValue =
            "Struct {\n"
            "  Double: 42.33\n"
            "}\n"
            "Struct {\n"
            "  Bytes: \"abc\"\n"
            "}\n";
        TestConvertValueToYdb(inputType, inputValue,
            "items {\n"
            "  double_value: 42.33\n"
            "}\n"
            "items {\n"
            "  bytes_value: \"abc\"\n"
            "}\n");
    }

    Y_UNIT_TEST(Dict) {
        const TString inputType =
            "Kind: Dict\n"
            "Dict {\n"
            "  Key {\n"
            "    Kind: Data\n"
            "    Data {\n"
            "      Scheme: 1\n"
            "    }\n"
            "  }\n"
            "  Payload {\n"
            "    Kind: Data\n"
            "    Data {\n"
            "      Scheme: 4097\n"
            "    }\n"
            "  }\n"
            "}\n";
        const TString inputValue =
            "Dict {\n"
            "  Key {\n"
            "    Int32: 42\n"
            "  }\n"
            "  Payload {\n"
            "    Bytes: \"abc\"\n"
            "  }\n"
            "}\n";
        TestConvertValueToYdb(inputType, inputValue,
            "pairs {\n"
            "  key {\n"
            "    int32_value: 42\n"
            "  }\n"
            "  payload {\n"
            "    bytes_value: \"abc\"\n"
            "  }\n"
            "}\n");
    }

    Y_UNIT_TEST(Tuple) {
        const TString inputType =
            "Kind: Tuple\n"
            "Tuple {\n"
            "  Element {\n"
            "    Kind: Data\n"
            "    Data {\n"
            "      Scheme: 1\n"
            "    }\n"
            "  }\n"
            "  Element {\n"
            "    Kind: Data\n"
            "    Data {\n"
            "      Scheme: 4097\n"
            "    }\n"
            "  }\n"
            "}\n";

        const TString inputValue =
            "Tuple {\n"
            "  Int32: 42\n"
            "}\n"
            "Tuple {\n"
            "  Bytes: \"abc\"\n"
            "}\n";

        TestConvertValueToYdb(inputType, inputValue,
            "items {\n"
            "  int32_value: 42\n"
            "}\n"
            "items {\n"
            "  bytes_value: \"abc\"\n"
            "}\n");
    }

    Y_UNIT_TEST(Variant) {
        const TString inputType = R"___(Kind: Variant
Variant {
  TupleItems {
    Element {
      Kind: Data
      Data {
        Scheme: 4
      }
    }
    Element {
      Kind: Data
      Data {
        Scheme: 2
      }
    }
  }
}
)___";

        const TString inputValue = R"___(Optional {
  Uint32: 66
}
VariantIndex: 1
)___";

        const TString expected = R"___(nested_value {
  uint32_value: 66
}
variant_index: 1
)___";

        TestConvertValueToYdb(inputType, inputValue, expected);
    }

} // ConvertMiniKQLValueToYdbValueTest

Y_UNIT_TEST_SUITE(ConvertYdbValueToMiniKQLValueTest) {
    Y_UNIT_TEST(Void) {
        TestConvertValueFromYdb("void_type: NULL_VALUE\n", "", "");
    }

    Y_UNIT_TEST(SimpleBool) {
        const TString inputType =
            "type_id: BOOL\n";
        TestConvertValueFromYdb(inputType, "bool_value: true\n", "Bool: true\n");
    }

    Y_UNIT_TEST(SimpleBoolTypeMissmatch) {
        const TString inputType =
            "type_id: BOOL\n";
        UNIT_ASSERT_EXCEPTION(TestConvertValueFromYdb(inputType, "int32_value: -42\n", "Bool: true\n"), yexception);
    }

    Y_UNIT_TEST(SimpleInt32) {
        const TString inputType =
            "type_id: INT32\n";
        TestConvertValueFromYdb(inputType, "int32_value: -42\n", "Int32: -42\n");
    }

    Y_UNIT_TEST(SimpleTzDate) {
         const TString inputType =
            "type_id: TZ_DATE\n";
        TestConvertValueFromYdb(inputType, "text_value: \"2020-09-22,Europe/Moscow\"", "Text: \"2020-09-22,Europe/Moscow\"\n");
    }

    Y_UNIT_TEST(SimpleTzDateTime) {
         const TString inputType =
            "type_id: TZ_DATETIME\n";
        TestConvertValueFromYdb(inputType, "text_value: \"2020-09-22T15:00:00,Europe/Moscow\"", "Text: \"2020-09-22T15:00:00,Europe/Moscow\"\n");
    }

    Y_UNIT_TEST(SimpleTzTimeStamp) {
         const TString inputType =
            "type_id: TZ_TIMESTAMP\n";
        TestConvertValueFromYdb(inputType, "text_value: \"2020-09-22T15:00:00,Europe/Moscow\"", "Text: \"2020-09-22T15:00:00,Europe/Moscow\"\n");
    }

    Y_UNIT_TEST(SimpleInt32TypeMissmatch) {
        const TString inputType =
            "type_id: INT32\n";
        const TString inputValue = "bytes_value: \"abc\"\n";
        UNIT_ASSERT_EXCEPTION(TestConvertValueFromYdb(inputType, inputValue, "Int32: -42\n"), yexception);
    }

    Y_UNIT_TEST(SimpleUuid) {
         const TString inputType =
            "type_id: UUID\n";
         const TString uuidStr = R"__(Low128: 1
Hi128: 2
)__";
        TestConvertValueFromYdb(inputType, "low_128: 1\nhigh_128: 2\n", uuidStr);
    }

    Y_UNIT_TEST(SimpleUuidTypeMissmatch) {
         const TString inputType =
            "type_id: UUID\n";
         const TString uuidStr = R"__(Low128: 1
Hi128: 2
)__";
        UNIT_ASSERT_EXCEPTION(TestConvertValueFromYdb(inputType, "bytes_value: \"abc\"\n", uuidStr), yexception);
    }

    Y_UNIT_TEST(SimpleDecimal) {
         const TString inputType =
            "decimal_type {\n"
            "  precision: 21\n"
            "  scale: 8\n"
            "}\n";
        TestConvertValueFromYdb(inputType, "low_128: 123\nhigh_128: 456\n", "Low128: 123\nHi128: 456\n");
    }

    Y_UNIT_TEST(SimpleDecimalTypeMissmatch) {
         const TString inputType =
            "decimal_type {\n"
            "  precision: 21\n"
            "  scale: 8\n"
            "}\n";
        UNIT_ASSERT_EXCEPTION(TestConvertValueFromYdb(inputType, "bytes_value: \"abc\"\n", "Low128: 123\nHi128: 456\n"), yexception);
    }

    Y_UNIT_TEST(OptionalString) {
        const TString inputType =
            "optional_type {\n"
            "  item {\n"
            "    type_id: STRING\n"
            "  }\n"
            "}\n";
        const TString inputValue = "bytes_value: \"abc\"\n";
        TestConvertValueFromYdb(inputType, inputValue,
            "Optional {\n"
            "  Bytes: \"abc\"\n"
            "}\n");
    }

    Y_UNIT_TEST(OptionalEmpty) {
        const TString inputType =
            "optional_type {\n"
            "  item {\n"
            "    type_id: STRING\n"
            "  }\n"
            "}\n";
        const TString inputValue = "null_flag_value: NULL_VALUE\n";
        TestConvertValueFromYdb(inputType, inputValue, "");
    }

    Y_UNIT_TEST(OptionalOptionalEmpty) {
        const TString inputType =
            "optional_type {\n"
            "  item {\n"
            "    optional_type {\n"
            "      item {\n"
            "        type_id: STRING\n"
            "      }\n"
            "    }\n"
            "  }\n"
            "}\n";
        const TString inputValue =
            "nested_value {\n"
            "  null_flag_value: NULL_VALUE\n"
            "}\n";
        TestConvertValueFromYdb(inputType, inputValue,
            "Optional {\n"
            "}\n");
    }

    Y_UNIT_TEST(OptionalOptionalEmpty2) {
        const TString inputType =
            "optional_type {\n"
            "  item {\n"
            "    optional_type {\n"
            "      item {\n"
            "        type_id: STRING\n"
            "      }\n"
            "    }\n"
            "  }\n"
            "}\n";
        const TString inputValue = "null_flag_value: NULL_VALUE\n";
        TestConvertValueFromYdb(inputType, inputValue, "");
    }

    Y_UNIT_TEST(List) {
        const TString inputType =
            "list_type {\n"
            "  item {\n"
            "    type_id: STRING\n"
            "  }\n"
            "}\n";
        const TString inputValue =
            "items {\n"
            "  bytes_value: \"abc\"\n"
            "}\n"
            "items {\n"
            "  bytes_value: \"zxc\"\n"
            "}\n";
        TestConvertValueFromYdb(inputType, inputValue,
            "List {\n"
            "  Bytes: \"abc\"\n"
            "}\n"
            "List {\n"
            "  Bytes: \"zxc\"\n"
            "}\n");
    }

    Y_UNIT_TEST(Struct) {
        const TString inputType =
            "struct_type {\n"
            "  members {\n"
            "    name: \"x\"\n"
            "    type {\n"
            "      type_id: DOUBLE\n"
            "    }\n"
            "  }\n"
            "  members {\n"
            "    name: \"y\"\n"
            "    type {\n"
            "      type_id: STRING\n"
            "    }\n"
            "  }\n"
            "}\n";
        const TString inputValue =
            "items {\n"
            "  double_value: 42.33\n"
            "}\n"
            "items {\n"
            "  bytes_value: \"abc\"\n"
            "}\n";
        TestConvertValueFromYdb(inputType, inputValue,
            "Struct {\n"
            "  Double: 42.33\n"
            "}\n"
            "Struct {\n"
            "  Bytes: \"abc\"\n"
            "}\n");
    }

    Y_UNIT_TEST(Dict) {
        const TString inputType =
            "dict_type {\n"
            "  key {\n"
            "    type_id: INT32\n"
            "  }\n"
            "  payload {\n"
            "    type_id: STRING\n"
            "  }\n"
            "}\n";
        const TString inputValue =
            "pairs {\n"
            "  key {\n"
            "    int32_value: 42\n"
            "  }\n"
            "  payload {\n"
            "    bytes_value: \"abc\"\n"
            "  }\n"
            "}\n";

        TestConvertValueFromYdb(inputType, inputValue,
            "Dict {\n"
            "  Key {\n"
            "    Int32: 42\n"
            "  }\n"
            "  Payload {\n"
            "    Bytes: \"abc\"\n"
            "  }\n"
            "}\n");

    }

    Y_UNIT_TEST(Tuple) {
        const TString inputType =
            "tuple_type {\n"
            "  elements {\n"
            "    type_id: INT32\n"
            "  }\n"
            "  elements {\n"
            "    type_id: STRING\n"
            "  }\n"
            "}\n";

        const TString inputValue =
            "items {\n"
            "  int32_value: 42\n"
            "}\n"
            "items {\n"
            "  bytes_value: \"abc\"\n"
            "}\n";

        TestConvertValueFromYdb(inputType, inputValue,
            "Tuple {\n"
            "  Int32: 42\n"
            "}\n"
            "Tuple {\n"
            "  Bytes: \"abc\"\n"
            "}\n");
    }

    Y_UNIT_TEST(Variant) {
        const TString inputType = R"___(variant_type {
  tuple_items {
    elements {
      type_id: UINT64
    }
    elements {
      type_id: UINT32
    }
  }
}
)___";

        const TString inputValue = R"___(nested_value {
  uint32_value: 66
}
variant_index: 1
)___";

        const TString expected = R"___(Optional {
  Uint32: 66
}
VariantIndex: 1
)___";

        TestConvertValueFromYdb(inputType, inputValue, expected);
    }

    Y_UNIT_TEST(VariantIndexUnderflow) {
        const TString inputType = R"___(variant_type {
  tuple_items {
    elements {
      type_id: UINT64
    }
    elements {
      type_id: UINT32
    }
  }
}
)___";

        const TString inputValue = R"___(nested_value {
  uint32_value: 66
}
variant_index: 3435973836
)___";

        const TString expected = "";

        UNIT_ASSERT_EXCEPTION(TestConvertValueFromYdb(inputType, inputValue, expected), yexception);
    }

    Y_UNIT_TEST(PgValue) {
        const TString inputType =
            "Kind: Pg\n"
            "Pg {\n"
            "  oid: 16\n"
            "}\n";
        const TString inputValue =
            "Text: \"123\"\n";
        const TString expectedValue =
            "text_value: \"123\"\n";
        TestConvertValueToYdb(inputType, inputValue, expectedValue);
    }

} // ConvertYdbValueToMiniKQLValueTest

Y_UNIT_TEST_SUITE(ConvertYdbPermissionNameToACLAttrs) {
Y_UNIT_TEST(SimpleConvertGood) {
    using namespace NACLib;
    auto aclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.database.connect");
    UNIT_ASSERT_EQUAL(aclAttr.AccessMask, EAccessRights::ConnectDatabase);
    UNIT_ASSERT_EQUAL(aclAttr.InheritanceType, EInheritanceType::InheritNone);

    aclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.tables.modify");
    UNIT_ASSERT_EQUAL(aclAttr.AccessMask, EAccessRights(UpdateRow | EraseRow));
    UNIT_ASSERT_EQUAL(aclAttr.InheritanceType, EInheritanceType::InheritObject | EInheritanceType::InheritContainer);

    aclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.tables.read");
    UNIT_ASSERT_EQUAL(aclAttr.AccessMask, EAccessRights(SelectRow | ReadAttributes));
    UNIT_ASSERT_EQUAL(aclAttr.InheritanceType, EInheritanceType::InheritObject | EInheritanceType::InheritContainer);

    aclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.generic.list");
    UNIT_ASSERT_EQUAL(aclAttr.AccessMask, EAccessRights::GenericList);
    UNIT_ASSERT_EQUAL(aclAttr.InheritanceType, EInheritanceType::InheritObject | EInheritanceType::InheritContainer);

    aclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.generic.read");
    UNIT_ASSERT_EQUAL(aclAttr.AccessMask, EAccessRights::GenericRead);
    UNIT_ASSERT_EQUAL(aclAttr.InheritanceType, EInheritanceType::InheritObject | EInheritanceType::InheritContainer);

    aclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.generic.write");
    UNIT_ASSERT_EQUAL(aclAttr.AccessMask, EAccessRights::GenericWrite);
    UNIT_ASSERT_EQUAL(aclAttr.InheritanceType, EInheritanceType::InheritObject | EInheritanceType::InheritContainer);

    aclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.generic.use_legacy");
    UNIT_ASSERT_EQUAL(aclAttr.AccessMask, EAccessRights::GenericUseLegacy);
    UNIT_ASSERT_EQUAL(aclAttr.InheritanceType, EInheritanceType::InheritObject | EInheritanceType::InheritContainer);

    aclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.generic.use");
    UNIT_ASSERT_EQUAL(aclAttr.AccessMask, EAccessRights::GenericUse);
    UNIT_ASSERT_EQUAL(aclAttr.InheritanceType, EInheritanceType::InheritObject | EInheritanceType::InheritContainer);

    aclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.generic.manage");
    UNIT_ASSERT_EQUAL(aclAttr.AccessMask, EAccessRights::GenericManage);
    UNIT_ASSERT_EQUAL(aclAttr.InheritanceType, EInheritanceType::InheritObject | EInheritanceType::InheritContainer);

    aclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.generic.full_legacy");
    UNIT_ASSERT_EQUAL(aclAttr.AccessMask, EAccessRights::GenericFullLegacy);
    UNIT_ASSERT_EQUAL(aclAttr.InheritanceType, EInheritanceType::InheritObject | EInheritanceType::InheritContainer);

    aclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.generic.full");
    UNIT_ASSERT_EQUAL(aclAttr.AccessMask, EAccessRights::GenericFull);
    UNIT_ASSERT_EQUAL(aclAttr.InheritanceType, EInheritanceType::InheritObject | EInheritanceType::InheritContainer);

    aclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.database.create");
    UNIT_ASSERT_EQUAL(aclAttr.AccessMask, EAccessRights::CreateDatabase);
    UNIT_ASSERT_EQUAL(aclAttr.InheritanceType, EInheritanceType::InheritObject | EInheritanceType::InheritContainer);

    aclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.database.drop");
    UNIT_ASSERT_EQUAL(aclAttr.AccessMask, EAccessRights::DropDatabase);
    UNIT_ASSERT_EQUAL(aclAttr.InheritanceType, EInheritanceType::InheritObject | EInheritanceType::InheritContainer);

    aclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.access.grant");
    UNIT_ASSERT_EQUAL(aclAttr.AccessMask, EAccessRights::GrantAccessRights);
    UNIT_ASSERT_EQUAL(aclAttr.InheritanceType, EInheritanceType::InheritObject | EInheritanceType::InheritContainer);

    aclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.granular.select_row");
    UNIT_ASSERT_EQUAL(aclAttr.AccessMask, EAccessRights::SelectRow);
    UNIT_ASSERT_EQUAL(aclAttr.InheritanceType, EInheritanceType::InheritObject | EInheritanceType::InheritContainer);

    aclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.granular.update_row");
    UNIT_ASSERT_EQUAL(aclAttr.AccessMask, EAccessRights::UpdateRow);
    UNIT_ASSERT_EQUAL(aclAttr.InheritanceType, EInheritanceType::InheritObject | EInheritanceType::InheritContainer);

    aclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.granular.erase_row");
    UNIT_ASSERT_EQUAL(aclAttr.AccessMask, EAccessRights::EraseRow);
    UNIT_ASSERT_EQUAL(aclAttr.InheritanceType, EInheritanceType::InheritObject | EInheritanceType::InheritContainer);

    aclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.granular.read_attributes");
    UNIT_ASSERT_EQUAL(aclAttr.AccessMask, EAccessRights::ReadAttributes);
    UNIT_ASSERT_EQUAL(aclAttr.InheritanceType, EInheritanceType::InheritObject | EInheritanceType::InheritContainer);

    aclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.granular.write_attributes");
    UNIT_ASSERT_EQUAL(aclAttr.AccessMask, EAccessRights::WriteAttributes);
    UNIT_ASSERT_EQUAL(aclAttr.InheritanceType, EInheritanceType::InheritObject | EInheritanceType::InheritContainer);

    aclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.granular.create_directory");
    UNIT_ASSERT_EQUAL(aclAttr.AccessMask, EAccessRights::CreateDirectory);
    UNIT_ASSERT_EQUAL(aclAttr.InheritanceType, EInheritanceType::InheritObject | EInheritanceType::InheritContainer);

    aclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.granular.create_table");
    UNIT_ASSERT_EQUAL(aclAttr.AccessMask, EAccessRights::CreateTable);
    UNIT_ASSERT_EQUAL(aclAttr.InheritanceType, EInheritanceType::InheritObject | EInheritanceType::InheritContainer);

    aclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.granular.create_queue");
    UNIT_ASSERT_EQUAL(aclAttr.AccessMask, EAccessRights::CreateQueue);
    UNIT_ASSERT_EQUAL(aclAttr.InheritanceType, EInheritanceType::InheritObject | EInheritanceType::InheritContainer);

    aclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.granular.remove_schema");
    UNIT_ASSERT_EQUAL(aclAttr.AccessMask, EAccessRights::RemoveSchema);
    UNIT_ASSERT_EQUAL(aclAttr.InheritanceType, EInheritanceType::InheritObject | EInheritanceType::InheritContainer);

    aclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.granular.describe_schema");
    UNIT_ASSERT_EQUAL(aclAttr.AccessMask, EAccessRights::DescribeSchema);
    UNIT_ASSERT_EQUAL(aclAttr.InheritanceType, EInheritanceType::InheritObject | EInheritanceType::InheritContainer);

    aclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.granular.alter_schema");
    UNIT_ASSERT_EQUAL(aclAttr.AccessMask, EAccessRights::AlterSchema);
    UNIT_ASSERT_EQUAL(aclAttr.InheritanceType, EInheritanceType::InheritObject | EInheritanceType::InheritContainer);
}

Y_UNIT_TEST(TestEqualGranularAndDeprecatedAcl) {
    using namespace NACLib;
    auto deprecatedAclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.deprecated.select_row");
    auto granularAclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.granular.select_row");
    UNIT_ASSERT_EQUAL(deprecatedAclAttr.AccessMask, granularAclAttr.AccessMask);
    UNIT_ASSERT_EQUAL(deprecatedAclAttr.InheritanceType, granularAclAttr.InheritanceType);

    deprecatedAclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.deprecated.update_row");
    granularAclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.granular.update_row");
    UNIT_ASSERT_EQUAL(deprecatedAclAttr.AccessMask, granularAclAttr.AccessMask);
    UNIT_ASSERT_EQUAL(deprecatedAclAttr.InheritanceType, granularAclAttr.InheritanceType);

    deprecatedAclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.deprecated.erase_row");
    granularAclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.granular.erase_row");
    UNIT_ASSERT_EQUAL(deprecatedAclAttr.AccessMask, granularAclAttr.AccessMask);
    UNIT_ASSERT_EQUAL(deprecatedAclAttr.InheritanceType, granularAclAttr.InheritanceType);

    deprecatedAclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.deprecated.read_attributes");
    granularAclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.granular.read_attributes");
    UNIT_ASSERT_EQUAL(deprecatedAclAttr.AccessMask, granularAclAttr.AccessMask);
    UNIT_ASSERT_EQUAL(deprecatedAclAttr.InheritanceType, granularAclAttr.InheritanceType);

    deprecatedAclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.granular.write_attributes");
    granularAclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.granular.write_attributes");
    UNIT_ASSERT_EQUAL(deprecatedAclAttr.AccessMask, granularAclAttr.AccessMask);
    UNIT_ASSERT_EQUAL(deprecatedAclAttr.InheritanceType, granularAclAttr.InheritanceType);

    deprecatedAclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.deprecated.create_directory");
    granularAclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.granular.create_directory");
    UNIT_ASSERT_EQUAL(deprecatedAclAttr.AccessMask, granularAclAttr.AccessMask);
    UNIT_ASSERT_EQUAL(deprecatedAclAttr.InheritanceType, granularAclAttr.InheritanceType);

    deprecatedAclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.deprecated.create_table");
    granularAclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.granular.create_table");
    UNIT_ASSERT_EQUAL(deprecatedAclAttr.AccessMask, granularAclAttr.AccessMask);
    UNIT_ASSERT_EQUAL(deprecatedAclAttr.InheritanceType, granularAclAttr.InheritanceType);

    deprecatedAclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.deprecated.create_queue");
    granularAclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.granular.create_queue");
    UNIT_ASSERT_EQUAL(deprecatedAclAttr.AccessMask, granularAclAttr.AccessMask);
    UNIT_ASSERT_EQUAL(deprecatedAclAttr.InheritanceType, granularAclAttr.InheritanceType);

    deprecatedAclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.deprecated.remove_schema");
    granularAclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.granular.remove_schema");
    UNIT_ASSERT_EQUAL(deprecatedAclAttr.AccessMask, granularAclAttr.AccessMask);
    UNIT_ASSERT_EQUAL(deprecatedAclAttr.InheritanceType, granularAclAttr.InheritanceType);

    deprecatedAclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.deprecated.describe_schema");
    granularAclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.granular.describe_schema");
    UNIT_ASSERT_EQUAL(deprecatedAclAttr.AccessMask, granularAclAttr.AccessMask);
    UNIT_ASSERT_EQUAL(deprecatedAclAttr.InheritanceType, granularAclAttr.InheritanceType);

    deprecatedAclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.deprecated.alter_schema");
    granularAclAttr = ConvertYdbPermissionNameToACLAttrs("ydb.granular.alter_schema");
    UNIT_ASSERT_EQUAL(deprecatedAclAttr.AccessMask, granularAclAttr.AccessMask);
    UNIT_ASSERT_EQUAL(deprecatedAclAttr.InheritanceType, granularAclAttr.InheritanceType);
}

} // ConvertYdbPermissionNameToACLAttrs

} // namespace NKikimr
