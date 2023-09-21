#include "result_formatter.h"

#include <ydb/services/ydb/ydb_common_ut.h>

using namespace NFq;

Y_UNIT_TEST_SUITE(ResultFormatter) {
    Y_UNIT_TEST(Primitive) {
        Ydb::ResultSet rs;
        {
            auto& column = *rs.add_columns();
            column.set_name("column0");
            auto& type = *column.mutable_type();
            type.set_type_id(Ydb::Type::INT32);
        }
        {
            auto& column = *rs.add_columns();
            column.set_name("column1");
            auto& type = *column.mutable_type();
            type.set_type_id(Ydb::Type::INT64);
        }
        {
            auto& value = *rs.add_rows();
            value.add_items()->set_int32_value(31337);
            value.add_items()->set_int64_value(1000000001);
        }
        {
            auto& value = *rs.add_rows();
            value.add_items()->set_int32_value(31338);
            value.add_items()->set_int64_value(1000000002);
        }

        {
            NJson::TJsonValue root;
            FormatResultSet(root, rs);

            TStringStream stream;
            NJson::WriteJson(&stream, &root);

            // Cerr << stream.Str() << Endl;

            TString expected = R"___({"data":[{"column0":"31337","column1":"1000000001"},{"column0":"31338","column1":"1000000002"}],"columns":[{"name":"column0","type":["DataType","Int32"]},{"name":"column1","type":["DataType","Int64"]}]})___";

            UNIT_ASSERT_VALUES_EQUAL(stream.Str(), expected);
        }

        // pretty format
        {
            NJson::TJsonValue root;
            FormatResultSet(root, rs, true, true);

            TStringStream stream;
            NJson::WriteJson(&stream, &root);

            // Cerr << stream.Str() << Endl;

            TString expected = R"___({"data":[[31337,1000000001],[31338,1000000002]],"columns":[{"name":"column0","type":"Int32"},{"name":"column1","type":"Int64"}]})___";

            UNIT_ASSERT_VALUES_EQUAL(stream.Str(), expected);
        }
    }

    Y_UNIT_TEST(Utf8WithQuotes) {
        Ydb::ResultSet rs;
        {
            auto& column = *rs.add_columns();
            column.set_name("column0");
            auto& type = *column.mutable_type();
            type.set_type_id(Ydb::Type::UTF8);
        }
        {
            auto& value = *rs.add_rows();
            value.add_items()->set_text_value("he\"llo");
        }

        {
            NJson::TJsonValue root;
            FormatResultSet(root, rs);

            TStringStream stream;
            NJson::WriteJson(&stream, &root);

            // Cerr << stream.Str() << Endl;

            TString expected = R"___({"data":[{"column0":"he\"llo"}],"columns":[{"name":"column0","type":["DataType","Utf8"]}]})___";

            UNIT_ASSERT_VALUES_EQUAL(stream.Str(), expected);
        }

        // pretty format
        {
            NJson::TJsonValue root;
            FormatResultSet(root, rs, true, true);

            TStringStream stream;
            NJson::WriteJson(&stream, &root);

            // Cerr << stream.Str() << Endl;

            TString expected = R"___({"data":[["he\"llo"]],"columns":[{"name":"column0","type":"Utf8"}]})___";

            UNIT_ASSERT_VALUES_EQUAL(stream.Str(), expected);
        }
    }

    Y_UNIT_TEST(EmptyResultSet) {
        Ydb::ResultSet rs;
        {
            NJson::TJsonValue root;
            FormatResultSet(root, rs);

            TStringStream stream;
            NJson::WriteJson(&stream, &root);

            // Cerr << stream.Str() << Endl;

            TString expected = R"___({"data":[],"columns":[]})___";

            UNIT_ASSERT_VALUES_EQUAL(stream.Str(), expected);
        }

        // pretty format
        {
            NJson::TJsonValue root;
            FormatResultSet(root, rs, true, true);

            TStringStream stream;
            NJson::WriteJson(&stream, &root);

            // Cerr << stream.Str() << Endl;

            TString expected = R"___({"data":[],"columns":[]})___";

            UNIT_ASSERT_VALUES_EQUAL(stream.Str(), expected);
        }
    }

    Y_UNIT_TEST(List) {
        Ydb::ResultSet rs;
        {
            auto& column = *rs.add_columns();
            column.set_name("column0");
            auto& type = *column.mutable_type()->mutable_list_type()->mutable_item();
            type.set_type_id(Ydb::Type::INT32);
        }
        {
            auto& value = *rs.add_rows();
            auto& cell = *value.add_items();
            cell.add_items()->set_int32_value(31337);
            cell.add_items()->set_int32_value(1000000001);
        }

        {
            NJson::TJsonValue root;
            FormatResultSet(root, rs);

            TStringStream stream;
            NJson::WriteJson(&stream, &root);

            //Cerr << stream.Str() << Endl;

            TString expected = R"___({"data":[{"column0":["31337","1000000001"]}],"columns":[{"name":"column0","type":["ListType",["DataType","Int32"]]}]})___";

            UNIT_ASSERT_VALUES_EQUAL(stream.Str(), expected);
        }

        // pretty format
        {
            NJson::TJsonValue root;
            FormatResultSet(root, rs, true, true);

            TStringStream stream;
            NJson::WriteJson(&stream, &root);

            //Cerr << stream.Str() << Endl;

            TString expected = R"___({"data":[[[31337,1000000001]]],"columns":[{"name":"column0","type":"List<Int32>"}]})___";

            UNIT_ASSERT_VALUES_EQUAL(stream.Str(), expected);
        }
    }

    Y_UNIT_TEST(Optional) {
        Ydb::ResultSet rs;
        {
            auto& column = *rs.add_columns();
            column.set_name("column0");
            auto& type = *column.mutable_type()->mutable_optional_type()->mutable_item();
            type.set_type_id(Ydb::Type::INT32);
        }
        {
            auto& value = *rs.add_rows();
            value.add_items()->set_int32_value(31337);
        }
        {
            auto& value = *rs.add_rows();
            value.add_items()->set_null_flag_value(::google::protobuf::NullValue());
        }

        {
            NJson::TJsonValue root;
            FormatResultSet(root, rs);

            TStringStream stream;
            NJson::WriteJson(&stream, &root);

            //Cerr << stream.Str() << Endl;
            TString expected = R"___({"data":[{"column0":["31337"]},{"column0":null}],"columns":[{"name":"column0","type":["OptionalType",["DataType","Int32"]]}]})___";

            UNIT_ASSERT_VALUES_EQUAL(stream.Str(), expected);
        }

        // pretty format
        {
            NJson::TJsonValue root;
            FormatResultSet(root, rs, true, true);

            TStringStream stream;
            NJson::WriteJson(&stream, &root);

            //Cerr << stream.Str() << Endl;
            TString expected = R"___({"data":[[[31337]],[[]]],"columns":[{"name":"column0","type":"Optional<Int32>"}]})___";

            UNIT_ASSERT_VALUES_EQUAL(stream.Str(), expected);
        }
    }

    Y_UNIT_TEST(Struct) {
        Ydb::ResultSet rs;
        {
            auto& column = *rs.add_columns();
            column.set_name("column0");
            auto& struct_type = *column.mutable_type()->mutable_struct_type();
            auto& m1 = *struct_type.add_members();
            auto& m2 = *struct_type.add_members();

            m1.set_name("k1");
            m1.mutable_type()->set_type_id(Ydb::Type::INT32);

            m2.set_name("k2");
            m2.mutable_type()->set_type_id(Ydb::Type::INT64);
        }
        {
            auto& value = *rs.add_rows();
            auto& cell = *value.add_items();
            cell.add_items()->set_int32_value(31337); // k1
            cell.add_items()->set_int64_value(113370); // k2
        }

        {
            NJson::TJsonValue root;
            FormatResultSet(root, rs);

            TStringStream stream;
            NJson::WriteJson(&stream, &root);

            //Cerr << stream.Str() << Endl;
            TString expected = R"___({"data":[{"column0":["31337","113370"]}],"columns":[{"name":"column0","type":["StructType",[["k1",["DataType","Int32"]],["k2",["DataType","Int64"]]]]}]})___";

            UNIT_ASSERT_VALUES_EQUAL(stream.Str(), expected);
        }

        // pretty format
        {
            NJson::TJsonValue root;
            FormatResultSet(root, rs, true, true);

            TStringStream stream;
            NJson::WriteJson(&stream, &root);

            //Cerr << stream.Str() << Endl;
            TString expected = R"___({"data":[[{"k2":113370,"k1":31337}]],"columns":[{"name":"column0","type":"Struct<'k1':Int32,'k2':Int64>"}]})___";

            UNIT_ASSERT_VALUES_EQUAL(stream.Str(), expected);
        }
    }

    Y_UNIT_TEST(StructWithNoFields) {
        Ydb::ResultSet rs;
        {
            auto& column = *rs.add_columns();
            column.set_name("column0");
            column.mutable_type()->mutable_struct_type();
        }
        {
            auto& value = *rs.add_rows();
            value.add_items();
        }

        {
            NJson::TJsonValue root;
            FormatResultSet(root, rs);

            TStringStream stream;
            NJson::WriteJson(&stream, &root);

            //Cerr << stream.Str() << Endl;
            TString expected = R"___({"data":[{"column0":[]}],"columns":[{"name":"column0","type":["StructType",[]]}]})___";

            UNIT_ASSERT_VALUES_EQUAL(stream.Str(), expected);
        }

        // pretty format
        {
            NJson::TJsonValue root;
            FormatResultSet(root, rs, true, true);

            TStringStream stream;
            NJson::WriteJson(&stream, &root);

            //Cerr << stream.Str() << Endl;
            TString expected = R"___({"data":[[{}]],"columns":[{"name":"column0","type":"Struct<>"}]})___";

            UNIT_ASSERT_VALUES_EQUAL(stream.Str(), expected);
        }
    }

    Y_UNIT_TEST(StructTypeNameAsString) {
        Ydb::ResultSet rs;
        {
            auto& column = *rs.add_columns();
            column.set_name("column0");
            auto& struct_type = *column.mutable_type()->mutable_struct_type();
            auto& m1 = *struct_type.add_members();
            auto& m2 = *struct_type.add_members();

            m1.set_name("k1");
            m1.mutable_type()->set_type_id(Ydb::Type::INT32);

            m2.set_name("k2");
            m2.mutable_type()->set_type_id(Ydb::Type::INT64);
        }
        {
            auto& value = *rs.add_rows();
            auto& cell = *value.add_items();
            cell.add_items()->set_int32_value(31337); // k1
            cell.add_items()->set_int64_value(113370); // k2
        }

        NJson::TJsonValue root;
        FormatResultSet(root, rs, true);

        TStringStream stream;
        NJson::WriteJson(&stream, &root);

        //Cerr << stream.Str() << Endl;
        TString expected = R"___({"data":[{"column0":["31337","113370"]}],"columns":[{"name":"column0","type":"Struct<'k1':Int32,'k2':Int64>"}]})___";

        UNIT_ASSERT_VALUES_EQUAL(stream.Str(), expected);
    }

    Y_UNIT_TEST(Void) {
        Ydb::ResultSet rs;
        {
            auto& column = *rs.add_columns();
            column.set_name("column0");
            column.mutable_type()->set_void_type(google::protobuf::NullValue());
        }
        {
            auto& value = *rs.add_rows();
            auto& cell = *value.add_items();
            Y_UNUSED(cell);
        }

        NJson::TJsonValue root;
        FormatResultSet(root, rs);

        TStringStream stream;
        NJson::WriteJson(&stream, &root);

        // Cerr << stream.Str() << Endl;
        TString expected = R"___({"data":[{"column0":"Void"}],"columns":[{"name":"column0","type":["VoidType"]}]})___";
        UNIT_ASSERT_VALUES_EQUAL(stream.Str(), expected);
    }

    Y_UNIT_TEST(Null) {
        Ydb::ResultSet rs;
        {
            auto& column = *rs.add_columns();
            column.set_name("column0");
            column.mutable_type()->set_null_type(google::protobuf::NullValue());
        }
        {
            auto& value = *rs.add_rows();
            auto& cell = *value.add_items();
            Y_UNUSED(cell);
        }

        NJson::TJsonValue root;
        FormatResultSet(root, rs);

        TStringStream stream;
        NJson::WriteJson(&stream, &root);

        // Cerr << stream.Str() << Endl;
        TString expected = R"___({"data":[{"column0":null}],"columns":[{"name":"column0","type":["NullType"]}]})___";
        UNIT_ASSERT_VALUES_EQUAL(stream.Str(), expected);
    }

    Y_UNIT_TEST(EmptyList) {
        Ydb::ResultSet rs;
        {
            auto& column = *rs.add_columns();
            column.set_name("column0");
            column.mutable_type()->set_empty_list_type(google::protobuf::NullValue());
        }
        {
            auto& value = *rs.add_rows();
            auto& cell = *value.add_items();
            Y_UNUSED(cell);
        }

        NJson::TJsonValue root;
        FormatResultSet(root, rs);

        TStringStream stream;
        NJson::WriteJson(&stream, &root);

        // Cerr << stream.Str() << Endl;
        TString expected = R"___({"data":[{"column0":[]}],"columns":[{"name":"column0","type":["EmptyListType"]}]})___";
        UNIT_ASSERT_VALUES_EQUAL(stream.Str(), expected);
    }

    Y_UNIT_TEST(EmptyDict) {
        Ydb::ResultSet rs;
        {
            auto& column = *rs.add_columns();
            column.set_name("column0");
            column.mutable_type()->set_empty_dict_type(google::protobuf::NullValue());
        }
        {
            auto& value = *rs.add_rows();
            auto& cell = *value.add_items();
            Y_UNUSED(cell);
        }

        NJson::TJsonValue root;
        FormatResultSet(root, rs);

        TStringStream stream;
        NJson::WriteJson(&stream, &root);

        // Cerr << stream.Str() << Endl;
        TString expected = R"___({"data":[{"column0":[]}],"columns":[{"name":"column0","type":["EmptyDictType"]}]})___";
        UNIT_ASSERT_VALUES_EQUAL(stream.Str(), expected);
    }

    Y_UNIT_TEST(Tuple) {
        Ydb::ResultSet rs;
        {
            auto& column = *rs.add_columns();
            column.set_name("column0");
            auto& tuple_type = *column.mutable_type()->mutable_tuple_type();
            tuple_type.add_elements()->set_type_id(Ydb::Type::INT32);
            tuple_type.add_elements()->set_type_id(Ydb::Type::INT64);
        }
        {
            auto& value = *rs.add_rows();
            auto& cell = *value.add_items();
            cell.add_items()->set_int32_value(31337); // k1
            cell.add_items()->set_int64_value(113370); // k2
        }

        NJson::TJsonValue root;
        FormatResultSet(root, rs);

        TStringStream stream;
        NJson::WriteJson(&stream, &root);

        //Cerr << stream.Str() << Endl;
        TString expected = R"___({"data":[{"column0":["31337","113370"]}],"columns":[{"name":"column0","type":["TupleType",[["DataType","Int32"],["DataType","Int64"]]]}]})___";

        UNIT_ASSERT_VALUES_EQUAL(stream.Str(), expected);
    }

    Y_UNIT_TEST(EmptyTuple) {
        Ydb::ResultSet rs;
        {
            auto& column = *rs.add_columns();
            column.set_name("column0");
            column.mutable_type()->mutable_tuple_type();
        }
        {
            auto& value = *rs.add_rows();
            value.add_items();
        }

        NJson::TJsonValue root;
        FormatResultSet(root, rs);

        TStringStream stream;
        NJson::WriteJson(&stream, &root);

        //Cerr << stream.Str() << Endl;
        TString expected = R"___({"data":[{"column0":[]}],"columns":[{"name":"column0","type":["TupleType",[]]}]})___";

        UNIT_ASSERT_VALUES_EQUAL(stream.Str(), expected);
    }

    Y_UNIT_TEST(Dict) {
        Ydb::ResultSet rs;
        {
            auto& column = *rs.add_columns();
            column.set_name("column0");
            auto& dict_type = *column.mutable_type()->mutable_dict_type();
            dict_type.mutable_key()->set_type_id(Ydb::Type::INT32);
            dict_type.mutable_payload()->set_type_id(Ydb::Type::INT64);
        }
        {
            auto& value = *rs.add_rows();
            auto& cell = *value.add_items();
            auto* pair = cell.add_pairs();
            pair->mutable_key()->set_int32_value(31337);
            pair->mutable_payload()->set_int64_value(113370);

            pair = cell.add_pairs();
            pair->mutable_key()->set_int32_value(113370);
            pair->mutable_payload()->set_int64_value(31337);
        }

        NJson::TJsonValue root;
        FormatResultSet(root, rs);

        TStringStream stream;
        NJson::WriteJson(&stream, &root);

        //Cerr << stream.Str() << Endl;
        TString expected1 = R"___({"data":[{"column0":[["31337","113370"],["113370","31337"]]}],"columns":[{"name":"column0","type":["DictType",["DataType","Int32"],["DataType","Int64"]]}]})___";
        TString expected2 = R"___({"data":[{"column0":[["113370","31337"],["31337","113370"]]}],"columns":[{"name":"column0","type":["DictType",["DataType","Int32"],["DataType","Int64"]]}]})___";

        auto actual = stream.Str();

        UNIT_ASSERT_C(actual == expected1 || actual == expected2, "expected either " << expected1 << " or " << expected2
            << ", got " << actual);
    }

    Y_UNIT_TEST(VariantTuple) {
        Ydb::ResultSet rs;
        {
            auto& column = *rs.add_columns();
            column.set_name("column0");
            auto& variant_type = *column.mutable_type()->mutable_variant_type();
            auto& tuple_items = *variant_type.mutable_tuple_items();
            tuple_items.add_elements()->set_type_id(Ydb::Type::INT32);
            tuple_items.add_elements()->set_type_id(Ydb::Type::INT64);
        }
        {
            auto& value = *rs.add_rows();
            auto& cell = *value.add_items();
            cell.set_variant_index(0);
            cell.set_int32_value(31337);
        }
        {
            auto& value = *rs.add_rows();
            auto& cell = *value.add_items();
            cell.set_variant_index(1);
            cell.set_int64_value(131337);
        }

        NJson::TJsonValue root;
        FormatResultSet(root, rs);

        TStringStream stream;
        NJson::WriteJson(&stream, &root);
        // Cerr << "Stream >> " << stream.Str() << Endl;
        TString expected = R"___({"data":[{"column0":["0","31337"]},{"column0":["1","131337"]}],"columns":[{"name":"column0","type":["VariantType",["TupleType",[["DataType","Int32"],["DataType","Int64"]]]]}]})___";
        UNIT_ASSERT_VALUES_EQUAL(stream.Str(), expected);
    }

    Y_UNIT_TEST(Decimal) {
        Ydb::ResultSet rs;
        {
            auto& column = *rs.add_columns();
            column.set_name("column0");
            auto& type = *column.mutable_type()->mutable_decimal_type();
            type.set_precision(21);
            type.set_scale(7);
        }
        {
            auto& value = *rs.add_rows();
            value.add_items()->set_low_128(1);
        }

        NJson::TJsonValue root;
        FormatResultSet(root, rs);

        TStringStream stream;
        NJson::WriteJson(&stream, &root);

        // Cerr << stream.Str() << Endl;

        TString expected = R"___({"data":[{"column0":"0.0000001"}],"columns":[{"name":"column0","type":["DataType","Decimal","21","7"]}]})___";

        UNIT_ASSERT_VALUES_EQUAL(stream.Str(), expected);
    }

    Y_UNIT_TEST(Pg) {
        Ydb::ResultSet rs;
        {
            auto& column = *rs.add_columns();
            column.set_name("column0");
            auto& pg_type = *column.mutable_type()->mutable_pg_type();
            pg_type.set_type_name("pgbool");
            pg_type.set_oid(16);
            pg_type.set_typlen(234);
            pg_type.set_typmod(-987);
        }
        {
            auto& value = *rs.add_rows();
            auto& cell = *value.add_items();
            cell.set_text_value("my_text_value");
        }
        // empty text value
        {
            auto& value = *rs.add_rows();
            auto& cell = *value.add_items();
            cell.set_text_value("");
        }
        // null
        {
            auto& value = *rs.add_rows();
            value.add_items();
        }

        NJson::TJsonValue root;
        FormatResultSet(root, rs);

        TStringStream stream;
        NJson::WriteJson(&stream, &root);
        //Cerr << "Stream >> " << stream.Str() << Endl;
        TString expected = R"___({"data":[{"column0":"my_text_value"},{"column0":""},{"column0":null}],"columns":[{"name":"column0","type":["PgType","bool"]}]})___";
        UNIT_ASSERT_VALUES_EQUAL(stream.Str(), expected);
    }

    Y_UNIT_TEST(VariantStruct) {
        Ydb::ResultSet rs;
        {
            auto& column = *rs.add_columns();
            column.set_name("column0");
            auto& variant_type = *column.mutable_type()->mutable_variant_type();
            auto& struct_items = *variant_type.mutable_struct_items();

            auto& m1 = *struct_items.add_members();
            auto& m2 = *struct_items.add_members();

            m1.set_name("k1");
            m1.mutable_type()->set_type_id(Ydb::Type::INT32);

            m2.set_name("k2");
            m2.mutable_type()->set_type_id(Ydb::Type::INT64);
        }
        {
            auto& value = *rs.add_rows();
            auto& cell = *value.add_items();
            cell.set_variant_index(0);
            cell.set_int32_value(31337);
        }
        {
            auto& value = *rs.add_rows();
            auto& cell = *value.add_items();
            cell.set_variant_index(1);
            cell.set_int64_value(131337);
        }

        NJson::TJsonValue root;
        FormatResultSet(root, rs);

        TStringStream stream;
        NJson::WriteJson(&stream, &root);
        //Cerr << "Stream >> " << stream.Str() << Endl;
        TString expected = R"___({"data":[{"column0":["0","31337"]},{"column0":["1","131337"]}],"columns":[{"name":"column0","type":["VariantType",["StructType",[["k1",["DataType","Int32"]],["k2",["DataType","Int64"]]]]]}]})___";
        UNIT_ASSERT_VALUES_EQUAL(stream.Str(), expected);
    }

    Y_UNIT_TEST(Tagged) {
        Ydb::ResultSet rs;
        {
            auto& column = *rs.add_columns();
            column.set_name("column0");
            auto& tagged_type = *column.mutable_type()->mutable_tagged_type();
            tagged_type.set_tag("tag");
            auto& type = *tagged_type.mutable_type();
            type.set_type_id(Ydb::Type::INT32);
        }
        {
            auto& value = *rs.add_rows();
            value.add_items()->set_int32_value(31337);
        }

        NJson::TJsonValue root;
        FormatResultSet(root, rs);

        TStringStream stream;
        NJson::WriteJson(&stream, &root);
        // Cerr << "Stream >> " << stream.Str() << Endl;
        TString expected = R"___({"data":[{"column0":"31337"}],"columns":[{"name":"column0","type":["TaggedType","tag",["DataType","Int32"]]}]})___";
        UNIT_ASSERT_VALUES_EQUAL(stream.Str(), expected);
    }

    Y_UNIT_TEST(FormatEmptySchema) {
        FederatedQuery::Schema s;
        auto result = FormatSchema(s);
        UNIT_ASSERT_VALUES_EQUAL(result, R"(["StructType";[]])");
    }

    Y_UNIT_TEST(FormatNonEmptySchema) {
        FederatedQuery::Schema s;
        auto& c1 = *s.Addcolumn();
        c1.set_name("key");
        c1.mutable_type()->set_type_id(Ydb::Type::STRING);

        auto& c2 = *s.Addcolumn();
        c2.set_name("value");
        Ydb::Type t2;
        t2.set_type_id(Ydb::Type::UINT64);
        *c2.mutable_type()->mutable_optional_type()->mutable_item() = t2;

        auto result = FormatSchema(s);
        UNIT_ASSERT_VALUES_EQUAL(result, R"(["StructType";[["key";["DataType";"String"]];["value";["OptionalType";["DataType";"Uint64"]]]]])");
    }
}
