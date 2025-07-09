#include <ydb/public/lib/ydb_cli/common/yql_parser/yql_parser.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/file.h>

using namespace NYdb;
using namespace NYdb::NConsoleClient;

Y_UNIT_TEST_SUITE(TYqlParamParserTest) {
    Y_UNIT_TEST(TestBasicTypes) {
        {
            auto types = TYqlParamParser::GetParamTypes("DECLARE $id AS Uint64;");
            UNIT_ASSERT(types.has_value());
            UNIT_ASSERT_VALUES_EQUAL(types->size(), 1);
            auto it = types->find("$id");
            UNIT_ASSERT(it != types->end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);
        }

        {
            auto types = TYqlParamParser::GetParamTypes("DECLARE $name AS Utf8;");
            UNIT_ASSERT(types.has_value());
            UNIT_ASSERT_VALUES_EQUAL(types->size(), 1);
            auto it = types->find("$name");
            UNIT_ASSERT(it != types->end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Utf8);
        }
    }

    Y_UNIT_TEST(TestListType) {
        auto types = TYqlParamParser::GetParamTypes("DECLARE $values AS List<Uint64>;");
        UNIT_ASSERT(types.has_value());
        UNIT_ASSERT_VALUES_EQUAL(types->size(), 1);
        auto it = types->find("$values");
        UNIT_ASSERT(it != types->end());
        TTypeParser parser(it->second);
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::List);
        
        parser.OpenList();
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
        UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);
        parser.CloseList();
    }

    Y_UNIT_TEST(TestStructType) {
        auto types = TYqlParamParser::GetParamTypes("DECLARE $user AS Struct<id:Uint64,name:Utf8>;");
        UNIT_ASSERT(types.has_value());
        UNIT_ASSERT_VALUES_EQUAL(types->size(), 1);
        auto it = types->find("$user");
        UNIT_ASSERT(it != types->end());
        TTypeParser parser(it->second);
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Struct);

        parser.OpenStruct();
        
        UNIT_ASSERT(parser.TryNextMember());
        UNIT_ASSERT_VALUES_EQUAL(parser.GetMemberName(), "id");
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
        UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);

        UNIT_ASSERT(parser.TryNextMember());
        UNIT_ASSERT_VALUES_EQUAL(parser.GetMemberName(), "name");
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
        UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Utf8);

        UNIT_ASSERT(!parser.TryNextMember());
        parser.CloseStruct();
    }

    Y_UNIT_TEST(TestMultipleParams) {
        TString query = R"(
            DECLARE $id AS Uint64;
            DECLARE $name AS Utf8;
            DECLARE $age AS Uint32;
        )";
        auto types = TYqlParamParser::GetParamTypes(query);
        UNIT_ASSERT(types.has_value());
        UNIT_ASSERT_VALUES_EQUAL(types->size(), 3);

        {
            auto it = types->find("$id");
            UNIT_ASSERT(it != types->end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);
        }
        {
            auto it = types->find("$name");
            UNIT_ASSERT(it != types->end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Utf8);
        }
        {
            auto it = types->find("$age");
            UNIT_ASSERT(it != types->end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint32);
        }
    }

    Y_UNIT_TEST(TestDecimalType) {
        auto types = TYqlParamParser::GetParamTypes("DECLARE $price AS Decimal(22,9);");
        UNIT_ASSERT(types.has_value());
        UNIT_ASSERT_VALUES_EQUAL(types->size(), 1);
        auto it = types->find("$price");
        UNIT_ASSERT(it != types->end());
        TTypeParser parser(it->second);
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Decimal);
        auto decimal = parser.GetDecimal();
        UNIT_ASSERT_VALUES_EQUAL(decimal.Precision, 22);
        UNIT_ASSERT_VALUES_EQUAL(decimal.Scale, 9);
    }

    Y_UNIT_TEST(TestDictType) {
        auto types = TYqlParamParser::GetParamTypes("DECLARE $dict AS Dict<Utf8,Uint64>;");
        UNIT_ASSERT(types.has_value());
        UNIT_ASSERT_VALUES_EQUAL(types->size(), 1);
        auto it = types->find("$dict");
        UNIT_ASSERT(it != types->end());
        TTypeParser parser(it->second);
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Dict);

        parser.OpenDict();
        
        parser.DictKey();
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
        UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Utf8);

        parser.DictPayload();
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
        UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);

        parser.CloseDict();
    }

    Y_UNIT_TEST(TestTupleType) {
        auto types = TYqlParamParser::GetParamTypes("DECLARE $tuple AS Tuple<Uint64,Utf8,Bool>;");
        UNIT_ASSERT(types.has_value());
        UNIT_ASSERT_VALUES_EQUAL(types->size(), 1);
        auto it = types->find("$tuple");
        UNIT_ASSERT(it != types->end());
        TTypeParser parser(it->second);
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Tuple);

        parser.OpenTuple();

        UNIT_ASSERT(parser.TryNextElement());
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
        UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);

        UNIT_ASSERT(parser.TryNextElement());
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
        UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Utf8);

        UNIT_ASSERT(parser.TryNextElement());
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
        UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Bool);

        UNIT_ASSERT(!parser.TryNextElement());
        parser.CloseTuple();
    }

    Y_UNIT_TEST(TestNestedTypes) {
        TString query = R"(
            DECLARE $nested AS List<Struct<
                id: Uint64,
                name: Utf8,
                tags: List<Utf8>,
                meta: Dict<Utf8, List<Uint32>>
            >>;
        )";
        auto types = TYqlParamParser::GetParamTypes(query);
        UNIT_ASSERT(types.has_value());
        UNIT_ASSERT_VALUES_EQUAL(types->size(), 1);
        auto it = types->find("$nested");
        UNIT_ASSERT(it != types->end());

        TTypeParser parser(it->second);
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::List);

        parser.OpenList();
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Struct);

        parser.OpenStruct();

        UNIT_ASSERT(parser.TryNextMember());
        UNIT_ASSERT_VALUES_EQUAL(parser.GetMemberName(), "id");
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
        UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);

        UNIT_ASSERT(parser.TryNextMember());
        UNIT_ASSERT_VALUES_EQUAL(parser.GetMemberName(), "name");
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
        UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Utf8);

        UNIT_ASSERT(parser.TryNextMember());
        UNIT_ASSERT_VALUES_EQUAL(parser.GetMemberName(), "tags");
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::List);
        parser.OpenList();
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
        UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Utf8);
        parser.CloseList();

        UNIT_ASSERT(parser.TryNextMember());
        UNIT_ASSERT_VALUES_EQUAL(parser.GetMemberName(), "meta");
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Dict);
        
        parser.OpenDict();
        
        parser.DictKey();
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
        UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Utf8);

        parser.DictPayload();
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::List);
        parser.OpenList();
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
        UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint32);
        parser.CloseList();

        parser.CloseDict();

        UNIT_ASSERT(!parser.TryNextMember());
        parser.CloseStruct();
        parser.CloseList();
    }

    Y_UNIT_TEST(TestCaseInsensitiveTypes) {
        {
            auto types = TYqlParamParser::GetParamTypes("DECLARE $id AS UINT64;");
            UNIT_ASSERT(types.has_value());
            UNIT_ASSERT_VALUES_EQUAL(types->size(), 1);
            auto it = types->find("$id");
            UNIT_ASSERT(it != types->end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);
        }

        {
            auto types = TYqlParamParser::GetParamTypes("DECLARE $list AS LIST<UINT32>;");
            UNIT_ASSERT(types.has_value());
            UNIT_ASSERT_VALUES_EQUAL(types->size(), 1);
            auto it = types->find("$list");
            UNIT_ASSERT(it != types->end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::List);
            parser.OpenList();
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint32);
            parser.CloseList();
        }

        {
            auto types = TYqlParamParser::GetParamTypes("DECLARE $struct AS STRUCT<ID:UINT64,NAME:UTF8>;");
            UNIT_ASSERT(types.has_value());
            UNIT_ASSERT_VALUES_EQUAL(types->size(), 1);
            auto it = types->find("$struct");
            UNIT_ASSERT(it != types->end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Struct);

            parser.OpenStruct();
            
            UNIT_ASSERT(parser.TryNextMember());
            UNIT_ASSERT_VALUES_EQUAL(parser.GetMemberName(), "ID");
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);

            UNIT_ASSERT(parser.TryNextMember());
            UNIT_ASSERT_VALUES_EQUAL(parser.GetMemberName(), "NAME");
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Utf8);

            UNIT_ASSERT(!parser.TryNextMember());
            parser.CloseStruct();
        }

        {
            auto types = TYqlParamParser::GetParamTypes("DECLARE $dict AS DICT<UTF8,UINT64>;");
            UNIT_ASSERT(types.has_value());
            UNIT_ASSERT_VALUES_EQUAL(types->size(), 1);
            auto it = types->find("$dict");
            UNIT_ASSERT(it != types->end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Dict);

            parser.OpenDict();
            
            parser.DictKey();
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Utf8);

            parser.DictPayload();
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);

            parser.CloseDict();
        }

        {
            auto types = TYqlParamParser::GetParamTypes("DECLARE $tuple AS TUPLE<UINT64,UTF8,BOOL>;");
            UNIT_ASSERT(types.has_value());
            UNIT_ASSERT_VALUES_EQUAL(types->size(), 1);
            auto it = types->find("$tuple");
            UNIT_ASSERT(it != types->end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Tuple);

            parser.OpenTuple();

            UNIT_ASSERT(parser.TryNextElement());
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);

            UNIT_ASSERT(parser.TryNextElement());
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Utf8);

            UNIT_ASSERT(parser.TryNextElement());
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Bool);

            UNIT_ASSERT(!parser.TryNextElement());
            parser.CloseTuple();
        }

        {
            auto types = TYqlParamParser::GetParamTypes("DECLARE $price AS DECIMAL(22,9);");
            UNIT_ASSERT(types.has_value());
            UNIT_ASSERT_VALUES_EQUAL(types->size(), 1);
            auto it = types->find("$price");
            UNIT_ASSERT(it != types->end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Decimal);
            auto decimal = parser.GetDecimal();
            UNIT_ASSERT_VALUES_EQUAL(decimal.Precision, 22);
            UNIT_ASSERT_VALUES_EQUAL(decimal.Scale, 9);
        }

        {
            auto types = TYqlParamParser::GetParamTypes("declare $id as UINT64;");
            UNIT_ASSERT(types.has_value());
            UNIT_ASSERT_VALUES_EQUAL(types->size(), 1);
            auto it = types->find("$id");
            UNIT_ASSERT(it != types->end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);
        }
    }

    Y_UNIT_TEST(TestOptionalTypes) {
        {
            auto types = TYqlParamParser::GetParamTypes("DECLARE $id AS Uint64?;");
            UNIT_ASSERT(types.has_value());
            UNIT_ASSERT_VALUES_EQUAL(types->size(), 1);
            auto it = types->find("$id");
            UNIT_ASSERT(it != types->end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Optional);
            parser.OpenOptional();
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);
            parser.CloseOptional();
        }

        {
            auto types = TYqlParamParser::GetParamTypes("DECLARE $id AS Optional<Uint64>;");
            UNIT_ASSERT(types.has_value());
            UNIT_ASSERT_VALUES_EQUAL(types->size(), 1);
            auto it = types->find("$id");
            UNIT_ASSERT(it != types->end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Optional);
            parser.OpenOptional();
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);
            parser.CloseOptional();
        }

        {
            auto types = TYqlParamParser::GetParamTypes("DECLARE $list AS Optional<List<Uint64>>;");
            UNIT_ASSERT(types.has_value());
            UNIT_ASSERT_VALUES_EQUAL(types->size(), 1);
            auto it = types->find("$list");
            UNIT_ASSERT(it != types->end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Optional);
            parser.OpenOptional();
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::List);
            parser.OpenList();
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);
            parser.CloseList();
            parser.CloseOptional();
        }

        {
            auto types = TYqlParamParser::GetParamTypes("DECLARE $struct AS Struct<id:Uint64,name:Utf8>?;");
            UNIT_ASSERT(types.has_value());
            UNIT_ASSERT_VALUES_EQUAL(types->size(), 1);
            auto it = types->find("$struct");
            UNIT_ASSERT(it != types->end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Optional);
            parser.OpenOptional();
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Struct);
            parser.OpenStruct();
            
            UNIT_ASSERT(parser.TryNextMember());
            UNIT_ASSERT_VALUES_EQUAL(parser.GetMemberName(), "id");
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);

            UNIT_ASSERT(parser.TryNextMember());
            UNIT_ASSERT_VALUES_EQUAL(parser.GetMemberName(), "name");
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Utf8);

            UNIT_ASSERT(!parser.TryNextMember());
            parser.CloseStruct();
            parser.CloseOptional();
        }

        {
            auto types = TYqlParamParser::GetParamTypes("DECLARE $dict AS Optional<Dict<Utf8,Uint64>>;");
            UNIT_ASSERT(types.has_value());
            UNIT_ASSERT_VALUES_EQUAL(types->size(), 1);
            auto it = types->find("$dict");
            UNIT_ASSERT(it != types->end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Optional);
            parser.OpenOptional();
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Dict);
            
            parser.OpenDict();
            parser.DictKey();
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Utf8);

            parser.DictPayload();
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);
            parser.CloseDict();
            
            parser.CloseOptional();
        }

        {
            auto types = TYqlParamParser::GetParamTypes("DECLARE $tuple AS Optional<Tuple<Uint64,Utf8>>;");
            UNIT_ASSERT(types.has_value());
            UNIT_ASSERT_VALUES_EQUAL(types->size(), 1);
            auto it = types->find("$tuple");
            UNIT_ASSERT(it != types->end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Optional);
            parser.OpenOptional();
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Tuple);
            
            parser.OpenTuple();
            UNIT_ASSERT(parser.TryNextElement());
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);

            UNIT_ASSERT(parser.TryNextElement());
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Utf8);

            UNIT_ASSERT(!parser.TryNextElement());
            parser.CloseTuple();
            
            parser.CloseOptional();
        }

        {
            auto types = TYqlParamParser::GetParamTypes("DECLARE $decimal AS Optional<Decimal(10,2)>;");
            UNIT_ASSERT(types.has_value());
            UNIT_ASSERT_VALUES_EQUAL(types->size(), 1);
            auto it = types->find("$decimal");
            UNIT_ASSERT(it != types->end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Optional);
            parser.OpenOptional();
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Decimal);
            auto decimal = parser.GetDecimal();
            UNIT_ASSERT_VALUES_EQUAL(decimal.Precision, 10);
            UNIT_ASSERT_VALUES_EQUAL(decimal.Scale, 2);
            parser.CloseOptional();
        }

        {
            auto types = TYqlParamParser::GetParamTypes("DECLARE $nested AS Optional<List<Struct<id:Uint64,name:Utf8>>>;");
            UNIT_ASSERT(types.has_value());
            UNIT_ASSERT_VALUES_EQUAL(types->size(), 1);
            auto it = types->find("$nested");
            UNIT_ASSERT(it != types->end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Optional);
            parser.OpenOptional();
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::List);
            
            parser.OpenList();
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Struct);
            parser.OpenStruct();
            
            UNIT_ASSERT(parser.TryNextMember());
            UNIT_ASSERT_VALUES_EQUAL(parser.GetMemberName(), "id");
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);

            UNIT_ASSERT(parser.TryNextMember());
            UNIT_ASSERT_VALUES_EQUAL(parser.GetMemberName(), "name");
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Utf8);

            UNIT_ASSERT(!parser.TryNextMember());
            parser.CloseStruct();
            parser.CloseList();
            
            parser.CloseOptional();
        }
    }

    Y_UNIT_TEST(TestInvalidQuery) {
        {
            auto types = TYqlParamParser::GetParamTypes("DECLARE $id AS @#$%^;");
            UNIT_ASSERT(!types.has_value());
        }

        {
            auto types = TYqlParamParser::GetParamTypes("DECLARE $id AS");
            UNIT_ASSERT(!types.has_value());
        }

        {
            auto types = TYqlParamParser::GetParamTypes("DECLARE AS $id Uint64;");
            UNIT_ASSERT(!types.has_value());
        }

        {
            auto types = TYqlParamParser::GetParamTypes("DECLARE $invalid AS Optional;");
            UNIT_ASSERT(!types.has_value());
        }

        {
            auto types = TYqlParamParser::GetParamTypes("DECLARE $invalid AS Optional<>;");
            UNIT_ASSERT(!types.has_value());
        }

        {
            auto types = TYqlParamParser::GetParamTypes("DECLARE $invalid AS Optional<Uint64,Utf8>;");
            UNIT_ASSERT(!types.has_value());
        }

        {
            auto types = TYqlParamParser::GetParamTypes("DECLARE $invalid AS SomeUnknownType;");
            UNIT_ASSERT(!types.has_value());
        }

        auto types = *TYqlParamParser::GetParamTypes(R"(
            DECLARE $id AS Uint64;
            abacaba abacaba;
            lol lol lol
            DECLARE $name AS Utf8;
        )");
    
        UNIT_ASSERT(types.size() == 2);

        {
            auto it = types.find("$id");
            UNIT_ASSERT(it != types.end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);
        }

        {
            auto it = types.find("$name"); 
            UNIT_ASSERT(it != types.end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Utf8);
        }
    }

    Y_UNIT_TEST(TestWhitespace) {
        {
            auto types = *TYqlParamParser::GetParamTypes("DECLARE  $id  AS  Uint64;");
            UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
            auto it = types.find("$id");
            UNIT_ASSERT(it != types.end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);
        }

        {
            auto types = *TYqlParamParser::GetParamTypes("DECLARE\t$id\tAS\tUint64;");
            UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
            auto it = types.find("$id");
            UNIT_ASSERT(it != types.end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);
        }

        {
            auto types = *TYqlParamParser::GetParamTypes("DECLARE\n$id\nAS\nUint64;");
            UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
            auto it = types.find("$id");
            UNIT_ASSERT(it != types.end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);
        }

        {
            auto types = *TYqlParamParser::GetParamTypes("DECLARE $id AS List< Uint64 >;");
            UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
            auto it = types.find("$id");
            UNIT_ASSERT(it != types.end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::List);
            parser.OpenList();
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);
            parser.CloseList();
        }

        {
            auto types = *TYqlParamParser::GetParamTypes("DECLARE $id AS Struct< id : Uint64, name : Utf8 >;");
            UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
            auto it = types.find("$id");
            UNIT_ASSERT(it != types.end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Struct);
            parser.OpenStruct();
            
            UNIT_ASSERT(parser.TryNextMember());
            UNIT_ASSERT_VALUES_EQUAL(parser.GetMemberName(), "id");
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);

            UNIT_ASSERT(parser.TryNextMember());
            UNIT_ASSERT_VALUES_EQUAL(parser.GetMemberName(), "name");
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Utf8);

            UNIT_ASSERT(!parser.TryNextMember());
            parser.CloseStruct();
        }

        {
            auto types = *TYqlParamParser::GetParamTypes("DECLARE $id AS Tuple< Uint64, Utf8 >;");
            UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
            auto it = types.find("$id");
            UNIT_ASSERT(it != types.end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Tuple);
            parser.OpenTuple();
            
            UNIT_ASSERT(parser.TryNextElement());
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);

            UNIT_ASSERT(parser.TryNextElement());
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Utf8);

            UNIT_ASSERT(!parser.TryNextElement());
            parser.CloseTuple();
        }

        {
            auto types = *TYqlParamParser::GetParamTypes("DECLARE $id AS Dict< Utf8, Uint64 >;");
            UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
            auto it = types.find("$id");
            UNIT_ASSERT(it != types.end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Dict);
            parser.OpenDict();
            
            parser.DictKey();
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Utf8);

            parser.DictPayload();
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);
            parser.CloseDict();
        }

        {
            auto types = *TYqlParamParser::GetParamTypes("DECLARE $id AS Decimal( 10, 2 );");
            UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
            auto it = types.find("$id");
            UNIT_ASSERT(it != types.end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Decimal);
            auto decimal = parser.GetDecimal();
            UNIT_ASSERT_VALUES_EQUAL(decimal.Precision, 10);
            UNIT_ASSERT_VALUES_EQUAL(decimal.Scale, 2);
        }

        {
            auto types = *TYqlParamParser::GetParamTypes("DECLARE $id AS Uint64 ?;");
            UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
            auto it = types.find("$id");
            UNIT_ASSERT(it != types.end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Optional);
            parser.OpenOptional();
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);
            parser.CloseOptional();
        }

        {
            auto types = *TYqlParamParser::GetParamTypes("DECLARE $id AS Optional < Uint64 >;");
            UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
            auto it = types.find("$id");
            UNIT_ASSERT(it != types.end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Optional);
            parser.OpenOptional();
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);
            parser.CloseOptional();
        }

        {
            auto types = *TYqlParamParser::GetParamTypes(R"(
                DECLARE $id AS Uint64;
                DECLARE $name AS Utf8;
                DECLARE $age AS Uint32;
            )");
            UNIT_ASSERT_VALUES_EQUAL(types.size(), 3);
            auto it = types.find("$id");
            UNIT_ASSERT(it != types.end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);
        }
    }

    Y_UNIT_TEST(TestComplexQuery) {
        TString query = R"(
            DECLARE $user_id AS Uint64;
            DECLARE $start_date AS Date;
            
            SELECT 
                user_id,
                name,
                email,
                created_at,
                last_login
            FROM users
            WHERE user_id = $user_id
            AND created_at >= $start_date;
            
            DECLARE $user_data AS Struct<
                name: Utf8,
                email: Utf8,
                age: Uint32?,
                preferences: Dict<Utf8, Json>
            >;
            
            DECLARE $user_tags AS List<Utf8>;
            
            UPDATE users
            SET 
                name = $user_data.name,
                email = $user_data.email,
                age = $user_data.age,
                preferences = $user_data.preferences,
                tags = $user_tags,
                updated_at = CurrentUtcTimestamp()
            WHERE user_id = $user_id;
            
            DECLARE $stats AS Struct<
                total_users: Uint64,
                active_users: Uint64,
                avg_age: Decimal(5,2)
            >;
            
            SELECT 
                COUNT(*) as total_users,
                COUNT(CASE WHEN last_login >= $start_date THEN 1 END) as active_users,
                AVG(CAST(age AS Decimal(5,2))) as avg_age
            FROM users;
        )";

        auto types = TYqlParamParser::GetParamTypes(query);
        UNIT_ASSERT(types.has_value());
        UNIT_ASSERT_VALUES_EQUAL(types->size(), 5);

        {
            auto it = types->find("$user_id");
            UNIT_ASSERT(it != types->end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);
        }

        {
            auto it = types->find("$start_date");
            UNIT_ASSERT(it != types->end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Date);
        }

        {
            auto it = types->find("$user_data");
            UNIT_ASSERT(it != types->end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Struct);
            parser.OpenStruct();
            
            UNIT_ASSERT(parser.TryNextMember());
            UNIT_ASSERT_VALUES_EQUAL(parser.GetMemberName(), "name");
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Utf8);

            UNIT_ASSERT(parser.TryNextMember());
            UNIT_ASSERT_VALUES_EQUAL(parser.GetMemberName(), "email");
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Utf8);

            UNIT_ASSERT(parser.TryNextMember());
            UNIT_ASSERT_VALUES_EQUAL(parser.GetMemberName(), "age");
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Optional);
            parser.OpenOptional();
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint32);
            parser.CloseOptional();

            UNIT_ASSERT(parser.TryNextMember());
            UNIT_ASSERT_VALUES_EQUAL(parser.GetMemberName(), "preferences");
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Dict);
            parser.OpenDict();
            parser.DictKey();
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Utf8);
            parser.DictPayload();
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Json);
            parser.CloseDict();

            UNIT_ASSERT(!parser.TryNextMember());
            parser.CloseStruct();
        }

        {
            auto it = types->find("$user_tags");
            UNIT_ASSERT(it != types->end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::List);
            parser.OpenList();
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Utf8);
            parser.CloseList();
        }

        {
            auto it = types->find("$stats");
            UNIT_ASSERT(it != types->end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Struct);
            parser.OpenStruct();
            
            UNIT_ASSERT(parser.TryNextMember());
            UNIT_ASSERT_VALUES_EQUAL(parser.GetMemberName(), "total_users");
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);

            UNIT_ASSERT(parser.TryNextMember());
            UNIT_ASSERT_VALUES_EQUAL(parser.GetMemberName(), "active_users");
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);

            UNIT_ASSERT(parser.TryNextMember());
            UNIT_ASSERT_VALUES_EQUAL(parser.GetMemberName(), "avg_age");
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Decimal);
            auto decimal = parser.GetDecimal();
            UNIT_ASSERT_VALUES_EQUAL(decimal.Precision, 5);
            UNIT_ASSERT_VALUES_EQUAL(decimal.Scale, 2);

            UNIT_ASSERT(!parser.TryNextMember());
            parser.CloseStruct();
        }
    }

    Y_UNIT_TEST(TestAllTypes) {
        TString jsonContent;
        TFileInput file(TStringBuilder() << ArcadiaSourceRoot() << "/yql/essentials/data/language/types.json");
        jsonContent = file.ReadAll();

        NJson::TJsonValue jsonValue;
        UNIT_ASSERT(NJson::ReadJsonTree(jsonContent, &jsonValue));

        for (const auto& [typeName, typeInfo] : jsonValue.GetMap()) {
            if (typeName == "Generic" || typeName == "Unit" || typeName == "Void" ||
                typeName == "EmptyList" || typeName == "EmptyDict" || typeName == "Null" ||
                typeName == "TzDate32" || typeName == "TzDatetime64" || typeName == "TzTimestamp64" ||
                typeInfo.GetMap().at("kind").GetString() == "Pg") {
                continue;
            }

            TString query = TStringBuilder() << "DECLARE $param AS " << typeName << ";";

            auto types = TYqlParamParser::GetParamTypes(query);
            UNIT_ASSERT_C(types.has_value(), "Unknown type: " << typeName);
            UNIT_ASSERT_VALUES_EQUAL(types->size(), 1);
            UNIT_ASSERT(types->contains("$param"));
        }
    }
}
