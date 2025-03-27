#include <ydb/public/lib/ydb_cli/common/yql_parser/yql_parser.h>

#include <library/cpp/testing/unittest/registar.h>

#include <iostream>

using namespace NYdb;
using namespace NYdb::NConsoleClient;

Y_UNIT_TEST_SUITE(TYqlParserTest) {
    Y_UNIT_TEST(TestBasicTypes) {
        {
            auto types = *TYqlParser::GetParamTypes("DECLARE $id AS Uint64;");
            std::cout << types.size() << std::endl;
            UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
            auto it = types.find("$id");
            UNIT_ASSERT(it != types.end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);
        }

        {
            auto types = *TYqlParser::GetParamTypes("DECLARE $name AS Utf8;");
            UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
            auto it = types.find("$name");
            UNIT_ASSERT(it != types.end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Utf8);
        }
    }

    Y_UNIT_TEST(TestComplexTypes) {
        {
            auto types = *TYqlParser::GetParamTypes("DECLARE $values AS List<Uint64>;");
            UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
            auto it = types.find("$values");
            UNIT_ASSERT(it != types.end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::List);
            
            parser.OpenList();
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);
            parser.CloseList();
        }

        {
            auto types = *TYqlParser::GetParamTypes("DECLARE $user AS Struct<id:Uint64,name:Utf8>;");
            UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
            auto it = types.find("$user");
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
    }

    Y_UNIT_TEST(TestMultipleParams) {
        TString query = R"(
            DECLARE $id AS Uint64;
            DECLARE $name AS Utf8;
            DECLARE $age AS Uint32;
        )";
        auto types = *TYqlParser::GetParamTypes(query);
        UNIT_ASSERT_VALUES_EQUAL(types.size(), 3);

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
        {
            auto it = types.find("$age");
            UNIT_ASSERT(it != types.end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint32);
        }
    }

    Y_UNIT_TEST(TestDecimalType) {
        auto types = *TYqlParser::GetParamTypes("DECLARE $price AS Decimal(22,9);");
        UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
        auto it = types.find("$price");
        UNIT_ASSERT(it != types.end());
        TTypeParser parser(it->second);
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Decimal);
        auto decimal = parser.GetDecimal();
        UNIT_ASSERT_VALUES_EQUAL(decimal.Precision, 22);
        UNIT_ASSERT_VALUES_EQUAL(decimal.Scale, 9);
    }

    Y_UNIT_TEST(TestDictType) {
        auto types = *TYqlParser::GetParamTypes("DECLARE $dict AS Dict<Utf8,Uint64>;");
        UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
        auto it = types.find("$dict");
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

    Y_UNIT_TEST(TestTupleType) {
        auto types = *TYqlParser::GetParamTypes("DECLARE $tuple AS Tuple<Uint64,Utf8,Bool>;");
        UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
        auto it = types.find("$tuple");
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
        auto types = *TYqlParser::GetParamTypes(query);
        UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
        auto it = types.find("$nested");
        UNIT_ASSERT(it != types.end());

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
            auto types = *TYqlParser::GetParamTypes("DECLARE $id AS UINT64;");
            UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
            auto it = types.find("$id");
            UNIT_ASSERT(it != types.end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);
        }

        {
            auto types = *TYqlParser::GetParamTypes("DECLARE $list AS LIST<UINT32>;");
            UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
            auto it = types.find("$list");
            UNIT_ASSERT(it != types.end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::List);
            parser.OpenList();
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint32);
            parser.CloseList();
        }

        {
            auto types = *TYqlParser::GetParamTypes("DECLARE $struct AS STRUCT<ID:UINT64,NAME:UTF8>;");
            UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
            auto it = types.find("$struct");
            UNIT_ASSERT(it != types.end());
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
            auto types = *TYqlParser::GetParamTypes("DECLARE $dict AS DICT<UTF8,UINT64>;");
            UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
            auto it = types.find("$dict");
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
            auto types = *TYqlParser::GetParamTypes("DECLARE $tuple AS TUPLE<UINT64,UTF8,BOOL>;");
            UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
            auto it = types.find("$tuple");
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

            UNIT_ASSERT(parser.TryNextElement());
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Bool);

            UNIT_ASSERT(!parser.TryNextElement());
            parser.CloseTuple();
        }

        {
            auto types = *TYqlParser::GetParamTypes("DECLARE $price AS DECIMAL(22,9);");
            UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
            auto it = types.find("$price");
            UNIT_ASSERT(it != types.end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Decimal);
            auto decimal = parser.GetDecimal();
            UNIT_ASSERT_VALUES_EQUAL(decimal.Precision, 22);
            UNIT_ASSERT_VALUES_EQUAL(decimal.Scale, 9);
        }

        {
            auto types = *TYqlParser::GetParamTypes("declare $id as UINT64;");
            UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
            auto it = types.find("$id");
            UNIT_ASSERT(it != types.end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);
        }
    }

    Y_UNIT_TEST(TestInvalidQuery) {
        // Проверяем, что метод возвращает nullopt для некорректного запроса
        auto types = TYqlParser::GetParamTypes("DECLARE $id AS @#$%^;");
        UNIT_ASSERT(!types.has_value());

        // Проверяем, что метод возвращает nullopt для незавершенного запроса
        types = TYqlParser::GetParamTypes("DECLARE $id AS");
        UNIT_ASSERT(!types.has_value());

        // Проверяем, что метод возвращает nullopt для запроса с неправильным синтаксисом
        types = TYqlParser::GetParamTypes("DECLARE AS $id Uint64;");
        UNIT_ASSERT(!types.has_value());
    }
}
