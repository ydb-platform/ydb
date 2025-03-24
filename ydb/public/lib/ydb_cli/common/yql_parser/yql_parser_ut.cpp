#include <library/cpp/testing/unittest/registar.h>
#include <ydb/public/lib/ydb_cli/common/yql_parser/yql_parser.h>

using namespace NYdb::NConsoleClient;
using NYdb::TTypeParser;
using NYdb::EPrimitiveType;
using NYdb::TType;

Y_UNIT_TEST_SUITE(TYqlParserTest) {
    Y_UNIT_TEST(TestBasicTypes) {
        // Тестируем базовые типы
        {
            TString query = "DECLARE $id AS Uint64;";
            auto types = TYqlParser::GetParamTypes(query);
            UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
            auto it = types.find("$id");
            UNIT_ASSERT(it != types.end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);
        }

        {
            TString query = "DECLARE $name AS Utf8;";
            auto types = TYqlParser::GetParamTypes(query);
            UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
            auto it = types.find("$name");
            UNIT_ASSERT(it != types.end());
            TTypeParser parser(it->second);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Utf8);
        }
    }

    Y_UNIT_TEST(TestComplexTypes) {
        // Тестируем сложные типы (List, Struct)
        {
            TString query = "DECLARE $values AS List<Uint64>;";
            auto types = TYqlParser::GetParamTypes(query);
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
            TString query = "DECLARE $user AS Struct<id:Uint64,name:Utf8>;";
            auto types = TYqlParser::GetParamTypes(query);
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
        // Тестируем несколько параметров в одном запросе
        TString query = R"(
            DECLARE $id AS Uint64;
            DECLARE $name AS Utf8;
            DECLARE $age AS Uint32;
        )";
        auto types = TYqlParser::GetParamTypes(query);
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
        // Тестируем тип Decimal
        TString query = "DECLARE $price AS Decimal(22,9);";
        auto types = TYqlParser::GetParamTypes(query);
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
        // Тестируем тип Dict
        TString query = "DECLARE $dict AS Dict<Utf8,Uint64>;";
        auto types = TYqlParser::GetParamTypes(query);
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
        // Тестируем тип Tuple
        TString query = "DECLARE $tuple AS Tuple<Uint64,Utf8,Bool>;";
        auto types = TYqlParser::GetParamTypes(query);
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
        // Тестируем вложенные типы
        TString query = R"(
            DECLARE $nested AS List<Struct<
                id: Uint64,
                name: Utf8,
                tags: List<Utf8>,
                meta: Dict<Utf8, List<Uint32>>
            >>;
        )";
        auto types = TYqlParser::GetParamTypes(query);
        UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
        auto it = types.find("$nested");
        UNIT_ASSERT(it != types.end());

        TTypeParser parser(it->second);
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::List);

        parser.OpenList();
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Struct);

        parser.OpenStruct();

        // Проверяем id
        UNIT_ASSERT(parser.TryNextMember());
        UNIT_ASSERT_VALUES_EQUAL(parser.GetMemberName(), "id");
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
        UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Uint64);

        // Проверяем name
        UNIT_ASSERT(parser.TryNextMember());
        UNIT_ASSERT_VALUES_EQUAL(parser.GetMemberName(), "name");
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
        UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Utf8);

        // Проверяем tags
        UNIT_ASSERT(parser.TryNextMember());
        UNIT_ASSERT_VALUES_EQUAL(parser.GetMemberName(), "tags");
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::List);
        parser.OpenList();
        UNIT_ASSERT_VALUES_EQUAL(parser.GetKind(), TTypeParser::ETypeKind::Primitive);
        UNIT_ASSERT_VALUES_EQUAL(parser.GetPrimitive(), EPrimitiveType::Utf8);
        parser.CloseList();

        // Проверяем meta
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
} 