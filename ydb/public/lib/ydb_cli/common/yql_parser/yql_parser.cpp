#include "yql_parser.h"

#include <yql/essentials/parser/lexer_common/lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <util/string/split.h>
#include <util/string/strip.h>

namespace NYdb {
namespace NConsoleClient {

std::map<std::string, TType> TYqlParser::GetParamTypes(const TString& queryText) {
    std::map<std::string, TType> result;
    
    // Создаем лексер
    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();

    auto lexer = MakeLexer(lexers, /* ansi = */ false, /* antlr4 = */ true);

    // Токенизируем запрос
    TVector<NSQLTranslation::TParsedToken> tokens;
    NYql::TIssues issues;
    [[maybe_unused]] auto tokenizeResult = NSQLTranslation::Tokenize(*lexer, queryText, "Query", tokens, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS);

    // Анализируем токены для поиска параметров
    for (size_t i = 0; i < tokens.size(); ++i) {
        const auto& token = tokens[i];
        
        // Ищем объявление параметра (DECLARE $name AS type)
        if (token.Name == "DECLARE" && i + 4 < tokens.size()) {
            const auto& paramToken = tokens[i + 1];
            const auto& asToken = tokens[i + 2];
            const auto& typeToken = tokens[i + 3];
            const auto& semicolonToken = tokens[i + 4];
            
            if (paramToken.Name == "DOLLAR" && 
                asToken.Name == "AS" && 
                semicolonToken.Name == "SEMICOLON") {
                
                // Получаем имя параметра
                TString paramName = "$" + paramToken.Content;
                
                // Получаем тип данных
                TString typeStr = StripString(typeToken.Content);
                
                // Создаем тип на основе строкового представления
                TTypeBuilder builder;
                ProcessType(typeStr, builder);
                
                // Сохраняем тип параметра
                result.emplace(paramName, builder.Build());
                
                // Пропускаем обработанные токены
                i += 4;
            }
        }
    }
    
    return result;
}

void TYqlParser::ProcessType(const TString& typeStr, TTypeBuilder& builder) {
    // Обрабатываем базовые типы
    if (typeStr == "Bool") {
        builder.Primitive(EPrimitiveType::Bool);
    } else if (typeStr == "Int8") {
        builder.Primitive(EPrimitiveType::Int8);
    } else if (typeStr == "Uint8") {
        builder.Primitive(EPrimitiveType::Uint8);
    } else if (typeStr == "Int16") {
        builder.Primitive(EPrimitiveType::Int16);
    } else if (typeStr == "Uint16") {
        builder.Primitive(EPrimitiveType::Uint16);
    } else if (typeStr == "Int32") {
        builder.Primitive(EPrimitiveType::Int32);
    } else if (typeStr == "Uint32") {
        builder.Primitive(EPrimitiveType::Uint32);
    } else if (typeStr == "Int64") {
        builder.Primitive(EPrimitiveType::Int64);
    } else if (typeStr == "Uint64") {
        builder.Primitive(EPrimitiveType::Uint64);
    } else if (typeStr == "Float") {
        builder.Primitive(EPrimitiveType::Float);
    } else if (typeStr == "Double") {
        builder.Primitive(EPrimitiveType::Double);
    } else if (typeStr == "String") {
        builder.Primitive(EPrimitiveType::String);
    } else if (typeStr == "Utf8") {
        builder.Primitive(EPrimitiveType::Utf8);
    } else if (typeStr == "Json") {
        builder.Primitive(EPrimitiveType::Json);
    } else if (typeStr == "Yson") {
        builder.Primitive(EPrimitiveType::Yson);
    } else if (typeStr == "Date") {
        builder.Primitive(EPrimitiveType::Date);
    } else if (typeStr == "Datetime") {
        builder.Primitive(EPrimitiveType::Datetime);
    } else if (typeStr == "Timestamp") {
        builder.Primitive(EPrimitiveType::Timestamp);
    } else if (typeStr == "Interval") {
        builder.Primitive(EPrimitiveType::Interval);
    } else if (typeStr.StartsWith("Decimal")) {
        // Обрабатываем тип Decimal
        TString params = StripString(typeStr.substr(7)); // Убираем "Decimal"
        if (params.StartsWith("(") && params.EndsWith(")")) {
            params = params.substr(1, params.length() - 2);
            TVector<TString> parts;
            StringSplitter(params).Split(',').SkipEmpty().Collect(&parts);
            if (parts.size() == 2) {
                ui8 precision = FromString<ui8>(parts[0]);
                ui8 scale = FromString<ui8>(parts[1]);
                builder.Decimal(TDecimalType(precision, scale));
            }
        }
    } else if (typeStr.StartsWith("List<")) {
        // Обработка списков
        TString itemType = typeStr.substr(5, typeStr.length() - 6);
        builder.BeginList();
        ProcessType(itemType, builder);
        builder.EndList();
    } else if (typeStr.StartsWith("Struct<")) {
        // Обработка структур
        builder.BeginStruct();
        TString fields = typeStr.substr(7, typeStr.length() - 8);
        TVector<TString> fieldParts;
        StringSplitter(fields).Split(',').SkipEmpty().Collect(&fieldParts);
        for (const auto& field : fieldParts) {
            size_t colonPos = field.find(':');
            if (colonPos != TString::npos) {
                TString fieldName = field.substr(0, colonPos);
                TString fieldType = field.substr(colonPos + 1);
                builder.AddMember(fieldName);
                ProcessType(fieldType, builder);
            }
        }
        builder.EndStruct();
    } else if (typeStr.StartsWith("Tuple<")) {
        // Обработка кортежей
        builder.BeginTuple();
        TString elements = typeStr.substr(6, typeStr.length() - 7);
        TVector<TString> elementTypes;
        StringSplitter(elements).Split(',').SkipEmpty().Collect(&elementTypes);
        for (const auto& elementType : elementTypes) {
            ProcessType(elementType, builder);
        }
        builder.EndTuple();
    } else if (typeStr.StartsWith("Dict<")) {
        // Обработка словарей
        builder.BeginDict();
        TString params = typeStr.substr(5, typeStr.length() - 6);
        size_t commaPos = params.find(',');
        if (commaPos != TString::npos) {
            TString keyType = params.substr(0, commaPos);
            TString valueType = params.substr(commaPos + 1);
            
            // Добавляем тип ключа
            ProcessType(keyType, builder);
            
            // Добавляем тип значения
            ProcessType(valueType, builder);
        }
        builder.EndDict();
    }
}

} // namespace NConsoleClient
} // namespace NYdb 