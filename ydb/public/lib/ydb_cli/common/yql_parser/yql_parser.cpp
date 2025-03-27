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

TString TYqlParser::ToLower(const TString& s) {
    TString result = s;
    for (char& c : result) {
        c = std::tolower(c);
    }
    return result;
}

std::optional<std::map<std::string, TType>> TYqlParser::GetParamTypes(const TString& queryText) {
    std::map<std::string, TType> result;

    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();

    auto lexer = MakeLexer(lexers, /* ansi = */ false, /* antlr4 = */ true);

    TVector<NSQLTranslation::TParsedToken> tokens;
    NYql::TIssues issues;
    if (!NSQLTranslation::Tokenize(*lexer, queryText, "Query", tokens, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS)) {
        return std::nullopt;
    }

    // Анализируем токены для поиска параметров
    for (size_t i = 0; i < tokens.size(); ++i) {
        const auto& token = tokens[i];
        
        // Ищем объявление параметра (DECLARE $name AS type)
        if (ToLower(token.Content) == "declare") {
            // Пропускаем пробелы
            size_t j = i + 1;
            while (j < tokens.size() && tokens[j].Name == "WS") {
                j++;
            }
            
            // Проверяем, что следующий токен - $
            if (j < tokens.size() && tokens[j].Content == "$") {
                j++;

                // Получаем имя параметра
                if (j < tokens.size()) {
                    TString paramName = "$" + tokens[j].Content;
                    j++;

                    // Пропускаем пробелы
                    while (j < tokens.size() && tokens[j].Name == "WS") {
                        j++;
                    }

                    // Проверяем наличие AS
                    if (j < tokens.size() && (ToLower(tokens[j].Content) == "as")) {
                        j++;

                        // Пропускаем пробелы после AS
                        while (j < tokens.size() && tokens[j].Name == "WS") {
                            j++;
                        }
                        
                        // Собираем тип до точки с запятой или конца запроса
                        TString typeStr;
                        int angleBrackets = 0;
                        int parentheses = 0;

                        while (j < tokens.size() && tokens[j].Content != ";") {
                            if (tokens[j].Name != "WS") {
                                if (tokens[j].Content == "<") {
                                    angleBrackets++;
                                    typeStr += "<";
                                } else if (tokens[j].Content == ">") {
                                    angleBrackets--;
                                    typeStr += ">";
                                } else if (tokens[j].Content == "(") {
                                    parentheses++;
                                    typeStr += "(";
                                } else if (tokens[j].Content == ")") {
                                    parentheses--;
                                    typeStr += ")";
                                } else if (tokens[j].Content == ",") {
                                    typeStr += ",";
                                } else if (tokens[j].Content == "?") {
                                    typeStr += "?";
                                } else {
                                    if (!typeStr.empty() && typeStr.back() != '<' && typeStr.back() != '(' && typeStr.back() != ',' && typeStr.back() != '?') {
                                        typeStr += " ";
                                    }
                                    typeStr += tokens[j].Content;
                                }
                            }
                            j++;

                            if (angleBrackets == 0 && parentheses == 0 && j < tokens.size() && tokens[j].Content == ";") {
                                break;
                            }
                        }

                        if (!typeStr.empty()) {
                            // Создаем тип на основе строкового представления
                            TTypeBuilder builder;
                            ProcessType(typeStr, builder);

                            // Сохраняем тип параметра
                            result.emplace(paramName, builder.Build());
                        }
                    }
                }
            }

            // Обновляем индекс
            i = j;
        }
    }

    return result;
}

void TYqlParser::ProcessType(const TString& typeStr, TTypeBuilder& builder) {
    // Удаляем лишние пробелы
    TString cleanTypeStr = StripString(typeStr);

    // Проверяем на опциональный тип
    bool isOptional = cleanTypeStr.EndsWith("?");
    if (isOptional) {
        cleanTypeStr = cleanTypeStr.substr(0, cleanTypeStr.size() - 1);
        builder.BeginOptional();
    }

    // Приводим к нижнему регистру для сравнения
    TString lowerTypeStr;
    for (char c : cleanTypeStr) {
        lowerTypeStr += std::tolower(c);
    }

    // Обрабатываем базовые типы
    if (lowerTypeStr == "bool") {
        builder.Primitive(EPrimitiveType::Bool);
    } else if (lowerTypeStr == "int8") {
        builder.Primitive(EPrimitiveType::Int8);
    } else if (lowerTypeStr == "uint8") {
        builder.Primitive(EPrimitiveType::Uint8);
    } else if (lowerTypeStr == "int16") {
        builder.Primitive(EPrimitiveType::Int16);
    } else if (lowerTypeStr == "uint16") {
        builder.Primitive(EPrimitiveType::Uint16);
    } else if (lowerTypeStr == "int32") {
        builder.Primitive(EPrimitiveType::Int32);
    } else if (lowerTypeStr == "uint32") {
        builder.Primitive(EPrimitiveType::Uint32);
    } else if (lowerTypeStr == "int64") {
        builder.Primitive(EPrimitiveType::Int64);
    } else if (lowerTypeStr == "uint64" || lowerTypeStr == "uint64") {
        builder.Primitive(EPrimitiveType::Uint64);
    } else if (lowerTypeStr == "float") {
        builder.Primitive(EPrimitiveType::Float);
    } else if (lowerTypeStr == "double") {
        builder.Primitive(EPrimitiveType::Double);
    } else if (lowerTypeStr == "string") {
        builder.Primitive(EPrimitiveType::String);
    } else if (lowerTypeStr == "utf8") {
        builder.Primitive(EPrimitiveType::Utf8);
    } else if (lowerTypeStr == "json") {
        builder.Primitive(EPrimitiveType::Json);
    } else if (lowerTypeStr == "yson") {
        builder.Primitive(EPrimitiveType::Yson);
    } else if (lowerTypeStr == "date") {
        builder.Primitive(EPrimitiveType::Date);
    } else if (lowerTypeStr == "datetime") {
        builder.Primitive(EPrimitiveType::Datetime);
    } else if (lowerTypeStr == "timestamp") {
        builder.Primitive(EPrimitiveType::Timestamp);
    } else if (lowerTypeStr == "interval") {
        builder.Primitive(EPrimitiveType::Interval);
    } else if (lowerTypeStr.StartsWith("decimal")) {
        // Обрабатываем тип Decimal
        TString params = StripString(cleanTypeStr.substr(7)); // Убираем "Decimal"
        if (params.StartsWith("(") && params.EndsWith(")")) {
            params = params.substr(1, params.length() - 2);
            TVector<TString> parts;
            StringSplitter(params).Split(',').SkipEmpty().Collect(&parts);
            if (parts.size() == 2) {
                ui8 precision = FromString<ui8>(StripString(parts[0]));
                ui8 scale = FromString<ui8>(StripString(parts[1]));
                builder.Decimal(TDecimalType(precision, scale));
            }
        }
    } else if (lowerTypeStr.StartsWith("list<")) {
        // Обработка списков
        TString itemType = cleanTypeStr.substr(5, cleanTypeStr.length() - 6);
        builder.BeginList();
        ProcessType(itemType, builder);
        builder.EndList();
    } else if (lowerTypeStr.StartsWith("struct<")) {
        // Обработка структур
        builder.BeginStruct();
        TString fields = cleanTypeStr.substr(7, cleanTypeStr.length() - 8);

        // Разбираем поля с учетом вложенных типов
        TVector<TString> fieldParts;
        int angleBrackets = 0;
        TString currentField;

        for (size_t i = 0; i < fields.length(); ++i) {
            if (fields[i] == '<') {
                angleBrackets++;
                currentField += fields[i];
            } else if (fields[i] == '>') {
                angleBrackets--;
                currentField += fields[i];
            } else if (fields[i] == ',' && angleBrackets == 0) {
                if (!currentField.empty()) {
                    fieldParts.push_back(StripString(currentField));
                    currentField.clear();
                }
            } else {
                currentField += fields[i];
            }
        }
        
        if (!currentField.empty()) {
            fieldParts.push_back(StripString(currentField));
        }

        for (const auto& field : fieldParts) {
            size_t colonPos = field.find(':');
            if (colonPos != TString::npos) {
                TString fieldName = StripString(field.substr(0, colonPos));
                TString fieldType = StripString(field.substr(colonPos + 1));
                builder.AddMember(fieldName);
                ProcessType(fieldType, builder);
            }
        }
        builder.EndStruct();
    } else if (lowerTypeStr.StartsWith("tuple<")) {
        // Обработка кортежей
        TString elements = cleanTypeStr.substr(6, cleanTypeStr.length() - 7);
        TVector<TString> elementTypes;
        StringSplitter(elements).Split(',').SkipEmpty().Collect(&elementTypes);

        builder.BeginTuple();
        for (const auto& elementType : elementTypes) {
            builder.AddElement();
            ProcessType(StripString(elementType), builder);
        }
        builder.EndTuple();
    } else if (lowerTypeStr.StartsWith("dict<")) {
        // Обработка словарей
        TString params = cleanTypeStr.substr(5, cleanTypeStr.length() - 6);
        size_t commaPos = params.find(',');
        if (commaPos != TString::npos) {
            TString keyType = StripString(params.substr(0, commaPos));
            TString valueType = StripString(params.substr(commaPos + 1));
            
            builder.BeginDict();
            builder.DictKey();
            ProcessType(keyType, builder);
            builder.DictPayload();
            ProcessType(valueType, builder);
            builder.EndDict();
        }
    }

    if (isOptional) {
        builder.EndOptional();
    }
}

} // namespace NConsoleClient
} // namespace NYdb

