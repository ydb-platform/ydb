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
    if (!NSQLTranslation::Tokenize(*lexer, queryText, "Query", tokens, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS) ||
        !issues.Empty()) {
        return std::nullopt;
    }

    // Анализируем токены для поиска параметров
    for (size_t i = 0; i < tokens.size(); ++i) {
        const auto& token = tokens[i];
        
        // Ищем объявление параметра (DECLARE $name AS type)
        if (TYqlParser::ToLower(token.Content) == "declare") {
            size_t j = i + 1;
            while (j < tokens.size() && tokens[j].Name == "WS") {
                j++;
            }
            
            if (j >= tokens.size() || tokens[j].Content != "$") {
                return std::nullopt;
            }
            j++;

            if (j >= tokens.size()) {
                return std::nullopt;
            }
            TString paramName = "$" + tokens[j].Content;
            j++;

            while (j < tokens.size() && tokens[j].Name == "WS") {
                j++;
            }

            if (j >= tokens.size() || TYqlParser::ToLower(tokens[j].Content) != "as") {
                return std::nullopt;
            }
            j++;

            while (j < tokens.size() && tokens[j].Name == "WS") {
                j++;
            }
            
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
                        if (angleBrackets < 0) {
                            return std::nullopt;
                        }
                        typeStr += ">";
                    } else if (tokens[j].Content == "(") {
                        parentheses++;
                        typeStr += "(";
                    } else if (tokens[j].Content == ")") {
                        parentheses--;
                        if (parentheses < 0) {
                            return std::nullopt;
                        }
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

            if (angleBrackets != 0 || parentheses != 0 || j >= tokens.size() || tokens[j].Content != ";") {
                return std::nullopt;
            }

            if (!typeStr.empty()) {
                TTypeBuilder builder;
                if (!ProcessType(typeStr, builder)) {
                    return std::nullopt;
                }

                result.emplace(paramName, builder.Build());
            } else {
                return std::nullopt;
            }

            i = j;
        }
    }

    return result;
}

bool TYqlParser::ProcessType(const TString& typeStr, TTypeBuilder& builder) {
    TString cleanTypeStr = StripString(typeStr);

    bool isOptional = cleanTypeStr.EndsWith("?");
    if (isOptional) {
        cleanTypeStr = cleanTypeStr.substr(0, cleanTypeStr.size() - 1);
        builder.BeginOptional();
    }

    TString lowerTypeStr = TYqlParser::ToLower(cleanTypeStr);

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
        TString itemType = cleanTypeStr.substr(5, cleanTypeStr.length() - 6);
        builder.BeginList();
        ProcessType(itemType, builder);
        builder.EndList();
    } else if (lowerTypeStr.StartsWith("struct<")) {
        builder.BeginStruct();
        TString fields = cleanTypeStr.substr(7, cleanTypeStr.length() - 8);

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
    } else {
        return false;
    }

    if (isOptional) {
        builder.EndOptional();
    }

    return true;
}

} // namespace NConsoleClient
} // namespace NYdb
