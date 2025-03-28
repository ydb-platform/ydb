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

namespace {

TString ToLower(const TString& s) {
    TString result = s;
    for (char& c : result) {
        c = std::tolower(c);
    }
    return result;
}

class TYqlTypeParser {
public:
    TYqlTypeParser(const TVector<NSQLTranslation::TParsedToken>& tokens)
        : Tokens(tokens)
    {}

    std::optional<TType> Build(size_t& pos) {
        auto node = Parse(pos);
        if (!node || (pos < Tokens.size() && Tokens[pos].Content != ";")) {
            return std::nullopt;
        }

        TTypeBuilder builder;
        if (!BuildType(*node, builder)) {
            return std::nullopt;
        }
        return builder.Build();
    }

private:
    struct TypeNode {
        TString TypeName;
        std::vector<TypeNode> Children;
        TString Name; // для полей структуры
        bool IsKey = false; // для словарей
    };

    std::optional<TypeNode> Parse(size_t& pos) {
        TypeNode node;

        while (pos < Tokens.size() && Tokens[pos].Name == "WS") {
            pos++;
        }

        if (pos >= Tokens.size()) {
            return node;
        }

        TString content = Tokens[pos].Content;
        TString lowerContent = ToLower(content);

        if (lowerContent == "list" || lowerContent == "struct" ||
            lowerContent == "tuple" || lowerContent == "dict") {

            node.TypeName = lowerContent;
            pos++;

            if (pos >= Tokens.size() || Tokens[pos].Content != "<") {
                return node;
            }
            pos++;

            while (pos < Tokens.size() && Tokens[pos].Content != ">") {
                if (Tokens[pos].Name == "WS" || Tokens[pos].Content == ",") {
                    pos++;
                    continue;
                }

                if (lowerContent == "struct") {
                    TypeNode field;
                    field.Name = Tokens[pos].Content;
                    pos++;

                    while (pos < Tokens.size() && Tokens[pos].Name == "WS") {
                        pos++;
                    }

                    if (pos >= Tokens.size() || Tokens[pos].Content != ":") {
                        return node;
                    }
                    pos++;

                    auto parseResult = Parse(pos);
                    if (!parseResult) {
                        return std::nullopt;
                    }
                    field.Children.push_back(*parseResult);
                    node.Children.push_back(field);
                } else if (lowerContent == "dict") {
                    auto parseResult = Parse(pos);
                    if (!parseResult) {
                        return std::nullopt;
                    }
                    TypeNode key = *parseResult;
                    key.IsKey = true;
                    node.Children.push_back(key);

                    while (pos < Tokens.size() && (Tokens[pos].Name == "WS" || Tokens[pos].Content == ",")) {
                        pos++;
                    }

                    parseResult = Parse(pos);
                    if (!parseResult) {
                        return std::nullopt;
                    }
                    node.Children.push_back(*parseResult);
                } else {
                    auto parseResult = Parse(pos);
                    if (!parseResult) {
                        return std::nullopt;
                    }
                    node.Children.push_back(*parseResult);
                }
            }

            if (pos < Tokens.size() && Tokens[pos].Content == ">") {
                pos++;
            }
        } else if (lowerContent == "decimal") {
            node.TypeName = lowerContent;
            pos++;

            if (pos >= Tokens.size() || Tokens[pos].Content != "(") {
                return node;
            }
            pos++;

            TypeNode precision;
            precision.TypeName = Tokens[pos].Content;
            node.Children.push_back(precision);
            pos++;

            if (pos >= Tokens.size() || Tokens[pos].Content != ",") {
                return node;
            }
            pos++;

            TypeNode scale;
            scale.TypeName = Tokens[pos].Content;
            node.Children.push_back(scale);
            pos++;

            if (pos >= Tokens.size() || Tokens[pos].Content != ")") {
                return node;
            }
            pos++;
        } else {
            node.TypeName = lowerContent;
            pos++;
        }

        // Проверяем, является ли тип опциональным
        while (pos < Tokens.size() && Tokens[pos].Name == "WS") {
            pos++;
        }

        if (pos < Tokens.size() && Tokens[pos].Content == "?") {
            TypeNode optionalNode;
            optionalNode.TypeName = "optional";
            optionalNode.Children.push_back(node);
            pos++;
            return optionalNode;
        }

        return node;
    }

    bool BuildType(const TypeNode& node, TTypeBuilder& builder) {
        if (node.TypeName == "optional") {
            builder.BeginOptional();
            BuildType(node.Children[0], builder);
            builder.EndOptional();
        } else if (node.TypeName == "list") {
            builder.BeginList();
            BuildType(node.Children[0], builder);
            builder.EndList();
        } else if (node.TypeName == "struct") {
            builder.BeginStruct();
            for (const auto& field : node.Children) {
                builder.AddMember(field.Name);
                BuildType(field.Children[0], builder);
            }
            builder.EndStruct();
        } else if (node.TypeName == "tuple") {
            builder.BeginTuple();
            for (const auto& element : node.Children) {
                builder.AddElement();
                BuildType(element, builder);
            }
            builder.EndTuple();
        } else if (node.TypeName == "dict") {
            builder.BeginDict();
            builder.DictKey();
            BuildType(node.Children[0], builder);
            builder.DictPayload();
            BuildType(node.Children[1], builder);
            builder.EndDict();
        } else if (node.TypeName == "decimal") {
            ui32 precision = FromString<ui32>(node.Children[0].TypeName);
            ui32 scale = FromString<ui32>(node.Children[1].TypeName);
            builder.Decimal(TDecimalType(precision, scale));
        } else {
            auto primitiveType = GetPrimitiveType(node.TypeName);
            if (!primitiveType) {
                return false;
            }

            builder.Primitive(*primitiveType);
        }

        return true;
    }

    std::optional<EPrimitiveType> GetPrimitiveType(const TString& typeStr) {
        if (typeStr == "bool") {
            return EPrimitiveType::Bool;
        } else if (typeStr == "int8") {
            return EPrimitiveType::Int8;
        } else if (typeStr == "uint8") {
            return EPrimitiveType::Uint8;
        } else if (typeStr == "int16") {
            return EPrimitiveType::Int16;
        } else if (typeStr == "uint16") {
            return EPrimitiveType::Uint16;
        } else if (typeStr == "int32") {
            return EPrimitiveType::Int32;
        } else if (typeStr == "uint32") {
            return EPrimitiveType::Uint32;
        } else if (typeStr == "int64") {
            return EPrimitiveType::Int64;
        } else if (typeStr == "uint64") {
            return EPrimitiveType::Uint64;
        } else if (typeStr == "float") {
            return EPrimitiveType::Float;
        } else if (typeStr == "double") {
            return EPrimitiveType::Double;
        } else if (typeStr == "string") {
            return EPrimitiveType::String;
        } else if (typeStr == "utf8") {
            return EPrimitiveType::Utf8;
        } else if (typeStr == "json") {
            return EPrimitiveType::Json;
        } else if (typeStr == "yson") {
            return EPrimitiveType::Yson;
        } else if (typeStr == "date") {
            return EPrimitiveType::Date;
        } else if (typeStr == "datetime") {
            return EPrimitiveType::Datetime;
        } else if (typeStr == "timestamp") {
            return EPrimitiveType::Timestamp;
        } else if (typeStr == "interval") {
            return EPrimitiveType::Interval;
        }

        return std::nullopt;
    }

    const TVector<NSQLTranslation::TParsedToken>& Tokens;
};

}


std::map<std::string, TType> TYqlParamParser::GetParamTypes(const TString& queryText) {
    enum class EParseState {
        Start,
        Declare,
        ParamName,
        As,
        Type
    };

    std::map<std::string, TType> result;

    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();

    auto lexer = MakeLexer(lexers, /* ansi = */ false, /* antlr4 = */ true);

    TVector<NSQLTranslation::TParsedToken> tokens;
    NYql::TIssues issues;
    if (!NSQLTranslation::Tokenize(*lexer, queryText, "Query", tokens, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS) ||
        !issues.Empty()) {
        return {};
    }

    EParseState state = EParseState::Start;
    TString paramName;

    for (size_t i = 0; i < tokens.size(); ++i) {      
        if (tokens[i].Name == "WS") {
            continue;
        }

        if (state == EParseState::Start) {
            if (ToLower(tokens[i].Content) != "declare") {
                continue;   
            }

            state = EParseState::Declare;
        } else if (state == EParseState::Declare) {
            if (tokens[i].Content != "$") { 
                state = EParseState::Start;
                continue;
            }

            state = EParseState::ParamName;
        } else if (state == EParseState::ParamName) {
            paramName = "$" + tokens[i].Content;
            state = EParseState::As;
        } else if (state == EParseState::As) {
            if (ToLower(tokens[i].Content) != "as") {
                state = EParseState::Start;
                continue;
            }

            state = EParseState::Type;
        } else if (state == EParseState::Type) {
            TYqlTypeParser parser(tokens);
            auto root = parser.Build(i);

            if (root) {
                result.emplace(paramName, *root);
            } else {
                result.clear();
            }

            state = EParseState::Start;
        }
    }

    return result;
}

} // namespace NConsoleClient
} // namespace NYdb
