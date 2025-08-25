#include "yql_parser.h"

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>

#include <util/generic/scope.h>
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
        auto node = Parse(SkipWS(pos));
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
        TTypeParser::ETypeKind TypeKind;
        std::vector<TypeNode> Children;

        // For primitive type
        EPrimitiveType PrimitiveType;

        // For struct type
        TString Name;

        // For decimal type
        ui32 precision = 0;
        ui32 scale = 0;
    };

    std::optional<TypeNode> Parse(size_t& pos) {
        TypeNode node;

        TString lowerContent = ToLower(Tokens[pos].Content);

        if (lowerContent == "struct" || lowerContent == "tuple") {
            node.TypeKind = lowerContent == "struct" ? TTypeParser::ETypeKind::Struct :
                            TTypeParser::ETypeKind::Tuple;

            if (SkipCurrentTokenAndWS(pos) >= Tokens.size() || Tokens[pos].Content != "<") {
                return std::nullopt;
            }

            while (pos < Tokens.size() && Tokens[pos].Content != ">") {
                if (lowerContent == "struct") {
                    SkipCurrentTokenAndWS(pos);
                    auto name = Tokens[pos].Content;

                    if (SkipCurrentTokenAndWS(pos) >= Tokens.size() || Tokens[pos].Content != ":") {
                        return std::nullopt;
                    }

                    auto parseResult = Parse(SkipCurrentTokenAndWS(pos));
                    if (!parseResult) {
                        return std::nullopt;
                    }
                    node.Children.push_back(*parseResult);
                    node.Children.back().Name = name;
                } else {
                    auto parseResult = Parse(SkipCurrentTokenAndWS(pos));
                    if (!parseResult) {
                        return std::nullopt;
                    }
                    node.Children.push_back(*parseResult);
                }

                if (pos >= Tokens.size() || (Tokens[pos].Content != "," && Tokens[pos].Content != ">")) {
                    return std::nullopt;
                }
            }
        } else if (lowerContent == "list" ||
                   lowerContent == "optional" ||
                   lowerContent == "dict") {
            node.TypeKind = lowerContent == "list" ? TTypeParser::ETypeKind::List :
                            lowerContent == "optional" ? TTypeParser::ETypeKind::Optional :
                            TTypeParser::ETypeKind::Dict;

            if (SkipCurrentTokenAndWS(pos) >= Tokens.size() || Tokens[pos].Content != "<") {
                return std::nullopt;
            }

            auto parseResult = Parse(SkipCurrentTokenAndWS(pos));
            if (!parseResult) {
                return std::nullopt;
            }
            node.Children.push_back(*parseResult);

            if (lowerContent == "dict") {
                if (pos >= Tokens.size() || Tokens[pos].Content != ",") {
                    return std::nullopt;
                }

                parseResult = Parse(SkipCurrentTokenAndWS(pos));
                if (!parseResult) {
                    return std::nullopt;
                }
                node.Children.push_back(*parseResult);
            }

            if (pos >= Tokens.size() || Tokens[pos].Content != ">") {
                return std::nullopt;
            }
        } else if (lowerContent == "decimal") {
            auto parseResult = ParseDecimal(pos);
            if (!parseResult) {
                return std::nullopt;
            }
            node = *parseResult;
        } else {
            auto parseResult = ParsePrimitive(lowerContent);
            if (!parseResult) {
                return std::nullopt;
            }
            node = *parseResult;
        }

        if (SkipCurrentTokenAndWS(pos) < Tokens.size() && Tokens[pos].Content == "?") {
            TypeNode optionalNode;
            optionalNode.TypeKind = TTypeParser::ETypeKind::Optional;
            optionalNode.Children.push_back(node);
            SkipCurrentTokenAndWS(pos);

            return optionalNode;
        }

        return node;
    }

    std::optional<TypeNode> ParsePrimitive(const TString& content) {
        TypeNode node;
        node.TypeKind = TTypeParser::ETypeKind::Primitive;

        if (content == "bool") {
            node.PrimitiveType = EPrimitiveType::Bool;
        } else if (content == "int8") {
            node.PrimitiveType = EPrimitiveType::Int8;
        } else if (content == "uint8") {
            node.PrimitiveType = EPrimitiveType::Uint8;
        } else if (content == "int16") {
            node.PrimitiveType = EPrimitiveType::Int16;
        } else if (content == "uint16") {
            node.PrimitiveType = EPrimitiveType::Uint16;
        } else if (content == "int32") {
            node.PrimitiveType = EPrimitiveType::Int32;
        } else if (content == "uint32") {
            node.PrimitiveType = EPrimitiveType::Uint32;
        } else if (content == "int64") {
            node.PrimitiveType = EPrimitiveType::Int64;
        } else if (content == "uint64") {
            node.PrimitiveType = EPrimitiveType::Uint64;
        } else if (content == "float") {
            node.PrimitiveType = EPrimitiveType::Float;
        } else if (content == "double") {
            node.PrimitiveType = EPrimitiveType::Double;
        } else if (content == "string") {
            node.PrimitiveType = EPrimitiveType::String;
        } else if (content == "utf8") {
            node.PrimitiveType = EPrimitiveType::Utf8;
        } else if (content == "json") {
            node.PrimitiveType = EPrimitiveType::Json;
        } else if (content == "yson") {
            node.PrimitiveType = EPrimitiveType::Yson;
        } else if (content == "date") {
            node.PrimitiveType = EPrimitiveType::Date;
        } else if (content == "datetime") {
            node.PrimitiveType = EPrimitiveType::Datetime;
        } else if (content == "timestamp") {
            node.PrimitiveType = EPrimitiveType::Timestamp;
        } else if (content == "interval") {
            node.PrimitiveType = EPrimitiveType::Interval;
        } else if (content == "date32") {
            node.PrimitiveType = EPrimitiveType::Date32;
        } else if (content == "datetime64") {
            node.PrimitiveType = EPrimitiveType::Datetime64;
        } else if (content == "timestamp64") {
            node.PrimitiveType = EPrimitiveType::Timestamp64;
        } else if (content == "interval64") {
            node.PrimitiveType = EPrimitiveType::Interval64;
        } else if (content == "tzdate") {
            node.PrimitiveType = EPrimitiveType::TzDate;
        } else if (content == "tzdatetime") {
            node.PrimitiveType = EPrimitiveType::TzDatetime;
        } else if (content == "tztimestamp") {
            node.PrimitiveType = EPrimitiveType::TzTimestamp;
        } else if (content == "uuid") {
            node.PrimitiveType = EPrimitiveType::Uuid;
        } else if (content == "jsondocument") {
            node.PrimitiveType = EPrimitiveType::JsonDocument;
        } else if (content == "dynumber") {
            node.PrimitiveType = EPrimitiveType::DyNumber;
        } else if (content == "emptylist") {
            node.TypeKind = TTypeParser::ETypeKind::EmptyList;
        } else if (content == "emptydict") {
            node.TypeKind = TTypeParser::ETypeKind::EmptyDict;
        } else if (content == "void") {
            node.TypeKind = TTypeParser::ETypeKind::Void;
        } else if (content == "null") {
            node.TypeKind = TTypeParser::ETypeKind::Null;
        } else {
            return std::nullopt;
        }

        return node;
    }

    std::optional<TypeNode> ParseDecimal(size_t& pos) {
        TypeNode node;
        node.TypeKind = TTypeParser::ETypeKind::Decimal;

        if (SkipCurrentTokenAndWS(pos) >= Tokens.size() || Tokens[pos].Content != "(") {
            return std::nullopt;
        }

        if (SkipCurrentTokenAndWS(pos) >= Tokens.size() || !TryFromString<ui32>(Tokens[pos].Content, node.precision)) {
            return std::nullopt;
        }

        if (SkipCurrentTokenAndWS(pos) >= Tokens.size() || Tokens[pos].Content != ",") {
            return std::nullopt;
        }

        if (SkipCurrentTokenAndWS(pos) >= Tokens.size() || !TryFromString<ui32>(Tokens[pos].Content, node.scale)) {
            return std::nullopt;
        }

        if (SkipCurrentTokenAndWS(pos) >= Tokens.size() || Tokens[pos].Content != ")") {
            return std::nullopt;
        }

        return node;
    }

    size_t& SkipWS(size_t& pos) {
        while (pos < Tokens.size() && Tokens[pos].Name == "WS") {
            pos++;
        }
        return pos;
    }

    size_t& SkipCurrentTokenAndWS(size_t& pos) {
        pos++;
        return SkipWS(pos);
    }

    bool BuildType(const TypeNode& node, TTypeBuilder& builder) {
        if (node.TypeKind == TTypeParser::ETypeKind::Optional) {
            builder.BeginOptional();
            BuildType(node.Children[0], builder);
            builder.EndOptional();
        } else if (node.TypeKind == TTypeParser::ETypeKind::List) {
            builder.BeginList();
            BuildType(node.Children[0], builder);
            builder.EndList();
        } else if (node.TypeKind == TTypeParser::ETypeKind::Struct) {
            builder.BeginStruct();
            for (const auto& field : node.Children) {
                builder.AddMember(field.Name);
                BuildType(field, builder);
            }
            builder.EndStruct();
        } else if (node.TypeKind == TTypeParser::ETypeKind::Tuple) {
            builder.BeginTuple();
            for (const auto& element : node.Children) {
                builder.AddElement();
                BuildType(element, builder);
            }
            builder.EndTuple();
        } else if (node.TypeKind == TTypeParser::ETypeKind::Dict) {
            builder.BeginDict();
            builder.DictKey();
            BuildType(node.Children[0], builder);
            builder.DictPayload();
            BuildType(node.Children[1], builder);
            builder.EndDict();
        } else if (node.TypeKind == TTypeParser::ETypeKind::Decimal) {
            builder.Decimal(TDecimalType(node.precision, node.scale));
        } else if (node.TypeKind == TTypeParser::ETypeKind::Primitive) {
            builder.Primitive(node.PrimitiveType);
        } else {
            return false;
        }

        return true;
    }

    const TVector<NSQLTranslation::TParsedToken>& Tokens;
};

}


std::optional<std::map<std::string, TType>> TYqlParamParser::GetParamTypes(const TString& queryText) {
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
        return std::nullopt;
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
                return std::nullopt;
            }

            state = EParseState::ParamName;
        } else if (state == EParseState::ParamName) {
            paramName = "$" + tokens[i].Content;
            state = EParseState::As;
        } else if (state == EParseState::As) {
            if (ToLower(tokens[i].Content) != "as") {
                return std::nullopt;
            }

            state = EParseState::Type;
        } else if (state == EParseState::Type) {
            TYqlTypeParser parser(tokens);
            auto parsedType = parser.Build(i);

            if (!parsedType) {
                return std::nullopt;
            }

            result.emplace(paramName, *parsedType);
            state = EParseState::Start;
        }
    }

    return result;
}

} // namespace NConsoleClient
} // namespace NYdb
