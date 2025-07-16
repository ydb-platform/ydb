#include "generate_textmate.h"

#include "generate.h"
#include "json.h"

#include <library/cpp/json/json_value.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NSQLHighlight {

    namespace {

        TString ToTextMateRegex(EUnitKind kind, const NSQLTranslationV1::TRegexPattern& pattern) {
            TStringBuilder regex;

            if (pattern.IsCaseInsensitive) {
                regex << "(?i)";
            }

            if (IsPlain(kind)) {
                regex << R"re(\b)re";
            }

            regex << "(" << pattern.Body << ")";

            if (!pattern.After.empty()) {
                regex << "(?=" << pattern.After << ")";
            }

            if (IsPlain(kind)) {
                regex << R"re(\b)re";
            }

            return regex;
        }

        TStringBuf ToTextMateGroup(EUnitKind kind) {
            switch (kind) {
                case EUnitKind::Keyword:
                    return "keyword.control";
                case EUnitKind::Punctuation:
                    return "keyword.operator.custom";
                case EUnitKind::QuotedIdentifier:
                    return "string.quoted.double.custom";
                case EUnitKind::BindParamterIdentifier:
                    return "variable.other.dollar.custom";
                case EUnitKind::TypeIdentifier:
                    return "entity.name.type";
                case EUnitKind::FunctionIdentifier:
                    return "entity.name.function";
                case EUnitKind::Identifier:
                    return "variable.other.custom";
                case EUnitKind::Literal:
                    return "constant.numeric.custom";
                case EUnitKind::StringLiteral:
                    return "string.quoted.double.custom";
                case EUnitKind::Comment:
                    return "comment.block.custom";
                case EUnitKind::Whitespace:
                    return "";
                case EUnitKind::Error:
                    return "";
            }
        }

        TMaybe<std::tuple<TStringBuf, TStringBuf>> TextMateRange(EUnitKind kind) {
            switch (kind) {
                case EUnitKind::Comment: {
                    return std::make_tuple(R"re(/\*)re", R"re(\*/)re");
                } break;
                case EUnitKind::StringLiteral: {
                    return std::make_tuple("@@", "@@");
                } break;
                default: {
                    return Nothing();
                } break;
            }
        }

        TMaybe<NJson::TJsonMap> TextMateMultilinePattern(EUnitKind kind) {
            auto range = TextMateRange(kind);
            if (!range) {
                return Nothing();
            }

            return NJson::TJsonMap({
                {"begin", std::get<0>(*range)},
                {"end", std::get<1>(*range)},
                {"name", ToTextMateGroup(kind)},
            });
        }

        NJson::TJsonMap ToTextMatePattern(EUnitKind kind, const NSQLTranslationV1::TRegexPattern& pattern) {
            return NJson::TJsonMap({
                {"match", ToTextMateRegex(kind, pattern)},
                {"name", ToTextMateGroup(kind)},
            });
        }

        TString ToTextMateName(EUnitKind kind) {
            return ToString(kind);
        }

    } // namespace

    void GenerateTextMate(IOutputStream& out, const THighlighting& highlighting) {
        NJson::TJsonMap root;
        root["$schema"] = "https://raw.githubusercontent.com/martinring/tmlanguage/master/tmlanguage.json";
        root["name"] = "yql";
        root["scopeName"] = "source.yql";
        root["fileTypes"] = NJson::TJsonArray({"sql", "yql"});

        for (const TUnit& unit : highlighting.Units) {
            if (IsIgnored(unit.Kind)) {
                continue;
            }

            TString name = ToTextMateName(unit.Kind);

            root["patterns"].AppendValue(NJson::TJsonMap({
                {"include", "#" + name},
            }));

            for (const NSQLTranslationV1::TRegexPattern& pattern : unit.Patterns) {
                auto textmate = ToTextMatePattern(unit.Kind, pattern);
                root["repository"][name]["patterns"].AppendValue(std::move(textmate));
            }

            if (auto textmate = TextMateMultilinePattern(unit.Kind)) {
                root["repository"][name]["patterns"].AppendValue(*textmate);
            }
        }

        Print(out, root);
    }

} // namespace NSQLHighlight
