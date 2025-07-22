#include "generate_textmate.h"

#include "json.h"

#include <library/cpp/json/json_value.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NSQLHighlight {

    namespace {

        TString ToTextMateRegex(const TUnit& unit, const NSQLTranslationV1::TRegexPattern& pattern) {
            TStringBuilder regex;

            if (pattern.IsCaseInsensitive) {
                regex << "(?i)";
            }

            if (unit.IsPlain) {
                regex << R"re(\b)re";
            }

            regex << "(" << pattern.Body << ")";

            if (!pattern.After.empty()) {
                regex << "(?=" << pattern.After << ")";
            }

            if (unit.IsPlain) {
                regex << R"re(\b)re";
            }

            return regex;
        }

        TStringBuf ToTextMateGroup(EUnitKind kind) {
            switch (kind) {
                case EUnitKind::Keyword:
                    return "keyword.control";
                case EUnitKind::Punctuation:
                    return "keyword.operator";
                case EUnitKind::QuotedIdentifier:
                    return "string.interpolated";
                case EUnitKind::BindParameterIdentifier:
                    return "variable.parameter";
                case EUnitKind::TypeIdentifier:
                    return "entity.name.type";
                case EUnitKind::FunctionIdentifier:
                    return "entity.name.function";
                case EUnitKind::Identifier:
                    return "variable.other";
                case EUnitKind::Literal:
                    return "constant.numeric";
                case EUnitKind::StringLiteral:
                    return "string.quoted.double";
                case EUnitKind::Comment:
                    return "comment.block";
                case EUnitKind::Whitespace:
                    return "";
                case EUnitKind::Error:
                    return "";
            }
        }

        TMaybe<NJson::TJsonMap> TextMateMultilinePattern(const TUnit& unit) {
            auto range = unit.RangePattern;
            if (!range) {
                return Nothing();
            }

            return NJson::TJsonMap({
                {"begin", range->Begin},
                {"end", range->End},
                {"name", ToTextMateGroup(unit.Kind)},
            });
        }

        NJson::TJsonMap ToTextMatePattern(const TUnit& unit, const NSQLTranslationV1::TRegexPattern& pattern) {
            return NJson::TJsonMap({
                {"match", ToTextMateRegex(unit, pattern)},
                {"name", ToTextMateGroup(unit.Kind)},
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
        root["fileTypes"] = NJson::TJsonArray({"yql"});

        for (const TUnit& unit : highlighting.Units) {
            if (unit.IsCodeGenExcluded) {
                continue;
            }

            TString name = ToTextMateName(unit.Kind);

            root["patterns"].AppendValue(NJson::TJsonMap({
                {"include", "#" + name},
            }));

            for (const NSQLTranslationV1::TRegexPattern& pattern : unit.Patterns) {
                auto textmate = ToTextMatePattern(unit, pattern);
                root["repository"][name]["patterns"].AppendValue(std::move(textmate));
            }

            if (auto textmate = TextMateMultilinePattern(unit)) {
                root["repository"][name]["patterns"].AppendValue(*textmate);
            }
        }

        Print(out, root);
    }

} // namespace NSQLHighlight
