#include "generator_highlight_js.h"

#include "json.h"
#include "highlighting.h"

#include <contrib/libs/re2/re2/re2.h>
#include <util/string/builder.h>

namespace NSQLHighlight {

    TString ToHighlightJSClass(EUnitKind kind) {
        switch (kind) {
            case EUnitKind::Keyword:
                return "keyword";
            case EUnitKind::Punctuation:
                return "punctuation";
            case EUnitKind::QuotedIdentifier:
                return "symbol";
            case EUnitKind::BindParameterIdentifier:
                return "variable";
            case EUnitKind::TypeIdentifier:
                return "type";
            case EUnitKind::FunctionIdentifier:
                return "title.function";
            case EUnitKind::Identifier:
                return "";
            case EUnitKind::Literal:
                return "number";
            case EUnitKind::StringLiteral:
                return "string";
            case EUnitKind::Comment:
                return "comment";
            case EUnitKind::Whitespace:
                return "";
            case EUnitKind::Error:
                return "";
        }
    }

    // FIXME: copy-pasted from generator_textmate.cpp.
    TString ToTextMateRegex(const TUnit& unit, const NSQLTranslationV1::TRegexPattern& pattern) {
        TStringBuilder regex;

        if (unit.IsPlain) {
            regex << R"re(\b)re";
        }

        if (!pattern.Before.empty()) {
            regex << "(?<=" << pattern.Before << ")";
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

    NJson::TJsonValue ToHighlightJSPattern(const TUnit& unit, const NSQLTranslationV1::TRegexPattern& pattern) {
        NJson::TJsonMap json;
        json["className"] = ToHighlightJSClass(unit.Kind);
        json["begin"] = ToTextMateRegex(unit, pattern);
        return json;
    }

    NJson::TJsonValue ToHighlightJSPattern(const TUnit& unit, const TRangePattern& pattern) {
        NJson::TJsonMap json;
        json["className"] = ToHighlightJSClass(unit.Kind);
        json["begin"] = RE2::QuoteMeta(pattern.Begin);
        json["end"] = RE2::QuoteMeta(pattern.End);
        return json;
    }

    NJson::TJsonValue ToHighlightJSContains(const THighlighting& highlighting) {
        NJson::TJsonArray array;

        for (const TUnit& unit : highlighting.Units) {
            if (unit.IsCodeGenExcluded || unit.Kind == EUnitKind::Identifier) {
                continue;
            }

            for (const NSQLTranslationV1::TRegexPattern& pattern : unit.Patterns) {
                array.AppendValue(ToHighlightJSPattern(unit, pattern));
            }
            if (auto range = unit.RangePattern) {
                array.AppendValue(ToHighlightJSPattern(unit, *range));
            }
        }

        return array;
    }

    NJson::TJsonValue ToHighlightJSON(const THighlighting& highlighting) {
        NJson::TJsonMap json;
        json["name"] = highlighting.Name;
        json["case_insensitive"] = IsCaseInsensitive(highlighting);
        json["contains"] = NJson::TJsonArray{{NJson::TJsonMap{
            {"begin", ""},
            {"end", ";"},
            {"endsWithParent", true},
            {"lexemes", R"re(\w+)re"},
            {"contains", ToHighlightJSContains(highlighting)},
        }}};
        return json;
    }

    void GenerateHighlightJS(IOutputStream& out, const THighlighting& highlighting) {
        Print(out, ToHighlightJSON(highlighting));
    }

    IGenerator::TPtr MakeHighlightJSGenerator() {
        return MakeOnlyFileGenerator(GenerateHighlightJS);
    }

} // namespace NSQLHighlight
