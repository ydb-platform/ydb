#include "generator_monarch.h"

#include "highlighting.h"

#include <yql/essentials/utils/yql_panic.h>

#include <contrib/libs/re2/re2/re2.h>

#include <library/cpp/json/json_writer.h>

#include <util/string/builder.h>

namespace NSQLHighlight {

template <std::invocable<const TUnit&, const NSQLTranslationV1::TRegexPattern&, size_t> Action>
void ForEachBeforablePattern(const THighlighting& highlighting, Action action) {
    for (const TUnit& unit : highlighting.Units) {
        size_t i = 0;
        for (const NSQLTranslationV1::TRegexPattern& regex : unit.Patterns) {
            if (!regex.Before.empty()) {
                i += 1;
                action(unit, regex, i);
            }
        }
    }
}

TString ToMonarchRegex(const TUnit& unit, const NSQLTranslationV1::TRegexPattern& pattern) {
    TStringBuilder regex;

    if (unit.IsPlain && pattern.Before.empty()) {
        regex << R"re(\b)re";
    }

    regex << "(" << pattern.Body << ")";

    if (!pattern.After.empty()) {
        regex << "(?=" << pattern.After << ")";
    }

    if (unit.IsPlain && pattern.Before.empty()) {
        regex << R"re(\b)re";
    }

    return regex;
}

TString ToMonarchSelector(EUnitKind kind) {
    switch (kind) {
        case EUnitKind::Keyword:
            return "keyword";
        case EUnitKind::Punctuation:
            return "operator.sql";
        case EUnitKind::QuotedIdentifier:
            return "string.tablepath";
        case EUnitKind::BindParameterIdentifier:
            return "variable";
        case EUnitKind::OptionIdentifier:
            return "identifier";
        case EUnitKind::TypeIdentifier:
            return "keyword.type";
        case EUnitKind::FunctionIdentifier:
            return "support.function";
        case EUnitKind::Identifier:
            return "identifier";
        case EUnitKind::Literal:
            return "number";
        case EUnitKind::StringLiteral:
            return "string";
        case EUnitKind::Comment:
            return "comment";
        case EUnitKind::Whitespace:
            return "white";
        case EUnitKind::Error:
            return "";
    }
}

TString ToMonarchStateName(EUnitKind kind) {
    switch (kind) {
        case EUnitKind::Keyword:
            return "keyword";
        case EUnitKind::Punctuation:
            return "punctuation";
        case EUnitKind::QuotedIdentifier:
            return "quotedIdentifier";
        case EUnitKind::BindParameterIdentifier:
            return "bindParameterIdentifier";
        case EUnitKind::OptionIdentifier:
            return "optionIdentifier";
        case EUnitKind::TypeIdentifier:
            return "typeIdentifier";
        case EUnitKind::FunctionIdentifier:
            return "functionIdentifier";
        case EUnitKind::Identifier:
            return "identifier";
        case EUnitKind::Literal:
            return "literal";
        case EUnitKind::StringLiteral:
            return "stringLiteral";
        case EUnitKind::Comment:
            return "comment";
        case EUnitKind::Whitespace:
            return "whitespace";
        case EUnitKind::Error:
            return "error";
    }
}

TMaybe<NJson::TJsonMap> EmbeddedLanguage(const TRangePattern& range) {
    if (range.BeginPlain.StartsWith(TRangePattern::EmbeddedPythonBegin)) {
        return NJson::TJsonMap{
            {"token", "string.python"},
            {"nextEmbedded", "python"},
            {"next", "@embedded"},
            {"goBack", 3},
        };
    }

    if (range.BeginPlain.StartsWith(TRangePattern::EmbeddedJavaScriptBegin)) {
        return NJson::TJsonMap{
            {"token", "string.js"},
            {"nextEmbedded", "javascript"},
            {"next", "@embedded"},
            {"goBack", 4},
        };
    }

    return Nothing();
}

NJson::TJsonValue ToMonarchMultiLineState(const TUnit& unit, const TRangePattern& pattern, bool ansi) {
    TString group = ToMonarchSelector(unit.Kind);
    TString begin = RE2::QuoteMeta(pattern.BeginPlain);
    TString end = RE2::QuoteMeta(pattern.EndPlain);

    TMaybe<TString> escape;
    if (pattern.EscapeRegex) {
        escape = *pattern.EscapeRegex;
    }

    NJson::TJsonValue json;
    if (unit.Kind == EUnitKind::StringLiteral) {
        for (const auto& range : unit.RangePatterns) {
            if (auto embedded = EmbeddedLanguage(range)) {
                YQL_ENSURE(range.BeginPlain.StartsWith("@@"));
                TString tag = RE2::QuoteMeta(range.BeginPlain.substr(2));
                json.AppendValue(NJson::TJsonArray{std::move(tag), *embedded});
            }
        }
        if (escape) {
            json.AppendValue(NJson::TJsonArray{*escape, group + ".escape"});
        }
    } else if (unit.Kind == EUnitKind::Comment && ansi) {
        json.AppendValue(NJson::TJsonArray{begin, group, "@" + group});
    }
    json.AppendValue(NJson::TJsonArray{"[^" + begin + "]", group});
    json.AppendValue(NJson::TJsonArray{end, group, "@pop"});
    json.AppendValue(NJson::TJsonArray{begin, group});

    return json;
}

NJson::TJsonValue ToMonarchBeforableState(const TUnit& unit, const NSQLTranslationV1::TRegexPattern& pattern) {
    NJson::TJsonValue json;
    json.AppendValue(ToMonarchRegex(unit, pattern));
    json.AppendValue(ToMonarchSelector(unit.Kind));
    json.AppendValue("@pop");
    return NJson::TJsonArray({json});
}

NJson::TJsonValue MonarchEmbeddedState() {
    return NJson::TJsonArray{{NJson::TJsonArray{
        "([^@]|^)([@]{4})*[@]{2}([@]([^@]|$)|[^@]|$)",
        NJson::TJsonMap{
            {"token", "@rematch"},
            {"next", "@pop"},
            {"nextEmbedded", "@pop"},
        },
    }}};
}

NJson::TJsonValue ToMonarchBeforableState(const THighlighting& highlighting) {
    NJson::TJsonArray json;

    ForEachBeforablePattern(highlighting, [&](const auto& unit, const auto& pattern, auto i) {
        // Note: Assume that before is always a keyword with trailing spaces.
        json.AppendValue(NJson::TJsonArray{
            pattern.Before,
            ToMonarchSelector(EUnitKind::Keyword),
            "@" + ToMonarchStateName(unit.Kind) + ToString(i),
        });
    });

    return json;
}

NJson::TJsonValue ToMonarchWhitespaceState(const THighlighting& highlighting) {
    NJson::TJsonValue json;

    const TUnit& ws = *FindIfPtr(highlighting.Units, [](const TUnit& unit) {
        return unit.Kind == EUnitKind::Whitespace;
    });

    Y_ENSURE(ws.Patterns.size() == 1);
    json.AppendValue(NJson::TJsonArray{ToMonarchRegex(ws, ws.Patterns.at(0)), "white"});

    ForEachMultiLineExceptEmbedded(highlighting, [&](const TUnit& unit, const TRangePattern& pattern) {
        json.AppendValue(NJson::TJsonArray{
            RE2::QuoteMeta(pattern.BeginPlain),
            ToMonarchSelector(unit.Kind),
            "@" + ToMonarchStateName(unit.Kind) + pattern.BeginPlain,
        });
    });

    return json;
}

NJson::TJsonValue ToMonarchRootState(const THighlighting& highlighting, bool ansi) {
    NJson::TJsonValue json;

    json.AppendValue(NJson::TJsonMap{{"include", "@beforable"}});

    for (const TUnit& unit : highlighting.Units) {
        if (unit.IsCodeGenExcluded) {
            continue;
        }

        TString group = ToMonarchSelector(unit.Kind);

        const auto* patterns = &unit.Patterns;
        if (!unit.PatternsANSI.Empty() && ansi) {
            patterns = unit.PatternsANSI.Get();
        }

        for (const NSQLTranslationV1::TRegexPattern& pattern : *patterns) {
            if (!pattern.Before.empty()) {
                continue;
            }

            TString regex = ToMonarchRegex(unit, pattern);
            json.AppendValue(NJson::TJsonArray{regex, group});
        }
    }

    json.AppendValue(NJson::TJsonMap{{"include", "@whitespace"}});

    return json;
}

void GenerateMonarch(IOutputStream& out, const THighlighting& highlighting, bool ansi) {
    NJsonWriter::TBuf buf(NJsonWriter::HEM_DONT_ESCAPE_HTML, &out);
    buf.SetIndentSpaces(4);

    const auto write_json = [&](TStringBuf key, const NJson::TJsonValue& json) {
        buf.WriteKey(key);
        buf.WriteJsonValue(&json);
    };

    buf.BeginObject();

    buf.WriteKey("ignoreCase");
    buf.WriteBool(IsCaseInsensitive(highlighting));

    buf.WriteKey("tokenizer");
    buf.BeginObject();
    write_json("root", ToMonarchRootState(highlighting, ansi));
    write_json("whitespace", ToMonarchWhitespaceState(highlighting));
    write_json("beforable", ToMonarchBeforableState(highlighting));
    ForEachMultiLineExceptEmbedded(highlighting, [&](const TUnit& unit, const TRangePattern& pattern) {
        write_json(
            ToMonarchStateName(unit.Kind) + pattern.BeginPlain,
            ToMonarchMultiLineState(unit, pattern, ansi));
    });
    write_json("embedded", MonarchEmbeddedState());
    ForEachBeforablePattern(highlighting, [&](const auto& unit, const auto& pattern, auto i) {
        write_json(
            ToMonarchStateName(unit.Kind) + ToString(i),
            ToMonarchBeforableState(unit, pattern));
    });

    buf.EndObject();

    buf.EndObject();
}

IGenerator::TPtr MakeMonarchGenerator() {
    return MakeOnlyFileGenerator(GenerateMonarch);
}

} // namespace NSQLHighlight
