#include "generator_monarch.h"

#include <contrib/libs/re2/re2/re2.h>

#include <library/cpp/json/json_writer.h>

#include <util/string/builder.h>

namespace NSQLHighlight {

    bool IsCaseInsensitive(const THighlighting& highlighting) {
        return AnyOf(highlighting.Units, [](const TUnit& unit) {
            return AnyOf(unit.Patterns, [](const NSQLTranslationV1::TRegexPattern& p) {
                return p.IsCaseInsensitive;
            });
        });
    }

    template <std::invocable<const TUnit&> Action>
    void ForEachMultiLine(const THighlighting& highlighting, Action action) {
        for (const TUnit& unit : highlighting.Units) {
            TMaybe<TRangePattern> range = unit.RangePattern;
            if (!range) {
                continue;
            }

            action(unit);
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

    NJson::TJsonValue ToMonarchMultiLineState(const TUnit& unit) {
        Y_ENSURE(unit.RangePattern);

        NJson::TJsonValue json;

        if (unit.Kind == EUnitKind::StringLiteral) {
            json.AppendValue(NJson::TJsonArray{
                "#py",
                NJson::TJsonMap{
                    {"token", "string.python"},
                    {"nextEmbedded", "python"},
                    {"next", "@embedded"},
                    {"goBack", 3},
                },
            });
            json.AppendValue(NJson::TJsonArray{
                "\\/\\/js",
                NJson::TJsonMap{
                    {"token", "string.js"},
                    {"nextEmbedded", "javascript"},
                    {"next", "@embedded"},
                    {"goBack", 4},
                },
            });
        }

        TString group = ToMonarchSelector(unit.Kind);
        TString begin = RE2::QuoteMeta(unit.RangePattern->Begin);
        TString end = RE2::QuoteMeta(unit.RangePattern->End);

        json.AppendValue(NJson::TJsonArray{"[^" + begin + "]", group});
        json.AppendValue(NJson::TJsonArray{end, group, "@pop"});
        json.AppendValue(NJson::TJsonArray{"[" + begin + "]", group});

        return json;
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

    NJson::TJsonValue ToMonarchWhitespaceState(const THighlighting& highlighting) {
        NJson::TJsonValue json;

        const TUnit& ws = *FindIfPtr(highlighting.Units, [](const TUnit& unit) {
            return unit.Kind == EUnitKind::Whitespace;
        });
        Y_ENSURE(ws.Patterns.size() == 1);
        json.AppendValue(NJson::TJsonArray{ToMonarchRegex(ws, ws.Patterns.at(0)), "white"});

        ForEachMultiLine(highlighting, [&](const TUnit& unit) {
            json.AppendValue(NJson::TJsonArray{
                RE2::QuoteMeta(unit.RangePattern->Begin),
                ToMonarchSelector(unit.Kind),
                "@" + ToMonarchStateName(unit.Kind),
            });
        });

        return json;
    }

    NJson::TJsonValue ToMonarchRootState(const THighlighting& highlighting) {
        NJson::TJsonValue json;
        json.AppendValue(NJson::TJsonMap{{"include", "@whitespace"}});
        for (const TUnit& unit : highlighting.Units) {
            if (unit.IsCodeGenExcluded) {
                continue;
            }

            TString group = ToMonarchSelector(unit.Kind);
            for (const NSQLTranslationV1::TRegexPattern& pattern : unit.Patterns) {
                TString regex = ToMonarchRegex(unit, pattern);
                json.AppendValue(NJson::TJsonArray{regex, group});
            }
        }
        return json;
    }

    void GenerateMonarch(IOutputStream& out, const THighlighting& highlighting) {
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
        write_json("root", ToMonarchRootState(highlighting));
        write_json("whitespace", ToMonarchWhitespaceState(highlighting));
        ForEachMultiLine(highlighting, [&](const TUnit& unit) {
            write_json(ToMonarchStateName(unit.Kind), ToMonarchMultiLineState(unit));
        });
        write_json("embedded", MonarchEmbeddedState());
        buf.EndObject();

        buf.EndObject();
    }

    IGenerator::TPtr MakeMonarchGenerator() {
        return MakeOnlyFileGenerator(GenerateMonarch);
    }

} // namespace NSQLHighlight
