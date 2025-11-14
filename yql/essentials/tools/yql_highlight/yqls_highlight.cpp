#include "yqls_highlight.h"

namespace NSQLHighlight {

using TRe = NSQLTranslationV1::TRegexPattern;
using NSQLTranslationV1::Merged;

THighlighting MakeYQLsHighlighting() {
    const THighlighting yql = MakeHighlighting();
    const auto unit = [&](EUnitKind kind) {
        return *FindIf(yql.Units, [&](const TUnit& unit) {
            return unit.Kind == kind;
        });
    };

    auto strings = unit(EUnitKind::StringLiteral).RangePatterns;
    EraseIf(strings, [](const TRangePattern& range) {
        return range.BeginPlain.StartsWith("'");
    });

    auto types = unit(EUnitKind::TypeIdentifier).Patterns;
    for (NSQLTranslationV1::TRegexPattern& pattern : types) {
        pattern.IsCaseInsensitive = false;
    }

    TString id = R"re([A-Za-z_\-0-9]+)re";
    TString lower = R"re([a-z_0-9])re" + SubstGlobalCopy(id, '+', '*');
    TString title = R"re([A-Z])re" + SubstGlobalCopy(id, '+', '*');

    return {
        .Name = "YQLs",
        .Extension = "yqls",
        .Units = {
            TUnit{
                .Kind = EUnitKind::Comment,
                .Patterns = {TRe{R"re(#.*)re"}},
                .IsPlain = false,
            },
            TUnit{
                .Kind = EUnitKind::Keyword,
                .Patterns = {
                    Merged({
                        {"let"},
                        {"return"},
                        {"quote"},
                        {"block"},
                        {"lambda"},
                        {"declare"},
                        {"import"},
                        {"export"},
                        {"library"},
                        {"override_library"},
                        {"package"},
                        {"set_package_version"},
                    }),
                },
            },
            TUnit{
                .Kind = EUnitKind::BindParameterIdentifier,
                .Patterns = {TRe{"world"}},
            },
            TUnit{
                .Kind = EUnitKind::QuotedIdentifier,
                .Patterns = {
                    TRe{.Body = title + "!"},
                },
                .IsPlain = false,
            },
            TUnit{
                .Kind = EUnitKind::TypeIdentifier,
                .Patterns = types,
            },
            TUnit{
                .Kind = EUnitKind::FunctionIdentifier,
                .Patterns = {
                    TRe{.Body = title},
                    TRe{.Body = "'" + id + "\\." + id},
                    TRe{.Body = "'\"" + id + "\\." + id + "\""},
                },
                .IsPlain = false,
            },
            TUnit{
                .Kind = EUnitKind::Literal,
                .Patterns = {TRe{"'" + id}},
                .IsPlain = false,
            },
            TUnit{
                .Kind = EUnitKind::Identifier,
                .Patterns = {TRe{lower}},
            },
            TUnit{
                .Kind = EUnitKind::StringLiteral,
                .RangePatterns = strings,
                .IsPlain = false,
            },
            TUnit{
                .Kind = EUnitKind::Punctuation,
                .Patterns = {TRe{R"re(['\(\)])re"}},
                .IsPlain = false,
                .IsCodeGenExcluded = true,
            },
            TUnit{
                .Kind = EUnitKind::Whitespace,
                .Patterns = {TRe{R"re(\s+)re"}},
                .IsPlain = false,
                .IsCodeGenExcluded = true,
            },
        },
    };
}

} // namespace NSQLHighlight
