#include "yqls_highlight.h"

namespace NSQLHighlight {

    using TRe = NSQLTranslationV1::TRegexPattern;
    using NSQLTranslationV1::Merged;

    THighlighting MakeYQLsHighlighting() {
        TString id = R"re([A-Za-z_\-0-9]+)re";

        TRe keywords = Merged({
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
        });
        keywords.Before = R"re(\()re";

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
                    .Patterns = {keywords},
                },
                TUnit{
                    .Kind = EUnitKind::BindParameterIdentifier,
                    .Patterns = {TRe{"world"}},
                },
                TUnit{
                    .Kind = EUnitKind::QuotedIdentifier,
                    .Patterns = {
                        TRe{.Body = id + "!", .Before = R"re(\()re"},
                    },
                    .IsPlain = false,
                },
                TUnit{
                    .Kind = EUnitKind::FunctionIdentifier,
                    .Patterns = {
                        TRe{.Body = id, .Before = R"re(\()re"},
                        TRe{.Body = "'" + id + "\\." + id},
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
                    .Patterns = {TRe{id}},
                },
                TUnit{
                    .Kind = EUnitKind::StringLiteral,
                    .Patterns = {
                        TRe{R"re(\"[^\"\n]*\")re"},
                        TRe{R"re(\@\@(.|\n)*\@\@)re"},
                    },
                    .RangePattern = TRangePattern{
                        .Begin = "@@",
                        .End = "@@",
                    },
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
