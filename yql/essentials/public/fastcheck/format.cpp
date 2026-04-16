#include "check_runner.h"
#include "check_state.h"
#include <yql/essentials/sql/v1/format/sql_format.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/core/issue/yql_issue.h>
#include <util/charset/utf8.h>
#include <util/string/builder.h>

namespace NYql::NFastCheck {

namespace {

constexpr size_t FormatContextLimit = 100;

TString NormalizeEOL(TStringBuf input) {
    TStringBuilder res;
    TStringBuf tok;
    while (input.ReadLine(tok)) {
        res << tok << '\n';
    }

    return res;
}

TString ReplaceHidden(TStringBuf input) {
    TStringBuilder res;
    for (const auto c : input) {
        if (c == ' ') {
            res << "\xe2\x80\xa2";
        } else if (c == '\t') {
            res << "\xe2\x86\x92";
        } else {
            res << c;
        }
    }

    return res;
}

class TFormatRunner: public TCheckRunnerBase {
public:
    TString GetCheckName() const final {
        return "format";
    }

    TCheckResponse DoRun(const TChecksRequest& request, TCheckState& state) final {
        switch (state.GetEffectiveSyntax()) {
            case ESyntax::SExpr:
                return RunSExpr(request, state);
            case ESyntax::PG:
                return RunPg(request, state);
            case ESyntax::YQL:
                return RunYql(request, state);
        }
    }

private:
    TCheckResponse RunSExpr(const TChecksRequest& request, TCheckState& state) {
        Y_UNUSED(request);
        Y_UNUSED(state);
        // no separate check for format here
        return TCheckResponse{.CheckName = GetCheckName(), .Success = true};
    }

    TCheckResponse RunPg(const TChecksRequest& request, TCheckState& state) {
        Y_UNUSED(request);
        Y_UNUSED(state);
        // no separate check for format here
        return TCheckResponse{.CheckName = GetCheckName(), .Success = true};
    }

    TCheckResponse RunYql(const TChecksRequest& request, TCheckState& state) {
        Y_UNUSED(state);
        TCheckResponse res{.CheckName = GetCheckName()};
        if (request.SyntaxVersion != 1) {
            res.Issues.AddIssue(TIssue({}, "Only SyntaxVersion 1 is supported"));
            return res;
        }

        google::protobuf::Arena arena;
        NSQLTranslation::TTranslationSettings settings;
        settings.Arena = &arena;
        settings.File = request.File;
        settings.Antlr4Parser = true;
        settings.AnsiLexer = request.IsAnsiLexer;
        settings.LangVer = request.LangVer;

        NSQLTranslationV1::TLexers lexers;
        lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
        lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
        NSQLTranslationV1::TParsers parsers;
        parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();
        parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory();
        auto formatter = NSQLFormat::MakeSqlFormatter(lexers, parsers, settings);
        TString formattedQuery;
        res.Success = formatter->Format(request.Program, formattedQuery, res.Issues);
        if (res.Success && NormalizeEOL(formattedQuery) != NormalizeEOL(request.Program)) {
            res.Success = false;
            TPosition origPos(0, 1, request.File);
            TTextWalker origWalker(origPos, true);
            size_t i = 0;
            for (; i < Min(request.Program.size(), formattedQuery.size()); ++i) {
                if (request.Program[i] == formattedQuery[i]) {
                    origWalker.Advance(request.Program[i]);
                    continue;
                }

                while (i > 0 && IsUTF8ContinuationByte(request.Program[i])) {
                    --i;
                }

                break;
            }

            TString formattedSample = formattedQuery.substr(i, FormatContextLimit);
            while (!formattedSample.empty() && IsUTF8ContinuationByte(formattedQuery.back())) {
                formattedSample.erase(formattedSample.size() - 1);
            }

            TString origSample = request.Program.substr(i, FormatContextLimit);
            while (!origSample.empty() && IsUTF8ContinuationByte(origSample.back())) {
                origSample.erase(origSample.size() - 1);
            }

            auto issue = TIssue(origPos, TStringBuilder() << "Format mismatch, expected:\n"
                                                          << ReplaceHidden(formattedSample) << "\nbut got:\n"
                                                          << ReplaceHidden(origSample) << "\n");
            issue.SetCode(EYqlIssueCode::TIssuesIds_EIssueCode_WARNING, ESeverity::TSeverityIds_ESeverityId_S_WARNING);
            res.Issues.AddIssue(issue);
        }

        return res;
    }
};

} // namespace

std::unique_ptr<ICheckRunner> MakeFormatRunner() {
    return std::make_unique<TFormatRunner>();
}

} // namespace NYql::NFastCheck
