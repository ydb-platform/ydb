#include "check_runner.h"
#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/settings/translation_settings.h>

namespace NYql {
namespace NFastCheck {

namespace {

class TLexerRunner : public ICheckRunner {
public:
    TString GetCheckName() const final {
        return "lexer";
    }

    TCheckResponse Run(const TChecksRequest& request) final {
        switch (request.Syntax) {
        case ESyntax::SExpr:
            return RunSExpr(request);
        case ESyntax::PG:
            return RunPg(request);
        case ESyntax::YQL:
            return RunYql(request);
        }
    }

private:
    TCheckResponse RunSExpr(const TChecksRequest& request) {
        Y_UNUSED(request);
        // no separate check for lexer here
        return TCheckResponse{.CheckName = GetCheckName(), .Success = true};
    }

    TCheckResponse RunPg(const TChecksRequest& request) {
        Y_UNUSED(request);
        // no separate check for lexer here
        return TCheckResponse{.CheckName = GetCheckName(), .Success = true};
    }

    TCheckResponse RunYql(const TChecksRequest& request) {
        TCheckResponse res {.CheckName = GetCheckName()};
        NSQLTranslation::TTranslationSettings settings;
        settings.SyntaxVersion = request.SyntaxVersion;
        settings.AnsiLexer = request.IsAnsiLexer;
        settings.File = request.File;
        if (!ParseTranslationSettings(request.Program, settings, res.Issues)) {
            return res;
        }

        NSQLTranslationV1::TLexers lexers;
        lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
        lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
        auto lexer = NSQLTranslationV1::MakeLexer(lexers, settings.AnsiLexer, true);
        auto onNextToken = [&](NSQLTranslation::TParsedToken&& token) {
            Y_UNUSED(token);
        };

        if (lexer->Tokenize(request.Program, request.File, onNextToken, res.Issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS)) {
            res.Success = true;
        }

        return res;
    }
};

}

std::unique_ptr<ICheckRunner> MakeLexerRunner() {
    return std::make_unique<TLexerRunner>();
}

}
}
