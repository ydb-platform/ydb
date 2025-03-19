#include "check_runner.h"
#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/parser/pg_wrapper/interface/raw_parser.h>

namespace NYql {
namespace NFastCheck {

namespace {

class TParserRunner : public ICheckRunner {
public:
    TString GetCheckName() const final {
        return "parser";
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
        TAstParseResult res = ParseAst(request.Program);
        return TCheckResponse{
            .CheckName = GetCheckName(),
            .Success = res.IsOk(),
            .Issues = res.Issues
        };
    }

    class TPGParseEventsHandler : public IPGParseEvents {
    public:
        TPGParseEventsHandler(TCheckResponse& res)
            : Res_(res)
        {}

        void OnResult(const List* raw) final {
            Y_UNUSED(raw);
            Res_.Success = true;
        }

        void OnError(const TIssue& issue) final {
            Res_.Issues.AddIssue(issue);
        }

    private:
        TCheckResponse& Res_;
    };

    TCheckResponse RunPg(const TChecksRequest& request) {
        TCheckResponse res {.CheckName = GetCheckName()};
        TPGParseEventsHandler handler(res);
        PGParse(request.Program, handler);
        return res;
    }

    TCheckResponse RunYql(const TChecksRequest& request) {
        TCheckResponse res {.CheckName = GetCheckName()};
        NSQLTranslation::TTranslationSettings settings;
        settings.SyntaxVersion = request.SyntaxVersion;
        settings.AnsiLexer = request.IsAnsiLexer;
        settings.Antlr4Parser = true;
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
            google::protobuf::Arena arena;
            NSQLTranslationV1::TParsers parsers;
            parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();
            parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory();
            auto msg = NSQLTranslationV1::SqlAST(parsers, request.Program, request.File, res.Issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS,
                settings.AnsiLexer, true, &arena);
            if (msg) {
                res.Success = true;
            }
        }

        return res;
    }
};

}

std::unique_ptr<ICheckRunner> MakeParserRunner() {
    return std::make_unique<TParserRunner>();
}

}
}
