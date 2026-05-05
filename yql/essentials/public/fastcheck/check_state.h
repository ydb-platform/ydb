#pragma once

#include "linter.h"
#include "settings.h"

#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/proto_parser.h>
#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/parser/lexer_common/hints.h>
#include <yql/essentials/parser/pg_wrapper/interface/raw_parser.h>

#include <google/protobuf/arena.h>
#include <google/protobuf/message.h>
#include <util/generic/maybe.h>

namespace NYql::NFastCheck {

class TCheckState {
private:
    struct TLexerResult {
        bool Success;
        NSQLTranslation::TSQLHints Hints;
        TIssues Issues;
    };

    struct TParserResult {
        google::protobuf::Message* Msg;
        TIssues Issues;
    };

    struct TPgParserResult {
        NYql::TPGParseResult Result;
        TIssues Issues;
    };

    struct TAstResultCache {
        TAstParseResult Result;
        TIssues Issues;
    };

public:
    explicit TCheckState(const TChecksRequest& request);
    ~TCheckState();

    ESyntax GetEffectiveSyntax();

    const TAstParseResult* TranslateSql(TIssues& issues);
    const TAstParseResult* TranslatePg(TIssues& issues);
    const TAstParseResult* TranslateSExpr(TIssues& issues);

    bool CheckLexer(TIssues& issues);
    google::protobuf::Message* ParseSql(TIssues& issues);
    const NYql::TPGParseResult* ParsePg(TIssues& issues);
    const TAstParseResult* ParseSExpr(TIssues& issues);

private:
    const TChecksRequest& Request_;
    google::protobuf::Arena Arena_;

    TMaybe<TParsedSettingsCache> ParsedSettingsCache_;
    TMaybe<TLexerResult> LexerCache_;
    TMaybe<TParserResult> ParserCache_;
    TMaybe<TPgParserResult> PgParserCache_;
    TMaybe<TAstResultCache> TranslateCache_;
};

} // namespace NYql::NFastCheck
