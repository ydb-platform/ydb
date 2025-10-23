#include "create_view_formatter.h"

#include <ydb/public/lib/ydb_cli/dump/util/query_utils.h>
#include <ydb/public/lib/ydb_cli/dump/util/view_utils.h>

#include <yql/essentials/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/sql/v1/sql.h>

#include <library/cpp/protobuf/util/simple_reflection.h>

#include <util/folder/path.h>
#include <util/string/builder.h>

using namespace NSQLTranslation;
using namespace NSQLTranslationV1;
using namespace NSQLv1Generated;
using namespace NYql;

namespace NKikimr::NSysView {

namespace {

bool BuildTranslationSettings(const TString& query, google::protobuf::Arena& arena, TTranslationSettings& settings, TIssues& issues) {
    settings.Arena = &arena;
    return ParseTranslationSettings(query, settings, issues);
}

TLexers BuildLexers() {
    TLexers lexers;
    lexers.Antlr4 = MakeAntlr4LexerFactory();
    lexers.Antlr4Ansi = MakeAntlr4AnsiLexerFactory();
    return lexers;
}

TParsers BuildParsers() {
    TParsers parsers;
    parsers.Antlr4 = MakeAntlr4ParserFactory();
    parsers.Antlr4Ansi = MakeAntlr4AnsiParserFactory();
    return parsers;
}

struct TTokenCollector {
    mutable TStringBuilder Tokens;

    void operator()(const NProtoBuf::Message& message) const {
        if (const auto* token = dynamic_cast<const TToken*>(&message)) {
            const auto& value = token->GetValue();
            if (token->GetValue() != "<EOF>") {
                if (!Tokens.empty()) {
                    Tokens << ' ';
                }
                Tokens << value;
            }
        }
    }
};

template <typename TCallback>
void VisitAllFields(const NProtoBuf::Message& msg, const TCallback& callback) {
    const auto* md = msg.GetDescriptor();
    for (int i = 0; i < md->field_count(); ++i) {
        const auto* fd = md->field(i);
        NProtoBuf::TConstField field(msg, fd);
        if (field.IsMessage()) {
            for (size_t j = 0; j < field.Size(); ++j) {
                const auto& message = *field.Get<NProtoBuf::Message>(j);
                callback(message);
                VisitAllFields(message, callback);
            }
        }
    }
}

TString GetTokens(const NProtoBuf::Message& message) {
    TTokenCollector tokenCollector;
    VisitAllFields(message, tokenCollector);
    return tokenCollector.Tokens;
}

TString TrimQuotes(TString&& s) {
    if (s.size() > 1 && ((s.StartsWith('"') && s.EndsWith('"')) || (s.StartsWith('\'') && s.EndsWith('\'')))) {
        return s.substr(1, s.size() - 2);
    }
    return s;
}

bool GetTablePathPrefix(const TString& query, const TParsers& parsers, const TTranslationSettings& settings, TString& tablePathPrefix, TIssues& issues) {
    const auto* message = SqlAST(parsers, query, settings.File, issues, SQL_MAX_PARSER_ERRORS, settings.AnsiLexer, settings.Antlr4Parser, settings.Arena);
    if (!message || message->GetDescriptor()->name() != "TSQLv1ParserAST") {
        issues.AddIssue(TStringBuilder() << "Cannot parse query: " << query.Quote());
        return false;
    }
    const auto& proto = static_cast<const TSQLv1ParserAST&>(*message);
    VisitAllFields(proto, [&tablePathPrefix](const NProtoBuf::Message& message) {
        if (const auto* pragmaMessage = dynamic_cast<const TRule_pragma_stmt*>(&message)) {
            const auto pragma = to_lower(GetTokens(pragmaMessage->GetRule_an_id3()));
            if (pragma == "tablepathprefix" && pragmaMessage->HasBlock4()) {
                if (pragmaMessage->GetBlock4().HasAlt1()) {
                    const auto& pragmaValue = pragmaMessage->GetBlock4().GetAlt1().GetRule_pragma_value2();
                    tablePathPrefix = TrimQuotes(GetTokens(pragmaValue));
                } else if (pragmaMessage->GetBlock4().GetAlt2().GetBlock3().size() == 1) {
                    // unfortunately, this syntax is also supported
                    const auto& pragmaValue = pragmaMessage->GetBlock4().GetAlt2().GetBlock3().Get(0).GetRule_pragma_value2();
                    tablePathPrefix = TrimQuotes(GetTokens(pragmaValue));
                }
            }
        }
    });
    return true;
}

TString GetRelativePath(const TFsPath& prefix, const TFsPath& absolutePath) {
    try {
        return absolutePath.RelativePath(prefix);
    } catch (...) {
        return absolutePath;
    }
}

}

TFormatResult TCreateViewFormatter::Format(const TString& viewRelativePath, const TString& viewAbsolutePath, const NKikimrSchemeOp::TViewDescription& viewDesc) {
    const auto& query = viewDesc.GetQueryText();

    google::protobuf::Arena arena;
    TTranslationSettings translationSettings;
    TIssues issues;
    if (!BuildTranslationSettings(query, arena, translationSettings, issues)) {
        return TFormatResult(Ydb::StatusIds::SCHEME_ERROR, issues);
    }

    auto lexers = BuildLexers();
    auto parsers = BuildParsers();
    NYdb::NDump::TViewQuerySplit split;
    if (!SplitViewQuery(query, lexers, translationSettings, split, issues)) {
        return TFormatResult(Ydb::StatusIds::SCHEME_ERROR, issues);
    }

    TString tablePathPrefix;
    if (!GetTablePathPrefix(query, parsers, translationSettings, tablePathPrefix, issues)) {
        return TFormatResult(Ydb::StatusIds::SCHEME_ERROR, issues);
    }

    const TString path = tablePathPrefix.empty() ? viewRelativePath : GetRelativePath(tablePathPrefix, viewAbsolutePath);
    const TString creationQuery = std::format(
        "{}"
        "CREATE VIEW `{}` WITH (security_invoker = TRUE) AS\n"
        "{}\n",
        split.ContextRecreation.c_str(),
        path.c_str(),
        split.Select.c_str()
    );
    TString formattedQuery;
    if (!NYdb::NDump::Format(creationQuery, formattedQuery, issues)) {
        return TFormatResult(Ydb::StatusIds::SCHEME_ERROR, issues);
    }

    return TFormatResult(formattedQuery);
}

}
