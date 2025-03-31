#include "query_utils.h"
#include "view_utils.h"

#include <yql/essentials/parser/proto_ast/gen/v1_proto_split/SQLv1Parser.pb.main.h>
#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/sql/v1/lexer/antlr3/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr3_ansi/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr3/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr3_ansi/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/sql/v1/sql.h>

#include <util/string/builder.h>

#include <re2/re2.h>

namespace NYdb::NDump {

using namespace NSQLTranslationV1;
using namespace NSQLv1Generated;
using namespace NYql;

namespace {

bool ValidateViewQuery(const TString& query, TIssues& issues) {
    TRule_sql_query queryProto;
    if (!SqlToProtoAst(query, queryProto, issues)) {
        return false;
    }
    return ValidateTableRefs(queryProto, issues);
}

void ValidateViewQuery(const TString& query, const TString& dbPath, TIssues& issues) {
    TIssues subIssues;
    if (!ValidateViewQuery(query, subIssues)) {
        TIssue restorabilityIssue(
            TStringBuilder() << "Restorability of the view: " << dbPath.Quote()
            << " storing the following query:\n"
            << query.Quote()
            << "\ncannot be guaranteed. For more information, please refer to the 'ydb tools dump' documentation."
        );
        restorabilityIssue.Severity = TSeverityIds::S_WARNING;
        for (const auto& subIssue : subIssues) {
            restorabilityIssue.AddSubIssue(MakeIntrusive<TIssue>(subIssue));
        }
        issues.AddIssue(std::move(restorabilityIssue));
    }
}

bool RewriteTablePathPrefix(TString& query, TStringBuf backupRoot, TStringBuf restoreRoot,
    bool restoreRootIsDatabase, TIssues& issues
) {
    if (backupRoot == restoreRoot) {
        return true;
    }

    TString pathPrefix;
    if (!re2::RE2::PartialMatch(query, R"(PRAGMA TablePathPrefix = '(\S+)';)", &pathPrefix)) {
        if (!restoreRootIsDatabase) {
            // Initially, the view relied on the implicit table path prefix;
            // however, this approach is now incorrect because the requested restore root differs from the database root.
            // We need to explicitly set the TablePathPrefix pragma to ensure that the reference targets
            // keep the same relative positions to the view's location as before.

            size_t contextRecreationEnd = query.find("CREATE VIEW");
            if (contextRecreationEnd == TString::npos) {
                issues.AddIssue(TStringBuilder() << "no create view statement in the query: " << query.Quote());
                return false;
            }
            query.insert(contextRecreationEnd, TString(
                std::format("PRAGMA TablePathPrefix = '{}';\n", restoreRoot.data())
            ));
        }
        return true;
    }

    pathPrefix = RewriteAbsolutePath(pathPrefix, backupRoot, restoreRoot);

    constexpr TStringBuf pattern = R"(PRAGMA TablePathPrefix = '\S+';)";
    if (!re2::RE2::Replace(&query, pattern,
        std::format(R"(PRAGMA TablePathPrefix = '{}';)", pathPrefix.c_str())
    )) {
        issues.AddIssue(TStringBuilder() << "query: " << query.Quote()
            << " does not contain the pattern: \"" << pattern << "\""
        );
        return false;
    }

    return true;
}

} // anonymous

TViewQuerySplit::TViewQuerySplit(const TVector<TString>& statements) {
    TStringBuilder context;
    for (int i = 0; i < std::ssize(statements) - 1; ++i) {
        context << statements[i] << '\n';
    }
    ContextRecreation = context;
    Y_ENSURE(!statements.empty());
    Select = statements.back();
}

bool SplitViewQuery(const TString& query, TViewQuerySplit& split, TIssues& issues) {
    google::protobuf::Arena Arena;
    NSQLTranslation::TTranslationSettings translationSettings;
    translationSettings.Arena = &Arena;
    if (!NSQLTranslation::ParseTranslationSettings(query, translationSettings, issues)) {
        return false;
    }

    TLexers lexers;
    lexers.Antlr3 = MakeAntlr3LexerFactory();
    lexers.Antlr3Ansi = MakeAntlr3AnsiLexerFactory();
    lexers.Antlr4 = MakeAntlr4LexerFactory();
    lexers.Antlr4Ansi = MakeAntlr4AnsiLexerFactory();
    TParsers parsers;
    parsers.Antlr3 = MakeAntlr3ParserFactory();
    parsers.Antlr3Ansi = MakeAntlr3AnsiParserFactory();
    parsers.Antlr4 = MakeAntlr4ParserFactory();
    parsers.Antlr4Ansi = MakeAntlr4AnsiParserFactory();

    TVector<TString> statements;
    if (!SplitQueryToStatements(lexers, parsers, query, statements, issues, translationSettings)) {
        return false;
    }
    if (statements.empty()) {
        issues.AddIssue(TStringBuilder() << "No select statement in the view query: " << query.Quote());
        return false;
    }

    split = TViewQuerySplit(statements);
    return true;
}

TString BuildCreateViewQuery(
    const TString& name, const TString& dbPath, const TString& viewQuery, const TString& backupRoot,
    TIssues& issues
) {
    TViewQuerySplit split;
    if (!SplitViewQuery(viewQuery, split, issues)) {
        return "";
    }

    const TString creationQuery = std::format(
        "-- backup root: \"{}\"\n"
        "{}\n"
        "CREATE VIEW IF NOT EXISTS `{}` WITH (security_invoker = TRUE) AS\n"
        "    {};\n",
        backupRoot.c_str(),
        split.ContextRecreation.c_str(),
        name.c_str(),
        split.Select.c_str()
    );

    ValidateViewQuery(creationQuery, dbPath, issues);

    TString formattedQuery;
    if (!Format(creationQuery, formattedQuery, issues)) {
        return "";
    }
    return formattedQuery;
}

bool RewriteCreateViewQuery(TString& query, const TString& restoreRoot, bool restoreRootIsDatabase,
    const TString& dbPath, TIssues& issues
) {
    const auto backupRoot = GetBackupRoot(query);

    if (!RewriteTablePathPrefix(query, backupRoot, restoreRoot, restoreRootIsDatabase, issues)) {
        return false;
    }

    if (!RewriteTableRefs(query, backupRoot, restoreRoot, issues)) {
        return false;
    }

    return RewriteCreateQuery(query, "CREATE VIEW IF NOT EXISTS `{}`", dbPath, issues);
}

} // NYdb::NDump
