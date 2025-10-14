#include "query_utils.h"
#include "view_utils.h"

#include <yql/essentials/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_proto_split_antlr4/SQLv1Antlr4Parser.pb.main.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <util/string/builder.h>

#include <re2/re2.h>

namespace NYdb::NDump {

using namespace NSQLv1Generated;

namespace {

bool ValidateViewQuery(const TString& query, NYql::TIssues& issues) {
    TRule_sql_query queryProto;
    if (!SqlToProtoAst(query, queryProto, issues)) {
        return false;
    }
    return ValidateTableRefs(queryProto, issues);
}

void ValidateViewQuery(const TString& query, const TString& dbPath, NYql::TIssues& issues) {
    NYql::TIssues subIssues;
    if (!ValidateViewQuery(query, subIssues)) {
        NYql::TIssue restorabilityIssue(
            TStringBuilder() << "Restorability of the view: " << dbPath.Quote()
            << " storing the following query:\n"
            << query
            << "\ncannot be guaranteed. For more information, please refer to the 'ydb tools dump' documentation."
        );
        restorabilityIssue.Severity = NYql::TSeverityIds::S_WARNING;
        for (const auto& subIssue : subIssues) {
            restorabilityIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(subIssue));
        }
        issues.AddIssue(std::move(restorabilityIssue));
    }
}

bool RewriteTablePathPrefix(TString& query, TStringBuf backupRoot, TStringBuf restoreRoot,
    bool restoreRootIsDatabase, TString& backupPathPrefix, TString& restorePathPrefix, NYql::TIssues& issues
) {
    restorePathPrefix = restoreRoot;

    if (!re2::RE2::PartialMatch(query, R"(PRAGMA TablePathPrefix = '(\S+)';)", &backupPathPrefix)) {
        if (!restoreRootIsDatabase) {
            // Initially, the view relied on the implicit table path prefix;
            // however, this approach is now incorrect because the requested restore root differs from the database root.
            // We need to explicitly set the TablePathPrefix pragma to ensure that the reference targets
            // keep the same relative positions to the view's location as before.

            size_t contextRecreationEnd = query.find("CREATE VIEW");
            if (contextRecreationEnd == TString::npos) {
                issues.AddIssue(TStringBuilder() << "no create view statement in the query: " << query);
                return false;
            }
            query.insert(contextRecreationEnd, TString(
                std::format("PRAGMA TablePathPrefix = '{}';\n", restoreRoot.data())
            ));
        }
        return true;
    }

    if (backupRoot == restoreRoot) {
        return true;
    }

    restorePathPrefix = RewriteAbsolutePath(backupPathPrefix, backupRoot, restoreRoot);

    constexpr TStringBuf pattern = R"(PRAGMA TablePathPrefix = '\S+';)";
    if (!re2::RE2::Replace(&query, pattern,
        std::format(R"(PRAGMA TablePathPrefix = '{}';)", restorePathPrefix.c_str())
    )) {
        issues.AddIssue(TStringBuilder() << "query: " << query.Quote()
            << " does not contain the pattern: \"" << pattern << "\""
        );
        return false;
    }

    return true;
}

} // anonymous

TViewQuerySplit SplitViewQuery(TStringInput query) {
    // to do: make the implementation more versatile
    TViewQuerySplit split;

    TString line;
    while (query.ReadLine(line)) {
        (line.StartsWith("--") || line.StartsWith("PRAGMA ")
            ? split.ContextRecreation
            : split.Select
        ) += '\n' + line;
    }

    return split;
}

TString BuildCreateViewQuery(
    const TString& name, const TString& dbPath, const TString& viewQuery, const TString& database, const TString& backupRoot,
    NYql::TIssues& issues
) {
    auto [contextRecreation, select] = SplitViewQuery(viewQuery);

    const TString creationQuery = std::format(
        "-- database: \"{}\"\n"
        "-- backup root: \"{}\"\n"
        "{}\n"
        "CREATE VIEW IF NOT EXISTS `{}` WITH (security_invoker = TRUE) AS\n"
        "    {};\n",
        database.data(),
        backupRoot.data(),
        contextRecreation.data(),
        name.data(),
        select.data()
    );

    ValidateViewQuery(creationQuery, dbPath, issues);

    TString formattedQuery;
    if (!Format(creationQuery, formattedQuery, issues)) {
        return "";
    }
    return formattedQuery;
}

bool RewriteCreateViewQuery(TString& query, const TString& restoreRoot, bool restoreRootIsDatabase,
    const TString& dbPath, NYql::TIssues& issues
) {
    const auto backupRoot = GetBackupRoot(query);

    TString backupPathPrefix = GetDatabase(query);
    TString restorePathPrefix;
    if (!RewriteTablePathPrefix(query, backupRoot, restoreRoot, restoreRootIsDatabase, backupPathPrefix, restorePathPrefix, issues)) {
        return false;
    }

    if (!RewriteTableRefs(query, backupRoot, restoreRoot, backupPathPrefix, restorePathPrefix, issues)) {
        return false;
    }

    return RewriteCreateQuery(query, "CREATE VIEW IF NOT EXISTS `{}`", dbPath, issues);
}

} // NYdb::NDump
