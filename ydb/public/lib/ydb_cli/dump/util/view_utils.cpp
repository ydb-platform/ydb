#include "view_utils.h"

#include <yql/essentials/parser/proto_ast/gen/v1/SQLv1Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_proto_split/SQLv1Parser.pb.main.h>
#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/sql/v1/format/sql_format.h>
#include <yql/essentials/sql/v1/proto_parser/proto_parser.h>

#include <library/cpp/protobuf/util/simple_reflection.h>

#include <util/folder/pathsplit.h>
#include <util/string/builder.h>

#include <format>

using namespace NSQLv1Generated;

namespace {

struct TAbsolutePathRewriter {

    static bool IsAbsolutePath(TStringBuf path) {
        return path.StartsWith("`/") && path.EndsWith('`');
    }

    TString RewriteAbsolutePath(const TString& path) const {
        if (BackupRoot == RestoreRoot) {
            return path;
        }

        return TStringBuilder() << '`'
            << NYdb::NDump::RewriteAbsolutePath(TStringBuf(path.begin() + 1, path.end() - 1), BackupRoot, RestoreRoot)
            << '`';
    }

    TString operator()(const TString& path) const {
        if (IsAbsolutePath(path)) {
            return RewriteAbsolutePath(path);
        }
        return path;
    }

    TStringBuf BackupRoot;
    TStringBuf RestoreRoot;
};

struct TTokenCollector {
    TTokenCollector(std::function<TString(const TString&)>&& pathRewriter = {}) : PathRewriter(std::move(pathRewriter)) {}

    void operator()(const NProtoBuf::Message& message) {
        if (const auto* token = dynamic_cast<const TToken*>(&message)) {
            const auto& value = token->GetValue();
            if (token->GetId() != NALPDefault::SQLv1LexerTokens::TOKEN_EOF) {
                if (!Tokens.empty()) {
                    Tokens << ' ';
                }
                Tokens << (IsTableRefDescendent && PathRewriter ? PathRewriter(value) : value);
            }
        }
    }

    TStringBuilder Tokens;

    bool IsTableRefDescendent = false;
    std::function<TString(const TString&)> PathRewriter;
};

void VisitAllFields(const NProtoBuf::Message& msg, const std::function<bool(const NProtoBuf::Message&)>& callback) {
    const auto* descr = msg.GetDescriptor();
    for (int i = 0; i < descr->field_count(); ++i) {
        const auto* fd = descr->field(i);
        NProtoBuf::TConstField field(msg, fd);
        if (field.IsMessage()) {
            for (size_t j = 0; j < field.Size(); ++j) {
                const auto& message = *field.Get<NProtoBuf::Message>(j);
                if (callback(message)) {
                    VisitAllFields(message, callback);
                }
            }
        }
    }
}

void VisitAllFields(const NProtoBuf::Message& msg, TTokenCollector& callback) {
    const auto* descr = msg.GetDescriptor();
    for (int i = 0; i < descr->field_count(); ++i) {
        const auto* fd = descr->field(i);
        NProtoBuf::TConstField field(msg, fd);
        if (field.IsMessage()) {
            for (size_t j = 0; j < field.Size(); ++j) {
                const auto& message = *field.Get<NProtoBuf::Message>(j);
                const auto* tableRef = dynamic_cast<const TRule_table_ref*>(&message);
                if (tableRef) {
                    callback.IsTableRefDescendent = true;
                }

                callback(message);
                VisitAllFields(message, callback);

                if (tableRef) {
                    callback.IsTableRefDescendent = false;
                }
            }
        }
    }
}

struct TTableRefValidator {

    // returns true if the message is not a table ref and we need to dive deeper to find it
    bool operator()(const NProtoBuf::Message& message) {
        const auto* tableRef = dynamic_cast<const TRule_table_ref*>(&message);
        if (!tableRef) {
            return true;
        }

        // implementation note: a better idea might be to create a custom grammar for validation
        if (tableRef->HasBlock3() && tableRef->GetBlock3().HasAlt1() && tableRef->GetBlock3().GetAlt1().HasRule_table_key1()) {
            // Table keys are considered save for view backups.
            return false;
        }

        // The only kind of table references in views that we really cannot restore are evaluated absolute paths:
        // $path = "/old_db" || "/t"; select * from $path;
        // If the view is being restored to a different database (like "/new_db"),
        // then the saved create view statement will need manual patching to succeed.
        TTokenCollector tokenCollector;
        VisitAllFields(*tableRef, tokenCollector);
        const TString refString = tokenCollector.Tokens;

        Issues.AddIssue(TStringBuilder() << "Please check that the table reference: " << refString.Quote()
            << " contains no evaluated expressions."
        );
        Issues.back().Severity = NYql::TSeverityIds::S_WARNING;

        return false;
    }

    NYql::TIssues& Issues;
};

bool ValidateTableRefs(const TRule_sql_query& query, NYql::TIssues& issues) {
    TTableRefValidator tableRefValidator(issues);
    VisitAllFields(query, tableRefValidator);
    return tableRefValidator.Issues.Empty();
}

TString RewriteTableRefs(const TRule_sql_query& query, TStringBuf backupRoot, TStringBuf restoreRoot) {
    TAbsolutePathRewriter pathRewriter;
    pathRewriter.BackupRoot = backupRoot;
    pathRewriter.RestoreRoot = restoreRoot;

    TTokenCollector tokenCollector(std::move(pathRewriter));
    VisitAllFields(query, tokenCollector);
    return tokenCollector.Tokens;
}

bool SqlToProtoAst(const TString& query, TRule_sql_query& queryProto, NYql::TIssues& issues) {
    NSQLTranslation::TTranslationSettings settings;
    if (!NSQLTranslation::ParseTranslationSettings(query, settings, issues)) {
        return false;
    }
    if (settings.SyntaxVersion == 0) {
        issues.AddIssue("cannot handle YQL syntax version 0");
        return false;
    }

    google::protobuf::Arena arena;
    const auto* parserProto = NSQLTranslationV1::SqlAST(
        query, "query", issues, 0, settings.AnsiLexer, settings.Antlr4Parser, settings.TestAntlr4, &arena
    );
    if (!parserProto) {
        return false;
    }

    queryProto = static_cast<const TSQLv1ParserAST&>(*parserProto).GetRule_sql_query();
    return true;
}

bool ValidateViewQuery(const TString& query, NYql::TIssues& issues) {
    TRule_sql_query queryProto;
    if (!SqlToProtoAst(query, queryProto, issues)) {
        return false;
    }
    return ValidateTableRefs(queryProto, issues);
}

}

namespace NYdb::NDump {

TString RewriteAbsolutePath(TStringBuf path, TStringBuf backupRoot, TStringBuf restoreRoot) {
    if (backupRoot == restoreRoot) {
        return TString(path);
    }

    TPathSplitUnix pathSplit(path);
    TPathSplitUnix backupRootSplit(backupRoot);

    size_t matchedParts = 0;
    while (matchedParts < pathSplit.size() && matchedParts < backupRootSplit.size()
        && pathSplit[matchedParts] == backupRootSplit[matchedParts]
    ) {
        ++matchedParts;
    }

    TPathSplitUnix restoreRootSplit(restoreRoot);
    for (size_t unmatchedParts = matchedParts + 1; unmatchedParts <= backupRootSplit.size(); ++unmatchedParts) {
        restoreRootSplit.AppendComponent("..");
    }
    return restoreRootSplit.AppendMany(pathSplit.begin() + matchedParts, pathSplit.end()).Reconstruct();
}

bool RewriteTableRefs(TString& query, TStringBuf backupRoot, TStringBuf restoreRoot, NYql::TIssues& issues) {
    TRule_sql_query queryProto;
    if (!SqlToProtoAst(query, queryProto, issues)) {
        return false;
    }
    const auto rewrittenQuery = ::RewriteTableRefs(queryProto, backupRoot, restoreRoot);
    // formatting here is necessary for the view to have pretty text inside it after the creation
    if (!Format(rewrittenQuery, query, issues)) {
        return false;
    }
    return true;
}

TViewQuerySplit SplitViewQuery(TStringInput query) {
    // to do: make the implementation more versatile
    TViewQuerySplit split;

    TString line;
    while (query.ReadLine(line)) {
        (line.StartsWith("--") || line.StartsWith("PRAGMA ")
            ? split.ContextRecreation
            : split.Select
        ) += line;
    }

    return split;
}

void ValidateViewQuery(const TString& query, const TString& dbPath, NYql::TIssues& issues) {
    NYql::TIssues subIssues;
    if (!::ValidateViewQuery(query, subIssues)) {
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

bool Format(const TString& query, TString& formattedQuery, NYql::TIssues& issues) {
    google::protobuf::Arena arena;
    NSQLTranslation::TTranslationSettings settings;
    settings.Arena = &arena;

    auto formatter = NSQLFormat::MakeSqlFormatter(settings);
    return formatter->Format(query, formattedQuery, issues);
}

TString BuildCreateViewQuery(
    const TString& name, const TString& dbPath, const TString& viewQuery, const TString& backupRoot,
    NYql::TIssues& issues
) {
    auto [contextRecreation, select] = NDump::SplitViewQuery(viewQuery);

    const TString creationQuery = std::format(
        "-- backup root: \"{}\"\n"
        "{}\n"
        "CREATE VIEW IF NOT EXISTS `{}` WITH (security_invoker = TRUE) AS\n"
        "    {};\n",
        backupRoot.data(),
        contextRecreation.data(),
        name.data(),
        select.data()
    );

    NYdb::NDump::ValidateViewQuery(creationQuery, dbPath, issues);

    TString formattedQuery;
    if (!NYdb::NDump::Format(creationQuery, formattedQuery, issues)) {
        return "";
    }
    return formattedQuery;
}

}
