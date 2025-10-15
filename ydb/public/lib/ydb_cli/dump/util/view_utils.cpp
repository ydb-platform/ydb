#include "view_utils.h"

#include <ydb/library/yql/parser/proto_ast/gen/v1/SQLv1Lexer.h>
#include <ydb/library/yql/parser/proto_ast/gen/v1_proto_split/SQLv1Parser.pb.main.h>
#include <ydb/library/yql/sql/settings/translation_settings.h>
#include <ydb/library/yql/sql/v1/format/sql_format.h>
#include <ydb/library/yql/sql/v1/lexer/lexer.h>
#include <ydb/library/yql/sql/v1/proto_parser/proto_parser.h>

#include <library/cpp/protobuf/util/simple_reflection.h>

#include <util/folder/pathsplit.h>
#include <util/string/builder.h>
#include <util/string/strip.h>

#include <format>
#include <re2/re2.h>

using namespace NSQLTranslation;
using namespace NSQLTranslationV1;
using namespace NSQLv1Generated;

namespace {

TPathSplitUnix RewriteAbsolutePath(const TPathSplitUnix& pathSplit, TStringBuf backupRoot, TStringBuf restoreRoot) {
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

    return restoreRootSplit.AppendMany(pathSplit.begin() + matchedParts, pathSplit.end());
}

TString RewriteAbsolutePath(TStringBuf path, TStringBuf backupRoot, TStringBuf restoreRoot) {
    if (backupRoot == restoreRoot) {
        return TString(path);
    }

    TPathSplitUnix pathSplit(path);
    return RewriteAbsolutePath(pathSplit, backupRoot, restoreRoot).Reconstruct();
}

struct TPathRewriter {
    const TStringBuf BackupRoot;
    const TStringBuf RestoreRoot;
    const TStringBuf BackupPathPrefix;
    const TStringBuf RestorePathPrefix;

    static bool IsAbsolutePath(TStringBuf path) {
        return path.StartsWith("`/") && path.EndsWith('`');
    }

    TString RewriteAbsolutePath(TStringBuf path) const {
        if (BackupRoot == RestoreRoot) {
            return TString(path);
        }

        return TStringBuilder() << '`' << ::RewriteAbsolutePath(path.Skip(1).Chop(1), BackupRoot, RestoreRoot) << '`';
    }

    TPathSplitUnix BuildAbsolutePath(TStringBuf path) const {
        Y_DEBUG_ABORT_UNLESS(!IsAbsolutePath(path));
        if (path.StartsWith('`')) {
            Y_DEBUG_ABORT_UNLESS(path.EndsWith('`'));
            path.Skip(1).Chop(1);
        }
        TPathSplitUnix prefixSplit(BackupPathPrefix);
        TPathSplitUnix pathSplit(path);
        prefixSplit.AppendMany(pathSplit.begin(), pathSplit.end());
        return prefixSplit;
    }

    TString BuildRelativePath(const TPathSplitUnix& absoluteSplit) const {
        Y_DEBUG_ABORT_UNLESS(absoluteSplit.IsAbsolute);
        TPathSplitUnix relativeSplit;
        TPathSplitUnix prefixSplit(RestorePathPrefix);
        size_t matchedParts = 0;
        while (matchedParts < size(absoluteSplit) && matchedParts < size(prefixSplit) && absoluteSplit[matchedParts] == prefixSplit[matchedParts]) {
            // skip equal path components
            ++matchedParts;
        }
        for (size_t unmatchedParts = matchedParts; unmatchedParts < size(prefixSplit); ++unmatchedParts) {
            relativeSplit.AppendComponent("..");
        }
        relativeSplit.AppendMany(absoluteSplit.begin() + matchedParts, absoluteSplit.end());
        return relativeSplit.Reconstruct();
    }

public:
    explicit TPathRewriter(TStringBuf backupRoot, TStringBuf restoreRoot, TStringBuf backupPathPrefix, TStringBuf restorePathPrefix)
        : BackupRoot(backupRoot)
        , RestoreRoot(restoreRoot)
        , BackupPathPrefix(backupPathPrefix)
        , RestorePathPrefix(restorePathPrefix)
    {
    }

    TString operator()(const TString& path) const {
        if (IsAbsolutePath(path)) {
            return RewriteAbsolutePath(path);
        } else if (!BackupPathPrefix.empty() && !RestorePathPrefix.empty()) {
            return TStringBuilder() << '`' << BuildRelativePath(::RewriteAbsolutePath(BuildAbsolutePath(path), BackupRoot, RestoreRoot)) << '`';
        }

        return path;
    }
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

TString RewriteTableRefs(const TRule_sql_query& query, TStringBuf backupRoot, TStringBuf restoreRoot, TStringBuf backupPathPrefix, TStringBuf restorePathPrefix) {
    TPathRewriter pathRewriter(backupRoot, restoreRoot, backupPathPrefix, restorePathPrefix);

    TTokenCollector tokenCollector(std::move(pathRewriter));
    VisitAllFields(query, tokenCollector);
    return tokenCollector.Tokens;
}

struct TViewQuerySplit {
    TString ContextRecreation;
    TString Select;

    TViewQuerySplit() = default;
    TViewQuerySplit(const TVector<TString>& statements) {
        TStringBuilder context;
        for (int i = 0; i < std::ssize(statements) - 1; ++i) {
            context << statements[i] << '\n';
        }
        ContextRecreation = context;
        Y_ENSURE(!statements.empty());
        Select = statements.back();
    }
};

bool BuildTranslationSettings(const TString& query, google::protobuf::Arena& arena, TTranslationSettings& settings, NYql::TIssues& issues) {
    settings.Arena = &arena;
    return ParseTranslationSettings(query, settings, issues);
}

bool SplitViewQuery(const TString& query, const TTranslationSettings& translationSettings, TViewQuerySplit& split, NYql::TIssues& issues) {
    TVector<TString> statements;
    auto lexer = NSQLTranslationV1::MakeLexer(translationSettings.AnsiLexer);
    if (!SplitQueryToStatements(query, lexer, statements, issues)) {
        return false;
    }
    if (statements.empty()) {
        issues.AddIssue(TStringBuilder() << "No select statement in the view query: " << query.Quote());
        return false;
    }
    split = TViewQuerySplit(statements);
    return true;
}

bool SplitViewQuery(const TString& query, TViewQuerySplit& split, NYql::TIssues& issues) {
    google::protobuf::Arena arena;
    TTranslationSettings translationSettings;
    if (!BuildTranslationSettings(query, arena, translationSettings, issues)) {
        return false;
    }
    return SplitViewQuery(query, translationSettings, split, issues);
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
        query, "query", issues, 0, settings.AnsiLexer, &arena
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

TString GetToken(TStringInput query, TStringBuf pattern) {
    TString line;
    while (query.ReadLine(line)) {
        StripInPlace(line);
        if (line.StartsWith(pattern)) {
            return TString(TStringBuf(line).Skip(pattern.size()).Chop(1 /* last " */));
        }
    }

    return "";
}

TString GetBackupRoot(const TString& query) {
    return GetToken(query, R"(-- backup root: ")");
}

TString GetDatabase(const TString& query) {
    return GetToken(query, R"(-- database: ")");
}

bool Format(const TString& query, TString& formattedQuery, NYql::TIssues& issues) {
    google::protobuf::Arena arena;
    NSQLTranslation::TTranslationSettings settings;
    settings.Arena = &arena;

    auto formatter = NSQLFormat::MakeSqlFormatter(settings);
    return formatter->Format(query, formattedQuery, issues);
}

bool RewriteTableRefs(TString& query, TStringBuf backupRoot, TStringBuf restoreRoot, TStringBuf backupPathPrefix, TStringBuf restorePathPrefix, NYql::TIssues& issues) {
    TRule_sql_query queryProto;
    if (!SqlToProtoAst(query, queryProto, issues)) {
        return false;
    }
    const auto rewrittenQuery = RewriteTableRefs(queryProto, backupRoot, restoreRoot, backupPathPrefix, restorePathPrefix);
    // formatting here is necessary for the view to have pretty text inside it after the creation
    if (!Format(rewrittenQuery, query, issues)) {
        return false;
    }
    return true;
}

bool RewriteTablePathPrefix(TString& query, TStringBuf backupRoot, TStringBuf restoreRoot,
    bool restoreRootIsDatabase, TString& backupPathPrefix, TString& restorePathPrefix, NYql::TIssues& issues
) {
    restorePathPrefix = restoreRoot;

    re2::RE2::Options options;
    options.set_case_sensitive(false);
    re2::RE2 pattern(R"(PRAGMA TablePathPrefix = '(\S+)';)", options);

    if (!re2::RE2::PartialMatch(query, pattern, &backupPathPrefix)) {
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
                std::format("PRAGMA TablePathPrefix = \"{}\";\n", restoreRoot.data())
            ));
        }
        return true;
    }

    restorePathPrefix = RewriteAbsolutePath(backupPathPrefix, backupRoot, restoreRoot);

    if (backupRoot == restoreRoot) {
        return true;
    }

    if (!re2::RE2::Replace(&query, pattern,
        std::format(R"(PRAGMA TablePathPrefix = '{}';)", restorePathPrefix.c_str())
    )) {
        issues.AddIssue(TStringBuilder() << "query: " << query.Quote()
            << " does not contain the pattern: \"" << pattern.pattern() << "\""
        );
        return false;
    }

    return true;
}

}

namespace NYdb::NDump {

TString BuildCreateViewQuery(
    const TString& name, const TString& dbPath, const TString& viewQuery, const TString& database, const TString& backupRoot,
    NYql::TIssues& issues
) {
    TViewQuerySplit split;
    if (!SplitViewQuery(viewQuery, split, issues)) {
        return "";
    }

    const TString creationQuery = std::format(
        "-- database: \"{}\"\n"
        "-- backup root: \"{}\"\n"
        "{}\n"
        "CREATE VIEW IF NOT EXISTS `{}` WITH (security_invoker = TRUE) AS\n"
        "    {};\n",
        database.data(),
        backupRoot.data(),
        split.ContextRecreation.data(),
        name.data(),
        split.Select.data()
    );

    ValidateViewQuery(creationQuery, dbPath, issues);

    TString formattedQuery;
    if (!Format(creationQuery, formattedQuery, issues)) {
        return "";
    }
    return formattedQuery;
}

bool RewriteCreateViewQuery(TString& query, const TString& restoreRoot, bool restoreRootIsDatabase,
    const TString& dbPath, const TString& source, NYql::TIssues& issues
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

    constexpr TStringBuf pattern = R"(CREATE VIEW IF NOT EXISTS `\S+` )";
    if (!re2::RE2::Replace(&query, pattern, std::format(R"(CREATE VIEW IF NOT EXISTS `{}` )", dbPath.c_str()))) {
        issues.AddIssue(TStringBuilder()
            << "Cannot restore a view from the source: " << source
            << ". Pattern: \"" << pattern << "\", was not found in the create view statement: " << query.Quote()
        );
        return false;
    }

    return true;
}

}
