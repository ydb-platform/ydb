#include "query_utils.h"

#include <yql/essentials/parser/proto_ast/gen/v1/SQLv1Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_proto_split/SQLv1Parser.pb.main.h>
#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/sql/v1/format/sql_format.h>
#include <yql/essentials/sql/v1/proto_parser/proto_parser.h>
#include <yql/essentials/sql/v1/lexer/antlr3/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr3_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr3/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr3_ansi/proto_parser.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <library/cpp/protobuf/util/simple_reflection.h>

#include <util/folder/pathsplit.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/strip.h>

#include <re2/re2.h>

#include <format>

namespace NYdb::NDump {

using namespace NSQLv1Generated;

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

namespace {

struct TAbsolutePathRewriter {
    const TString Database;
    const TStringBuf BackupRoot;
    const TStringBuf RestoreRoot;

    static TString BuildDatabaseToken(TStringBuf database) {
        if (database) {
            return TStringBuilder() << "`" << database << "/";
        } else {
            return "";
        }
    }

    static bool IsAbsolutePath(TStringBuf path) {
        return path.StartsWith("`/") && path.EndsWith('`');
    }

    bool IsInDatabase(TStringBuf path) const {
        return path.StartsWith(Database);
    }

    TString RewriteAbsolutePath(TStringBuf path) const {
        if (BackupRoot == RestoreRoot) {
            return TString(path);
        }

        return TStringBuilder() << '`' << NDump::RewriteAbsolutePath(path.Skip(1).Chop(1), BackupRoot, RestoreRoot) << '`';
    }

public:
    explicit TAbsolutePathRewriter(TStringBuf database, TStringBuf backupRoot, TStringBuf restoreRoot)
        : Database(BuildDatabaseToken(database))
        , BackupRoot(backupRoot)
        , RestoreRoot(restoreRoot)
    {
    }

    TString operator()(const TString& path) const {
        if (IsAbsolutePath(path) && (!Database || IsInDatabase(path))) {
            return RewriteAbsolutePath(path);
        }

        return path;
    }
};

struct TTokenCollector {
    explicit TTokenCollector(std::function<TString(const TString&)>&& pathRewriter = {})
        : PathRewriter(std::move(pathRewriter))
    {
    }

    void operator()(const NProtoBuf::Message& message) {
        if (const auto* token = dynamic_cast<const TToken*>(&message)) {
            const auto& value = token->GetValue();
            if (token->GetId() != NALPDefault::SQLv1LexerTokens::TOKEN_EOF) {
                if (!Tokens.empty()) {
                    Tokens << ' ';
                }
                Tokens << (IsRefDescendent && PathRewriter ? PathRewriter(value) : value);
            }
        }
    }

    TStringBuilder Tokens;
    bool IsRefDescendent = false;
    std::function<TString(const TString&)> PathRewriter;
};

void VisitAllFields(const NProtoBuf::Message& msg, const std::function<bool(const NProtoBuf::Message&)>& callback) {
    const auto* md = msg.GetDescriptor();
    for (int i = 0; i < md->field_count(); ++i) {
        const auto* fd = md->field(i);
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

template <typename TRef>
void VisitAllFields(const NProtoBuf::Message& msg, TTokenCollector& callback) {
    const auto* md = msg.GetDescriptor();
    for (int i = 0; i < md->field_count(); ++i) {
        const auto* fd = md->field(i);
        NProtoBuf::TConstField field(msg, fd);
        if (field.IsMessage()) {
            for (size_t j = 0; j < field.Size(); ++j) {
                const auto& message = *field.Get<NProtoBuf::Message>(j);
                const auto* ref = dynamic_cast<const TRef*>(&message);
                if (ref) {
                    callback.IsRefDescendent = true;
                }

                callback(message);
                VisitAllFields<TRef>(message, callback);

                if (ref) {
                    callback.IsRefDescendent = false;
                }
            }
        }
    }
}

struct TTableRefValidator {
    // returns true if the message is not a table ref and we need to dive deeper to find it
    bool operator()(const NProtoBuf::Message& message) {
        const auto* ref = dynamic_cast<const TRule_table_ref*>(&message);
        if (!ref) {
            return true;
        }

        // implementation note: a better idea might be to create a custom grammar for validation
        if (ref->HasBlock3() && ref->GetBlock3().HasAlt1() && ref->GetBlock3().GetAlt1().HasRule_table_key1()) {
            // Table keys are considered save for view backups.
            return false;
        }

        // The only kind of table references in views that we really cannot restore are evaluated absolute paths:
        // $path = "/old_db" || "/t"; select * from $path;
        // If the view is being restored to a different database (like "/new_db"),
        // then the saved create view statement will need manual patching to succeed.
        TTokenCollector tokenCollector;
        VisitAllFields<TRule_table_ref>(*ref, tokenCollector);
        const TString refString = tokenCollector.Tokens;

        Issues.AddIssue(TStringBuilder() << "Please check that the reference: " << refString.Quote()
            << " contains no evaluated expressions."
        );
        Issues.back().Severity = NYql::TSeverityIds::S_WARNING;

        return false;
    }

    NYql::TIssues& Issues;
};

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

} // anonymous

TString GetBackupRoot(const TString& query) {
    return GetToken(query, R"(-- backup root: ")");
}

TString GetDatabase(const TString& query) {
    return GetToken(query, R"(-- database: ")");
}

TString GetSecretName(const TString& query) {
    TString secretName;
    if (auto pwd = GetToken(query, R"(PASSWORD_SECRET_NAME = ')")) {
        secretName = std::move(pwd);
    } else if (auto token = GetToken(query, R"(TOKEN_SECRET_NAME = ')")) {
        secretName = std::move(token);
    }

    if (secretName.EndsWith("'")) {
        secretName.resize(secretName.size() - 1);
    }

    return secretName;
}

bool SqlToProtoAst(const TString& queryStr, TRule_sql_query& queryProto, NYql::TIssues& issues) {
    NSQLTranslation::TTranslationSettings settings;
    if (!NSQLTranslation::ParseTranslationSettings(queryStr, settings, issues)) {
        return false;
    }
    if (settings.SyntaxVersion == 0) {
        issues.AddIssue("cannot handle YQL syntax version 0");
        return false;
    }

    NSQLTranslationV1::TParsers parsers;
    parsers.Antlr3 = NSQLTranslationV1::MakeAntlr3ParserFactory();
    parsers.Antlr3Ansi = NSQLTranslationV1::MakeAntlr3AnsiParserFactory();
    parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();
    parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory();

    google::protobuf::Arena arena;
    const auto* parserProto = NSQLTranslationV1::SqlAST(
        parsers, queryStr, "query", issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS, settings.AnsiLexer, settings.Antlr4Parser, &arena
    );
    if (!parserProto) {
        return false;
    }

    queryProto = static_cast<const TSQLv1ParserAST&>(*parserProto).GetRule_sql_query();
    return true;
}

bool Format(const TString& query, TString& formattedQuery, NYql::TIssues& issues) {
    google::protobuf::Arena arena;
    NSQLTranslation::TTranslationSettings settings;
    settings.Arena = &arena;

    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr3 = NSQLTranslationV1::MakeAntlr3LexerFactory();
    lexers.Antlr3Ansi = NSQLTranslationV1::MakeAntlr3AnsiLexerFactory();
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
    NSQLTranslationV1::TParsers parsers;
    parsers.Antlr3 = NSQLTranslationV1::MakeAntlr3ParserFactory();
    parsers.Antlr3Ansi = NSQLTranslationV1::MakeAntlr3AnsiParserFactory();
    parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();
    parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory();

    auto formatter = NSQLFormat::MakeSqlFormatter(lexers, parsers, settings);
    return formatter->Format(query, formattedQuery, issues);
}

bool ValidateTableRefs(const TRule_sql_query& query, NYql::TIssues& issues) {
    TTableRefValidator tableRefValidator(issues);
    VisitAllFields(query, tableRefValidator);
    return tableRefValidator.Issues.Empty();
}

template <typename TRef>
TString RewriteRefs(const TRule_sql_query& query, TStringBuf db, TStringBuf backupRoot, TStringBuf restoreRoot) {
    TTokenCollector tokenCollector(TAbsolutePathRewriter(db, backupRoot, restoreRoot));
    VisitAllFields<TRef>(query, tokenCollector);
    return tokenCollector.Tokens;
}

template <typename TRef>
bool RewriteRefs(TString& queryStr, TStringBuf db, TStringBuf backupRoot, TStringBuf restoreRoot, NYql::TIssues& issues) {
    TRule_sql_query queryProto;
    if (!SqlToProtoAst(queryStr, queryProto, issues)) {
        return false;
    }

    const auto rewrittenQuery = RewriteRefs<TRef>(queryProto, db, backupRoot, restoreRoot);
    // formatting here is necessary for the view to have pretty text inside it after the creation
    if (!Format(rewrittenQuery, queryStr, issues)) {
        return false;
    }

    return true;
}

bool RewriteTableRefs(TString& query, TStringBuf backupRoot, TStringBuf restoreRoot, NYql::TIssues& issues) {
    return RewriteRefs<TRule_table_ref>(query, "", backupRoot, restoreRoot, issues);
}

bool RewriteTableRefs(TString& query, TStringBuf restoreRoot, NYql::TIssues& issues) {
    return RewriteRefs<TRule_table_ref>(query, GetDatabase(query), GetBackupRoot(query), restoreRoot, issues);
}

bool RewriteObjectRefs(TString& query, TStringBuf restoreRoot, NYql::TIssues& issues) {
    return RewriteRefs<TRule_object_ref>(query, GetDatabase(query), GetBackupRoot(query), restoreRoot, issues);
}

bool RewriteCreateQuery(TString& query, std::string_view pattern, const std::string& dbPath, NYql::TIssues& issues) {
    const auto searchPattern = std::vformat(pattern, std::make_format_args("\\S+"));
    if (re2::RE2::Replace(&query, searchPattern, std::vformat(pattern, std::make_format_args(dbPath)))) {
        return true;
    }

    issues.AddIssue(TStringBuilder() << "Pattern: \"" << pattern << "\" was not found: " << query.Quote());
    return false;
}

} // NYdb::NDump
