#include "restore.h"
#include "limits.h"

#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <yql/essentials/sql/settings/translation_settings.h>

#include <util/string/ascii.h>
#include <util/string/strip.h>

namespace NKikimr::NSchemeShard {
namespace {

// A schema reference must be smaller than the producer's auth protobuf limit.
// Its database prefix is smaller still. Allow the canonical header, BOM and CRLF
// in addition to that limit. This bounds metadata inspection, not the SQL body.
constexpr size_t MetadataLimit = NExternalDataSource::MAX_PROTOBUF_SIZE
    + TStringBuf("-- database: \"\"\r\n").size() + 3;

bool IsCanonicalPath(TStringBuf path) {
    if (!path.StartsWith('/') || path.Contains('\0')) {
        return false;
    }
    if (path == "/") {
        return true;
    }
    path.Skip(1);
    while (!path.empty()) {
        const auto slash = path.find('/');
        const auto component = path.SubStr(0, slash);
        if (component.empty() || component == "." || component == "..") {
            return false;
        }
        if (slash == TStringBuf::npos) {
            return true;
        }
        path.Skip(slash + 1);
    }
    return false; // A trailing slash is not canonical.
}

// Scheme-secret navigation uses SplitPath, which ignores empty components.
// Keep the literal spelling: the secret-service cache lookup still uses the
// original requested string, so normalization here would change its behavior.
TStringBuf NextComponent(TStringBuf& path) {
    while (path.StartsWith('/')) {
        path.Skip(1);
    }
    const auto component = path.SubStr(0, path.find('/'));
    path.Skip(component.size());
    return component;
}

bool IsSafeSchemaPath(TStringBuf path) {
    if (!path.StartsWith('/') || path.Contains('\0')) {
        return false;
    }
    while (const auto component = NextComponent(path)) {
        if (component == "." || component == "..") {
            return false;
        }
    }
    return true;
}

bool GetDatabaseSuffix(TStringBuf reference, TStringBuf database, TStringBuf& suffix) {
    while (const auto component = NextComponent(database)) {
        if (NextComponent(reference) != component) {
            return false;
        }
    }
    suffix = reference; // Preserve all separators after the database component.
    return !NextComponent(reference).empty(); // A secret must be a strict descendant.
}

bool ReadSourceDatabase(TStringBuf query, TStringBuf& database) {
    query = query.SubStr(0, MetadataLimit);
    // Use the same directive parser as KQP, on a bounded complete-line prefix.
    // In particular, an original BOM or ordinary comment ends the flag preamble.
    // A complete line also avoids cutting a UTF-8 character at the size bound.
    const auto lastLineEnd = query.find_last_of("\r\n");
    if (lastLineEnd == TStringBuf::npos) {
        return false;
    }
    NSQLTranslation::TParsedSettings settings;
    NYql::TIssues issues;
    if (!NSQLTranslation::ParseTranslationSettingsFromComments(
            TString(query.SubStr(0, lastLineEnd + 1)), settings, issues))
    {
        return false;
    }
    if (query.StartsWith("\xEF\xBB\xBF")) {
        query.Skip(3);
    }
    while (!query.empty()) {
        while (!query.empty() && IsAsciiSpace(query.front())) {
            query.Skip(1);
        }
        if (query.StartsWith("--")) {
            const auto newline = query.find_first_of("\r\n");
            if (newline == TStringBuf::npos) {
                return false;
            }
            const auto line = StripString(query.SubStr(0, newline));
            if (line.StartsWith("-- database:")) {
                constexpr TStringBuf Prefix = "-- database: \"";
                if (!database.empty() || !line.StartsWith(Prefix) || !line.EndsWith('"')) {
                    return false;
                }
                database = line.SubStr(Prefix.size(), line.size() - Prefix.size() - 1);
                if (!IsCanonicalPath(database)) {
                    return false;
                }
            }
            query.Skip(newline + 1);
        } else if (query.StartsWith("/*")) {
            size_t depth = 1;
            query.Skip(2);
            while (depth && !query.empty()) {
                if (query.StartsWith("/*") && settings.HasAnsiLexer) {
                    ++depth;
                    query.Skip(2);
                } else if (query.StartsWith("*/")) {
                    --depth;
                    query.Skip(2);
                } else {
                    query.Skip(1);
                }
            }
            if (depth) {
                return false;
            }
        } else {
            // KQP has already parsed the statement. Do not tokenize it again,
            // and never treat comments or literals inside it as backup metadata.
            return !query.empty() && !database.empty();
        }
    }
    return false;
}

} // anonymous

void RebindImportedEdsInitialToken(Ydb::StatusIds::StatusCode& status,
    TImportedSchemeResult& result, TStringBuf creationQuery, TStringBuf destinationDatabase)
{
    if (status != Ydb::StatusIds::SUCCESS) {
        return;
    }
    auto* scheme = std::get_if<NKikimrSchemeOp::TModifyScheme>(&result);
    if (!scheme || scheme->GetOperationType() != NKikimrSchemeOp::ESchemeOpCreateExternalDataSource
        || !scheme->HasCreateExternalDataSource())
    {
        return;
    }
    const auto& auth = scheme->GetCreateExternalDataSource().GetAuth();
    if (auth.identity_case() != NKikimrSchemeOp::TAuth::kIam) {
        return;
    }
    const TStringBuf reference = auth.GetIam().GetInitialTokenSecretName();
    if (!reference.StartsWith('/')) {
        return; // Named references keep the server's existing resolution rules.
    }
    const auto fail = [&] {
        status = Ydb::StatusIds::BAD_REQUEST;
        result = TString("Cannot restore IAM secret reference: invalid database metadata or schema path");
    };
    constexpr size_t Limit = NExternalDataSource::MAX_PROTOBUF_SIZE;
    TStringBuf sourceDatabase;
    TStringBuf suffix;
    if (reference.size() > Limit || destinationDatabase.size() > Limit
        || !IsSafeSchemaPath(reference) || !IsCanonicalPath(destinationDatabase)
        || !ReadSourceDatabase(creationQuery, sourceDatabase)
        || !GetDatabaseSuffix(reference, sourceDatabase, suffix))
    {
        return fail();
    }
    if (sourceDatabase == destinationDatabase) {
        return; // Preserve even a legacy noncanonical literal byte-for-byte.
    }
    const TStringBuf destinationPrefix = destinationDatabase == "/" ? TStringBuf() : destinationDatabase;
    if (destinationPrefix.size() > Limit - suffix.size()) {
        return fail();
    }
    TString mapped(destinationPrefix);
    mapped += suffix;
    // The protobuf is the sole create payload. No SQL escaping or parser-mode
    // conversion is involved, and the caller already owns this result copy.
    scheme->MutableCreateExternalDataSource()->MutableAuth()->MutableIam()->SetInitialTokenSecretName(std::move(mapped));
}

} // namespace NKikimr::NSchemeShard
