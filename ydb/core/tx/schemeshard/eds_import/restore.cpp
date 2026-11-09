#include "restore.h"
#include "limits.h"

#include <ydb/core/protos/schemeshard/operations.pb.h>

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

bool ReadSourceDatabase(TStringBuf query, TStringBuf& database) {
    query = query.SubStr(0, MetadataLimit);
    if (query.StartsWith("\xEF\xBB\xBF")) {
        query.Skip(3);
    }
    while (!query.empty()) {
        while (!query.empty() && IsAsciiSpace(query.front())) {
            query.Skip(1);
        }
        if (query.StartsWith("--")) {
            const auto newline = query.find('\n');
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
            const auto end = query.find("*/", 2);
            if (end == TStringBuf::npos || query.SubStr(2, end - 2).Contains("/*")) {
                return false;
            }
            query.Skip(end + 2);
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
    if (reference.size() > Limit || destinationDatabase.size() > Limit
        || !IsCanonicalPath(reference) || !IsCanonicalPath(destinationDatabase)
        || !ReadSourceDatabase(creationQuery, sourceDatabase)
        || !reference.StartsWith(sourceDatabase) || reference.size() <= sourceDatabase.size()
        || (sourceDatabase != "/" && reference[sourceDatabase.size()] != '/'))
    {
        return fail();
    }
    const auto relative = reference.SubStr(sourceDatabase.size() + (sourceDatabase == "/" ? 0 : 1));
    const size_t separator = destinationDatabase == "/" ? 0 : 1;
    if (destinationDatabase.size() + separator > Limit - relative.size()) {
        return fail();
    }
    TString mapped(destinationDatabase);
    if (separator) {
        mapped += '/';
    }
    mapped += relative;
    // The protobuf is the sole create payload. No SQL escaping or parser-mode
    // conversion is involved, and the caller already owns this result copy.
    scheme->MutableCreateExternalDataSource()->MutableAuth()->MutableIam()->SetInitialTokenSecretName(std::move(mapped));
}

} // namespace NKikimr::NSchemeShard
