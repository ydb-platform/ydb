#include "catalog.h"

#include "protocol.h"

#include <library/cpp/svnversion/svnversion.h>

#include <util/string/ascii.h>

namespace NKikimr::NSql {

namespace {

TString Normalize(TStringBuf sql) {
    while (sql && IsAsciiSpace(sql.front())) {
        sql.Skip(1);
    }
    while (sql && IsAsciiSpace(sql.back())) {
        sql.Chop(1);
    }
    if (sql.EndsWith(';')) {
        sql.Chop(1);
        while (sql && IsAsciiSpace(sql.back())) {
            sql.Chop(1);
        }
    }
    TString folded(sql);
    folded.to_lower();
    return folded;
}

TString TextColumn(TStringBuf name, TStringBuf value) {
    const TResultColumn column{TString(name), 25, -1};
    const TVector<TString> row{TString(value)};
    const TVector<TVector<TString>> rows{row};
    return MakeTextResult(TArrayRef<const TResultColumn>(&column, 1), rows);
}

} // namespace

TString VersionText() {
    const char* chosen = nullptr;
    if (const char* release = GetReleaseVersion(); release && *release) {
        chosen = release;
    } else if (const char* tag = GetTag(); tag && *tag) {
        chosen = tag;
    } else if (const char* branch = GetBranch(); branch && *branch) {
        chosen = branch;
    }
    TString version = chosen ? chosen : "unknown";
    for (const char* prefix : {"tags/releases/", "releases/", "tags/", "ydb/"}) {
        if (version.StartsWith(prefix)) {
            version = TString(version.substr(strlen(prefix)));
        }
    }
    return "YDB " + version;
}

bool TryAnswerCatalog(TStringBuf sql, TString& reply) {
    const TString query = Normalize(sql);
    // psql 16.9 and PostgreSQL 18 libpq send no SQL before the prompt.
    // These are the catalog reads named for the handshake stub.
    if (query == "select version()" || query == "select pg_catalog.version()") {
        reply = TextColumn("version", VersionText());
        return true;
    }
    if (query == "select oid, typname from pg_catalog.pg_type") {
        const TResultColumn columns[] = {
            {"oid", 26, 4},
            {"typname", 19, 64},
        };
        const TVector<TVector<TString>> rows = {
            {"16", "bool"},
            {"20", "int8"},
            {"21", "int2"},
            {"23", "int4"},
            {"25", "text"},
            {"1043", "varchar"},
        };
        reply = MakeTextResult(columns, rows);
        return true;
    }
    return false;
}

} // namespace NKikimr::NSql
