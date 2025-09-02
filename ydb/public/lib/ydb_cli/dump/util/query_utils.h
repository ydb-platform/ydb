#pragma once

#include <util/generic/string.h>

#include <string>
#include <string_view>

namespace NYql {
    class TIssues;
}

namespace NSQLv1Generated {
    class TRule_sql_query;
}

namespace NYdb::NDump {

bool SqlToProtoAst(const TString& queryStr, NSQLv1Generated::TRule_sql_query& queryProto, NYql::TIssues& issues);
bool Format(const TString& query, TString& formattedQuery, NYql::TIssues& issues);

bool ValidateTableRefs(const NSQLv1Generated::TRule_sql_query& query, NYql::TIssues& issues);

TString RewriteAbsolutePath(TStringBuf path, TStringBuf backupRoot, TStringBuf restoreRoot);
bool RewriteTableRefs(TString& query, TStringBuf backupRoot, TStringBuf restoreRoot, NYql::TIssues& issues);
bool RewriteTableRefs(TString& query, TStringBuf restoreRoot, NYql::TIssues& issues);
bool RewriteObjectRefs(TString& query, TStringBuf restoreRoot, NYql::TIssues& issues);
bool RewriteCreateQuery(TString& query, std::string_view pattern, const std::string& dbPath, NYql::TIssues& issues);

TString GetBackupRoot(const TString& query);
TString GetDatabase(const TString& query);
TString GetSecretName(const TString& query);

} // NYdb::NDump
