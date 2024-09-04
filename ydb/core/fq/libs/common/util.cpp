#include "util.h"

#include <regex>
#include <re2/re2.h>

#include <util/generic/string.h>
#include <util/string/builder.h>
#include <util/string/subst.h>

namespace NFq {

namespace {

TString GetServiceAccountId(const FederatedQuery::IamAuth& auth) {
    return auth.has_service_account()
            ? auth.service_account().id()
            : TString{};
}

EYdbComputeAuth GetIamAuthMethod(const FederatedQuery::IamAuth& auth) {
    switch (auth.identity_case()) {
        case FederatedQuery::IamAuth::kNone:
            return EYdbComputeAuth::NONE;
        case FederatedQuery::IamAuth::kServiceAccount:
            return EYdbComputeAuth::SERVICE_ACCOUNT;
        case FederatedQuery::IamAuth::kCurrentIam:
        // Do not replace with default. Adding a new auth item should cause a compilation error
        case FederatedQuery::IamAuth::IDENTITY_NOT_SET:
            return EYdbComputeAuth::UNKNOWN;
    }
}

EYdbComputeAuth GetBasicAuthMethod(const FederatedQuery::IamAuth& auth) {
    switch (auth.identity_case()) {
        case FederatedQuery::IamAuth::kNone:
            return EYdbComputeAuth::BASIC;
        case FederatedQuery::IamAuth::kServiceAccount:
            return EYdbComputeAuth::MDB_BASIC;
        case FederatedQuery::IamAuth::kCurrentIam:
        // Do not replace with default. Adding a new auth item should cause a compilation error
        case FederatedQuery::IamAuth::IDENTITY_NOT_SET:
            return EYdbComputeAuth::UNKNOWN;
    }
}

class TIssueDatabaseRemover {
public:
    explicit TIssueDatabaseRemover(const TString& databasePath) 
        : DatabasePath(databasePath) {}

    TIntrusivePtr<NYql::TIssue> Run(const NYql::TIssue& issue) {
        auto msg = RemoveDatabaseFromStr(issue.GetMessage(), DatabasePath);
        auto newIssue = MakeIntrusive<NYql::TIssue>(issue.Position, issue.EndPosition, msg);
        newIssue->SetCode(issue.GetCode(), issue.GetSeverity());
        for (auto issue : issue.GetSubIssues()) {
            newIssue->AddSubIssue(Run(*issue));
        }
        return newIssue;
    }

private:
    TString DatabasePath; 
};

void EscapeBackslashes(TString& value) {
    SubstGlobal(value, "\\", "\\\\");
}

}

TString EscapeString(const TString& value,
                     const TString& enclosingSeq,
                     const TString& replaceWith) {
    auto escapedValue = value;
    EscapeBackslashes(escapedValue);
    SubstGlobal(escapedValue, enclosingSeq, replaceWith);
    return escapedValue;
}

TString EscapeString(const TString& value, char enclosingChar) {
    auto escapedValue = value;
    EscapeBackslashes(escapedValue);
    SubstGlobal(escapedValue,
                TString{enclosingChar},
                TStringBuilder{} << '\\' << enclosingChar);
    return escapedValue;
}

TString EncloseAndEscapeString(const TString& value, char enclosingChar) {
    return TStringBuilder{} << enclosingChar << EscapeString(value, enclosingChar)
                            << enclosingChar;
}

TString EncloseAndEscapeString(const TString& value,
                               const TString& enclosingSeq,
                               const TString& replaceWith) {
    return TStringBuilder{} << enclosingSeq
                            << EscapeString(value, enclosingSeq, replaceWith)
                            << enclosingSeq;
}

static TString SECRET_BEGIN = "/* 51a91b5d91a99eb7 */";
static TString SECRET_END   = "/* e87c9b191b202354 */";
static auto SECRET_REGEXP   = std::regex{
    "\\/\\* 51a91b5d91a99eb7 \\*\\/.*\\/\\* e87c9b191b202354 \\*\\/"};

TString EncloseSecret(const TString& value) {
    return TStringBuilder{} << SECRET_BEGIN << value << SECRET_END;
}

TString HideSecrets(const TString& value) {
    return std::regex_replace(std::string{value}, SECRET_REGEXP, "/*SECRET*/");
}

TString ExtractServiceAccountId(const FederatedQuery::ConnectionSetting& setting) {
    switch (setting.connection_case()) {
    case FederatedQuery::ConnectionSetting::kYdbDatabase: {
        return GetServiceAccountId(setting.ydb_database().auth());
    }
    case FederatedQuery::ConnectionSetting::kDataStreams: {
        return GetServiceAccountId(setting.data_streams().auth());
    }
    case FederatedQuery::ConnectionSetting::kObjectStorage: {
        return GetServiceAccountId(setting.object_storage().auth());
    }
    case FederatedQuery::ConnectionSetting::kMonitoring: {
        return GetServiceAccountId(setting.monitoring().auth());
    }
    case FederatedQuery::ConnectionSetting::kClickhouseCluster: {
        return GetServiceAccountId(setting.clickhouse_cluster().auth());
    }
    case FederatedQuery::ConnectionSetting::kPostgresqlCluster: {
        return GetServiceAccountId(setting.postgresql_cluster().auth());
    }
    case FederatedQuery::ConnectionSetting::kGreenplumCluster: {
        return GetServiceAccountId(setting.greenplum_cluster().auth());
    }
    case FederatedQuery::ConnectionSetting::kMysqlCluster: {
        return GetServiceAccountId(setting.mysql_cluster().auth());
    }
    // Do not replace with default. Adding a new connection should cause a compilation error
    case FederatedQuery::ConnectionSetting::CONNECTION_NOT_SET:
    break;
    }
    return {};
}

TString ExtractServiceAccountId(const FederatedQuery::ConnectionContent& content) {
    return ExtractServiceAccountId(content.setting());
}

TString ExtractServiceAccountId(const FederatedQuery::Connection& connection) {
    return ExtractServiceAccountId(connection.content());
}

TMaybe<TString> GetLogin(const FederatedQuery::ConnectionSetting& setting) {
    switch (setting.connection_case()) {
        case FederatedQuery::ConnectionSetting::CONNECTION_NOT_SET:
            return {};
        case FederatedQuery::ConnectionSetting::kYdbDatabase:
            return {};
        case FederatedQuery::ConnectionSetting::kClickhouseCluster:
            return setting.clickhouse_cluster().login();
        case FederatedQuery::ConnectionSetting::kDataStreams:
            return {};
        case FederatedQuery::ConnectionSetting::kObjectStorage:
            return {};
        case FederatedQuery::ConnectionSetting::kMonitoring:
            return {};
        case FederatedQuery::ConnectionSetting::kPostgresqlCluster:
            return setting.postgresql_cluster().login();
        case FederatedQuery::ConnectionSetting::kGreenplumCluster:
            return setting.greenplum_cluster().login();
        case FederatedQuery::ConnectionSetting::kMysqlCluster:
            return setting.mysql_cluster().login();
    }
}

TMaybe<TString> GetPassword(const FederatedQuery::ConnectionSetting& setting) {
    switch (setting.connection_case()) {
        case FederatedQuery::ConnectionSetting::CONNECTION_NOT_SET:
            return {};
        case FederatedQuery::ConnectionSetting::kYdbDatabase:
            return {};
        case FederatedQuery::ConnectionSetting::kClickhouseCluster:
            return setting.clickhouse_cluster().password();
        case FederatedQuery::ConnectionSetting::kDataStreams:
            return {};
        case FederatedQuery::ConnectionSetting::kObjectStorage:
            return {};
        case FederatedQuery::ConnectionSetting::kMonitoring:
            return {};
        case FederatedQuery::ConnectionSetting::kPostgresqlCluster:
            return setting.postgresql_cluster().password();
        case FederatedQuery::ConnectionSetting::kGreenplumCluster:
            return setting.greenplum_cluster().password();
        case FederatedQuery::ConnectionSetting::kMysqlCluster:
            return setting.mysql_cluster().password();
    }
}

EYdbComputeAuth GetYdbComputeAuthMethod(const FederatedQuery::ConnectionSetting& setting) {
    switch (setting.connection_case()) {
        case FederatedQuery::ConnectionSetting::CONNECTION_NOT_SET:
            return EYdbComputeAuth::UNKNOWN;
        case FederatedQuery::ConnectionSetting::kYdbDatabase:
            return GetIamAuthMethod(setting.ydb_database().auth());
        case FederatedQuery::ConnectionSetting::kClickhouseCluster:
            return GetBasicAuthMethod(setting.clickhouse_cluster().auth());
        case FederatedQuery::ConnectionSetting::kDataStreams:
            return GetIamAuthMethod(setting.data_streams().auth());
        case FederatedQuery::ConnectionSetting::kObjectStorage:
            return GetIamAuthMethod(setting.object_storage().auth());
        case FederatedQuery::ConnectionSetting::kMonitoring:
            return GetIamAuthMethod(setting.monitoring().auth());
        case FederatedQuery::ConnectionSetting::kPostgresqlCluster:
            return GetBasicAuthMethod(setting.postgresql_cluster().auth());
        case FederatedQuery::ConnectionSetting::kGreenplumCluster:
            return GetBasicAuthMethod(setting.greenplum_cluster().auth());
        case FederatedQuery::ConnectionSetting::kMysqlCluster:
            return GetBasicAuthMethod(setting.mysql_cluster().auth());
    }
}

FederatedQuery::IamAuth GetAuth(const FederatedQuery::Connection& connection) {
    switch (connection.content().setting().connection_case()) {
    case FederatedQuery::ConnectionSetting::kObjectStorage:
        return connection.content().setting().object_storage().auth();
    case FederatedQuery::ConnectionSetting::kYdbDatabase:
        return connection.content().setting().ydb_database().auth();
    case FederatedQuery::ConnectionSetting::kClickhouseCluster:
        return connection.content().setting().clickhouse_cluster().auth();
    case FederatedQuery::ConnectionSetting::kDataStreams:
        return connection.content().setting().data_streams().auth();
    case FederatedQuery::ConnectionSetting::kMonitoring:
        return connection.content().setting().monitoring().auth();
    case FederatedQuery::ConnectionSetting::kPostgresqlCluster:
        return connection.content().setting().postgresql_cluster().auth();
    case FederatedQuery::ConnectionSetting::kGreenplumCluster:
        return connection.content().setting().greenplum_cluster().auth();
    case FederatedQuery::ConnectionSetting::kMysqlCluster:
        return connection.content().setting().mysql_cluster().auth();
    case FederatedQuery::ConnectionSetting::CONNECTION_NOT_SET:
        return FederatedQuery::IamAuth{};
    }
}

TString RemoveDatabaseFromStr(TString str, const TString& databasePath) {
    TString escapedPath = RE2::QuoteMeta(databasePath);
    RE2::GlobalReplace(&str,
                       TStringBuilder {} << R"(db.\[)" << escapedPath << R"(\/([^ '"]+)\]|)" << escapedPath << R"(\/([^ '"]+))",
                       R"(\1\2)");
    return str;
}

NYql::TIssues RemoveDatabaseFromIssues(const NYql::TIssues& issues, const TString& databasePath) {
    TIssueDatabaseRemover remover(databasePath);
    TVector<NYql::TIssue> newIssues; 
    for (const auto& issue : issues) {
        newIssues.emplace_back(*remover.Run(issue));
    }
    return NYql::TIssues(newIssues);
}

} // namespace NFq
