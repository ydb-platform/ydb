#include "util.h"

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

}

TString EscapeString(const TString& value,
                     const TString& enclosingSeq,
                     const TString& replaceWith) {
    auto escapedValue = value;
    SubstGlobal(escapedValue, enclosingSeq, replaceWith);
    return escapedValue;
}

TString EscapeString(const TString& value, char enclosingChar) {
    auto escapedValue = value;
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
    case FederatedQuery::ConnectionSetting::CONNECTION_NOT_SET:
        return FederatedQuery::IamAuth{};
    }
}

} // namespace NFq
