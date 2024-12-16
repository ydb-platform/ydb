#include "request_validators.h"

namespace NFq {

template <typename TConnection>
void ValidateGenericConnectionSetting(
    const TConnection& connection, 
    const TString& dataSourceKind,
    bool disableCurrentIam,
    bool passwordRequired,
    NYql::TIssues& issues
) {
    if (!connection.has_auth() || connection.auth().identity_case() == FederatedQuery::IamAuth::IDENTITY_NOT_SET) {
        auto msg = TStringBuilder() << "content.setting." << dataSourceKind << "_cluster.auth is not specified";
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, msg));
    }

    if (connection.auth().identity_case() == FederatedQuery::IamAuth::kCurrentIam && disableCurrentIam) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "current iam authorization is disabled"));
    }

    if (!connection.database_id()) {
        auto msg = TStringBuilder() << "content.setting." << dataSourceKind << "_cluster.database_id field is not specified";
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST,msg));
    }

    if (!connection.database_name()) {
        auto msg = TStringBuilder() << "content.setting." << dataSourceKind << "_cluster.database_name field is not specified";
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST,msg));
    }    

    if (!connection.login()) {
        auto msg = TStringBuilder() << "content.setting." << dataSourceKind << "_cluster.login is not specified";
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, msg));
    }

    if (!connection.password() && passwordRequired) {
        auto msg = TStringBuilder() << "content.setting." << dataSourceKind << "_cluster.password is not specified";
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, msg));
    }
}

NYql::TIssues ValidateConnectionSetting(
    const FederatedQuery::ConnectionSetting &setting,
    const TSet<FederatedQuery::ConnectionSetting::ConnectionCase> &availableConnections,
    bool disableCurrentIam,
    bool passwordRequired) {
    NYql::TIssues issues;
    if (!availableConnections.contains(setting.connection_case())) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "connection of the specified type is disabled"));
    }

    switch (setting.connection_case()) {
    case FederatedQuery::ConnectionSetting::kYdbDatabase: {
        const FederatedQuery::YdbDatabase database = setting.ydb_database();
        if (!database.has_auth() || database.auth().identity_case() == FederatedQuery::IamAuth::IDENTITY_NOT_SET) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.ydb_database.auth field is not specified"));
        }

        if (database.auth().identity_case() == FederatedQuery::IamAuth::kCurrentIam && disableCurrentIam) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "current iam authorization is disabled"));
        }

        if (!database.database_id() && !(database.endpoint() && database.database())) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.ydb_database.{database_id or database,endpoint} field is not specified"));
        }
        break;
    }
    case FederatedQuery::ConnectionSetting::kClickhouseCluster: {
        ValidateGenericConnectionSetting(setting.clickhouse_cluster(), "clickhouse", disableCurrentIam, passwordRequired, issues);
        break;
    }
    case FederatedQuery::ConnectionSetting::kPostgresqlCluster: {
        ValidateGenericConnectionSetting(setting.postgresql_cluster(), "postgresql", disableCurrentIam, passwordRequired, issues);
        break;
    }
    case FederatedQuery::ConnectionSetting::kGreenplumCluster: {
        ValidateGenericConnectionSetting(setting.greenplum_cluster(), "greenplum", disableCurrentIam, passwordRequired, issues);
        break;
    }
    case FederatedQuery::ConnectionSetting::kMysqlCluster: {
        ValidateGenericConnectionSetting(setting.mysql_cluster(), "mysql", disableCurrentIam, passwordRequired, issues);
        break;
    }
    case FederatedQuery::ConnectionSetting::kObjectStorage: {
        const FederatedQuery::ObjectStorageConnection objectStorage = setting.object_storage();
        if (!objectStorage.has_auth() || objectStorage.auth().identity_case() == FederatedQuery::IamAuth::IDENTITY_NOT_SET) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.object_storage.auth field is not specified"));
        }

        if (objectStorage.auth().identity_case() == FederatedQuery::IamAuth::kCurrentIam && disableCurrentIam) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "current iam authorization is disabled"));
        }

        if (!objectStorage.bucket()) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.object_storage.bucket field is not specified"));
        }
        break;
    }
    case FederatedQuery::ConnectionSetting::kDataStreams: {
        const FederatedQuery::DataStreams dataStreams = setting.data_streams();
        if (!dataStreams.has_auth() || dataStreams.auth().identity_case() == FederatedQuery::IamAuth::IDENTITY_NOT_SET) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.data_streams.auth field is not specified"));
        }

        if (dataStreams.auth().identity_case() == FederatedQuery::IamAuth::kCurrentIam && disableCurrentIam) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "current iam authorization is disabled"));
        }

        if (!dataStreams.database_id() && !(dataStreams.endpoint() && dataStreams.database())) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.data_streams.{database_id or database,endpoint} field is not specified"));
        }
        break;
    }
    case FederatedQuery::ConnectionSetting::kMonitoring: {
        const FederatedQuery::Monitoring monitoring = setting.monitoring();
        if (!monitoring.has_auth() || monitoring.auth().identity_case() == FederatedQuery::IamAuth::IDENTITY_NOT_SET) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.monitoring.auth field is not specified"));
        }

        if (monitoring.auth().identity_case() == FederatedQuery::IamAuth::kCurrentIam && disableCurrentIam) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "current iam authorization is disabled"));
        }

        if (!monitoring.project()) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.monitoring.project field is not specified"));
        }

        if (!monitoring.cluster()) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.monitoring.cluster field is not specified"));
        }
        break;
    }
    case FederatedQuery::ConnectionSetting::kLogging: {
        const FederatedQuery::Logging logging = setting.logging();
        if (!logging.has_auth() || logging.auth().identity_case() == FederatedQuery::IamAuth::IDENTITY_NOT_SET) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.logging.auth field is not specified"));
        }

        if (logging.auth().identity_case() == FederatedQuery::IamAuth::kCurrentIam && disableCurrentIam) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "current iam authorization is disabled"));
        }

        if (!logging.folder_id()) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.logging.folder_id field is not specified"));
        }

        break;
    }
    case FederatedQuery::ConnectionSetting::CONNECTION_NOT_SET: {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "connection is not set"));
        break;
    }
    // Do not add default. Adding a new connection should cause a compilation error
    }
    return issues;
}

NYql::TIssues ValidateEntityName(const TString& name) {
    NYql::TIssues issues;

    if (!name) {
        issues.AddIssue(
            MakeErrorIssue(TIssuesIds::BAD_REQUEST, "name field is not specified"));
    }

    if (name.size() > 255) {
        issues.AddIssue(
            MakeErrorIssue(TIssuesIds::BAD_REQUEST,
                           TStringBuilder{}
                               << "Incorrect connection name: " << name
                               << ". Name length must not exceed 255 symbols. Current length is "
                               << name.size() << " symbol(s)"));
    }

    if (name != to_lower(name)) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST,
                                       TStringBuilder{}
                                           << "Incorrect binding name: " << name
                                           << ". Please use only lower case"));
    }

    if (AllOf(name, [](auto& ch) { return ch == '.'; })) {
        issues.AddIssue(
            MakeErrorIssue(TIssuesIds::BAD_REQUEST,
                           TStringBuilder{}
                               << "Incorrect connection name: " << name
                               << ". Name is not allowed path part contains only dots"));
    }

    static const std::regex allowListRegexp(
        "(?:[a-z0-9]|!|\\\\|#|\\$|%|&|\\(|\\)|\\*|\\+|,|-|\\.|:|;|<|=|>|\\?|@|\\[|\\]|\\^|_|\\{|\\||\\}|~)+");
    if (!std::regex_match(name.c_str(), allowListRegexp)) {
        issues.AddIssue(MakeErrorIssue(
            TIssuesIds::BAD_REQUEST,
            TStringBuilder{}
                << "Incorrect connection name: " << name
                << ". Please make sure that name consists of following symbols: ['a'-'z'], ['0'-'9'], '!', '\\', '#', '$', '%'. '&', '(', ')', '*', '+', ',', '-', '.', ':', ';', '<', '=', '>', '?', '@', '[', ']', '^', '_', '{', '|', '}', '~'"));
    }

    return issues;
}

} // namespace NFq
