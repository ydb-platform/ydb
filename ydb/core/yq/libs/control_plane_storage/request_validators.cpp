#include "request_validators.h"

namespace NYq {

NYql::TIssues ValidateConnectionSetting(const YandexQuery::ConnectionSetting& setting, const TSet<YandexQuery::ConnectionSetting::ConnectionCase>& availableConnections, bool disableCurrentIam,  bool clickHousePasswordRequire) {
    NYql::TIssues issues;
    if (!availableConnections.contains(setting.connection_case())) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "connection of the specified type is disabled"));
    }

    switch (setting.connection_case()) {
    case YandexQuery::ConnectionSetting::kYdbDatabase: {
        const YandexQuery::YdbDatabase database = setting.ydb_database();
        if (!database.has_auth() || database.auth().identity_case() == YandexQuery::IamAuth::IDENTITY_NOT_SET) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.ydb_database.auth field is not specified"));
        }

        if (database.auth().identity_case() == YandexQuery::IamAuth::kCurrentIam && disableCurrentIam) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "current iam authorization is disabled"));
        }

        if (!database.database_id() && !(database.endpoint() && database.database())) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.ydb_database.{database_id or database,endpoint} field is not specified"));
        }
        break;
    }
    case YandexQuery::ConnectionSetting::kClickhouseCluster: {
        const YandexQuery::ClickHouseCluster ch = setting.clickhouse_cluster();
        if (!ch.has_auth() || ch.auth().identity_case() == YandexQuery::IamAuth::IDENTITY_NOT_SET) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.clickhouse_cluster.auth field is not specified"));
        }

        if (ch.auth().identity_case() == YandexQuery::IamAuth::kCurrentIam && disableCurrentIam) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "current iam authorization is disabled"));
        }

        if (!ch.database_id() && !(ch.host() && ch.port())) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.clickhouse_cluster.{database_id or host,port} field is not specified"));
        }

        if (!ch.login()) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.clickhouse_cluster.login field is not specified"));
        }

        if (!ch.password() && clickHousePasswordRequire) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.clickhouse_cluster.password field is not specified"));
        }
        break;
    }
    case YandexQuery::ConnectionSetting::kObjectStorage: {
        const YandexQuery::ObjectStorageConnection objectStorage = setting.object_storage();
        if (!objectStorage.has_auth() || objectStorage.auth().identity_case() == YandexQuery::IamAuth::IDENTITY_NOT_SET) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.object_storage.auth field is not specified"));
        }

        if (objectStorage.auth().identity_case() == YandexQuery::IamAuth::kCurrentIam && disableCurrentIam) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "current iam authorization is disabled"));
        }

        if (!objectStorage.bucket()) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.object_storage.bucket field is not specified"));
        }
        break;
    }
    case YandexQuery::ConnectionSetting::kDataStreams: {
        const YandexQuery::DataStreams dataStreams = setting.data_streams();
        if (!dataStreams.has_auth() || dataStreams.auth().identity_case() == YandexQuery::IamAuth::IDENTITY_NOT_SET) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.data_streams.auth field is not specified"));
        }

        if (dataStreams.auth().identity_case() == YandexQuery::IamAuth::kCurrentIam && disableCurrentIam) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "current iam authorization is disabled"));
        }

        if (!dataStreams.database_id() && !(dataStreams.endpoint() && dataStreams.database())) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.data_streams.{database_id or database,endpoint} field is not specified"));
        }
        break;
    }
    case YandexQuery::ConnectionSetting::kMonitoring: {
        const YandexQuery::Monitoring monitoring = setting.monitoring();
        if (!monitoring.has_auth() || monitoring.auth().identity_case() == YandexQuery::IamAuth::IDENTITY_NOT_SET) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.monitoring.auth field is not specified"));
        }

        if (monitoring.auth().identity_case() == YandexQuery::IamAuth::kCurrentIam && disableCurrentIam) {
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
    case YandexQuery::ConnectionSetting::CONNECTION_NOT_SET: {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "connection is not set"));
        break;
    }
    // Do not add default. Adding a new connection should cause a compilation error
    }
    return issues;
}

NYql::TIssues ValidateFormatSetting(const TString& format, const google::protobuf::Map<TString, TString>& formatSetting) {
     NYql::TIssues issues;
    for (const auto& [key, value]: formatSetting) {
        if (key == "data.interval.unit") {
            if (!IsValidIntervalUnit(value)) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "unknown value for data.interval.unit " + value));
            }
        } else if (key == "csv_delimiter") {
            if (format != "csv_with_names") {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "csv_delimiter should be used only with format csv_with_names"));
            }
            if (value.size() != 1) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "csv_delimiter should contain only one character"));
            }
        } else {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "unknown format setting " + key));
        }
    }
    return issues;
}

} // namespace NYq
