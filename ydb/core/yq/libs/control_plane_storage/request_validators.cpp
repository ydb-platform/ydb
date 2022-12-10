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

NYql::TIssues ValidateDateFormatSetting(const google::protobuf::Map<TString, TString>& formatSetting, bool matchAllSettings) {
    NYql::TIssues issues;
    TSet<TString> conflictingKeys;
    for (const auto& [key, value]: formatSetting) {
        if (key == "data.datetime.format_name"sv) {
            if (!IsValidDateTimeFormatName(value)) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "unknown value for data.datetime.format_name " + value));
            }
            if (conflictingKeys.contains("data.datetime.format")) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "Don't use data.datetime.format_name and data.datetime.format together"));
            }
            conflictingKeys.insert("data.datetime.format_name");
            continue;
        }

        if (key == "data.datetime.format"sv) {
            if (conflictingKeys.contains("data.datetime.format_name")) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "Don't use data.datetime.format_name and data.datetime.format together"));
            }
            conflictingKeys.insert("data.datetime.format");
            continue;
        }

        if (key == "data.timestamp.format_name"sv) {
            if (!IsValidTimestampFormatName(value)) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "unknown value for data.timestamp.format_name " + value));
            }
            if (conflictingKeys.contains("data.timestamp.format")) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "Don't use data.timestamp.format_name and data.timestamp.format together"));
            }
            conflictingKeys.insert("data.timestamp.format_name");
            continue;
        }

        if (key == "data.timestamp.format"sv) {
            if (conflictingKeys.contains("data.timestamp.format_name")) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "Don't use data.timestamp.format_name and data.timestamp.format together"));
            }
            conflictingKeys.insert("data.timestamp.format");
            continue;
        }

        if (matchAllSettings) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "unknown format setting " + key));
        }
    }
    return issues;
}


NYql::TIssues ValidateFormatSetting(const TString& format, const google::protobuf::Map<TString, TString>& formatSetting) {
    NYql::TIssues issues;
    TSet<TString> conflictingKeys;
    issues.AddIssues(ValidateDateFormatSetting(formatSetting));
    for (const auto& [key, value]: formatSetting) {
        if (key == "file_pattern"sv) {
            continue;
        }

        if (key == "data.interval.unit"sv) {
            if (!IsValidIntervalUnit(value)) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "unknown value for data.interval.unit " + value));
            }
            continue;
        }

        if (IsIn({ "data.datetime.format_name"sv, "data.datetime.format"sv, "data.timestamp.format_name"sv, "data.timestamp.format"sv}, key)) {
            continue;
        }

        if (key == "csv_delimiter"sv) {
            if (format != "csv_with_names"sv) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "csv_delimiter should be used only with format csv_with_names"));
            }
            if (value.size() != 1) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "csv_delimiter should contain only one character"));
            }
            continue;
        }

        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "unknown format setting " + key));
    }
    return issues;
}

NYql::TIssues ValidateProjectionColumns(const YandexQuery::Schema& schema, const TVector<TString>& partitionedBy) {
    NYql::TIssues issues;
    TMap<TString, Ydb::Type> types;
    for (const auto& column: schema.column()) {
        types[column.name()] = column.type();
    }
    static const TSet<Ydb::Type::PrimitiveTypeId> availableProjectionTypes {
        Ydb::Type::STRING,
        Ydb::Type::UTF8,
        Ydb::Type::INT32,
        Ydb::Type::INT64,
        Ydb::Type::UINT32,
        Ydb::Type::UINT64,
        Ydb::Type::DATE
    };
    for (const auto& parititonedColumn: partitionedBy) {
        auto it = types.find(parititonedColumn);
        if (it == types.end()) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, TStringBuilder{} << "Column " << parititonedColumn << " from partitioned_by does not exist in the scheme. Please add such a column to your scheme"));
            continue;
        }
        const auto& type = it->second;
        const auto typeId = type.type_id();
        if (!availableProjectionTypes.contains(typeId)) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, TStringBuilder{} << "Column " << parititonedColumn << " from partitioned_by does not support " << Ydb::Type::PrimitiveTypeId_Name(typeId) << " type"));
        }
    }
    return issues;
}

} // namespace NYq
