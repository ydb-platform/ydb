#include "request_validators.h"

namespace NFq {

namespace {

NYql::TIssues ValidateProjectionType(const NYdb::TType& columnType, const TString& columnName, const std::vector<NYdb::TType>& availableTypes) {
    return FindIf(availableTypes, [&columnType](const auto& availableType) { return NYdb::TypesEqual(availableType, columnType); }) == availableTypes.end()
        ? NYql::TIssues{MakeErrorIssue(TIssuesIds::BAD_REQUEST, TStringBuilder{} << "Column \"" << columnName << "\" from projection does not support " << columnType.ToString() << " type")}
        : NYql::TIssues{};
}

NYql::TIssues ValidateIntegerProjectionType(const NYdb::TType& columnType, const TString& columnName) {
    static const std::vector<NYdb::TType> availableTypes {
        NYdb::TTypeBuilder{}
            .Primitive(NYdb::EPrimitiveType::String)
            .Build(),
        NYdb::TTypeBuilder{}
            .Primitive(NYdb::EPrimitiveType::Int32)
            .Build(),
        NYdb::TTypeBuilder{}
            .Primitive(NYdb::EPrimitiveType::Uint32)
            .Build(),
        NYdb::TTypeBuilder{}
            .Primitive(NYdb::EPrimitiveType::Int64)
            .Build(),
        NYdb::TTypeBuilder{}
            .Primitive(NYdb::EPrimitiveType::Uint64)
            .Build(),
        NYdb::TTypeBuilder{}
            .Primitive(NYdb::EPrimitiveType::Utf8)
            .Build()
    };
    return ValidateProjectionType(columnType, columnName, availableTypes);
}

NYql::TIssues ValidateEnumProjectionType(const NYdb::TType& columnType, const TString& columnName) {
    static const std::vector<NYdb::TType> availableTypes {
        NYdb::TTypeBuilder{}
            .Primitive(NYdb::EPrimitiveType::String)
            .Build()
    };
    return ValidateProjectionType(columnType, columnName, availableTypes);
}

NYql::TIssues ValidateCommonProjectionType(const NYdb::TType& columnType, const TString& columnName) {
    static const std::vector<NYdb::TType> availableTypes {
        NYdb::TTypeBuilder{}
            .Primitive(NYdb::EPrimitiveType::String)
            .Build(),
        NYdb::TTypeBuilder{}
            .Primitive(NYdb::EPrimitiveType::Int64)
            .Build(),
        NYdb::TTypeBuilder{}
            .Primitive(NYdb::EPrimitiveType::Utf8)
            .Build(),
        NYdb::TTypeBuilder{}
            .Primitive(NYdb::EPrimitiveType::Int32)
            .Build(),
        NYdb::TTypeBuilder{}
            .Primitive(NYdb::EPrimitiveType::Uint32)
            .Build(),
        NYdb::TTypeBuilder{}
            .Primitive(NYdb::EPrimitiveType::Uint64)
            .Build(),
        NYdb::TTypeBuilder{}
            .Primitive(NYdb::EPrimitiveType::Date)
            .Build(),
        NYdb::TTypeBuilder{}
            .Primitive(NYdb::EPrimitiveType::Datetime)
            .Build()
    };
    return ValidateProjectionType(columnType, columnName, availableTypes);
}

NYql::TIssues ValidateDateProjectionType(const NYdb::TType& columnType, const TString& columnName) {
    static const std::vector<NYdb::TType> availableTypes {
        NYdb::TTypeBuilder{}
            .Primitive(NYdb::EPrimitiveType::String)
            .Build(),
        NYdb::TTypeBuilder{}
            .Primitive(NYdb::EPrimitiveType::Utf8)
            .Build(),
        NYdb::TTypeBuilder{}
            .Primitive(NYdb::EPrimitiveType::Uint32)
            .Build(),
        NYdb::TTypeBuilder{}
            .Primitive(NYdb::EPrimitiveType::Date)
            .Build(),
        NYdb::TTypeBuilder{}
            .Primitive(NYdb::EPrimitiveType::Datetime)
            .Build()
    };
    return ValidateProjectionType(columnType, columnName, availableTypes);
}

TMap<TString, NYql::NUdf::EDataSlot> GetDataSlotColumns(const FederatedQuery::Schema& schema) {
    TMap<TString, NYql::NUdf::EDataSlot> dataSlotColumns;
    for (const auto& column: schema.column()) {
        if (column.has_type()) {
            const auto& type = column.type();
            if (type.has_type_id()) {
                dataSlotColumns[column.name()] = NYql::NUdf::GetDataSlot(type.type_id());
            }
        }
    }
    return dataSlotColumns;
}

}

NYql::TIssues ValidateConnectionSetting(const FederatedQuery::ConnectionSetting& setting, const TSet<FederatedQuery::ConnectionSetting::ConnectionCase>& availableConnections, bool disableCurrentIam,  bool clickHousePasswordRequire) {
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
        const FederatedQuery::ClickHouseCluster ch = setting.clickhouse_cluster();
        if (!ch.has_auth() || ch.auth().identity_case() == FederatedQuery::IamAuth::IDENTITY_NOT_SET) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.clickhouse_cluster.auth field is not specified"));
        }

        if (ch.auth().identity_case() == FederatedQuery::IamAuth::kCurrentIam && disableCurrentIam) {
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
    case FederatedQuery::ConnectionSetting::CONNECTION_NOT_SET: {
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

NYql::TIssues ValidateProjectionColumns(const FederatedQuery::Schema& schema, const TVector<TString>& partitionedBy) {
    NYql::TIssues issues;
    TMap<TString, Ydb::Type> types;
    for (const auto& column: schema.column()) {
        types[column.name()] = column.type();
    }
    for (const auto& parititonedColumn: partitionedBy) {
        auto it = types.find(parititonedColumn);
        if (it == types.end()) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, TStringBuilder{} << "Column " << parititonedColumn << " from partitioned_by does not exist in the scheme. Please add such a column to your scheme"));
            continue;
        }
        NYdb::TType columnType{it->second};
        issues.AddIssues(ValidateCommonProjectionType(columnType, parititonedColumn));
    }
    return issues;
}

NYql::TIssues ValidateProjection(const FederatedQuery::Schema& schema, const TString& projection, const TVector<TString>& partitionedBy, size_t pathsLimit) {
    auto generator = NYql::NPathGenerator::CreatePathGenerator(
        projection,
        partitionedBy,
        GetDataSlotColumns(schema),
        pathsLimit); // an exception is thrown if an error occurs
    TMap<TString, NYql::NPathGenerator::IPathGenerator::EType> projectionColumns;
    for (const auto& column: generator->GetConfig().Rules) {
        projectionColumns[column.Name] = column.Type;
    }
    NYql::TIssues issues;
    for (const auto& column: schema.column()) {
        auto it = projectionColumns.find(column.name());
        if (it != projectionColumns.end()) {
            switch (it->second) {
                case NYql::NPathGenerator::IPathGenerator::EType::INTEGER:
                    issues.AddIssues(ValidateIntegerProjectionType(NYdb::TType{column.type()}, column.name()));
                    break;
                case NYql::NPathGenerator::IPathGenerator::EType::ENUM:
                    issues.AddIssues(ValidateEnumProjectionType(NYdb::TType{column.type()}, column.name()));
                    break;
                case NYql::NPathGenerator::IPathGenerator::EType::DATE:
                    issues.AddIssues(ValidateDateProjectionType(NYdb::TType{column.type()}, column.name()));
                    break;
                case NYql::NPathGenerator::IPathGenerator::EType::UNDEFINED:
                    issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, TStringBuilder{} << "Column \"" << column.name() << "\" from projection has undefined generator type"));
                    break;
            }
        }
    }
    return issues;
}

} // namespace NFq
