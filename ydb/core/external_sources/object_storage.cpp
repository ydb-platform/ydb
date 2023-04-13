#include "external_source.h"

#include <ydb/core/protos/external_sources.pb.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/s3/path_generator/yql_s3_path_generator.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>

#include <library/cpp/scheme/scheme.h>

#include <util/string/builder.h>

#include <array>

namespace NKikimr::NExternalSource {

namespace {

struct TObjectStorageExternalSource : public IExternalSource {
    virtual TString Pack(const NKikimrExternalSources::TSchema& schema,
                         const NKikimrExternalSources::TGeneral& general) const override {
        NKikimrExternalSources::TObjectStorage objectStorage;
        for (const auto& [key, value]: general.attributes()) {
            if (key == "format") {
                objectStorage.set_format(value);
            } else if (key == "compression") {
                objectStorage.set_compression(value);
            } else if (key.StartsWith("projection.") || key == "storage.location.template") {
                objectStorage.mutable_projection()->insert({key, value});
            } else if (key == "partitioned_by") {
                auto json = NSc::TValue::FromJsonThrow(value);
                for (const auto& column: json.GetArray()) {
                    *objectStorage.add_partitioned_by() = column;
                }
            } else if (IsIn({"file_pattern"sv, "data.interval.unit"sv, "data.datetime.format_name"sv, "data.datetime.format"sv, "data.timestamp.format_name"sv, "data.timestamp.format"sv, "csv_delimiter"sv}, key)) {
                objectStorage.mutable_format_setting()->insert({key, value});
            } else {
                ythrow TExternalSourceException() << "Unknown attribute " << key;
            }
        }

        if (auto issues = Validate(schema, objectStorage)) {
            ythrow TExternalSourceException() << issues.ToString() << Endl;
        }

        return objectStorage.SerializeAsString();
    }

    virtual TString GetName() const override {
        return TString{NYql::S3ProviderName};
    }

    virtual TMap<TString, TString> GetParamters(const TString& content) const override {
        NKikimrExternalSources::TObjectStorage objectStorage;
        objectStorage.ParseFromStringOrThrow(content);

        TMap<TString, TString> parameters{objectStorage.format_setting().begin(), objectStorage.format_setting().end()};
        if (objectStorage.format()) {
            parameters["format"] = objectStorage.format();
        }

        if (objectStorage.compression()) {
            parameters["compression"] = objectStorage.compression();
        }

        NSc::TValue projection;
        for (const auto& [key, value]: objectStorage.projection()) {
            projection[key] = value;
        }

        if (!projection.DictEmpty()) {
            parameters["projection"] = projection.ToJson();
        }

        NSc::TValue partitionedBy;
        partitionedBy.AppendAll(objectStorage.partitioned_by());
        if (!partitionedBy.ArrayEmpty()) {
            parameters["partitioned_by"] = partitionedBy.ToJson();
        }

        return parameters;
    }

private:
    static NYql::TIssues Validate(const NKikimrExternalSources::TSchema& schema, const NKikimrExternalSources::TObjectStorage& objectStorage) {
        NYql::TIssues issues;
        issues.AddIssues(ValidateFormatSetting(objectStorage.format(), objectStorage.format_setting()));
        if (objectStorage.projection_size() || objectStorage.partitioned_by_size()) {
            try {
                TVector<TString> partitionedBy{objectStorage.partitioned_by().begin(), objectStorage.partitioned_by().end()};
                issues.AddIssues(ValidateProjectionColumns(schema, partitionedBy));
                TString projectionStr;
                if (objectStorage.projection_size()) {
                    NSc::TValue projection;
                    for (const auto& [key, value]: objectStorage.projection()) {
                        projection[key] = value;
                    }
                    projectionStr = projection.ToJsonPretty();
                }
                issues.AddIssues(ValidateProjection(schema, projectionStr, partitionedBy));
            } catch (...) {
                issues.AddIssue(MakeErrorIssue(Ydb::StatusIds::BAD_REQUEST, CurrentExceptionMessage()));
            }
        }
        return issues;
    }

    static NYql::TIssues ValidateFormatSetting(const TString& format, const google::protobuf::Map<TString, TString>& formatSetting) {
        NYql::TIssues issues;
        issues.AddIssues(ValidateDateFormatSetting(formatSetting));
        for (const auto& [key, value]: formatSetting) {
            if (key == "file_pattern"sv) {
                continue;
            }

            if (key == "data.interval.unit"sv) {
                if (!IsValidIntervalUnit(value)) {
                    issues.AddIssue(MakeErrorIssue(Ydb::StatusIds::BAD_REQUEST, "unknown value for data.interval.unit " + value));
                }
                continue;
            }

            if (IsIn({ "data.datetime.format_name"sv, "data.datetime.format"sv, "data.timestamp.format_name"sv, "data.timestamp.format"sv}, key)) {
                continue;
            }

            if (key == "csv_delimiter"sv) {
                if (format != "csv_with_names"sv) {
                    issues.AddIssue(MakeErrorIssue(Ydb::StatusIds::BAD_REQUEST, "csv_delimiter should be used only with format csv_with_names"));
                }
                if (value.size() != 1) {
                    issues.AddIssue(MakeErrorIssue(Ydb::StatusIds::BAD_REQUEST, "csv_delimiter should contain only one character"));
                }
                continue;
            }

            issues.AddIssue(MakeErrorIssue(Ydb::StatusIds::BAD_REQUEST, "unknown format setting " + key));
        }
        return issues;
    }

    static NYql::TIssues ValidateDateFormatSetting(const google::protobuf::Map<TString, TString>& formatSetting) {
        NYql::TIssues issues;
        TSet<TString> conflictingKeys;
        for (const auto& [key, value]: formatSetting) {
            if (key == "data.datetime.format_name"sv) {
                if (!IsValidDateTimeFormatName(value)) {
                    issues.AddIssue(MakeErrorIssue(Ydb::StatusIds::BAD_REQUEST, "unknown value for data.datetime.format_name " + value));
                }
                if (conflictingKeys.contains("data.datetime.format")) {
                    issues.AddIssue(MakeErrorIssue(Ydb::StatusIds::BAD_REQUEST, "Don't use data.datetime.format_name and data.datetime.format together"));
                }
                conflictingKeys.insert("data.datetime.format_name");
                continue;
            }

            if (key == "data.datetime.format"sv) {
                if (conflictingKeys.contains("data.datetime.format_name")) {
                    issues.AddIssue(MakeErrorIssue(Ydb::StatusIds::BAD_REQUEST, "Don't use data.datetime.format_name and data.datetime.format together"));
                }
                conflictingKeys.insert("data.datetime.format");
                continue;
            }

            if (key == "data.timestamp.format_name"sv) {
                if (!IsValidTimestampFormatName(value)) {
                    issues.AddIssue(MakeErrorIssue(Ydb::StatusIds::BAD_REQUEST, "unknown value for data.timestamp.format_name " + value));
                }
                if (conflictingKeys.contains("data.timestamp.format")) {
                    issues.AddIssue(MakeErrorIssue(Ydb::StatusIds::BAD_REQUEST, "Don't use data.timestamp.format_name and data.timestamp.format together"));
                }
                conflictingKeys.insert("data.timestamp.format_name");
                continue;
            }

            if (key == "data.timestamp.format"sv) {
                if (conflictingKeys.contains("data.timestamp.format_name")) {
                    issues.AddIssue(MakeErrorIssue(Ydb::StatusIds::BAD_REQUEST, "Don't use data.timestamp.format_name and data.timestamp.format together"));
                }
                conflictingKeys.insert("data.timestamp.format");
                continue;
            }
        }
        return issues;
    }

    static bool IsValidIntervalUnit(const TString& unit) {
        static constexpr std::array<std::string_view, 7> IntervalUnits = {
            "MICROSECONDS"sv,
            "MILLISECONDS"sv,
            "SECONDS"sv,
            "MINUTES"sv,
            "HOURS"sv,
            "DAYS"sv,
            "WEEKS"sv
        };
        return IsIn(IntervalUnits, unit);
    }

    static bool IsValidDateTimeFormatName(const TString& formatName) {
        static constexpr std::array<std::string_view, 2> FormatNames = {
            "POSIX"sv,
            "ISO"sv
        };
        return IsIn(FormatNames, formatName);
    }

    static bool IsValidTimestampFormatName(const TString& formatName) {
        static constexpr std::array<std::string_view, 5> FormatNames = {
            "POSIX"sv,
            "ISO"sv,
            "UNIX_TIME_MILLISECONDS"sv,
            "UNIX_TIME_SECONDS"sv,
            "UNIX_TIME_MICROSECONDS"sv
        };
        return IsIn(FormatNames, formatName);
    }

    static NYql::TIssue MakeErrorIssue(NYql::TIssueCode id, const TString& message) {
        NYql::TIssue issue;
        issue.SetCode(id, NYql::TSeverityIds::S_ERROR);
        issue.SetMessage(message);
        return issue;
    }

    static NYql::TIssues ValidateProjectionColumns(const NKikimrExternalSources::TSchema& schema, const TVector<TString>& partitionedBy) {
        NYql::TIssues issues;
        TMap<TString, Ydb::Type> types;
        for (const auto& column: schema.column()) {
            types[column.name()] = column.type();
        }
        for (const auto& parititonedColumn: partitionedBy) {
            auto it = types.find(parititonedColumn);
            if (it == types.end()) {
                issues.AddIssue(MakeErrorIssue(Ydb::StatusIds::BAD_REQUEST, TStringBuilder{} << "Column " << parititonedColumn << " from partitioned_by does not exist in the scheme. Please add such a column to your scheme"));
                continue;
            }
            NYdb::TType columnType{it->second};
            issues.AddIssues(ValidateCommonProjectionType(columnType, parititonedColumn));
        }
        return issues;
    }

    static NYql::TIssues ValidateProjectionType(const NYdb::TType& columnType, const TString& columnName, const std::vector<NYdb::TType>& availableTypes) {
        return FindIf(availableTypes, [&columnType](const auto& availableType) { return NYdb::TypesEqual(availableType, columnType); }) == availableTypes.end()
            ? NYql::TIssues{MakeErrorIssue(Ydb::StatusIds::BAD_REQUEST, TStringBuilder{} << "Column \"" << columnName << "\" from projection does not support " << columnType.ToString() << " type")}
            : NYql::TIssues{};
    }

    static NYql::TIssues ValidateIntegerProjectionType(const NYdb::TType& columnType, const TString& columnName) {
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

    static NYql::TIssues ValidateEnumProjectionType(const NYdb::TType& columnType, const TString& columnName) {
        static const std::vector<NYdb::TType> availableTypes {
            NYdb::TTypeBuilder{}
                .Primitive(NYdb::EPrimitiveType::String)
                .Build()
        };
        return ValidateProjectionType(columnType, columnName, availableTypes);
    }

    static NYql::TIssues ValidateCommonProjectionType(const NYdb::TType& columnType, const TString& columnName) {
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

    static NYql::TIssues ValidateDateProjectionType(const NYdb::TType& columnType, const TString& columnName) {
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
                .Primitive(NYdb::EPrimitiveType::Date)
                .Build()
        };
        return ValidateProjectionType(columnType, columnName, availableTypes);
    }

    static NYql::TIssues ValidateProjection(const NKikimrExternalSources::TSchema& schema, const TString& projection, const TVector<TString>& partitionedBy) {
        auto generator = NYql::NPathGenerator::CreatePathGenerator(projection, partitionedBy, GetDataSlotColumns(schema)); // an exception is thrown if an error occurs
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
                        issues.AddIssue(MakeErrorIssue(Ydb::StatusIds::BAD_REQUEST, TStringBuilder{} << "Column \"" << column.name() << "\" from projection has undefined generator type"));
                        break;
                }
            }
        }
        return issues;
    }

    static TMap<TString, NYql::NUdf::EDataSlot> GetDataSlotColumns(const NKikimrExternalSources::TSchema& schema) {
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
};

}

IExternalSource::TPtr CreateObjectStorageExternalSource() {
    return MakeIntrusive<TObjectStorageExternalSource>();
}

}
