#include "query_utils.h"

#include <contrib/libs/fmt/include/fmt/format.h>
#include <util/generic/maybe.h>

#include <ydb/core/fq/libs/common/util.h>
#include <ydb/core/fq/libs/result_formatter/result_formatter.h>
#include <ydb/core/kqp/provider/yql_kikimr_results.h>
#include <ydb/public/api/protos/draft/fq.pb.h>

namespace NFq {
namespace NPrivate {

TString MakeCreateExternalDataTableQuery(const FederatedQuery::BindingContent& content,
                                         const TString& connectionName) {
    using namespace fmt::literals;

    auto bindingName         = content.name();
    auto objectStorageParams = content.setting().object_storage();
    const auto& subset       = objectStorageParams.subset(0);

    // Schema
    NYql::TExprContext context;
    auto columnsTransformFunction = [&context](const Ydb::Column& column) -> TString {
        NYdb::TTypeParser typeParser(column.type());
        auto node     = MakeType(typeParser, context);
        auto typeName = NYql::FormatType(node);
        const TString notNull =
            (node->GetKind() == NYql::ETypeAnnotationKind::Optional) ? "" : "NOT NULL";
        return fmt::format("    {columnName} {columnType} {notNull}",
                           "columnName"_a = EncloseAndEscapeString(column.name(), '`'),
                           "columnType"_a = typeName,
                           "notNull"_a    = notNull);
    };

    // WithOptions
    auto withOptions = std::unordered_map<TString, TString>{};
    withOptions.insert({"DATA_SOURCE", TStringBuilder{} << '"' << connectionName << '"'});
    withOptions.insert({"LOCATION", EncloseAndEscapeString(subset.path_pattern(), '"')});
    if (!subset.format().Empty()) {
        withOptions.insert({"FORMAT", EncloseAndEscapeString(subset.format(), '"')});
    }
    if (!subset.compression().Empty()) {
        withOptions.insert(
            {"COMPRESSION", EncloseAndEscapeString(subset.compression(), '"')});
    }
    for (auto& kv : subset.format_setting()) {
        withOptions.insert({EncloseAndEscapeString(kv.first, '`'),
                            EncloseAndEscapeString(kv.second, '"')});
    }

    if (!subset.partitioned_by().empty()) {
        auto partitionBy = TStringBuilder{}
                           << "\"["
                           << JoinMapRange(", ",
                                           subset.partitioned_by().begin(),
                                           subset.partitioned_by().end(),
                                           [](const TString& value) {
                                               return EscapeString(value, '"');
                                           })
                           << "]\"";
        withOptions.insert({"PARTITIONED_BY", partitionBy});
    }

    for (auto& kv : subset.projection()) {
        withOptions.insert({EncloseAndEscapeString(kv.first, '`'),
                            EncloseAndEscapeString(kv.second, '"')});
    }

    return fmt::format(
        R"(
                CREATE EXTERNAL TABLE {externalTableName} (
                    {columns}
                ) WITH (
                    {withOptions}
                );)",
        "externalTableName"_a = EncloseAndEscapeString(bindingName, '`'),
        "columns"_a           = JoinMapRange(",\n",
                                   subset.schema().column().begin(),
                                   subset.schema().column().end(),
                                   columnsTransformFunction),
        "withOptions"_a       = JoinMapRange(",\n",
                                       withOptions.begin(),
                                       withOptions.end(),
                                       [](const std::pair<TString, TString>& kv) -> TString {
                                           return TStringBuilder{} << "   " << kv.first
                                                                   << " = " << kv.second;
                                       }));
}

TString SignAccountId(const TString& id, const TSigner::TPtr& signer) {
    return signer ? signer->SignAccountId(id) : TString{};
}

TMaybe<TString> CreateSecretObjectQuery(const FederatedQuery::IamAuth& auth,
                                        const TString& name,
                                        const TSigner::TPtr& signer) {
    using namespace fmt::literals;
    switch (auth.identity_case()) {
        case FederatedQuery::IamAuth::kServiceAccount: {
            if (!signer) {
                return {};
            }
            return fmt::format(R"(
                UPSERT OBJECT {external_source} (TYPE SECRET) WITH value={signature};
            )",
                               "external_source"_a = EncloseAndEscapeString(name, '`'),
                               "signature"_a       = EncloseAndEscapeString(
                                   SignAccountId(auth.service_account().id(), signer), '`'));
        }
        case FederatedQuery::IamAuth::kNone:
        case FederatedQuery::IamAuth::kCurrentIam:
        // Do not replace with default. Adding a new auth item should cause a compilation error
        case FederatedQuery::IamAuth::IDENTITY_NOT_SET:
            return {};
    }
}

TString CreateAuthParamsQuery(const FederatedQuery::IamAuth& auth,
                              const TString& name,
                              const TSigner::TPtr& signer) {
    using namespace fmt::literals;
    switch (auth.identity_case()) {
        case FederatedQuery::IamAuth::kNone:
            return R"(, AUTH_METHOD="NONE")";
        case FederatedQuery::IamAuth::kServiceAccount:
            return fmt::format(R"(,
                AUTH_METHOD="SERVICE_ACCOUNT",
                SERVICE_ACCOUNT_ID={service_account_id},
                SERVICE_ACCOUNT_SECRET_NAME={secret_name}
            )",
                               "service_account_id"_a =
                                   EncloseAndEscapeString(auth.service_account().id(), '"'),
                               "external_source"_a = EncloseAndEscapeString(name, '"'),
                               "secret_name"_a =
                                   EncloseAndEscapeString(signer ? name : TString{}, '"'));
        case FederatedQuery::IamAuth::kCurrentIam:
        // Do not replace with default. Adding a new auth item should cause a compilation error
        case FederatedQuery::IamAuth::IDENTITY_NOT_SET:
            return {};
    }
}

TString MakeCreateExternalDataSourceQuery(
    const FederatedQuery::ConnectionContent& connectionContent,
    const TString& objectStorageEndpoint,
    const TSigner::TPtr& signer) {
    using namespace fmt::literals;

    auto sourceName = connectionContent.name();
    auto bucketName = connectionContent.setting().object_storage().bucket();

    return fmt::format(
        R"(
                CREATE EXTERNAL DATA SOURCE {external_source} WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="{location}"
                    {auth_params}
                );
            )",
        "external_source"_a = EncloseAndEscapeString(sourceName, '`'),
        "location"_a = objectStorageEndpoint + "/" + EscapeString(bucketName, '"') + "/",
        "auth_params"_a =
            CreateAuthParamsQuery(connectionContent.setting().object_storage().auth(),
                                  connectionContent.name(),
                                  signer));
}

TMaybe<TString> DropSecretObjectQuery(const FederatedQuery::IamAuth& auth,
                                      const TString& name,
                                      const TSigner::TPtr& signer) {
    using namespace fmt::literals;
    switch (auth.identity_case()) {
        case FederatedQuery::IamAuth::kServiceAccount: {
            if (!signer) {
                return {};
            }
            return fmt::format("DROP OBJECT {secret_name} (TYPE SECRET);",
                               "secret_name"_a =
                                   EncloseAndEscapeString(name, '`'));
        }
        case FederatedQuery::IamAuth::kNone:
        case FederatedQuery::IamAuth::kCurrentIam:
        // Do not replace with default. Adding a new auth item should cause a compilation error
        case FederatedQuery::IamAuth::IDENTITY_NOT_SET:
            return {};
    }
}

TString MakeDeleteExternalDataTableQuery(const TString& tableName) {
    using namespace fmt::literals;
    return fmt::format("DROP EXTERNAL TABLE {external_table};",
                       "external_table"_a = EncloseAndEscapeString(tableName, '`'));
}

TString MakeDeleteExternalDataSourceQuery(const TString& sourceName) {
    using namespace fmt::literals;
    return fmt::format("DROP EXTERNAL DATA SOURCE {external_source};",
                       "external_source"_a = EncloseAndEscapeString(sourceName, '`'));
}

} // namespace NPrivate
} // namespace NFq
