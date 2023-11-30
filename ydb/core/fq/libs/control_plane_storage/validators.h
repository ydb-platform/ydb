#pragma once

#include "schema.h"

#include <functional>

#include <util/generic/fwd.h>
#include <util/string/printf.h>

#include <ydb/public/api/protos/draft/fq.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/fq/libs/db_schema/db_schema.h>
#include <ydb/core/fq/libs/exceptions/exceptions.h>

namespace NFq {

struct TValidationQuery {
    TString Query;
    NYdb::TParams Params;
    std::function<bool(NYdb::NTable::TDataQueryResult)> Validator;
};

TValidationQuery CreateUniqueNameValidator(const TString& tableName,
                                           FederatedQuery::Acl::Visibility visibility,
                                           const TString& scope,
                                           const TString& name,
                                           const TString& user,
                                           const TString& error,
                                           const TString& tablePathPrefix);

TValidationQuery CreateModifyUniqueNameValidator(const TString& tableName,
                                                 const TString& idColumnName,
                                                 FederatedQuery::Acl::Visibility visibility,
                                                 const TString& scope,
                                                 const TString& name,
                                                 const TString& user,
                                                 const TString& id,
                                                 const TString& error,
                                                 const TString& tablePathPrefix);

TValidationQuery CreateCountEntitiesValidator(const TString& scope,
                                              const TString& tableName,
                                              ui64 limit,
                                              const TString& error,
                                              const TString& tablePathPrefix);

TValidationQuery CreateRevisionValidator(const TString& tableName,
                                         const TString& columnName,
                                         const TString& scope,
                                         const TString& id,
                                         i64 previousRevision,
                                         const TString& error,
                                         const TString& tablePathPrefix);

TValidationQuery CreateViewAccessValidator(const TString& tableName,
                                           const TString& columnName,
                                           const TString& scope,
                                           const TString& id,
                                           TString user,
                                           const TString& error,
                                           TPermissions permissions,
                                           const TString& tablePathPrefix);

TValidationQuery CreateManageAccessValidator(const TString& tableName,
                                             const TString& columnName,
                                             const TString& scope,
                                             const TString& id,
                                             TString user,
                                             const TString& error,
                                             TPermissions permissions,
                                             const TString& tablePathPrefix);

TValidationQuery CreateRelatedBindingsValidator(const TString& scope,
                                                const TString& connectionId,
                                                const TString& error,
                                                const TString& tablePathPrefix);

TValidationQuery CreateConnectionExistsValidator(const TString& scope,
                                                 const TString& connectionId,
                                                 const TString& error,
                                                 TPermissions permissions,
                                                 const TString& user,
                                                 FederatedQuery::Acl::Visibility bindingVisibility,
                                                 const TString& tablePathPrefix);

TValidationQuery CreateConnectionOverrideBindingValidator(const TString& scope,
                                                          const TString& connectionName,
                                                          TPermissions permissions,
                                                          const TString& user,
                                                          const TString& tablePathPrefix);

TValidationQuery CreateBindingConnectionValidator(const TString& scope,
                                                 const TString& connectionId,
                                                 const TString& user,
                                                 const TString& tablePathPrefix);

TValidationQuery CreateTtlValidator(const TString& tableName,
                                    const TString& columnName,
                                    const TString& scope,
                                    const TString& id,
                                    const TString& error,
                                    const TString& tablePathPrefix);

TValidationQuery CreateQueryComputeStatusValidator(const std::vector<FederatedQuery::QueryMeta::ComputeStatus>& computeStatuses,
                                                   const TString& scope,
                                                   const TString& id,
                                                   const TString& error,
                                                   const TString& tablePathPrefix,
                                                   const ::NMonitoring::TDynamicCounters::TCounterPtr& parseProtobufError);

template<typename T>
TValidationQuery CreateIdempotencyKeyValidator(const TString& scope,
                                               const TString& idempotencyKey,
                                               std::shared_ptr<T> response,
                                               const TString& tablePathPrefix,
                                               const ::NMonitoring::TDynamicCounters::TCounterPtr& parseProtobufError) {
    TSqlQueryBuilder queryBuilder(tablePathPrefix);
    queryBuilder.AddString("idempotency_key", idempotencyKey);
    queryBuilder.AddString("scope", scope);
    queryBuilder.AddText(
        "SELECT `" RESPONSE_COLUMN_NAME "` FROM `" IDEMPOTENCY_KEYS_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" IDEMPOTENCY_KEY_COLUMN_NAME "` = $idempotency_key;\n"
    );

    auto validator = [response, parseProtobufError](NYdb::NTable::TDataQueryResult result) {
        const auto& resultSets = result.GetResultSets();
        if (resultSets.size() != 1) {
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "internal error, result set size is not equal to 1 but equal " << resultSets.size();
        }

        NYdb::TResultSetParser parser(resultSets.back());
        if (!parser.TryNextRow()) {
            return false; // idempotency key does not exist. check other validators
        }

        if (!response->first.ParseFromString(*parser.ColumnParser(RESPONSE_COLUMN_NAME).GetOptionalString())) {
            parseProtobufError->Inc();
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for response. Please contact internal support";
        }

        return true;
    };
    const auto query = queryBuilder.Build();
    return {query.Sql, query.Params, validator};
}

template<typename T, typename A>
TValidationQuery CreateIdempotencyKeyValidator(const TString& scope,
                                               const TString& idempotencyKey,
                                               std::shared_ptr<std::pair<T, A>> response,
                                               const TString& tablePathPrefix,
                                               const ::NMonitoring::TDynamicCounters::TCounterPtr& parseProtobufError) {
    TSqlQueryBuilder queryBuilder(tablePathPrefix);
    queryBuilder.AddString("idempotency_key", idempotencyKey);
    queryBuilder.AddString("scope", scope);
    queryBuilder.AddText(
        "SELECT `" RESPONSE_COLUMN_NAME "` FROM `" IDEMPOTENCY_KEYS_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" IDEMPOTENCY_KEY_COLUMN_NAME "` = $idempotency_key;\n"
    );

    auto validator = [response, parseProtobufError](NYdb::NTable::TDataQueryResult result) {
        const auto& resultSets = result.GetResultSets();
        if (resultSets.size() != 1) {
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "internal error, result set size is not equal to 1 but equal " << resultSets.size();
        }

        NYdb::TResultSetParser parser(resultSets.back());
        if (!parser.TryNextRow()) {
            return false; // idempotency key does not exist. check other validators
        }

        if (!response->first.ParseFromString(*parser.ColumnParser(RESPONSE_COLUMN_NAME).GetOptionalString())) {
            parseProtobufError->Inc();
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for response. Please contact internal support";
        }

        response->second.IdempotencyResult = true;

        return true;
    };
    const auto query = queryBuilder.Build();
    return {query.Sql, query.Params, validator};
}

} // namespace NFq
