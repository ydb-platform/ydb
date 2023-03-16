#pragma once

#include "validators.h"

#include <ydb/core/yq/libs/db_schema/db_schema.h>

namespace NFq {

template<typename T, typename A>
TValidationQuery CreateEntityExtractor(const TString& scope,
                                       const TString& id,
                                       const TString& entityColumnName,
                                       const TString& idColumnName,
                                       const TString& tableName,
                                       std::shared_ptr<std::pair<T, A>> response,
                                       const TString& tablePathPrefix) {
    TSqlQueryBuilder queryBuilder(tablePathPrefix);
    queryBuilder.AddString("scope", scope);
    queryBuilder.AddString("id", id);
    queryBuilder.AddText(
        "SELECT `" + entityColumnName + "` FROM `" + tableName + "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" + idColumnName + "` = $id;\n"
    );

    auto validator = [response, entityColumnName](NYdb::NTable::TDataQueryResult result) {
        const auto& resultSets = result.GetResultSets();
        if (resultSets.size() != 1) {
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "internal error, result set size is not equal to 1 but equal " << resultSets.size();
        }

        NYdb::TResultSetParser parser(resultSets.back());
        if (!parser.TryNextRow()) {
            return false; // continue
        }

        if (!response->second.Before.ConstructInPlace().ParseFromString(*parser.ColumnParser(entityColumnName).GetOptionalString())) {
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message. Please contact internal support";
        }
        return false;
    };
    const auto query = queryBuilder.Build();
    return {query.Sql, query.Params, validator};
}

}
