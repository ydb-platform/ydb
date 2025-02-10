#include "yql_generic_state.h"

namespace NYql {
    void TGenericState::AddTable(const TStringBuf& clusterName, const TStringBuf& tableName, TTableMeta&& tableMeta) {
        Tables_.emplace(TTableAddress(clusterName, tableName), tableMeta);
    }

    TGenericState::TGetTableResult TGenericState::GetTable(const TStringBuf& clusterName, const TStringBuf& tableName) const {
        auto result = Tables_.FindPtr(TTableAddress(clusterName, tableName));
        if (result) {
            return std::make_pair(result, TIssues{});
        }

        TIssues issues;
        issues.AddIssue(TIssue(TStringBuilder() << "no metadata for table " << clusterName << "." << tableName));

        return std::make_pair( std::nullopt, std::move(issues));
    };

} // namespace NYql
