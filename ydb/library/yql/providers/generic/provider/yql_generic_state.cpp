#include "yql_generic_state.h"

namespace NYql {
    void TGenericState::AddTable(const TTableAddress& tableAddress, TTableMeta&& tableMeta) {
        Tables_.emplace(tableAddress, tableMeta);
    }

    TGenericState::TGetTableResult TGenericState::GetTable(const TStringBuf& clusterName, const TStringBuf& tableName) const {
        const TTableAddress tableAddress = {.ClusterName=TString(clusterName), .TableName=TString(tableName)};
        auto result = Tables_.FindPtr(tableAddress);
        if (result) {
            return std::make_pair(result, TIssues{});
        }

        TIssues issues;
        issues.AddIssue(TIssue(TStringBuilder() << "no metadata for table " << tableAddress.String()));

        return std::make_pair( std::nullopt, std::move(issues));
    };

} // namespace NYql
