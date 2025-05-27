#include "yql_generic_state.h"

namespace NYql {
    void TGenericState::AddTable(const TTableAddress& tableAddress, TTableMeta&& tableMeta) {
        Tables_.emplace(tableAddress, std::move(tableMeta));
    }

    TGenericState::TGetTableResult TGenericState::GetTable(const TTableAddress& tableAddress) const {
        auto result = Tables_.FindPtr(tableAddress);
        if (result) {
            return std::make_pair(result, TIssues{});
        }

        TIssues issues;
        issues.AddIssue(TIssue(TStringBuilder() << "no metadata for table " << tableAddress.ToString()));

        return std::make_pair<TTableMeta*, TIssues>(nullptr, std::move(issues));
    }

} // namespace NYql
