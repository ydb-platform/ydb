#include "yql_generic_state.h"
#include <ydb/library/yql/providers/generic/connector/libcpp/utils.h>
#include <ydb/library/yql/providers/generic/provider/yql_generic_utils.h>

namespace NYql {
    bool TGenericState::TTableMeta::HasSplitsForSelect(const NConnector::NApi::TSelect& select) const {
        return SelectSplits.contains(GetSelectKey(select));
    }

    void TGenericState::TTableMeta::AttachSplitsForSelect(const NConnector::NApi::TSelect& select, std::vector<NYql::NConnector::NApi::TSplit>& splits) {
        auto k = GetSelectKey(select);

        Y_ENSURE(splits.size());
        Y_ENSURE(!SelectSplits.contains(k));

        SelectSplits.emplace(k, std::move(splits));
    }

    const std::vector<NYql::NConnector::NApi::TSplit>& TGenericState::TTableMeta::GetSplitsForSelect(
        const NConnector::NApi::TSelect& select) const {
        auto k = GetSelectKey(select);

        if (!SelectSplits.contains(k)) {
            throw yexception()
                << "Table metadata does not contains split for select: "
                << select.what().DebugString()
                << select.from().DebugString()
                << select.where().DebugString();
        }

        return SelectSplits.at(k);
    }

    bool TGenericState::HasTable(const TTableAddress& tableAddress) {
        return Tables_.contains(tableAddress);
    }

    void TGenericState::AddTable(const TTableAddress& tableAddress, TTableMeta&& tableMeta) {
        Y_ENSURE(!Tables_.contains(tableAddress));
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

    std::optional<TIssue> TGenericState::AttachSplitsToTable(const TTableAddress& tableAddress,
                                                             const NConnector::NApi::TSelect& select,
                                                             std::vector<NYql::NConnector::NApi::TSplit>& splits) {
        auto result = Tables_.FindPtr(tableAddress);

        if (!result) {
            return {TIssue(TStringBuilder() << "no metadata for table " << tableAddress.ToString())};
        }

        result->AttachSplitsForSelect(select, splits);
        return std::nullopt;
    }

} // namespace NYql
