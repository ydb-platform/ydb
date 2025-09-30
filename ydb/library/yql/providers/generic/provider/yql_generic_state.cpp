#include "yql_generic_state.h"
#include <ydb/library/yql/providers/generic/connector/libcpp/utils.h>
#include <ydb/library/yql/providers/generic/provider/yql_generic_utils.h>

namespace NYql {
    TGenericState::TTableAddress::operator size_t() const {
        auto seed = std::hash<TString>()(ClusterName);
        HashCombine(seed, std::hash<TString>()(TableName));
        return seed;
    }

    size_t TGenericState::TTableAddress::MakeKeyFor(const NConnector::NApi::TSelect& select) const {
        auto seed = std::hash<TString>()(ClusterName);
        HashCombine(seed, GetSelectKey(select));
        return seed;
    }

    bool TGenericState::TTableMeta::HasSplitsForSelect(const NConnector::NApi::TSelect& select) const {
        return SelectSplits.contains(GetSelectKey(select));
    }

    void TGenericState::TTableMeta::AttachSplitsForSelect(const NConnector::NApi::TSelect& select, std::vector<NYql::NConnector::NApi::TSplit>& splits) {
        auto k = GetSelectKey(select);

        Y_ENSURE(splits.size());
        Y_ENSURE(SelectSplits.emplace(k, std::move(splits)).second);
    }

    const std::vector<NYql::NConnector::NApi::TSplit>& TGenericState::TTableMeta::GetSplitsForSelect(
        const NConnector::NApi::TSelect& select) const {
        const auto it = SelectSplits.find(GetSelectKey(select));

        if (it == SelectSplits.end()) {
            throw yexception()
                << "Table metadata does not contains split for select: "
                << select.what().DebugString()
                << select.from().DebugString()
                << select.where().DebugString();
        }

        return it->second;
    }

    bool TGenericState::HasTable(const TTableAddress& tableAddress) {
        return Tables_.contains(tableAddress);
    }

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
