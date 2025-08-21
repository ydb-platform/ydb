#include "yql_generic_state.h"

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
                << select.DebugString();
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

    bool TGenericState::AttachSplitsToTable(const TTableAddress& tableAddress,
                                            const NConnector::NApi::TSelect& select,
                                            std::vector<NYql::NConnector::NApi::TSplit>& splits) {
        auto result = Tables_.FindPtr(tableAddress);

        if (!result) {
            return false;
        }

        result->AttachSplitsForSelect(select, splits);
        return true;
    }

    TString GetWhereKey(const NConnector::NApi::TSelect& select) {
        if (!select.has_where() || !select.where().has_filter_typed()) {
            return "";
        }

        return select.where().filter_typed().SerializeAsString();
    }

    TString GetColumnsKey(const NConnector::NApi::TSelect& select) {
        if (!select.has_what() || !select.what().items_size()) {
            return "";
        }

        // Use a set to preserve order of columns
        std::set<TString> columnNames;

        for (auto column: select.what().items()) {
            columnNames.emplace(column.column().name());
        }

        auto key = std::accumulate(
            columnNames.begin(),
            columnNames.end(),
            TString(),
            [](const TString& acc, const TString& str) -> TString {
                if (acc.empty()) {
                    return str;
                }

                return TStringBuilder() << acc << "!" << str;
            }
        );

        return key;
    }

    TString GetSelectKey(const NConnector::NApi::TSelect& select) {
        Y_ENSURE(select.has_from());

        return TStringBuilder()
            << select.from().table() << "!"
            << GetColumnsKey(select) << "!"
            << GetWhereKey(select);
    }

} // namespace NYql
