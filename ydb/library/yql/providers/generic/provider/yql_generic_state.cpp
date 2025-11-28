#include "yql_generic_state.h"
#include <ydb/library/yql/providers/generic/connector/libcpp/utils.h>
#include <ydb/library/yql/providers/generic/provider/yql_generic_utils.h>

namespace NYql {
    std::vector<TString> GetColumns(const NConnector::NApi::TSelect& select) {
        std::vector<TString> columns;

        if (select.has_what() && !select.what().items().empty()) {
            columns.reserve(select.what().items().size());

            for (const auto& item : select.what().items()) {
                if (!item.column().name().empty()) {
                    columns.push_back(item.column().name());
                }
            }

            std::sort(columns.begin(), columns.end());
        }

        return columns;
    }

    TSelectKey::TSelectKey(const TString& cluster, const NConnector::NApi::TSelect& select)
        : Cluster(cluster)
        , Table(select.has_from() ? select.from().table() : "")
        , Columns(GetColumns(select))
        , Where(select.has_where() && select.where().has_filter_typed() ?
            select.where().filter_typed().SerializeAsString() : "")
        , Hash(CalculateHash())
    { }

    size_t TSelectKey::CalculateHash() const {
        auto hash = CombineHashes(std::hash<TString>()(Cluster), std::hash<TString>()(Table));
        hash = CombineHashes(hash, std::hash<TString>()(Where));

        for (const auto& col : Columns) {
            hash = CombineHashes(hash, std::hash<TString>()(col));
        }

        return hash;
    }

    TGenericState::TTableAddress::operator size_t() const {
        return CombineHashes(std::hash<TString>()(ClusterName), std::hash<TString>()(TableName));
    }

    TSelectKey TGenericState::TTableAddress::MakeKeyFor(const NConnector::NApi::TSelect& select) const {
        return TSelectKey(ClusterName, select);
    }

    bool TGenericState::TTableMeta::HasSplitsForSelect(const TSelectKey& key) const {
        return SelectSplits.contains(key);
    }

    void TGenericState::TTableMeta::AttachSplitsForSelect(const TSelectKey& key, std::vector<NYql::NConnector::NApi::TSplit>& splits) {
        Y_ENSURE(splits.size());
        Y_ENSURE(SelectSplits.emplace(key, std::move(splits)).second);
    }

    const std::vector<NYql::NConnector::NApi::TSplit>& TGenericState::TTableMeta::GetSplitsForSelect(
        const TSelectKey& key) const {
        const auto it = SelectSplits.find(key);

        if (it == SelectSplits.end()) {
            throw yexception()
                << "Table metadata does not contain splits for a select from the table: " << key.Table
                << " with where:" << key.Where;
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
                                                             const TSelectKey& key,
                                                             std::vector<NYql::NConnector::NApi::TSplit>& splits) {
        auto result = Tables_.FindPtr(tableAddress);

        if (!result) {
            return {TIssue(TStringBuilder() << "no metadata for table " << tableAddress.ToString())};
        }

        result->AttachSplitsForSelect(key, splits);
        return std::nullopt;
    }

} // namespace NYql
