#include "yql_generic_state.h"

namespace NYql {
    void TGenericState::AddTable(const TStringBuf& clusterName, const TStringBuf& tableName, TTableMeta&& tableMeta) {
        Tables_.emplace(TTableAddress(clusterName, tableName), tableMeta);
    }

    TGenericState::TGetTableResult TGenericState::GetTable(const TStringBuf& clusterName, const TStringBuf& tableName) const {
        auto result = Tables_.FindPtr(TTableAddress(clusterName, tableName));
        if (result) {
            return std::make_pair(result, std::nullopt);
        }

        return std::make_pair(
            std::nullopt,
            TIssue(TStringBuilder() << "no metadata for table " << clusterName << "." << tableName));
    };

    TGenericState::TGetTableResult TGenericState::GetTable(const TStringBuf& clusterName, const TStringBuf& tableName, const TPosition& position) const {
        auto pair = TGenericState::GetTable(clusterName, tableName);
        if (pair.second.has_value()) {
            pair.second->Position = position;
        }

        return pair;
    }

}
