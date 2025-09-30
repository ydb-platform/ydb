#include "yql_generic_utils.h"

#include <util/string/builder.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/utils.h>
#include <ydb/library/yql/providers/generic/provider/yql_generic_predicate_pushdown.h>

namespace NYql {
    TString DumpGenericClusterConfig(const TGenericClusterConfig& clusterConfig) {
        TStringBuilder sb;
        sb << "name = " << clusterConfig.GetName()
           << ", kind = " << NYql::EGenericDataSourceKind_Name(clusterConfig.GetKind())
           << ", database name = " << clusterConfig.GetDatabaseName()
           << ", database id = " << clusterConfig.GetDatabaseId()
           << ", endpoint = " << clusterConfig.GetEndpoint()
           << ", use tls = " << clusterConfig.GetUseSsl()
           << ", protocol = " << NYql::EGenericProtocol_Name(clusterConfig.GetProtocol());

        for (const auto& [key, value] : clusterConfig.GetDataSourceOptions()) {
            sb << ", " << key << " = " << value;
        }

        return sb;
    }

    ///
    /// Fill a select from a TGenSourceSettings
    ///
    void FillSelectFromGenSourceSettings(NConnector::NApi::TSelect& select,
                                         const NNodes::TGenSourceSettings& settings,
                                         TExprContext& ctx,
                                         const TGenericState::TTableMeta* tableMeta) {
        const auto& tableName = settings.Table().StringValue();

        select.mutable_from()->set_table(TString(tableName));
        *select.mutable_data_source_instance() = tableMeta->DataSourceInstance;

        const auto& columns = settings.Columns();
        auto items = select.mutable_what()->mutable_items();

        for (size_t i = 0; i < columns.Size(); i++) {
            // assign column name
            auto column = items->Add()->mutable_column();
            auto columnName = columns.Item(i).StringValue();
            column->mutable_name()->assign(columnName);

            // assign column type
            auto type = NConnector::GetColumnTypeByName(tableMeta->Schema, columnName);
            *column->mutable_type() = type;
        }

        if (auto predicate = settings.FilterPredicate(); !IsEmptyFilterPredicate(predicate)) {
            TStringBuilder err;

            if (!SerializeFilterPredicate(ctx, predicate, select.mutable_where()->mutable_filter_typed(), err)) {
                throw yexception() << "Failed to serialize filter predicate for source: " << err;
            }
        }
    }

    ///
    /// Combine hash, inspired by boost::hash_combine. The constant 0x9e3779b9 is an integer
    /// representation of the fractional part of the Golden Ratio's reciprocal. It's primary purpose is
    /// achieving a strong avalanche effect (small changes in input lead to large changes in output)
    ///
    void HashCombine(size_t& currentSeed, const size_t& hash) {
        currentSeed ^= hash + 0x9e3779b9 + (currentSeed << 6) + (currentSeed >> 2);
    }

    ///
    /// Get an unique key for a select request
    ///
    size_t GetSelectKey(const NConnector::NApi::TSelect& select) {
        Y_ENSURE(select.has_from());
        size_t seed = 0;

        auto const where = select.has_where() && select.where().has_filter_typed() ?
            select.where().filter_typed().SerializeAsString() : "";

        HashCombine(seed, std::hash<TString>()(select.from().table()));
        HashCombine(seed, std::hash<TString>()(where));

        if (select.has_what() && !select.what().items().empty()) {
            std::vector<TString> columns;
            columns.reserve(select.what().items().size());

            for (const auto& item : select.what().items()) {
                if (!item.column().name().empty()) {
                    columns.push_back(item.column().name());
                }
            }

            std::sort(columns.begin(), columns.end());

            for (const auto& col : columns) {
                HashCombine(seed, std::hash<TString>()(col));
            }
        }

        return seed;
    }

} // namespace NYql
