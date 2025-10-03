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

} // namespace NYql
