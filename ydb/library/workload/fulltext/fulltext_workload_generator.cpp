#include "fulltext_workload_generator.h"
#include "fulltext_workload_params.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>

#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/generic/vector.h>

#include <format>

namespace NYdbWorkload {

    TFulltextWorkloadGenerator::TFulltextWorkloadGenerator(const TFulltextWorkloadParams* params)
        : TBase(params)
    {
    }

    void TFulltextWorkloadGenerator::Init() {
        LoadQueries();
    }

    void TFulltextWorkloadGenerator::LoadQueries() {
        const TString queryTablePath = Params.QueryTable;
        Y_ENSURE(!queryTablePath.empty(), "Query table name must be specified");

        const TString selectQuery = std::format(
            R"sql(
            SELECT `query`
            FROM `{}`
        )sql",
            Params.GetFullTableName(Params.QueryTable.c_str()).c_str());

        std::optional<NYdb::TResultSet> resultSet;
        NYdb::NStatusHelpers::ThrowOnError(Params.QueryClient->RetryQuerySync([&selectQuery, &resultSet](NYdb::NQuery::TSession session) {
            const auto result = session.ExecuteQuery(
                                           selectQuery,
                                           NYdb::NQuery::TTxControl::NoTx())
                                    .GetValueSync();

            Y_ENSURE(result.IsSuccess(), std::format("Failed to read query table: {}", result.GetIssues().ToString().c_str()));

            resultSet = result.GetResultSet(0);
            return result;
        }));

        NYdb::TResultSetParser parser(*resultSet);
        while (parser.TryNextRow()) {
            auto& column = parser.ColumnParser("query");

            const bool optional = column.GetKind() == NYdb::TTypeParser::ETypeKind::Optional;

            if (optional) {
                column.OpenOptional();
            }

            switch (column.GetPrimitiveType()) {
                case NYdb::EPrimitiveType::Utf8:
                    Queries.emplace_back(column.GetUtf8());
                    break;
                case NYdb::EPrimitiveType::String:
                    Queries.emplace_back(column.GetString());
                    break;
                default:
                    break;
            }

            if (optional) {
                column.CloseOptional();
            }
        }

        Y_ENSURE(!Queries.empty(), std::format("Query table '{}' is empty or has no 'query' column", queryTablePath.c_str()));
        Cout << "Loaded " << Queries.size() << " queries from table " << queryTablePath << Endl;
    }

    std::string TFulltextWorkloadGenerator::GetDDLQueries() const {
        return std::format(
            R"sql(
            CREATE TABLE `{}` (
                `id` Uint64,
                `text` String,
                PRIMARY KEY (`id`)
            ) WITH (
                AUTO_PARTITIONING_BY_LOAD = {},
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = {}
            );
        )sql",
            Params.GetFullTableName(Params.TableName.c_str()).c_str(),
            Params.AutoPartitioningByLoad ? "ENABLED" : "DISABLED",
            Params.MinPartitions);
    }

    TQueryInfoList TFulltextWorkloadGenerator::GetInitialData() {
        return {};
    }

    TVector<std::string> TFulltextWorkloadGenerator::GetCleanPaths() const {
        return {Params.TableName};
    }

    TQueryInfoList TFulltextWorkloadGenerator::GetWorkload(int type) {
        switch (static_cast<EFulltextWorkloadType>(type)) {
            case EFulltextWorkloadType::Select:
                return Select();
            default:
                return {};
        }
    }

    TQueryInfoList TFulltextWorkloadGenerator::Select() {
        Y_ENSURE(!Queries.empty(), "No queries loaded from query table");

        const TString& queryText = Queries[CurrentIndex];
        CurrentIndex = (CurrentIndex + 1) % Queries.size();

        const TString query = std::format(
            R"sql(
                DECLARE $query AS Utf8;
                SELECT *
                FROM `{}` VIEW `{}`
                WHERE FulltextMatch(`text`, $query)
                {};
            )sql",
            Params.GetFullTableName(Params.TableName.c_str()).c_str(),
            Params.IndexName.c_str(),
            (Params.Limit != 0 ? std::format("LIMIT {}", Params.Limit) : "").c_str());

        NYdb::TParams params = NYdb::TParamsBuilder()
                                   .AddParam("$query")
                                   .Utf8(queryText)
                                   .Build()
                                   .Build();

        TQueryInfo queryInfo(query, std::move(params));
        return TQueryInfoList{queryInfo};
    }

    TVector<IWorkloadQueryGenerator::TWorkloadType> TFulltextWorkloadGenerator::GetSupportedWorkloadTypes() const {
        TVector<TWorkloadType> result;
        result.emplace_back(static_cast<int>(EFulltextWorkloadType::Select), "select", "Run fulltext select queries from a query table");
        return result;
    }

} // namespace NYdbWorkload
