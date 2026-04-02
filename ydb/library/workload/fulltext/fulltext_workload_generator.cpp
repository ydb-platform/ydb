#include "fulltext_workload_generator.h"
#include "fulltext_workload_params.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>

#include <util/string/builder.h>
#include <util/generic/vector.h>

#include <format>
#include <random>

namespace NYdbWorkload {

    TFulltextWorkloadGenerator::TFulltextWorkloadGenerator(const TFulltextWorkloadParams* params)
        : TBase(params)
    {
    }

    void TFulltextWorkloadGenerator::Init() {
        if (!Params.ModelPath.empty()) {
            Evaluator = TMarkovModelEvaluator::LoadFromFile(Params.ModelPath);
            return;
        }
        Y_ENSURE(!Params.QueryTable.empty(), "QueryTable must be set when ModelPath is not provided");
        LoadQueries();
    }

    void TFulltextWorkloadGenerator::LoadQueries() {
        const TString limitClause = Params.TopSize != 0 ? std::format("LIMIT {}", Params.TopSize) : "";
        const TString selectQuery = std::format(
            R"sql(
            SELECT `query`
            FROM `{}`
            {}
        )sql",
            Params.GetFullTableName(Params.QueryTable.c_str()).c_str(),
            limitClause.c_str());

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

        Y_ENSURE(!Queries.empty(), std::format("Query table '{}' is empty or has no 'query' column", Params.QueryTable.c_str()));
        Cout << "Loaded " << Queries.size() << " queries from table " << Params.QueryTable << Endl;
    }

    std::string TFulltextWorkloadGenerator::GetDDLQueries() const {
        TStringBuilder ddl;
        ddl << "--!syntax_v1\n";
        ddl << "CREATE TABLE `" << Params.GetFullTableName(Params.TableName.c_str()) << "` (\n";
        ddl << "    `id` Uint64 NOT NULL,\n";
        ddl << "    `text` String NOT NULL,\n";
        ddl << "    PRIMARY KEY (`id`)\n";
        ddl << ") WITH (\n";
        ddl << "    AUTO_PARTITIONING_BY_SIZE = ENABLED,\n";
        ddl << "    AUTO_PARTITIONING_BY_LOAD = " << (Params.AutoPartitioningByLoad ? "ENABLED" : "DISABLED") << ",\n";
        ddl << "    AUTO_PARTITIONING_PARTITION_SIZE_MB = " << Params.PartitionSizeMb << ",\n";
        ddl << "    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = " << Params.MinPartitions << "\n";
        ddl << ");";
        return ddl;
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
            case EFulltextWorkloadType::Upsert:
                return Upsert();
            default:
                return {};
        }
    }

    TQueryInfoList TFulltextWorkloadGenerator::Select() {
        TString queryText;
        if (Evaluator) {
            static thread_local std::mt19937 rng(std::random_device{}());
            Y_ENSURE(Params.SelectMinQueryLen <= Params.SelectMaxQueryLen,
                "min-query-len (" << Params.SelectMinQueryLen << ") must be <= max-query-len (" << Params.SelectMaxQueryLen << ")");
            std::uniform_int_distribution<size_t> queryLenDist(Params.SelectMinQueryLen, Params.SelectMaxQueryLen);
            for (int attempt = 0; queryText.empty() && attempt < 10; ++attempt) {
                queryText = Evaluator->GenerateSentence(queryLenDist(rng), rng);
            }
            if (queryText.empty()) {
                return {};
            }
        } else {
            if (Queries.empty()) {
                return {};
            }
            queryText = Queries[CurrentIndex];
            CurrentIndex = (CurrentIndex + 1) % Queries.size();
        }

        const TString tablePath = Params.GetFullTableName(Params.TableName.c_str());
        const TString limitClause = Params.Limit != 0 ? std::format("LIMIT {}", Params.Limit) : "";

        TString query;
        if (Params.IndexIsRelevance) {
            query = std::format(
                R"sql(
                    --!syntax_v1
                    DECLARE $query AS Utf8;
                    SELECT `id`, FulltextScore(`text`, $query) AS score
                    FROM `{}` VIEW `{}`
                    WHERE FulltextScore(`text`, $query) > 0
                    ORDER BY score DESC
                    {};
                )sql",
                tablePath.c_str(),
                Params.IndexName.c_str(),
                limitClause.c_str());
        } else {
            query = std::format(
                R"sql(
                    --!syntax_v1
                    DECLARE $query AS Utf8;
                    SELECT `id`
                    FROM `{}` VIEW `{}`
                    WHERE FulltextMatch(`text`, $query)
                    {};
                )sql",
                tablePath.c_str(),
                Params.IndexName.c_str(),
                limitClause.c_str());
        }

        NYdb::TParams params = NYdb::TParamsBuilder()
                                   .AddParam("$query")
                                   .Utf8(queryText)
                                   .Build()
                                   .Build();

        TQueryInfo queryInfo(query, std::move(params));
        return TQueryInfoList{queryInfo};
    }

    TQueryInfoList TFulltextWorkloadGenerator::Upsert() {
        Y_ENSURE(Evaluator, "Markov model must be loaded for upsert workload. Specify --model.");
        Y_ENSURE(Params.UpsertMinSentenceLen <= Params.UpsertMaxSentenceLen,
            "min-sentence-len (" << Params.UpsertMinSentenceLen << ") must be <= max-sentence-len (" << Params.UpsertMaxSentenceLen << ")");
        static thread_local std::mt19937 rng(std::random_device{}());
        static thread_local std::uniform_int_distribution<ui64> idDist;
        std::uniform_int_distribution<size_t> sentenceLenDist(Params.UpsertMinSentenceLen, Params.UpsertMaxSentenceLen);

        const size_t batchSize = Params.UpsertBulkSize;

        TStringBuilder queryBuilder;
        queryBuilder << "--!syntax_v1\n";
        queryBuilder << "DECLARE $rows AS List<Struct<id:Uint64, text:String>>;\n";
        queryBuilder << "UPSERT INTO `" << Params.GetFullTableName(Params.TableName.c_str()) << "` SELECT * FROM AS_TABLE($rows);\n";

        NYdb::TParamsBuilder paramsBuilder;
        auto& listBuilder = paramsBuilder.AddParam("$rows").BeginList();

        for (size_t i = 0; i < batchSize; ++i) {
            const TString text = Evaluator->GenerateSentence(sentenceLenDist(rng), rng);

            auto& item = listBuilder.AddListItem().BeginStruct();
            item.AddMember("id").Uint64(idDist(rng));
            item.AddMember("text").String(text);
            item.EndStruct();
        }
        listBuilder.EndList().Build();

        return TQueryInfoList(1, TQueryInfo(queryBuilder, paramsBuilder.Build()));
    }

    TVector<IWorkloadQueryGenerator::TWorkloadType> TFulltextWorkloadGenerator::GetSupportedWorkloadTypes() const {
        TVector<TWorkloadType> result;
        result.emplace_back(static_cast<int>(EFulltextWorkloadType::Select), "select", "Retrieve rows by fulltext queries");
        result.emplace_back(static_cast<int>(EFulltextWorkloadType::Upsert), "upsert", "Insert or update rows in the table");
        return result;
    }

} // namespace NYdbWorkload
