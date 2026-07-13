#include "json_workload_generator.h"

#include <util/string/builder.h>

#include <format>
#include <random>


namespace NYdbWorkload {

TJsonWorkloadGenerator::TJsonWorkloadGenerator(const TJsonWorkloadParams* params)
    : TBase(params)
{
}

void TJsonWorkloadGenerator::Init() {
    NKikimr::NJsonIndex::TPredicateBuilderOptions opts{
        .EnableJsonExists = true,
        .EnableJsonValue = true,
        .EnableNonJsonFilters = true,
        .EnableJsonPathMethods = true,
        .EnableJsonPathPredicates = true,
        .EnablePassingVariables = true,
        .EnableSqlParameters = true,
        .EnableRangeComparisons = true,
        .EnableBetween = true,
        .EnableInList = true,
        .EnableAndCombinations = true,
        .EnableOrCombinations = true,
        .EnableJsonIsLiteral = true,
        .EnableArithmeticOperators = true,
        .EnableComplexJsonPathFilters = true,
    };

    Corpus = TJsonCorpus(TCorpusOptions{.RowCount = Params.RowCount, .Seed = Params.Seed});
    Predicates = NKikimr::NJsonIndex::TPredicateBuilder().BuildBatch(*Corpus, Params.IsStrict, Params.MaxPredicates, Params.Seed + 1, opts);
    if (!Predicates.empty()) {
        Cout << "Prepared " << Predicates.size() << " JSON index predicates" << Endl;
    }
}

std::string TJsonWorkloadGenerator::GetDDLQueries() const {
    TStringBuilder ddl;
    ddl << "--!syntax_v1\n";
    ddl << "CREATE TABLE `" << Params.GetFullTableName(Params.TableName.c_str()) << "` (\n";
    ddl << "    `Key` Uint64 NOT NULL,\n";
    ddl << "    `Text` " << Params.GetJsonTypeName() << ",\n";
    ddl << "    PRIMARY KEY (`Key`)\n";
    ddl << ") WITH (\n";
    ddl << "    AUTO_PARTITIONING_BY_SIZE = ENABLED,\n";
    ddl << "    AUTO_PARTITIONING_BY_LOAD = " << (Params.AutoPartitioningByLoad ? "ENABLED" : "DISABLED") << ",\n";
    ddl << "    AUTO_PARTITIONING_PARTITION_SIZE_MB = " << Params.PartitionSizeMb << ",\n";
    ddl << "    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = " << Params.MinPartitions << "\n";
    ddl << ");";
    return ddl;
}

TQueryInfoList TJsonWorkloadGenerator::GetInitialData() {
    return {};
}

TVector<std::string> TJsonWorkloadGenerator::GetCleanPaths() const {
    return {Params.TableName};
}

TQueryInfoList TJsonWorkloadGenerator::GetWorkload(int type) {
    switch (static_cast<EJsonWorkloadType>(type)) {
        case EJsonWorkloadType::Select:
            return Select();
        case EJsonWorkloadType::Upsert:
            return Upsert();
        default:
            return {};
    }
}

TQueryInfoList TJsonWorkloadGenerator::Select() {
    if (Predicates.empty()) {
        return {};
    }

    const auto& predicate = Predicates[PredicateIndex.load()];
    PredicateIndex.store((PredicateIndex.load() + 1) % Predicates.size());

    const TString limitClause = Params.Limit != 0 ? std::format("LIMIT {}", Params.Limit) : "";
    const TString tablePath = Params.GetFullTableName(Params.TableName.c_str());

    const TString query = std::format(
        R"sql(
            --!syntax_v1
            SELECT `Key`
            FROM `{}` VIEW `{}`
            WHERE {}
            ORDER BY `Key`
            {};
        )sql",
        tablePath.c_str(),
        Params.IndexName.c_str(),
        predicate.Sql.c_str(),
        limitClause.c_str());

    TQueryInfo queryInfo(query, predicate.Params ? *predicate.Params : NYdb::TParamsBuilder().Build());
    return TQueryInfoList{queryInfo};
}

TQueryInfoList TJsonWorkloadGenerator::Upsert() {
    static thread_local std::mt19937 rng(std::random_device{}());
    static thread_local std::uniform_int_distribution<ui64> idDist;
    static thread_local std::uniform_int_distribution<size_t> shapeDist(0, kJsonCorpusNumShapes - 2);

    const size_t batchSize = Params.UpsertBulkSize;
    const TString& jsonType = Params.GetJsonTypeName();
    const TString tablePath = Params.GetFullTableName(Params.TableName.c_str());

    TStringBuilder queryBuilder;
    queryBuilder << "--!syntax_v1\n";
    queryBuilder << "DECLARE $rows AS List<Struct<Key:Uint64, Text:" << jsonType << "?>>;\n";
    queryBuilder << "UPSERT INTO `" << tablePath << "` SELECT * FROM AS_TABLE($rows);\n";

    NYdb::TParamsBuilder paramsBuilder;
    auto& listBuilder = paramsBuilder.AddParam("$rows").BeginList();

    for (size_t i = 0; i < batchSize; ++i) {
        const ui64 key = idDist(rng);
        const auto shape = static_cast<EJsonShape>(shapeDist(rng));
        const auto row = TJsonCorpus::MakeRow(key, shape);

        auto& item = listBuilder.AddListItem().BeginStruct();
        item.AddMember("Key").Uint64(key);
        const std::optional<std::string> text = row.JsonText
            ? std::optional<std::string>(row.JsonText->c_str())
            : std::nullopt;
        if (Params.Binary) {
            item.AddMember("Text").OptionalJsonDocument(text);
        } else {
            item.AddMember("Text").OptionalJson(text);
        }
        item.EndStruct();
    }
    listBuilder.EndList().Build();

    return TQueryInfoList(1, TQueryInfo(queryBuilder, paramsBuilder.Build()));
}

TVector<IWorkloadQueryGenerator::TWorkloadType> TJsonWorkloadGenerator::GetSupportedWorkloadTypes() const {
    return {
        TWorkloadType(static_cast<int>(EJsonWorkloadType::Select), "select", "Retrieve rows by JSON index predicates"),
        TWorkloadType(static_cast<int>(EJsonWorkloadType::Upsert), "upsert", "Insert or update rows with JSON values"),
    };
}

} // namespace NYdbWorkload
