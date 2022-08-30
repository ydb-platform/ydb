#include "kv_workload.h"

#include <util/datetime/base.h>

#include <ydb/core/util/lz4_data_generator.h>

#include <cmath>
#include <iomanip>
#include <string>
#include <thread>
#include <random>
#include <sstream>

namespace NYdbWorkload {

TKvWorkloadGenerator::TKvWorkloadGenerator(const TKvWorkloadParams* params)
    : DbPath(params->DbPath)
    , Params(*params)
    , BigString(NKikimr::GenDataForLZ4(Params.StringLen))
    , Rd()
    , Gen(Rd())
    , KeyUniformDistGen(0, Params.MaxFirstKey)
{
    Gen.seed(Now().MicroSeconds());
}

TKvWorkloadParams* TKvWorkloadGenerator::GetParams() {
    return &Params;
}

std::string TKvWorkloadGenerator::GetDDLQueries() const {
    std::string partsNum = std::to_string(Params.MinPartitions);

    std::stringstream ss;

    ss << "--!syntax_v1\n";
    ss << "CREATE TABLE `" << DbPath << "/kv_test`(c0 Uint64, ";

    for (size_t i = 1; i < Params.ColumnsCnt; ++i) {
        ss << "c" << i << " " << "String, ";
    }

    ss << "PRIMARY KEY(c0)) WITH (";

    if (Params.PartitionsByLoad) {
        ss << "AUTO_PARTITIONING_BY_LOAD = ENABLED, ";
    }

    ss << "AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = " << partsNum << ", "
       << "UNIFORM_PARTITIONS = " << partsNum << ", AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 1000)";

    return ss.str();
}

TQueryInfoList TKvWorkloadGenerator::GetWorkload(int type) {
    switch (static_cast<EType>(type)) {
        case EType::UpsertRandom:
            return UpsertRandom();
        case EType::SelectRandom:
            return SelectRandom();
        default:
            return TQueryInfoList();
    }
}

TQueryInfoList TKvWorkloadGenerator::UpsertRandom() {
    std::stringstream ss;

    NYdb::TParamsBuilder paramsBuilder;

    ss << "--!syntax_v1\n";
    ss << "DECLARE $c0 AS Uint64;\n";
    paramsBuilder.AddParam("$c0").Uint64(KeyUniformDistGen(Gen)).Build();

    for (size_t col = 1; col < Params.ColumnsCnt; ++col) {
        ss << "DECLARE $c" << col << " AS String;\n";
        paramsBuilder.AddParam("$c" + std::to_string(col)).String(BigString).Build();
    }

    ss << "UPSERT INTO `kv_test`(";

    for (size_t col = 0; col < Params.ColumnsCnt; ++col) {
        ss << "c" << col;
        if (col + 1 < Params.ColumnsCnt) {
            ss << ", ";
        }
    }

    ss << ") VALUES (";

    for (size_t col = 0; col < Params.ColumnsCnt; ++col) {
        ss << "$c" << col;
        if (col + 1 < Params.ColumnsCnt) {
            ss << ", ";
        }
    }

    ss << ")";

    auto params = paramsBuilder.Build();

    return TQueryInfoList(1, TQueryInfo(ss.str(), std::move(params)));
}

TQueryInfoList TKvWorkloadGenerator::SelectRandom() {
    std::stringstream ss;

    ss << "--!syntax_v1\n";
    ss << "DECLARE $c0 AS Uint64;\n";
    ss << "SELECT ";
    for (size_t col = 0; col < Params.ColumnsCnt; ++col) {
        ss << "c" << col;
        if (col + 1 < Params.ColumnsCnt) {
            ss << ",";
        }
        ss << " ";
    }

    ss << "FROM `kv_test` WHERE c0 = $c0";

    ui64 x = KeyUniformDistGen(Gen);

    NYdb::TParamsBuilder paramsBuilder;
    auto params = paramsBuilder
        .AddParam("$c0")
            .Uint64(x)
            .Build()
        .Build();

    return TQueryInfoList(1, TQueryInfo(ss.str(), std::move(params)));
}

TQueryInfoList TKvWorkloadGenerator::GetInitialData() {
    TQueryInfoList res;
    for (size_t i = 0; i < Params.InitRowCount; ++i) {
        auto queryInfos = UpsertRandom();
        res.insert(res.end(), queryInfos.begin(), queryInfos.end());
    }

    return res;
}

std::string TKvWorkloadGenerator::GetCleanDDLQueries() const {
    std::string query = R"(
        DROP TABLE `kv_test`;
    )";

    return query;
}

}