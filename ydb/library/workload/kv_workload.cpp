#include "kv_workload.h"

#include <util/datetime/base.h>

#include <cmath>
#include <iomanip>
#include <string>
#include <thread>
#include <random>

namespace NYdbWorkload {

TKvWorkloadGenerator::TKvWorkloadGenerator(const TKvWorkloadParams* params)
    : DbPath(params->DbPath)
    , Params(*params)
    , Rd()
    , Gen(Rd())
    , UniformDistGen(0, Params.MaxFirstKey)
{
    Gen.seed(Now().MicroSeconds());
}

TKvWorkloadParams* TKvWorkloadGenerator::GetParams() {
    return &Params;
}

std::string TKvWorkloadGenerator::GetDDLQueries() const {
    std::string partsNum = std::to_string(Params.MinPartitions);
    std::string KvPartitionsDdl = "";

    if (Params.PartitionsByLoad) {
        KvPartitionsDdl = "WITH (AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = " + 
            partsNum + ", UNIFORM_PARTITIONS = " + partsNum + ", AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 1000)";
    } else {
        KvPartitionsDdl = "WITH (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = " + 
            partsNum + ", UNIFORM_PARTITIONS = " + partsNum + ", AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 1000)";
    }

    static const char TablesDdl[] = R"(--!syntax_v1
        CREATE TABLE `%s/kv_test`(a Uint64, b Uint64, PRIMARY KEY(a, b)) %s;
    )";

    char buf[sizeof(TablesDdl) + sizeof(KvPartitionsDdl) + 8192*3]; // 32*256 for DbPath
    int res = std::sprintf(buf, TablesDdl, 
        DbPath.c_str(), KvPartitionsDdl.c_str()
    );

    if (res < 0) {
        return "";
    }

    return buf;
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
    std::string query = R"(
        --!syntax_v1
        DECLARE $a AS Uint64;
        DECLARE $b AS Uint64;

        UPSERT INTO `kv_test` (a, b) VALUES ($a, $b);
    )";

    ui64 a = UniformDistGen(Gen);
    ui64 b = Gen();

    NYdb::TParamsBuilder paramsBuilder;
    auto params = paramsBuilder
        .AddParam("$a")
            .Uint64(a)
            .Build()
        .AddParam("$b")
            .Uint64(b)
            .Build()
        .Build();

    return TQueryInfoList(1, TQueryInfo(query, std::move(params)));
}

TQueryInfoList TKvWorkloadGenerator::SelectRandom() {
    std::string query = R"(
        --!syntax_v1
        DECLARE $a AS Uint64;

        SELECT * FROM `kv_test` WHERE a = $a
    )";

    ui64 a = UniformDistGen(Gen);

    NYdb::TParamsBuilder paramsBuilder;
    auto params = paramsBuilder
        .AddParam("$a")
            .Uint64(a)
            .Build()
        .Build();

    return TQueryInfoList(1, TQueryInfo(query, std::move(params)));
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