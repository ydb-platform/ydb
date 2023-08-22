#include "kv_workload.h"
#include "util/random/random.h"

#include <util/datetime/base.h>

#include <ydb/core/util/lz4_data_generator.h>

#include <cmath>
#include <iomanip>
#include <string>
#include <thread>
#include <random>
#include <sstream>

template <>
void Out<NYdbWorkload::KvWorkloadConstants>(IOutputStream& out, NYdbWorkload::KvWorkloadConstants constant)
{
    out << static_cast<ui64>(constant);
}

namespace NYdbWorkload {

TKvWorkloadGenerator::TKvWorkloadGenerator(const TKvWorkloadParams* params)
    : DbPath(params->DbPath)
    , Params(*params)
    , BigString(NKikimr::GenDataForLZ4(Params.StringLen))
{
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
        case EType::InsertRandom:
            return InsertRandom();
        case EType::SelectRandom:
            return SelectRandom();
        case EType::ReadRowsRandom:
            return ReadRowsRandom();
        default:
            return TQueryInfoList();
    }
}


TQueryInfoList TKvWorkloadGenerator::AddOperation(TString operation) {
    std::stringstream ss;

    NYdb::TParamsBuilder paramsBuilder;

    ss << "--!syntax_v1\n";

    for (size_t row = 0; row < Params.RowsCnt; ++row) {
        TString pkname = "$r" + std::to_string(row);
        ss << "DECLARE " << pkname << " AS Uint64;\n";
        paramsBuilder.AddParam(pkname).Uint64(RandomNumber<ui64>(Params.MaxFirstKey)).Build();

        for (size_t col = 1; col < Params.ColumnsCnt; ++col) {
            TString cname = "$c" + std::to_string(row) + std::to_string(col);
            ss << "DECLARE " << cname << " AS String;\n";
            paramsBuilder.AddParam(cname).String(BigString).Build();
        }
    }

    ss << operation << " INTO `kv_test`(";

    for (size_t col = 0; col < Params.ColumnsCnt; ++col) {
        ss << "c" << col;
        if (col + 1 < Params.ColumnsCnt) {
            ss << ", ";
        }
    }

    ss << ") VALUES ";

    for (size_t row = 0; row < Params.RowsCnt; ++row) {

        ss << "(";
        ss << "$r" << row;

        for (size_t col = 1; col < Params.ColumnsCnt; ++col) {
            ss << ", $c" << row << col;
        }

        ss << ")";

        if (row + 1 < Params.RowsCnt) {
            ss << ", ";
        }

    }

    auto params = paramsBuilder.Build();

    return TQueryInfoList(1, TQueryInfo(ss.str(), std::move(params)));
}

TQueryInfoList TKvWorkloadGenerator::UpsertRandom() {
    return AddOperation("UPSERT");
}

TQueryInfoList TKvWorkloadGenerator::InsertRandom() {
    return AddOperation("INSERT");
}

TQueryInfoList TKvWorkloadGenerator::SelectRandom() {
    std::stringstream ss;

    NYdb::TParamsBuilder paramsBuilder;

    ss << "--!syntax_v1\n";

    for (size_t row = 0; row < Params.RowsCnt; ++row) {
        ss << "DECLARE $r" << row << " AS Uint64;\n";
        paramsBuilder.AddParam("$r" + std::to_string(row)).Uint64(RandomNumber<ui64>(Params.MaxFirstKey)).Build();
    }

    ss << "SELECT ";
    for (size_t col = 0; col < Params.ColumnsCnt; ++col) {
        ss << "c" << col;
        if (col + 1 < Params.ColumnsCnt) {
            ss << ",";
        }
        ss << " ";
    }

    ss << "FROM `kv_test` WHERE ";
    for (size_t row = 0; row < Params.RowsCnt; ++row) {
        ss << "c0 = $r" << row;
        if (row + 1 < Params.RowsCnt) {
            ss << " OR ";
        }
    }

    auto params = paramsBuilder.Build();

    return TQueryInfoList(1, TQueryInfo(ss.str(), std::move(params)));
}

TQueryInfoList TKvWorkloadGenerator::ReadRowsRandom() {
    NYdb::TValueBuilder keys;
    keys.BeginList();
    for (size_t i = 0; i < Params.RowsCnt; ++i) {
        keys.AddListItem()
            .BeginStruct()
                .AddMember("c0").Uint64(RandomNumber<ui64>(Params.MaxFirstKey))
            .EndStruct();
    }
    keys.EndList();

    TQueryInfo info;
    info.TablePath = DbPath + "/kv_test";
    info.UseReadRows = true;
    info.KeyToRead = keys.Build();
    return TQueryInfoList(1, std::move(info));
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