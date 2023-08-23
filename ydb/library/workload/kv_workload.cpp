#include "kv_workload.h"
#include "format"
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
    ss << "CREATE TABLE `" << DbPath << "/kv_test`(";

    for (size_t i = 0; i < Params.ColumnsCnt; ++i) {
        if (i < Params.IntColumnsCnt) {
            ss << "c" << i << " Uint64, ";
        } else {
            ss << "c" << i << " String, ";
        }
    }

    ss << "PRIMARY KEY(";
    for (size_t i = 0; i < Params.KeyColumnsCnt; ++i) {
        ss << "c" << i;
        if (i + 1 < Params.KeyColumnsCnt) {
            ss << ", ";
        }
    }
    ss << ")) WITH (";

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
        for (size_t col = 0; col < Params.ColumnsCnt; ++col) {
            TString cname = "$c" + std::to_string(row) + "_" + std::to_string(col);
            if (col < Params.IntColumnsCnt) {
                ss << "DECLARE " << cname << " AS Uint64;\n";
            } else {
                ss << "DECLARE " << cname << " AS String;\n";
            }
            if (col < Params.IntColumnsCnt) {
                ui64 val = col < Params.KeyColumnsCnt ? RandomNumber<ui64>(Params.MaxFirstKey) : RandomNumber<ui64>();
                paramsBuilder.AddParam(cname).Uint64(val).Build();
            } else {
                TString val = col < Params.KeyColumnsCnt ? TString(std::format("{:x}", RandomNumber<ui64>())) : BigString;
                paramsBuilder.AddParam(cname).String(val).Build();
            }
        }
    }

    ss << operation << " INTO `kv_test` (";

    for (size_t col = 0; col < Params.ColumnsCnt; ++col) {
        ss << "c" << col;
        if (col + 1 < Params.ColumnsCnt) {
            ss << ", ";
        }
    }

    ss << ") VALUES (";

    for (size_t row = 0; row < Params.RowsCnt; ++row) {
        for (size_t col = 0; col < Params.ColumnsCnt; ++col) {
            ss << "$c" << row << "_" << col;
            if (col + 1 < Params.ColumnsCnt) {
                ss << ", ";
            }
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
        for (size_t col = 0; col < Params.KeyColumnsCnt; ++col) {
            TString paramName = "$r" + std::to_string(row) + "_" + std::to_string(col);
            if (col < Params.IntColumnsCnt) {
                ss << "DECLARE " << paramName << " AS Uint64;\n";
                paramsBuilder.AddParam(paramName).Uint64(RandomNumber<ui64>(Params.MaxFirstKey)).Build();
            } else {
                ss << "DECLARE " << paramName << " AS String;\n";
                paramsBuilder.AddParam(paramName).String(std::format("{:x}", RandomNumber<ui64>())).Build();
            }
        }
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
        for (size_t col = 0; col < Params.KeyColumnsCnt; ++col) {
            TString paramName = "$r" + std::to_string(row) + "_" + std::to_string(col);
            ss << "c" << col << " = " << paramName;
            if (col + 1 < Params.KeyColumnsCnt) {
                ss << " AND ";
            }
        }
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
        keys.AddListItem().BeginStruct();
        for (size_t col = 0; col < Params.KeyColumnsCnt; ++col) {
            if (col < Params.IntColumnsCnt) {
                keys.AddMember("c" + std::to_string(col)).Uint64(RandomNumber<ui64>(Params.MaxFirstKey));
            } else {
                keys.AddMember("c" + std::to_string(col)).String(std::format("{:x}", RandomNumber<ui64>()));
            }
        }
        keys.EndStruct();
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