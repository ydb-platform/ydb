#include "warehouse.h"

#include <util/string/printf.h>

#include <sstream>

namespace NYdbWorkload {
namespace NTPCC {

TWarehouseLoadQueryGenerator::TWarehouseLoadQueryGenerator(TTPCCWorkloadParams& params, ui64 seed)
    : TLoadQueryGenerator(params, seed)
{
    WarehouseId = 1;
}

bool TWarehouseLoadQueryGenerator::Finished() {
    return (WarehouseId > Params.Warehouses);
}

TString TWarehouseLoadQueryGenerator::GetCreateDDL(TString path) {
    TString query = Sprintf(R"(
        CREATE TABLE `%s/warehouse` (
            W_ID       Int32          NOT NULL,
            W_YTD      Double,
            W_TAX      Double,
            W_NAME     Utf8,
            W_STREET_1 Utf8,
            W_STREET_2 Utf8,
            W_CITY     Utf8,
            W_STATE    Utf8,
            W_ZIP      Utf8,
            PRIMARY KEY (W_ID)
        ) WITH (
            AUTO_PARTITIONING_BY_LOAD = DISABLED
        );
    )", path.c_str());

    return query;
}

std::string TWarehouseLoadQueryGenerator::GetCleanDDL() {
    std::string cleanQuery = "DROP TABLE `warehouse`;";
    
    return cleanQuery;
}

NYdb::TValue TWarehouseLoadQueryGenerator::GetNextBatchLoadDDL() {
    NYdb::TValueBuilder rows;
    if (Finished()) {
        return rows.Build();
    }

    i32 whEnd = std::min(Params.Warehouses, WarehouseId + Params.LoadBatchSize - 1);

    rows.BeginList();
    for (i32 row = WarehouseId; row <= whEnd; ++row) {
        rows.AddListItem().BeginStruct();
        rows.AddMember("W_ID").Int32(row);
        rows.AddMember("W_YTD").Double(300000);
        rows.AddMember("W_TAX").Double(static_cast<double>(UniformRandom32(0, 2000, Rng)) / 10000.0);
        rows.AddMember("W_NAME").Utf8(RandomString(UniformRandom32(6, 10, Rng)));
        rows.AddMember("W_STREET_1").Utf8(RandomString(UniformRandom32(10, 20, Rng)));
        rows.AddMember("W_STREET_2").Utf8(RandomString(UniformRandom32(10, 20, Rng)));
        rows.AddMember("W_CITY").Utf8(RandomString(UniformRandom32(10, 20, Rng)));
        rows.AddMember("W_STATE").Utf8(RandomString(2));
        rows.AddMember("W_ZIP").Utf8("123456789");
        rows.EndStruct();
    }
    rows.EndList();

    WarehouseId = whEnd + 1;

    return rows.Build();
}

}
}
