#include "stock.h"

#include <util/string/printf.h>

#include <sstream>

namespace NYdbWorkload {
namespace NTPCC {

TStockLoadQueryGenerator::TStockLoadQueryGenerator(TTPCCWorkloadParams& params, ui32 partNum, ui64 seed)
    : TLoadQueryGenerator(params, seed)
    , PartNum(partNum)
{
    WarehouseId = (partNum - 1) * params.Warehouses / params.Threads + 1;
    ItemId = 1;
}

bool TStockLoadQueryGenerator::Finished() {
    i32 whEnd = PartNum * Params.Warehouses / Params.Threads;
    return (WarehouseId > whEnd);
}

TString TStockLoadQueryGenerator::GetCreateDDL(TString path, ui32 partNum, TString partAtKeys) {
    std::string partNumS = std::to_string(partNum);
    TString query = Sprintf(R"(
        CREATE TABLE `%s/stock` (
            S_W_ID       Int32           NOT NULL,
            S_I_ID       Int32           NOT NULL,
            S_QUANTITY   Int32,
            S_YTD        Double,
            S_ORDER_CNT  Int32,
            S_REMOTE_CNT Int32,
            S_DATA       Utf8,
            S_DIST_01    Utf8,
            S_DIST_02    Utf8,
            S_DIST_03    Utf8,
            S_DIST_04    Utf8,
            S_DIST_05    Utf8,
            S_DIST_06    Utf8,
            S_DIST_07    Utf8,
            S_DIST_08    Utf8,
            S_DIST_09    Utf8,
            S_DIST_10    Utf8,
            PRIMARY KEY (S_W_ID, S_I_ID)
        ) WITH (
            AUTO_PARTITIONING_BY_LOAD = DISABLED,
            AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %s,
            AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = %s,
            PARTITION_AT_KEYS = (%s)
        );
    )", path.c_str(), partNumS.c_str(), partNumS.c_str(), partAtKeys.c_str());

    return query;
}

std::string TStockLoadQueryGenerator::GetCleanDDL() {
    std::string cleanQuery = "DROP TABLE `stock`;";
    
    return cleanQuery;
}

NYdb::TValue TStockLoadQueryGenerator::GetNextBatchLoadDDL() {
    NYdb::TValueBuilder rows;
    if (Finished()) {
        return rows.Build();
    }

    std::uniform_int_distribution<ui64> quantityGen(10, 100);
    std::uniform_int_distribution<ui64> dataLengthGen(26, 50);
    std::uniform_int_distribution<ui64> dataTypeGen(1, 100);
    
    rows.BeginList();
    for (i32 id = 1; id <= Params.LoadBatchSize; ++id) {
        rows.AddListItem().BeginStruct();
        rows.AddMember("S_W_ID").Int32(WarehouseId);
        rows.AddMember("S_I_ID").Int32(ItemId);
        rows.AddMember("S_QUANTITY").Int32(UniformRandom32(10, 100, Rng));
        rows.AddMember("S_ORDER_CNT").Int32(0);
        rows.AddMember("S_REMOTE_CNT").Int32(0);

        ui64 dataType = UniformRandom32(1, 100, Rng);
        ui64 length = UniformRandom32(26, 50, Rng);
        if (dataType > 10) {
            // 90% of time i_data isa random string of length [26 .. 50]
            rows.AddMember("S_DATA").Utf8(RandomString(length));
        } else {
            // 10% of time i_data has "ORIGINAL" crammed somewhere in middle
            ui64 placeForOriginal = UniformRandom32(1, length - 9, Rng);
            rows.AddMember("S_DATA").Utf8(
                RandomString(placeForOriginal) + "ORIGINAL" + RandomString(length - 8 - placeForOriginal)
            );
        }
        
        for (int distNum = 1; distNum <= 9; ++distNum) {
            rows.AddMember("S_DIST_0" + std::to_string(distNum)).Utf8(RandomString(24));
        }
        rows.AddMember("S_DIST_10").Utf8(RandomString(24));
        rows.EndStruct();

        ItemId++;
        if (ItemId > ETPCCWorkloadConstants::TPCC_ITEM_COUNT) {
            ItemId = 1;
            WarehouseId++;
            if (Finished()) {
                break;
            }
        }
    }
    rows.EndList();

    return rows.Build();
}

}
}
