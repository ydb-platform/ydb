#include "order_line.h"

#include <util/string/printf.h>

#include <sstream>

namespace NYdbWorkload {
namespace NTPCC {

TOrderLineLoadQueryGenerator::TOrderLineLoadQueryGenerator(TTPCCWorkloadParams& params, ui32 partNum, ui64 seed)
    : TLoadQueryGenerator(params, seed)
    , PartNum(partNum)
{
    WarehouseId = (partNum - 1) * params.Warehouses / params.Threads + 1;
    DistrictId = 1;
    CustomerId = 1;
    OlNumber = 1;
    RandomCount = UniformRandom32(5, 15, Rng);
}

bool TOrderLineLoadQueryGenerator::Finished() {
    i32 whEnd = PartNum * Params.Warehouses / Params.Threads;
    return (WarehouseId > whEnd);
}

TString TOrderLineLoadQueryGenerator::GetCreateDDL(TString path, ui32 partNum, TString partAtKeys) {
    std::string partNumS = std::to_string(partNum);
    TString query = Sprintf(R"(
        CREATE TABLE `%s/order_line` (
            OL_W_ID        Int32           NOT NULL,
            OL_D_ID        Int32           NOT NULL,
            OL_O_ID        Int32           NOT NULL,
            OL_NUMBER      Int32           NOT NULL,
            OL_I_ID        Int32,
            OL_DELIVERY_D  Timestamp,
            OL_AMOUNT      Double,
            OL_SUPPLY_W_ID Int32,
            OL_QUANTITY    Double,
            OL_DIST_INFO   Utf8,
            PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER)
        ) WITH (
            AUTO_PARTITIONING_BY_LOAD = DISABLED,
            AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %s,
            AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = %s,
            PARTITION_AT_KEYS = (%s)
        );
    )", path.c_str(), partNumS.c_str(), partNumS.c_str(), partAtKeys.c_str());

    return query;
}

std::string TOrderLineLoadQueryGenerator::GetCleanDDL() {
    std::string cleanQuery = "DROP TABLE `order_line`;";
    
    return cleanQuery;
}

NYdb::TValue TOrderLineLoadQueryGenerator::GetNextBatchLoadDDL() {
    NYdb::TValueBuilder rows;
    if (Finished()) {
        return rows.Build();
    }

    rows.BeginList();
    for (i32 id = 1; id <= Params.LoadBatchSize; ++id) {
        rows.AddListItem().BeginStruct();
        rows.AddMember("OL_W_ID").Int32(WarehouseId);
        rows.AddMember("OL_D_ID").Int32(DistrictId);
        rows.AddMember("OL_O_ID").Int32(CustomerId);
        rows.AddMember("OL_NUMBER").Int32(OlNumber);

        ui64 itemId = UniformRandom32(1, ETPCCWorkloadConstants::TPCC_ITEM_COUNT, Rng);
        rows.AddMember("OL_I_ID").Int32(itemId);
        if (itemId < ETPCCWorkloadConstants::TPCC_FIRST_UNPROCESSED_O_ID) {
            rows.AddMember("OL_DELIVERY_D").Timestamp(Now());
            rows.AddMember("OL_AMOUNT").Double(0.0);
        } else {
            rows.AddMember("OL_DELIVERY_D").Timestamp(TInstant::Zero());
            rows.AddMember("OL_AMOUNT").Double(static_cast<double>(UniformRandom32(1, 999999, Rng)) / 100.0);
        }

        rows.AddMember("OL_SUPPLY_W_ID").Int32(WarehouseId);
        rows.AddMember("OL_QUANTITY").Double(5.0);
        rows.AddMember("OL_DIST_INFO").Utf8(RandomString(24));
        rows.EndStruct();

        OlNumber++;
        if (OlNumber > RandomCount) {
            CustomerId++;
            OlNumber = 1;
            RandomCount = UniformRandom32(5, 15, Rng);
            if (CustomerId > ETPCCWorkloadConstants::TPCC_CUST_PER_DIST) {
                CustomerId = 1;
                DistrictId++;
                if (DistrictId > ETPCCWorkloadConstants::TPCC_DIST_PER_WH) {
                    DistrictId = 1;
                    WarehouseId++;
                    if (Finished()) {
                        break;
                    }
                }
            }
        }
    }
    rows.EndList();

    return rows.Build();
}

}
}
