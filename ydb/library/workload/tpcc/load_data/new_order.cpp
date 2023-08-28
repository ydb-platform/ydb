#include "new_order.h"

#include <util/string/printf.h>

#include <sstream>

namespace NYdbWorkload {
namespace NTPCC {

TNewOrderLoadQueryGenerator::TNewOrderLoadQueryGenerator(TTPCCWorkloadParams& params, ui32 partNum, ui64 seed)
    : TLoadQueryGenerator(params, seed)
    , PartNum(partNum)
{
    WarehouseId = (partNum - 1) * params.Warehouses / params.Threads + 1;
    DistrictId = 1;
    // 900 rows in the NEW-ORDER table corresponding to the last
    // 900 rows in the ORDER table for that district
    // (i.e., with NO_O_ID between 2,101 and 3,000)
    CustomerId = ETPCCWorkloadConstants::TPCC_FIRST_UNPROCESSED_O_ID;
}

bool TNewOrderLoadQueryGenerator::Finished() {
    i32 whEnd = PartNum * Params.Warehouses / Params.Threads;
    return (WarehouseId > whEnd);
}

TString TNewOrderLoadQueryGenerator::GetCreateDDL(TString path, ui32 partNum, TString partAtKeys) {
    std::string partNumS = std::to_string(partNum);
    TString query = Sprintf(R"(
        CREATE TABLE `%s/new_order` (
            NO_W_ID Int32 NOT NULL,
            NO_D_ID Int32 NOT NULL,
            NO_O_ID Int32 NOT NULL,
            PRIMARY KEY (NO_W_ID, NO_D_ID, NO_O_ID)
        ) WITH (
            AUTO_PARTITIONING_BY_LOAD = DISABLED,
            AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %s,
            AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = %s,
            PARTITION_AT_KEYS = (%s)
        );
    )", path.c_str(), partNumS.c_str(), partNumS.c_str(), partAtKeys.c_str());

    return query;
}

std::string TNewOrderLoadQueryGenerator::GetCleanDDL() {
    std::string cleanQuery = "DROP TABLE `new_order`;";
    
    return cleanQuery;
}

NYdb::TValue TNewOrderLoadQueryGenerator::GetNextBatchLoadDDL() {
    NYdb::TValueBuilder rows;
    if (Finished()) {
        return rows.Build();
    }

    rows.BeginList();
    for (i32 id = 1; id <= Params.LoadBatchSize; ++id) {
        rows.AddListItem().BeginStruct();
        rows.AddMember("NO_W_ID").Int32(WarehouseId);
        rows.AddMember("NO_D_ID").Int32(DistrictId);
        rows.AddMember("NO_O_ID").Int32(CustomerId);
        rows.EndStruct();

        CustomerId++;
        if (CustomerId > ETPCCWorkloadConstants::TPCC_CUST_PER_DIST) {
            CustomerId = ETPCCWorkloadConstants::TPCC_FIRST_UNPROCESSED_O_ID;
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
    rows.EndList();

    return rows.Build();
}

}
}
