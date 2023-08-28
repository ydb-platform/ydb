#include "oorder.h"

#include <util/string/printf.h>

#include <sstream>

namespace NYdbWorkload {
namespace NTPCC {

TOorderLoadQueryGenerator::TOorderLoadQueryGenerator(TTPCCWorkloadParams& params, ui32 partNum, ui64 seed)
    : TLoadQueryGenerator(params, seed)
    , PartNum(partNum)
    , CustShuffle(ETPCCWorkloadConstants::TPCC_CUST_PER_DIST)
{
    WarehouseId = (partNum - 1) * params.Warehouses / params.Threads + 1;
    DistrictId = 1;
    CustomerId = 0;
    for (ui64 id = 0; id < CustShuffle.size(); ++id) {
        CustShuffle[id] = id + 1;
    }
    std::shuffle(CustShuffle.begin(), CustShuffle.end(), Rng);
}

bool TOorderLoadQueryGenerator::Finished() {
    i32 whEnd = PartNum * Params.Warehouses / Params.Threads;
    return (WarehouseId > whEnd);
}

TString TOorderLoadQueryGenerator::GetCreateDDL(TString path, ui32 partNum, TString partAtKeys) {
    std::string partNumS = std::to_string(partNum);
    TString query = Sprintf(R"(
        CREATE TABLE `%s/oorder` (
            O_W_ID       Int32       NOT NULL,
            O_D_ID       Int32       NOT NULL,
            O_ID         Int32       NOT NULL,
            O_C_ID       Int32,
            O_CARRIER_ID Int32,
            O_OL_CNT     Int32,
            O_ALL_LOCAL  Int32,
            O_ENTRY_D    Timestamp,
            PRIMARY KEY (O_W_ID, O_D_ID, O_ID)
        ) WITH (
            AUTO_PARTITIONING_BY_LOAD = DISABLED,
            AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %s,
            AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = %s,
            PARTITION_AT_KEYS = (%s)
        );
    )", path.c_str(), partNumS.c_str(), partNumS.c_str(), partAtKeys.c_str());

    return query;
}

std::string TOorderLoadQueryGenerator::GetCleanDDL() {
    std::string cleanQuery = "DROP TABLE `oorder`;";
    
    return cleanQuery;
}

NYdb::TValue TOorderLoadQueryGenerator::GetNextBatchLoadDDL() {
    NYdb::TValueBuilder rows;
    if (Finished()) {
        return rows.Build();
    }

    rows.BeginList();
    for (i32 id = 1; id <= Params.LoadBatchSize; ++id) {
        rows.AddListItem().BeginStruct();
        rows.AddMember("O_W_ID").Int32(WarehouseId);
        rows.AddMember("O_D_ID").Int32(DistrictId);
        rows.AddMember("O_ID").Int32(CustomerId);
        rows.AddMember("O_C_ID").Int32(CustShuffle[CustomerId]);
        if (CustomerId + 1 < ETPCCWorkloadConstants::TPCC_FIRST_UNPROCESSED_O_ID) {
            rows.AddMember("O_CARRIER_ID").Int32(UniformRandom32(10, 24, Rng));
        } else {
            rows.AddMember("O_CARRIER_ID").Int32(0);
        }
        rows.AddMember("O_OL_CNT").Int32(UniformRandom32(5, 15, Rng));
        rows.AddMember("O_ALL_LOCAL").Int32(1);
        rows.AddMember("O_ENTRY_D").Timestamp(Now());
        rows.EndStruct();

        CustomerId++;
        if (CustomerId == ETPCCWorkloadConstants::TPCC_CUST_PER_DIST) {
            CustomerId = 0;
            DistrictId++;
            std::shuffle(CustShuffle.begin(), CustShuffle.end(), Rng);
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
