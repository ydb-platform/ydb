#include "history.h"

#include <util/string/printf.h>

#include <sstream>

namespace NYdbWorkload {
namespace NTPCC {

THistoryLoadQueryGenerator::THistoryLoadQueryGenerator(TTPCCWorkloadParams& params, ui32 partNum, ui64 seed)
    : TLoadQueryGenerator(params, seed)
    , PartNum(partNum)
{
    WarehouseId = (partNum - 1) * params.Warehouses / params.Threads + 1;
    DistrictId = 1;
    CustomerId = 1;
}

bool THistoryLoadQueryGenerator::Finished() {
    i32 whEnd = PartNum * Params.Warehouses / Params.Threads;
    return (WarehouseId > whEnd);
}

TString THistoryLoadQueryGenerator::GetCreateDDL(TString path, ui32 partNum, TString partAtKeys) {
    std::string partNumS = std::to_string(partNum);
    TString query = Sprintf(R"(
        CREATE TABLE `%s/history` (
            H_C_W_ID    Int32,
            H_C_ID      Int32,
            H_C_D_ID    Int32,
            H_D_ID      Int32,
            H_W_ID      Int32,
            H_DATE      Timestamp,
            H_AMOUNT    Double,
            H_DATA      Utf8,
            H_C_NANO_TS Int64        NOT NULL,
            PRIMARY KEY (H_C_W_ID, H_C_NANO_TS)
        ) WITH (
            AUTO_PARTITIONING_BY_LOAD = DISABLED,
            AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %s,
            AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = %s,
            PARTITION_AT_KEYS = (%s)
        );
    )", path.c_str(), partNumS.c_str(), partNumS.c_str(), partAtKeys.c_str());

    return query;
}

std::string THistoryLoadQueryGenerator::GetCleanDDL() {
    std::string cleanQuery = "DROP TABLE `history`;";
    
    return cleanQuery;
}

NYdb::TValue THistoryLoadQueryGenerator::GetNextBatchLoadDDL() {
    NYdb::TValueBuilder rows;
    if (Finished()) {
        return rows.Build();
    }

    rows.BeginList();
    for (i32 id = 1; id <= Params.LoadBatchSize; ++id) {
        rows.AddListItem().BeginStruct();
        rows.AddMember("H_C_ID").Int32(CustomerId);
        rows.AddMember("H_C_D_ID").Int32(DistrictId);
        rows.AddMember("H_C_W_ID").Int32(WarehouseId);
        rows.AddMember("H_D_ID").Int32(DistrictId);
        rows.AddMember("H_W_ID").Int32(WarehouseId);
        rows.AddMember("H_DATE").Timestamp(Now());
        rows.AddMember("H_AMOUNT").Double(10.0);
        rows.AddMember("H_DATA").Utf8(RandomString(UniformRandom32(10, 24, Rng)));
        rows.AddMember("H_C_NANO_TS").Int64(GetNowNanoSeconds());
        rows.EndStruct();

        CustomerId++;
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
    rows.EndList();

    return rows.Build();
}

}
}
