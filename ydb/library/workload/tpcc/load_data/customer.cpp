#include "customer.h"

#include <util/string/printf.h>

#include <sstream>

namespace NYdbWorkload {
namespace NTPCC {

TCustomerLoadQueryGenerator::TCustomerLoadQueryGenerator(TTPCCWorkloadParams& params, ui32 partNum, ui64 seed)
    : TLoadQueryGenerator(params, seed)
    , PartNum(partNum)
{
    WarehouseId = (partNum - 1) * params.Warehouses / params.Threads + 1;
    DistrictId = 1;
    CustomerId = 1;
}

bool TCustomerLoadQueryGenerator::Finished() {
    i32 whEnd = PartNum * Params.Warehouses / Params.Threads;
    return (WarehouseId > whEnd);
}

TString TCustomerLoadQueryGenerator::GetCreateDDL(TString path, ui32 partNum, TString partAtKeys) {
    std::string partNumS = std::to_string(partNum);
    TString query = Sprintf(R"(
        CREATE TABLE `%s/customer` (
            C_W_ID         Int32            NOT NULL,
            C_D_ID         Int32            NOT NULL,
            C_ID           Int32            NOT NULL,
            C_DISCOUNT     Double,
            C_CREDIT       Utf8,
            C_LAST         Utf8,
            C_FIRST        Utf8,
            C_CREDIT_LIM   Double,
            C_BALANCE      Double,
            C_YTD_PAYMENT  Double,
            C_PAYMENT_CNT  Int32,
            C_DELIVERY_CNT Int32,
            C_STREET_1     Utf8,
            C_STREET_2     Utf8,
            C_CITY         Utf8,
            C_STATE        Utf8,
            C_ZIP          Utf8,
            C_PHONE        Utf8,
            C_SINCE        Timestamp,
            C_MIDDLE       Utf8,
            C_DATA         Utf8,
            PRIMARY KEY (C_W_ID, C_D_ID, C_ID)
        ) WITH (
            AUTO_PARTITIONING_BY_LOAD = DISABLED,
            AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %s,
            AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = %s,
            PARTITION_AT_KEYS = (%s)
        );
    )", path.c_str(), partNumS.c_str(), partNumS.c_str(), partAtKeys.c_str());

    return query;
}

std::string TCustomerLoadQueryGenerator::GetCleanDDL() {
    std::string cleanQuery = "DROP TABLE `customer`;";
    
    return cleanQuery;
}

NYdb::TValue TCustomerLoadQueryGenerator::GetNextBatchLoadDDL() {
    NYdb::TValueBuilder rows;
    if (Finished()) {
        return rows.Build();
    }

    rows.BeginList();
    for (i32 id = 1; id <= Params.LoadBatchSize; ++id) {
        rows.AddListItem().BeginStruct();
        rows.AddMember("C_W_ID").Int32(WarehouseId);
        rows.AddMember("C_D_ID").Int32(DistrictId);
        rows.AddMember("C_ID").Int32(CustomerId);
        rows.AddMember("C_DISCOUNT").Double(static_cast<double>(UniformRandom32(1, 5000, Rng)) / 10000.0);
        if (UniformRandom32(1, 100, Rng) <= 10) {
            rows.AddMember("C_CREDIT").Utf8("BC");
        } else {
            rows.AddMember("C_CREDIT").Utf8("GC");
        }
        if (CustomerId <= 1000) {
            rows.AddMember("C_LAST").Utf8(GetLastName(CustomerId - 1));
        } else {
            rows.AddMember("C_LAST").Utf8(GetNonUniformRandomLastNameForLoad(Rng));
        }
        rows.AddMember("C_FIRST").Utf8(RandomString(UniformRandom32(8, 16, Rng)));
        rows.AddMember("C_CREDIT_LIM").Double(50000.0);
        rows.AddMember("C_BALANCE").Double(-10.0);
        rows.AddMember("C_YTD_PAYMENT").Double(10.0);
        rows.AddMember("C_PAYMENT_CNT").Int32(1);
        rows.AddMember("C_DELIVERY_CNT").Int32(0);
        rows.AddMember("C_STREET_1").Utf8(RandomString(UniformRandom32(10, 20, Rng)));
        rows.AddMember("C_STREET_2").Utf8(RandomString(UniformRandom32(10, 20, Rng)));
        rows.AddMember("C_CITY").Utf8(RandomString(UniformRandom32(10, 20, Rng)));
        rows.AddMember("C_STATE").Utf8(RandomString(2));
        rows.AddMember("C_ZIP").Utf8(RandomNumberString(4) + "11111");
        rows.AddMember("C_PHONE").Utf8(RandomNumberString(16));
        rows.AddMember("C_SINCE").Timestamp(Now());
        rows.AddMember("C_MIDDLE").Utf8("OE");
        rows.AddMember("C_DATA").Utf8(RandomString(UniformRandom32(300, 500, Rng)));
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
