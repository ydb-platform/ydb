#include "district.h"

#include <util/string/printf.h>

#include <sstream>

namespace NYdbWorkload {
namespace NTPCC {

TDistrictLoadQueryGenerator::TDistrictLoadQueryGenerator(TTPCCWorkloadParams& params, ui64 seed)
    : TLoadQueryGenerator(params, seed)
{
    WarehouseId = 1;
    DistrictId = 1;
}

bool TDistrictLoadQueryGenerator::Finished() {
    return (WarehouseId > Params.Warehouses);
}


TString TDistrictLoadQueryGenerator::GetCreateDDL(TString path) {
    TString query = Sprintf(R"(
        CREATE TABLE `%s/district` (
            D_W_ID      Int32            NOT NULL,
            D_ID        Int32            NOT NULL,
            D_YTD       Double,
            D_TAX       Double,
            D_NEXT_O_ID Int32,
            D_NAME      Utf8,
            D_STREET_1  Utf8,
            D_STREET_2  Utf8,
            D_CITY      Utf8,
            D_STATE     Utf8,
            D_ZIP       Utf8,
            PRIMARY KEY (D_W_ID, D_ID)
        ) WITH (
            AUTO_PARTITIONING_BY_LOAD = DISABLED
        );
    )", path.c_str());

    return query;
}

std::string TDistrictLoadQueryGenerator::GetCleanDDL() {
    std::string cleanQuery = "DROP TABLE `district`;";
    
    return cleanQuery;
}

NYdb::TValue TDistrictLoadQueryGenerator::GetNextBatchLoadDDL() {
    NYdb::TValueBuilder rows;
    if (Finished()) {
        return rows.Build();
    }

    rows.BeginList();
    for (i32 id = 1; id <= Params.LoadBatchSize; ++id) {
        rows.AddListItem().BeginStruct();
        rows.AddMember("D_W_ID").Int32(WarehouseId);
        rows.AddMember("D_ID").Int32(DistrictId);
        rows.AddMember("D_YTD").Double(300000);
        rows.AddMember("D_TAX").Double(static_cast<double>(UniformRandom32(0, 2000, Rng)) / 10000.0);
        rows.AddMember("D_NEXT_O_ID").Int32(ETPCCWorkloadConstants::TPCC_CUST_PER_DIST + 1);
        rows.AddMember("D_NAME").Utf8(RandomString(UniformRandom32(6, 10, Rng)));
        rows.AddMember("D_STREET_1").Utf8(RandomString(UniformRandom32(10, 20, Rng)));
        rows.AddMember("D_STREET_2").Utf8(RandomString(UniformRandom32(10, 20, Rng)));
        rows.AddMember("D_CITY").Utf8(RandomString(UniformRandom32(10, 20, Rng)));
        rows.AddMember("D_STATE").Utf8(RandomString(2));
        rows.AddMember("D_ZIP").Utf8("123456789");
        rows.EndStruct();

        DistrictId++;
        if (DistrictId > ETPCCWorkloadConstants::TPCC_DIST_PER_WH) {
            DistrictId = 1;
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
