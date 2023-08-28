#include "item.h"

#include <util/string/printf.h>

#include <sstream>

namespace NYdbWorkload {
namespace NTPCC {

TItemLoadQueryGenerator::TItemLoadQueryGenerator(TTPCCWorkloadParams& params, ui64 seed)
    : TLoadQueryGenerator(params, seed)
{
    ItemId = 1;  
}

bool TItemLoadQueryGenerator::Finished() {
    return (ItemId > ETPCCWorkloadConstants::TPCC_ITEM_COUNT);
}

TString TItemLoadQueryGenerator::GetCreateDDL(TString path) {
    TString query = Sprintf(R"(
        CREATE TABLE `%s/item` (
            I_ID    Int32           NOT NULL,
            I_NAME  Utf8,
            I_PRICE Double,
            I_DATA  Utf8,
            I_IM_ID Int32,
            PRIMARY KEY (I_ID)
        ) WITH (
            AUTO_PARTITIONING_BY_LOAD = DISABLED
        );
    )", path.c_str());

    return query;
}

std::string TItemLoadQueryGenerator::GetCleanDDL() {
    std::string cleanQuery = "DROP TABLE `item`;";
    
    return cleanQuery;
}

NYdb::TValue TItemLoadQueryGenerator::GetNextBatchLoadDDL() {
    NYdb::TValueBuilder rows;
    if (Finished()) {
        return rows.Build();
    }
    i32 partStart = ItemId;
    i32 partEnd = std::min(static_cast<i32>(ETPCCWorkloadConstants::TPCC_ITEM_COUNT),
                           partStart + Params.LoadBatchSize - 1);

    rows.BeginList();
    for (i32 row = partStart; row <= partEnd; ++row) {
        rows.AddListItem().BeginStruct();
        rows.AddMember("I_ID").Int32(row);
        rows.AddMember("I_NAME").Utf8(RandomString(UniformRandom32(14, 24, Rng)));
        rows.AddMember("I_PRICE").Double(static_cast<double>(UniformRandom32(100, 10000, Rng)) / 100.0);

        ui64 dataType = UniformRandom32(1, 100, Rng);
        ui64 length = UniformRandom32(26, 50, Rng);
        if (dataType > 10) {
            // 90% of time i_data isa random string of length [26 .. 50]
            rows.AddMember("I_DATA").Utf8(RandomString(length));
        } else {
            // 10% of time i_data has "ORIGINAL" crammed somewhere in middle
            ui64 placeForOriginal = UniformRandom32(1, length - 9, Rng);
            rows.AddMember("I_DATA").Utf8(
                RandomString(placeForOriginal) + "ORIGINAL" + RandomString(length - 8 - placeForOriginal)
            );
        }
        rows.AddMember("I_IM_ID").Int32(UniformRandom32(1, 10000, Rng));
        rows.EndStruct();
    }
    rows.EndList();
    
    ItemId = partEnd + 1;
    
    return rows.Build();
}

}
}
