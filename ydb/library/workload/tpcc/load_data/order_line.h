#pragma once

#include "query_generator.h"


namespace NYdbWorkload {
namespace NTPCC {

class TOrderLineLoadQueryGenerator : public TLoadQueryGenerator {
public:

    TOrderLineLoadQueryGenerator(TLoadParams& params, ui32 partNum, TLog& log, ui64 seed);

    static TString GetCreateDDL(TString path, ui32 partNum, TString partAtKeys);

    static std::string GetCleanDDL();
    
    NYdb::TValue GetNextBatchLoadDDL() override;

    bool Finished() override;

private:
    ui32 PartNum;
    i32 WarehouseId;
    i32 DistrictId;
    i32 CustomerId;
    ui64 RandomCount;
    ui64 OlNumber;

    std::uniform_int_distribution<ui64> RandomCountGen;
};

}
}
