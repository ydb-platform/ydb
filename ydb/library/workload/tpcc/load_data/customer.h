#pragma once

#include "query_generator.h"


namespace NYdbWorkload {
namespace NTPCC {

class TCustomerLoadQueryGenerator : public TLoadQueryGenerator {
public:

    TCustomerLoadQueryGenerator(TTPCCWorkloadParams& params, ui32 partNum, ui64 seed);

    static TString GetCreateDDL(TString path, ui32 partNum, TString partAtKeys);

    static std::string GetCleanDDL();

    NYdb::TValue GetNextBatchLoadDDL() override;

    bool Finished() override;

private:
    ui32 PartNum;
    i32 WarehouseId;
    i32 DistrictId;
    i32 CustomerId;
};

}
}
