#pragma once

#include "query_generator.h"


namespace NYdbWorkload {
namespace NTPCC {

class TDistrictLoadQueryGenerator : public TLoadQueryGenerator {
public:

    TDistrictLoadQueryGenerator(TTPCCWorkloadParams& params, ui64 seed);

    static TString GetCreateDDL(TString path);

    static std::string GetCleanDDL();

    NYdb::TValue GetNextBatchLoadDDL() override;

    bool Finished() override;

private:
    i32 WarehouseId;
    i32 DistrictId;
};

}
}
