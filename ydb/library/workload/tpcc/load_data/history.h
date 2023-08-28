#pragma once

#include "query_generator.h"


namespace NYdbWorkload {
namespace NTPCC {

class THistoryLoadQueryGenerator : public TLoadQueryGenerator {
public:

    THistoryLoadQueryGenerator(TTPCCWorkloadParams& params, ui32 partNum, ui64 seed);

    static TString GetCreateDDL(TString path, ui32 partNum, TString partAtKeys);

    static std::string GetCleanDDL();
    
    NYdb::TValue GetNextBatchLoadDDL() override;

    bool Finished() override;

private:
    ui32 PartNum;
    i32 WarehouseId;
    i32 DistrictId;
    i32 CustomerId;

    ui64 GetNowNanoSeconds() {
        std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
        auto duration = now.time_since_epoch();
        auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(duration);
        return static_cast<ui64>(nanoseconds.count());
    }
};

}
}
