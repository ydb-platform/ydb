#pragma once

#include "query_generator.h"


namespace NYdbWorkload {
namespace NTPCC {

class TItemLoadQueryGenerator : public TLoadQueryGenerator {
public:

    TItemLoadQueryGenerator(TTPCCWorkloadParams& params, ui64 seed);

    static TString GetCreateDDL(TString path);

    static std::string GetCleanDDL();
    
    NYdb::TValue GetNextBatchLoadDDL() override;

    bool Finished() override;

private:
    i32 ItemId;
};

}
}
