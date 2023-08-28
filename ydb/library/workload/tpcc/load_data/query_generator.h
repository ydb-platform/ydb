#pragma once

#include <ydb/library/workload/tpcc/tpcc_config.h>
#include <ydb/library/workload/tpcc/tpcc_util.h>

#include <util/random/fast.h>

#include <random>

namespace NYdbWorkload {
namespace NTPCC {

class TLoadQueryGenerator {
public:

    TLoadQueryGenerator(TTPCCWorkloadParams& params, ui64 seed);

    virtual ~TLoadQueryGenerator() = default;

    virtual NYdb::TValue GetNextBatchLoadDDL() = 0;

    virtual bool Finished() = 0;

    TTPCCWorkloadParams GetParams();

protected:
    TString RandomString(ui64 length);
    TString RandomNumberString(ui64 length);

    TTPCCWorkloadParams Params;

    TFastRng32 Rng;
};

}
}
