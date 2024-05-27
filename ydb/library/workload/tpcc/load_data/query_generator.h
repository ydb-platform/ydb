#pragma once

#include <ydb/library/workload/tpcc/config.h>
#include <ydb/library/workload/tpcc/util.h>

#include <ydb/public/sdk/cpp/client/ydb_value/value.h>

#include <util/random/fast.h>
#include <library/cpp/logger/log.h>


namespace NYdbWorkload {
namespace NTPCC {

class TLoadQueryGenerator {
public:

    TLoadQueryGenerator(TLoadParams& params,
                        ETablesType type, TLog& log,
                        ui64 seed);

    virtual ~TLoadQueryGenerator() = default;

    virtual NYdb::TValue GetNextBatchLoadDDL() = 0;

    virtual bool Finished() = 0;

    TLoadParams GetParams();

    ETablesType GetType();

protected:
    TString RandomString(ui64 length);
    TString RandomNumberString(ui64 length);

    TLoadParams Params;

    ETablesType Type;

    TLog& Log;


    TFastRng32 Rng;
};

}
}
