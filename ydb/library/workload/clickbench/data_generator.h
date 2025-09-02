#pragma once

#include "clickbench.h"

namespace NYdbWorkload {

class TClickbenchWorkloadDataInitializerGenerator: public TWorkloadDataInitializerBase {
public:
    TClickbenchWorkloadDataInitializerGenerator(const TClickbenchWorkloadParams& params);
    void ConfigureOpts(NLastGetopt::TOpts& opts) override;
    YDB_READONLY_DEF(TFsPath, DataFiles);

protected:
    TBulkDataGeneratorList DoGetBulkInitialData() override;

private:
    static constexpr ui64 DataSetSize = 99997497;
};

}