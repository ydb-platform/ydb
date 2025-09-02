#pragma once

#include "query.h"

namespace NYdbWorkload {

namespace NQuery {

class TQueryWorkloadDataInitializer final: public TWorkloadDataInitializerBase {
public:
    TQueryWorkloadDataInitializer(const TQueryWorkloadParams& params);
    void ConfigureOpts(NLastGetopt::TOpts& opts) override;

protected:
    TBulkDataGeneratorList DoGetBulkInitialData() override;

private:
    TFsPath SuitePath;
    TSet<TString> TablesForUpload;
};

}

}