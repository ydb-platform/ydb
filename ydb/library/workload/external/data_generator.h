#pragma once

#include "external.h"

namespace NYdbWorkload {

namespace NExternal {

class TExternalWorkloadDataInitializer final: public TWorkloadDataInitializerBase {
public:
    TExternalWorkloadDataInitializer(const TExternalWorkloadParams& params);
    void ConfigureOpts(NLastGetopt::TOpts& opts) override;

protected:
    TBulkDataGeneratorList DoGetBulkInitialData() override;

private:
    void InitTableColumns();
    const TExternalWorkloadParams& Params;
    TFsPath DataPath;
    TSet<TString> TablesForUpload;
    TMap<TString, TVector<TString>> TableColumns;
    bool WithoutHeaders = false;
};

}

}