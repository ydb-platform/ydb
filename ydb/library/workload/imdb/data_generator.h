#pragma once

#include "imdb.h"

namespace NYdbWorkload {

class TImdbWorkloadDataInitializer final: public TWorkloadDataInitializerBase {
public:
    explicit TImdbWorkloadDataInitializer(const TImdbWorkloadParams& params);
    void ConfigureOpts(NLastGetopt::TOpts& opts) override;
    int PostImport() override;

protected:
    TBulkDataGeneratorList DoGetBulkInitialData() override;

private:
    const TImdbWorkloadParams& ImdbParams;
    TFsPath DataFiles;
};

} // namespace NYdbWorkload
