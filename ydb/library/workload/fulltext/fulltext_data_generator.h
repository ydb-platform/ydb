#pragma once

#include "fulltext_workload_params.h"
#include <ydb/library/workload/benchmark_base/workload.h>
#include <ydb/library/workload/benchmark_base/data_generator.h>

namespace NYdbWorkload {

    class TFulltextWorkloadDataInitializer: public TWorkloadDataInitializerBase {
    private:
        const TFulltextWorkloadParams& Params;
        TString DataFiles;

    public:
        TFulltextWorkloadDataInitializer(const TFulltextWorkloadParams& params);

        void ConfigureOpts(NLastGetopt::TOpts& opts) override;
        TBulkDataGeneratorList DoGetBulkInitialData() override;
        int PostImport() override;
    };

} // namespace NYdbWorkload
