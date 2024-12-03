#pragma once

#include <ydb/library/workload/tpc_base/tpc_base.h>
#include <util/folder/path.h>

namespace NYdbWorkload {

class TTpchWorkloadParams final: public TTpcBaseWorkloadParams {
public:
    THolder<IWorkloadQueryGenerator> CreateGenerator() const override;
    TString GetWorkloadName() const override;
    TWorkloadDataInitializer::TList CreateDataInitializers() const override;
};

class TTpchWorkloadGenerator final: public TTpcBaseWorkloadGenerator {
public:
    explicit TTpchWorkloadGenerator(const TTpchWorkloadParams& params);

protected:
    TString GetTablesYaml() const override;
    TWorkloadGeneratorBase::TSpecialDataTypes GetSpecialDataTypes() const override;

private:
    const TTpchWorkloadParams& Params;
};

} // namespace NYdbWorkload
