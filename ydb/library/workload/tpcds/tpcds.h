#pragma once

#include <ydb/library/workload/tpc_base/tpc_base.h>
#include <util/folder/path.h>

namespace NYdbWorkload {

class TTpcdsWorkloadParams final: public TTpcBaseWorkloadParams {
public:
    THolder<IWorkloadQueryGenerator> CreateGenerator() const override;
    TString GetWorkloadName() const override;
    TWorkloadDataInitializer::TList CreateDataInitializers() const override;
};

class TTpcdsWorkloadGenerator final: public TTpcBaseWorkloadGenerator {
public:
    explicit TTpcdsWorkloadGenerator(const TTpcdsWorkloadParams& params);

protected:
    TString GetTablesYaml() const override;
    TWorkloadGeneratorBase::TSpecialDataTypes GetSpecialDataTypes() const override;

private:
    const TTpcdsWorkloadParams& Params;
};

} // namespace NYdbWorkload
