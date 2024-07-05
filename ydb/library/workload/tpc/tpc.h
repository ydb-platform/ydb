#pragma once

#include <ydb/library/workload/benchmark_base/workload.h>
#include <util/folder/path.h>

namespace NYdbWorkload {

class TTpcWorkloadParams: public TWorkloadBaseParams {
public:
    enum class EFloatMode {
        FLOAT /* "float" */,
        DECIMAL /* "decimal" */,
        DECIMAL_YDB /* "decimal_ydb" */
    };
    void ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) override;
    YDB_READONLY(EFloatMode, FloatMode, EFloatMode::FLOAT);
};

class TTpcWorkloadGenerator: public TWorkloadGeneratorBase {
public:
    explicit TTpcWorkloadGenerator(const TTpcWorkloadParams& params);

protected:
    void PatchQuery(TString& query) const;

private:
    const TTpcWorkloadParams& Params;
};

} // namespace NYdbWorkload
