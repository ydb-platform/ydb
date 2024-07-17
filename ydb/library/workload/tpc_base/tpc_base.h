#pragma once

#include <ydb/library/workload/benchmark_base/workload.h>
#include <util/folder/path.h>

namespace NYdbWorkload {

class TTpcBaseWorkloadParams: public TWorkloadBaseParams {
public:
    enum class EFloatMode {
        FLOAT /* "float" */,
        DECIMAL /* "decimal" */,
        DECIMAL_YDB /* "decimal_ydb" */
    };
    void ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) override;
    YDB_READONLY(EFloatMode, FloatMode, EFloatMode::FLOAT);
};

class TTpcBaseWorkloadGenerator: public TWorkloadGeneratorBase {
public:
    explicit TTpcBaseWorkloadGenerator(const TTpcBaseWorkloadParams& params);

protected:
    void PatchQuery(TString& query) const;

private:
    const TTpcBaseWorkloadParams& Params;
    TString FilterHeader(TStringBuf header, const TString& query) const;
    void PatchHeader(TString& header) const;
};

} // namespace NYdbWorkload
