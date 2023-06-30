#pragma once

#include "yql_job_base.h"
#include "yql_job_factory.h"

#include <yt/cpp/mapreduce/interface/operation.h>
#include <yt/cpp/mapreduce/interface/io.h>

namespace NYql {

class TYqlInferSchemaJob : public TYqlJobBase {
public:
    TYqlInferSchemaJob() = default;
    ~TYqlInferSchemaJob() = default;

protected:
    void DoImpl(const TFile& inHandle, const TVector<TFile>& outHandles) override;
};

} // NYql
