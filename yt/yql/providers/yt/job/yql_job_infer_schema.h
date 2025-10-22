#pragma once

#include "yql_job_base.h"
#include "yql_job_factory.h"

#include <yt/cpp/mapreduce/interface/operation.h>
#include <yt/cpp/mapreduce/interface/io.h>

namespace NYql {

class TYqlInferSchemaJob : public TYqlJobBase, public NYT::IRawJob {
public:
    TYqlInferSchemaJob() = default;
    ~TYqlInferSchemaJob() = default;

public:
    void Do(const NYT::TRawJobContext& jobContext) override;

    void Save(IOutputStream& stream) const override;
    void Load(IInputStream& stream) override;

private:
    void DoImpl(const TFile& inHandle, const TVector<TFile>& outHandles);
};

} // NYql
