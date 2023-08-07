#pragma once

#include "yql_job_base.h"

#include <yt/cpp/mapreduce/interface/operation.h>
#include <yt/cpp/mapreduce/interface/io.h>

#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/generic/vector.h>
#include <util/generic/maybe.h>

namespace NYql {

class TYqlCalcJob : public TYqlJobBase {
public:
    TYqlCalcJob() = default;
    ~TYqlCalcJob() = default;

    void SetColumns(const TVector<TString>& columns) {
        Columns_ = columns;
    }

    void SetUseResultYson(bool flag) {
        UseResultYson_ = flag;
    }

    void Save(IOutputStream& stream) const override;
    void Load(IInputStream& stream) override;

protected:
    void DoImpl(const TFile& inHandle, const TVector<TFile>& outHandles) override;

private:
    TMaybe<TVector<TString>> Columns_;
    bool UseResultYson_ = false;
};

} // NYql
