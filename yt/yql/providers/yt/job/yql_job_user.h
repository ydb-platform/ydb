#pragma once

#include "yql_job_user_base.h"

namespace NYql {

class TYqlUserJob: public TYqlUserJobBase, public NYT::IRawJob {
public:
    TYqlUserJob()
        : TYqlUserJobBase()
    {
    }
    virtual ~TYqlUserJob() = default;

public:
    void Do(const NYT::TRawJobContext& jobContext) override;

    void Save(IOutputStream& stream) const override;
    void Load(IInputStream& stream) override;

protected:
    TIntrusivePtr<NYT::IReaderImplBase> MakeMkqlJobReader() override;

    TIntrusivePtr<TMkqlWriterImpl> MakeMkqlJobWriter() override;

    TString GetJobFactoryPrefix() const override;

private:
    TFile InputFile_;
    TVector<TFile> OutputFileList_;
};

} // namespace NYql
