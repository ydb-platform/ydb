#include "yql_job_user.h"
#include <yt/cpp/mapreduce/io/yamr_table_reader.h>

namespace NYql {

void TYqlUserJob::Save(IOutputStream& stream) const {
    TYqlUserJobBase::Save(stream);
}

void TYqlUserJob::Load(IInputStream& stream) {
    TYqlUserJobBase::Load(stream);
}

TString TYqlUserJob::GetJobFactoryPrefix() const {
    return "Yt";
}

TIntrusivePtr<NYT::IReaderImplBase> TYqlUserJob::MakeMkqlJobReader() {
    if (YamrInput) {
        return MakeIntrusive<NYT::TYaMRTableReader>(MakeIntrusive<NYT::TJobReader>(InputFile_));
    }
    return MakeIntrusive<TJobMkqlReaderImpl>(InputFile_);
}

TIntrusivePtr<TMkqlWriterImpl> TYqlUserJob::MakeMkqlJobWriter() {
    return MakeIntrusive<TJobMkqlWriterImpl>(OutputFileList_);
}

void TYqlUserJob::Do(const NYT::TRawJobContext& jobContext) {
    InputFile_ = jobContext.GetInputFile();
    OutputFileList_ = jobContext.GetOutputFileList();
    TYqlUserJobBase::Do();
}

} // namespace NYql
