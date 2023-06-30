#include "yt_codec_job.h"

#include <ydb/library/yql/providers/yt/common/yql_configuration.h>

#include <yt/cpp/mapreduce/io/job_reader.h>
#include <yt/cpp/mapreduce/io/job_writer.h>

#include <util/system/file.h>
#include <util/stream/output.h>
#include <util/generic/xrange.h>
#include <util/generic/vector.h>

namespace NYql {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

namespace NPrivate {

TInStreamHolder::TInStreamHolder(const TFile& inHandle)
    : Input(inHandle)
    , Proxy(&Input)
{
}

TOutStreamsHolder::TOutStreamsHolder(const TVector<TFile>& outHandles) {
    Outputs.reserve(outHandles.size());
    for (auto& h: outHandles) {
        Outputs.emplace_back(h);
    }
}

TVector<IOutputStream*> TOutStreamsHolder::GetVectorOfStreams() {
    TVector<IOutputStream*> res(Reserve(Outputs.size()));
    for (auto& s: Outputs) {
        res.push_back(&s);
    }
    return res;
}

} // NPrivate

TJobMkqlReaderImpl::TJobMkqlReaderImpl(const TFile& in)
    : NPrivate::TInStreamHolder(in)
    , TMkqlReaderImpl(GetYtStream(), YQL_JOB_CODEC_BLOCK_COUNT, YQL_JOB_CODEC_BLOCK_SIZE)
{
}

TJobMkqlWriterImpl::TJobMkqlWriterImpl(const TMkqlIOSpecs& specs, const TVector<TFile>& outHandles)
    : NPrivate::TOutStreamsHolder(outHandles)
    , TMkqlWriterImpl(GetVectorOfStreams(), YQL_JOB_CODEC_BLOCK_COUNT, YQL_JOB_CODEC_BLOCK_SIZE)
{
    SetSpecs(specs);
}

void TJobMkqlWriterImpl::DoFinish(bool abort) {
    TMkqlWriterImpl::DoFinish(abort);
    if (!abort) {
        for (auto& out: Outputs) {
            out.Flush();
        }
    }
}

} // NYql
