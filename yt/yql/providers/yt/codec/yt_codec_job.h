#pragma once

#include "yt_codec_io.h"

#include <yt/cpp/mapreduce/io/stream_table_reader.h>

#include <util/system/file.h>
#include <util/stream/file.h>


namespace NYql {

namespace NPrivate {
    class TInStreamHolder {
    public:
        TInStreamHolder(const TFile& inHandle);
        NYT::TRawTableReader& GetYtStream() {
            return Proxy;
        }

    protected:
        TUnbufferedFileInput Input;
        NYT::NDetail::TInputStreamProxy Proxy;
    };

    class TOutStreamsHolder {
    public:
        TOutStreamsHolder(const TVector<TFile>& outHandles);
        TVector<IOutputStream*> GetVectorOfStreams();

    protected:
        TVector<TUnbufferedFileOutput> Outputs;
    };
}

class TJobMkqlReaderImpl: protected NPrivate::TInStreamHolder, public TMkqlReaderImpl {
public:
    TJobMkqlReaderImpl(const TFile& in);
    ~TJobMkqlReaderImpl() = default;
};


class TJobMkqlWriterImpl: protected NPrivate::TOutStreamsHolder, public TMkqlWriterImpl {
public:
    TJobMkqlWriterImpl(const TMkqlIOSpecs& specs, const TVector<TFile>& outHandles);
    ~TJobMkqlWriterImpl() = default;

private:
    void DoFinish(bool abort) override;
};

} // NYql
