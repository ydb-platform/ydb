#pragma once

#include <yt/cpp/mapreduce/common/fwd.h>

#include <yt/cpp/mapreduce/http/requests.h>
#include <yt/cpp/mapreduce/interface/io.h>

namespace NYT {

struct TTableWriterOptions;

////////////////////////////////////////////////////////////////////////////////

class TClientWriter
    : public IProxyOutput
{
public:
    TClientWriter(
        const TRichYPath& path,
        IClientRetryPolicyPtr clientRetryPolicy,
        ITransactionPingerPtr transactionPinger,
        const TClientContext& context,
        const TTransactionId& transactionId,
        const TMaybe<TFormat>& format,
        const TTableWriterOptions& options);

    ~TClientWriter();

    size_t GetStreamCount() const override;
    IOutputStream* GetStream(size_t tableIndex) const override;
    void OnRowFinished(size_t tableIndex) override;
    void Abort() override;
    size_t GetBufferMemoryUsage() const override;

    void Finish();

private:
    const size_t BufferSize_ = 64 << 20;
    const bool AutoFinish_;

    ::TIntrusivePtr<TRawTableWriter> RawWriter_;
};

////////////////////////////////////////////////////////////////////////////////


} // namespace NYT
