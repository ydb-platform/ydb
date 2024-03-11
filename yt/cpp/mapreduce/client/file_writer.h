#pragma once

#include "retryful_writer.h"

#include <yt/cpp/mapreduce/common/fwd.h>

#include <yt/cpp/mapreduce/interface/io.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TFileWriter
    : public IFileWriter
{
public:
    TFileWriter(
        const TRichYPath& path,
        IClientRetryPolicyPtr clientRetryPolicy,
        ITransactionPingerPtr transactionPinger,
        const TClientContext& context,
        const TTransactionId& transactionId,
        const TFileWriterOptions& options = TFileWriterOptions());

    ~TFileWriter() override;

protected:
    void DoWrite(const void* buf, size_t len) override;
    void DoFinish() override;
    size_t GetBufferMemoryUsage() const override;

private:
    const bool AutoFinish_;
    TRetryfulWriter RetryfulWriter_;
    static const size_t BUFFER_SIZE = 64 << 20;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
