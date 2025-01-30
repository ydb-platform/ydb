#pragma once

#include <library/cpp/threading/future/core/future.h>

#include <yt/cpp/mapreduce/client/transaction.h>
#include <yt/cpp/mapreduce/common/fwd.h>
#include <yt/cpp/mapreduce/http/context.h>
#include <yt/cpp/mapreduce/interface/io.h>

#include <util/generic/size_literals.h>

namespace NYT::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TRetryfulWriterV2
    : public TRawTableWriter
{
public:
    TRetryfulWriterV2(
        const IRawClientPtr& rawClient,
        IClientRetryPolicyPtr clientRetryPolicy,
        ITransactionPingerPtr transactionPinger,
        const TClientContext& context,
        const TTransactionId& parentId,
        const TString& command,
        const TMaybe<TFormat>& format,
        const TRichYPath& path,
        const TNode& serializedWriterOptions,
        ssize_t bufferSize,
        bool createTranasaction);

    void NotifyRowEnd() override;
    void Abort() override;

    size_t GetBufferMemoryUsage() const override;

protected:
    void DoWrite(const void* buf, size_t len) override;
    void DoFinish() override;

    void DoStartBatch();

private:
    class TSentBuffer;
    class TSender;
    struct TSendTask;

    const ssize_t BufferSize_;
    const ssize_t SendStep_ = 64_KB;
    ssize_t NextSizeToSend_;
    std::unique_ptr<TSender> Sender_;
    std::unique_ptr<TPingableTransaction> WriteTransaction_;

    std::unique_ptr<TSendTask> Current_;
    std::unique_ptr<TSendTask> Previous_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPrivate
