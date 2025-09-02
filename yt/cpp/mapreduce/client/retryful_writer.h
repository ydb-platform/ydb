#pragma once

#include "transaction.h"
#include "transaction_pinger.h"

#include <yt/cpp/mapreduce/common/retry_lib.h>

#include <yt/cpp/mapreduce/common/retry_request.h>

#include <yt/cpp/mapreduce/interface/common.h>
#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/cpp/mapreduce/interface/raw_client.h>

#include <yt/cpp/mapreduce/io/helpers.h>

#include <library/cpp/threading/blocking_queue/blocking_queue.h>

#include <util/generic/buffer.h>
#include <util/stream/buffer.h>
#include <util/stream/output.h>
#include <util/system/thread.h>
#include <util/system/event.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TRetryfulWriter
    : public TRawTableWriter
{
public:
    template <class TWriterOptions>
    TRetryfulWriter(
        const IRawClientPtr& rawClient,
        IClientRetryPolicyPtr clientRetryPolicy,
        ITransactionPingerPtr transactionPinger,
        const TClientContext& context,
        const TTransactionId& parentId,
        const TMaybe<TFormat>& format,
        const TRichYPath& path,
        const TWriterOptions& options)
        : RawClient_(rawClient)
        , ClientRetryPolicy_(std::move(clientRetryPolicy))
        , TransactionPinger_(std::move(transactionPinger))
        , Context_(context)
        , AutoFinish_(options.AutoFinish_)
        , Options_(options)
        , Format_(format)
        , BufferSize_(GetBufferSize(options.WriterOptions_))
        , Path_(path)
        , ParentTransactionId_(parentId)
        , WriteTransaction_()
        , FilledBuffers_(2)
        , EmptyBuffers_(2)
        , Buffer_(BufferSize_ * 2)
        , Thread_(TThread::TParams{SendThread, this}.SetName("retryful_writer"))
    {
        SecondaryPath_ = path;
        SecondaryPath_.Append_ = true;
        SecondaryPath_.Schema_.Clear();
        SecondaryPath_.CompressionCodec_.Clear();
        SecondaryPath_.ErasureCodec_.Clear();
        SecondaryPath_.OptimizeFor_.Clear();

        if (options.CreateTransaction_) {
            WriteTransaction_.ConstructInPlace(rawClient, ClientRetryPolicy_, context, parentId, TransactionPinger_->GetChildTxPinger(), TStartTransactionOptions());
            auto append = path.Append_.GetOrElse(false);
            auto lockMode = (append  ? LM_SHARED : LM_EXCLUSIVE);
            NDetail::RequestWithRetry<void>(
                ClientRetryPolicy_->CreatePolicyForGenericRequest(),
                [this, &path, &lockMode] (TMutationId& mutationId) {
                    RawClient_->Lock(mutationId, WriteTransaction_->GetId(), path.Path_, lockMode);
                });
        }

        EmptyBuffers_.Push(TBuffer(BufferSize_ * 2));
    }

    ~TRetryfulWriter() override;
    void NotifyRowEnd() override;
    void Abort() override;

    size_t GetBufferMemoryUsage() const override;

    size_t GetRetryBlockRemainingSize() const
    {
        return (BufferSize_ > Buffer_.size()) ? (BufferSize_ - Buffer_.size()) : 0;
    }

protected:
    void DoWrite(const void* buf, size_t len) override;
    void DoFinish() override;

private:
    static size_t GetBufferSize(const TMaybe<TWriterOptions>& writerOptions);

private:
    const IRawClientPtr RawClient_;
    const IClientRetryPolicyPtr ClientRetryPolicy_;
    const ITransactionPingerPtr TransactionPinger_;
    const TClientContext Context_;
    const bool AutoFinish_;
    std::variant<TTableWriterOptions, TFileWriterOptions> Options_;

    TMaybe<TFormat> Format_;

    const size_t BufferSize_;

    TRichYPath Path_;
    TRichYPath SecondaryPath_;

    TTransactionId ParentTransactionId_;
    TMaybe<TPingableTransaction> WriteTransaction_;

    ::NThreading::TBlockingQueue<TBuffer> FilledBuffers_;
    ::NThreading::TBlockingQueue<TBuffer> EmptyBuffers_;

    TBuffer Buffer_;

    TThread Thread_;
    bool Started_ = false;
    std::exception_ptr Exception_ = nullptr;

    enum EWriterState {
        Ok,
        Completed,
        Error,
    } WriterState_ = Ok;

private:
    void FlushBuffer(bool lastBlock);
    void Send(const TBuffer& buffer);
    void CheckWriterState();

    void SendThread();
    static void* SendThread(void* opaque);
};

////////////////////////////////////////////////////////////////////////////////

}
