#include "partition_reader.h"

#include <yt/cpp/mapreduce/common/retry_lib.h>
#include <yt/cpp/mapreduce/common/retry_request.h>

#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/raw_client.h>

#include <util/system/guard.h>
#include <util/system/spinlock.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

class TPartitionTableReader
    : public TRawTableReader
{
public:
    TPartitionTableReader(std::unique_ptr<IAbortableInputStream> input)
        : Input_(std::move(input))
    { }

    bool Retry(
        const TMaybe<ui32>& /*rangeIndex*/,
        const TMaybe<ui64>& /*rowIndex*/,
        const std::exception_ptr& /*error*/) override
    {
        return false;
    }

    void ResetRetries() override
    { }

    bool HasRangeIndices() const override
    {
        return false;
    }

    void Abort() override
    {
        Input_->Abort();
    }

    bool IsAborted() const override
    {
        return Input_->IsAborted();
    }

protected:
    size_t DoRead(void* buf, size_t len) override
    {
        return Input_->Read(buf, len);
    }

private:
    std::unique_ptr<IAbortableInputStream> Input_;
};

////////////////////////////////////////////////////////////////////////////////

class TFilePartitionReader
    : public IFileReader
{
public:
    TFilePartitionReader(
        IRawClientPtr rawClient,
        IClientRetryPolicyPtr clientRetryPolicy,
        TString cookie,
        TFilePartitionReaderOptions options)
        : RawClient_(std::move(rawClient))
        , ClientRetryPolicy_(std::move(clientRetryPolicy))
        , Cookie_(std::move(cookie))
        , Options_(std::move(options))
    { }

    void Abort() override
    {
        auto g = Guard(Lock_);
        AbortRequested_ = true;
        if (Input_) {
            Input_->Abort();
        }
    }

    bool IsAborted() const override
    {
        auto g = Guard(Lock_);
        return AbortRequested_;
    }

protected:
    size_t DoRead(void* buf, size_t len) override
    {
        if (len == 0) {
            return 0;
        }
        return RequestWithRetry<size_t>(
            ClientRetryPolicy_->CreatePolicyForReaderRequest(),
            [&] (TMutationId /*mutationId*/) {
                try {
                    if (!Input_) {
                        Open();
                    }
                    auto read = Input_->Read(buf, len);
                    ReadBytes_ += read;
                    return read;
                } catch (...) {
                    auto g = Guard(Lock_);
                    Input_ = nullptr;
                    throw;
                }
            });
    }

private:
    void Open()
    {
        {
            auto g = Guard(Lock_);
            if (AbortRequested_) {
                ythrow TInputStreamAbortedError() << "File partition reader is aborted";
            }
        }

        auto input = RawClient_->ReadFilePartition(Cookie_, Options_);

        {
            auto g = Guard(Lock_);
            // NB: Abort could've been called while we are waiting for input.
            if (AbortRequested_) {
                input->Abort();
            }
            Input_ = std::move(input);
        }

        if (ReadBytes_ > 0) {
            auto skipped = Input_->Skip(ReadBytes_);
            Y_ENSURE(
                skipped == ReadBytes_,
                "File partition stream ended after " << skipped << " bytes while resuming at " << ReadBytes_);
        }
    }

private:
    const IRawClientPtr RawClient_;
    const IClientRetryPolicyPtr ClientRetryPolicy_;
    const TString Cookie_;
    const TFilePartitionReaderOptions Options_;

    // Avoid data race on Abort() calls.
    TAdaptiveLock Lock_;
    bool AbortRequested_ = false;
    std::unique_ptr<IAbortableInputStream> Input_;

    size_t ReadBytes_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

TRawTableReaderPtr CreateTablePartitionReader(
    const IRawClientPtr& rawClient,
    const IRequestRetryPolicyPtr& retryPolicy,
    const TString& cookie,
    const TFormat& format,
    const TTablePartitionReaderOptions& options)
{
    auto stream = NDetail::RequestWithRetry<std::unique_ptr<IAbortableInputStream>>(
        retryPolicy,
        [&] (TMutationId /*mutationId*/) {
            return rawClient->ReadTablePartition(cookie, format, options);
        }
    );
    return MakeIntrusive<TPartitionTableReader>(std::move(stream));
}

IFileReaderPtr CreateFilePartitionReader(
    const IRawClientPtr& rawClient,
    const IClientRetryPolicyPtr& clientRetryPolicy,
    const TString& cookie,
    const TFilePartitionReaderOptions& options)
{
    return MakeIntrusive<TFilePartitionReader>(rawClient, clientRetryPolicy, cookie, options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
