#include "partition_reader.h"

#include <yt/cpp/mapreduce/common/retry_request.h>

#include <yt/cpp/mapreduce/interface/raw_client.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

class TPartitionTableReader
    : public TRawTableReader
{
public:
    TPartitionTableReader(std::unique_ptr<IInputStream> input)
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

protected:
    size_t DoRead(void* buf, size_t len) override
    {
        return Input_->Read(buf, len);
    }

private:
    std::unique_ptr<IInputStream> Input_;
};

////////////////////////////////////////////////////////////////////////////////

TRawTableReaderPtr CreateTablePartitionReader(
    const IRawClientPtr& rawClient,
    const IRequestRetryPolicyPtr& retryPolicy,
    const TString& cookie,
    const TMaybe<TFormat>& format,
    const TTablePartitionReaderOptions& options)
{

    auto stream = NDetail::RequestWithRetry<std::unique_ptr<IInputStream>>(
        retryPolicy,
        [&] (TMutationId /*mutationId*/) {
            return rawClient->ReadTablePartition(cookie, format, options);
        }
    );
    return MakeIntrusive<TPartitionTableReader>(std::move(stream));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
