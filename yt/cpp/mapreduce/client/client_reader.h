#pragma once

#include <yt/cpp/mapreduce/common/fwd.h>

#include <yt/cpp/mapreduce/interface/io.h>

#include <yt/cpp/mapreduce/http/context.h>
#include <yt/cpp/mapreduce/http/requests.h>
#include <yt/cpp/mapreduce/http/http.h>
#include <yt/cpp/mapreduce/http/http_client.h>

namespace NYT {

class TPingableTransaction;

////////////////////////////////////////////////////////////////////////////////

class TClientReader
    : public TRawTableReader
{
public:
    TClientReader(
        const TRichYPath& path,
        IClientRetryPolicyPtr clientRetryPolicy,
        ITransactionPingerPtr transactionPinger,
        const TClientContext& context,
        const TTransactionId& transactionId,
        const TFormat& format,
        const TTableReaderOptions& options,
        bool useFormatFromTableAttributes);

    bool Retry(
        const TMaybe<ui32>& rangeIndex,
        const TMaybe<ui64>& rowIndex,
        const std::exception_ptr& error) override;

    void ResetRetries() override;

    bool HasRangeIndices() const override { return true; }

protected:
    size_t DoRead(void* buf, size_t len) override;

private:
    TRichYPath Path_;
    const IClientRetryPolicyPtr ClientRetryPolicy_;
    const TClientContext Context_;
    TTransactionId ParentTransactionId_;
    TMaybe<TFormat> Format_;
    TTableReaderOptions Options_;

    THolder<TPingableTransaction> ReadTransaction_;

    NHttpClient::IHttpResponsePtr Response_;
    IInputStream* Input_;

    IRequestRetryPolicyPtr CurrentRequestRetryPolicy_;

private:
    void TransformYPath();
    void CreateRequest(const TMaybe<ui32>& rangeIndex = Nothing(), const TMaybe<ui64>& rowIndex = Nothing());
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
