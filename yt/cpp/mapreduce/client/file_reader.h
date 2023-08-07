#pragma once

#include <yt/cpp/mapreduce/common/fwd.h>

#include <yt/cpp/mapreduce/interface/io.h>

#include <yt/cpp/mapreduce/http/context.h>
#include <yt/cpp/mapreduce/http/requests.h>

class IInputStream;

namespace NYT {

class THttpRequest;
class TPingableTransaction;

namespace NDetail {
////////////////////////////////////////////////////////////////////////////////

class TStreamReaderBase
    : public IFileReader
{
public:
    TStreamReaderBase(
        IClientRetryPolicyPtr clientRetryPolicy,
        ITransactionPingerPtr transactionPinger,
        const TClientContext& context,
        const TTransactionId& transactionId);

    ~TStreamReaderBase();

protected:
    TYPath Snapshot(const TYPath& path);

protected:
    const TClientContext Context_;

private:
    size_t DoRead(void* buf, size_t len) override;
    virtual NHttpClient::IHttpResponsePtr Request(const TClientContext& context, const TTransactionId& transactionId, ui64 readBytes) = 0;
    TString GetActiveRequestId() const;

private:
    const IClientRetryPolicyPtr ClientRetryPolicy_;
    TFileReaderOptions FileReaderOptions_;

    NHttpClient::IHttpResponsePtr Response_;
    IInputStream* Input_ = nullptr;

    THolder<TPingableTransaction> ReadTransaction_;

    ui64 CurrentOffset_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TFileReader
    : public TStreamReaderBase
{
public:
    TFileReader(
        const TRichYPath& path,
        IClientRetryPolicyPtr clientRetryPolicy,
        ITransactionPingerPtr transactionPinger,
        const TClientContext& context,
        const TTransactionId& transactionId,
        const TFileReaderOptions& options = TFileReaderOptions());

private:
    NHttpClient::IHttpResponsePtr Request(const TClientContext& context, const TTransactionId& transactionId, ui64 readBytes) override;

private:
    TFileReaderOptions FileReaderOptions_;

    TRichYPath Path_;
    const ui64 StartOffset_;
    const TMaybe<ui64> EndOffset_;
};

////////////////////////////////////////////////////////////////////////////////

class TBlobTableReader
    : public TStreamReaderBase
{
public:
    TBlobTableReader(
        const TYPath& path,
        const TKey& key,
        IClientRetryPolicyPtr clientRetryPolicy,
        ITransactionPingerPtr transactionPinger,
        const TClientContext& context,
        const TTransactionId& transactionId,
        const TBlobTableReaderOptions& options);

private:
    NHttpClient::IHttpResponsePtr Request(const TClientContext& context, const TTransactionId& transactionId, ui64 readBytes) override;

private:
    const TKey Key_;
    const TBlobTableReaderOptions Options_;
    TYPath Path_;
};

} // namespace NDetail
} // namespace NYT
