#pragma once

#include <yt/cpp/mapreduce/common/fwd.h>

#include <yt/cpp/mapreduce/interface/io.h>

#include <yt/cpp/mapreduce/http/context.h>
#include <yt/cpp/mapreduce/http/requests.h>

class IInputStream;

namespace NYT {

class TPingableTransaction;

namespace NDetail {
////////////////////////////////////////////////////////////////////////////////

class TStreamReaderBase
    : public IFileReader
{
public:
    TStreamReaderBase(
        const IRawClientPtr& rawClient,
        IClientRetryPolicyPtr clientRetryPolicy,
        ITransactionPingerPtr transactionPinger,
        const TClientContext& context,
        const TTransactionId& transactionId);

    ~TStreamReaderBase();

protected:
    TYPath Snapshot(const TYPath& path);

protected:
    const IRawClientPtr RawClient_;

private:
    size_t DoRead(void* buf, size_t len) override;
    virtual std::unique_ptr<IInputStream> Request(const TTransactionId& transactionId, ui64 readBytes) = 0;

private:
    const IClientRetryPolicyPtr ClientRetryPolicy_;
    TFileReaderOptions FileReaderOptions_;

    std::unique_ptr<IInputStream> Input_;

    std::unique_ptr<TPingableTransaction> ReadTransaction_;

    ui64 CurrentOffset_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TFileReader
    : public TStreamReaderBase
{
public:
    TFileReader(
        const TRichYPath& path,
        const IRawClientPtr& rawClient,
        IClientRetryPolicyPtr clientRetryPolicy,
        ITransactionPingerPtr transactionPinger,
        const TClientContext& context,
        const TTransactionId& transactionId,
        const TFileReaderOptions& options = {});

private:
    std::unique_ptr<IInputStream> Request(const TTransactionId& transactionId, ui64 readBytes) override;

private:
    const ui64 StartOffset_;
    const TMaybe<ui64> EndOffset_;

    TFileReaderOptions Options_;
    TRichYPath Path_;
};

////////////////////////////////////////////////////////////////////////////////

class TBlobTableReader
    : public TStreamReaderBase
{
public:
    TBlobTableReader(
        const TYPath& path,
        const TKey& key,
        const IRawClientPtr& rawClient,
        IClientRetryPolicyPtr clientRetryPolicy,
        ITransactionPingerPtr transactionPinger,
        const TClientContext& context,
        const TTransactionId& transactionId,
        const TBlobTableReaderOptions& options = {});

private:
    std::unique_ptr<IInputStream> Request(const TTransactionId& transactionId, ui64 readBytes) override;

private:
    const ui64 StartOffset_;
    const TKey Key_;

    TBlobTableReaderOptions Options_;
    TYPath Path_;
};

} // namespace NDetail
} // namespace NYT
