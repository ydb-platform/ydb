#pragma once

#include <yt/yt/client/api/row_batch_reader.h>
#include <yt/yt/client/api/rpc_proxy/public.h>

#include <yt/yt_proto/yt/client/api/rpc_proxy/proto/api_service.pb.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TRowBatchReader
    : public virtual IRowBatchReader
{
public:
    TRowBatchReader(
        NConcurrency::IAsyncZeroCopyInputStreamPtr underlying,
        bool isStreamWithStatistics);

    NTableClient::IUnversionedRowBatchPtr Read(const NTableClient::TRowBatchReadOptions& options) override;

    TFuture<void> GetReadyEvent() const override;

    const NTableClient::TNameTablePtr& GetNameTable() const override;

protected:
    i64 RowCount_ = 0;
    i64 DataWeight_ = 0;

    virtual void ApplyStatistics(const NProto::TRowsetStatistics& statistics);

private:
    const NConcurrency::IAsyncZeroCopyInputStreamPtr Underlying_;
    const NTableClient::TNameTablePtr NameTable_ = New<NTableClient::TNameTable>();
    const IRowStreamDecoderPtr Decoder_;

    TFuture<TSharedRange<NTableClient::TUnversionedRow>> RowsFuture_;
    TPromise<void> ReadyEvent_ = NewPromise<void>();

    std::vector<TSharedRange<NTableClient::TUnversionedRow>> StoredRows_;

    bool Finished_ = false;
    i64 CurrentRowsOffset_ = 0;

    const bool IsStreamWithStatistics_;

    TFuture<TSharedRange<NTableClient::TUnversionedRow>> GetRows();
};

////////////////////////////////////////////////////////////////////////////////

IRowBatchReaderPtr CreateRowBatchReader(
    NConcurrency::IAsyncZeroCopyInputStreamPtr inputStream,
    bool isStreamWithStatistics);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
