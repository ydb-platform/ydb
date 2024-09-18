#pragma once

#include "public.h"

#include <yt/yt/client/api/row_batch_writer.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TRowBatchWriter
    : public virtual IRowBatchWriter
{
public:
    explicit TRowBatchWriter(NConcurrency::IAsyncZeroCopyOutputStreamPtr underlying);

    bool Write(TRange<NTableClient::TUnversionedRow> rows) override;

    TFuture<void> GetReadyEvent() override;

    TFuture<void> Close() override;

    const NTableClient::TNameTablePtr& GetNameTable() const override;

private:
    const NConcurrency::IAsyncZeroCopyOutputStreamPtr Underlying_;
    const NTableClient::TNameTablePtr NameTable_ = New<NTableClient::TNameTable>();
    const IRowStreamEncoderPtr Encoder_;

    TPromise<void> ReadyEvent_ = MakePromise<void>(TError());
    bool Closed_ = false;
};

////////////////////////////////////////////////////////////////////////////////

IRowBatchWriterPtr CreateRowBatchWriter(
    NConcurrency::IAsyncZeroCopyOutputStreamPtr outputStream);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
