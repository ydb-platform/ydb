#pragma once

#include <yt/yt/client/api/row_batch_reader.h>
#include <yt/yt/client/api/rpc_proxy/public.h>

#include <yt/yt_proto/yt/client/api/rpc_proxy/proto/api_service.pb.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/system/spin_lock.h>

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

    //! Time the caller has spent waiting for the ready event.
    TDuration GetWaitTime() const;
    //! Time spent synchronously in #Read.
    TDuration GetReadTime() const;
    //! Time spent decoding blocks in the background.
    TDuration GetDecodeTime() const;

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

    YT_DECLARE_SPIN_LOCK(mutable TSpinLock, WaitTimerLock_);
    mutable NProfiling::TWallTimer WaitTimer_{/*start*/ false};
    NProfiling::TConcurrentTimer<NProfiling::TWallTimer> ReadTimer_{/*start*/ false};
    NProfiling::TConcurrentTimer<NProfiling::TWallTimer> DecodeTimer_{/*start*/ false};

    TFuture<TSharedRange<NTableClient::TUnversionedRow>> GetRows();
    void StopWaitTimerOnReady(const TPromise<void>& readyEvent);
};

////////////////////////////////////////////////////////////////////////////////

IRowBatchReaderPtr CreateRowBatchReader(
    NConcurrency::IAsyncZeroCopyInputStreamPtr inputStream,
    bool isStreamWithStatistics);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
