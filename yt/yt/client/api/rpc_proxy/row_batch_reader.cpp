#include "row_batch_reader.h"
#include "helpers.h"
#include "row_stream.h"
#include "wire_row_stream.h"

#include <yt/yt/client/table_client/name_table.h>

#include <yt_proto/yt/client/api/rpc_proxy/proto/api_service.pb.h>

#include <yt/yt/core/concurrency/async_stream_helpers.h>

#include <yt/yt/core/rpc/dispatcher.h>

namespace NYT::NApi::NRpcProxy {

using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TRowBatchReader::TRowBatchReader(
    IAsyncZeroCopyInputStreamPtr underlying,
    bool isStreamWithStatistics)
    : Underlying_(std::move(underlying))
    , Decoder_(CreateWireRowStreamDecoder(NameTable_, CreateUnlimitedWireProtocolOptions()))
    , IsStreamWithStatistics_(isStreamWithStatistics)
{
    YT_VERIFY(Underlying_);

    RowsFuture_ = GetRows();
    StopWaitTimerOnReady(ReadyEvent_);
    ReadyEvent_.TrySetFrom(RowsFuture_);
}

IUnversionedRowBatchPtr TRowBatchReader::Read(const TRowBatchReadOptions& options)
{
    auto readTimerGuard = NProfiling::TTimerGuard(&ReadTimer_);

    StoredRows_.clear();

    if (!ReadyEvent_.IsSet() || !ReadyEvent_.GetOrCrash().IsOK()) {
        return CreateEmptyUnversionedRowBatch();
    }

    if (!Finished_) {
        ReadyEvent_ = NewPromise<void>();
        StopWaitTimerOnReady(ReadyEvent_);
    }

    std::vector<TUnversionedRow> rows;
    rows.reserve(options.MaxRowsPerRead);
    i64 dataWeight = 0;

    while (RowsFuture_ &&
        RowsFuture_.IsSet() &&
        RowsFuture_.GetOrCrash().IsOK() &&
        !Finished_ &&
        std::ssize(rows) < options.MaxRowsPerRead &&
        dataWeight < options.MaxDataWeightPerRead)
    {
        const auto& currentRows = RowsFuture_.GetOrCrash().Value();

        if (currentRows.Empty()) {
            ReadyEvent_.Set();
            Finished_ = true;
            continue;
        }

        while (CurrentRowsOffset_ < std::ssize(currentRows) &&
            std::ssize(rows) < options.MaxRowsPerRead &&
            dataWeight < options.MaxDataWeightPerRead)
        {
            auto row = currentRows[CurrentRowsOffset_++];
            rows.push_back(row);
            dataWeight += GetDataWeight(row);
        }

        StoredRows_.push_back(currentRows);

        if (CurrentRowsOffset_ == std::ssize(currentRows)) {
            RowsFuture_ = GetRows();
            CurrentRowsOffset_ = 0;
        }
    }

    RowCount_ += rows.size();
    DataWeight_ += dataWeight;

    ReadyEvent_.TrySetFrom(RowsFuture_);
    return rows.empty()
        ? nullptr
        : CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
}

TFuture<void> TRowBatchReader::GetReadyEvent() const
{
    auto readyEvent = ReadyEvent_.ToFuture();
    auto guard = Guard(WaitTimerLock_);
    if (!readyEvent.IsSet()) {
        WaitTimer_.StartIfNotActive();
    }
    return readyEvent;
}

const TNameTablePtr& TRowBatchReader::GetNameTable() const
{
    return NameTable_;
}

TFuture<TSharedRange<TUnversionedRow>> TRowBatchReader::GetRows()
{
    return Underlying_->Read()
        .Apply(BIND([this, weakThis = MakeWeak(this)] (const TSharedRef& block) {
            auto this_ = weakThis.Lock();
            if (!this_) {
                THROW_ERROR_EXCEPTION(NYT::EErrorCode::Canceled, "Reader destroyed");
            }

            auto decodeTimerGuard = NProfiling::TTimerGuard(&DecodeTimer_);

            NProto::TRowsetDescriptor descriptor;
            NProto::TRowsetStatistics statistics;
            auto payloadRef = DeserializeRowStreamBlockEnvelope(
                block,
                &descriptor,
                IsStreamWithStatistics_ ? &statistics : nullptr);

            ValidateRowsetDescriptor(
                descriptor,
                CurrentWireFormatVersion,
                NProto::RK_UNVERSIONED,
                NProto::ERowsetFormat::RF_YT_WIRE);

            if (descriptor.rowset_format() != NApi::NRpcProxy::NProto::RF_YT_WIRE) {
                THROW_ERROR_EXCEPTION(
                    "Unsupported rowset format %Qv",
                    NApi::NRpcProxy::NProto::ERowsetFormat_Name(descriptor.rowset_format()));
            }

            auto batch = Decoder_->Decode(payloadRef, descriptor);
            auto rows = batch->MaterializeRows();

            if (IsStreamWithStatistics_) {
                ApplyStatistics(statistics);
            }

            if (rows.Empty()) {
                return CheckEndOfStream(Underlying_).Apply(BIND([=] () mutable {
                    return std::move(rows);
                }));
            }
            return MakeFuture(std::move(rows));
        }).AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
}

void TRowBatchReader::ApplyStatistics(const NProto::TRowsetStatistics& /*statistics*/)
{ }

TDuration TRowBatchReader::GetWaitTime() const
{
    auto guard = Guard(WaitTimerLock_);
    return WaitTimer_.GetElapsedTime();
}

TDuration TRowBatchReader::GetReadTime() const
{
    return ReadTimer_.GetElapsedTime();
}

TDuration TRowBatchReader::GetDecodeTime() const
{
    return DecodeTimer_.GetElapsedTime();
}

void TRowBatchReader::StopWaitTimerOnReady(const TPromise<void>& readyEvent)
{
    readyEvent.ToFuture().Subscribe(BIND([weakThis = MakeWeak(this)] (const TError& /*error*/) {
        if (auto this_ = weakThis.Lock()) {
            auto guard = Guard(this_->WaitTimerLock_);
            this_->WaitTimer_.Stop();
        }
    }));
}

////////////////////////////////////////////////////////////////////////////////

IRowBatchReaderPtr CreateRowBatchReader(
    IAsyncZeroCopyInputStreamPtr inputStream,
    bool isStreamWithStatistics)
{
    return New<TRowBatchReader>(std::move(inputStream), isStreamWithStatistics);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
