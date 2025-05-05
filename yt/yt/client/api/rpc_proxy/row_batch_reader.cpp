#include "row_batch_reader.h"
#include "helpers.h"
#include "row_stream.h"
#include "wire_row_stream.h"

#include <yt/yt/client/table_client/name_table.h>

#include <yt_proto/yt/client/api/rpc_proxy/proto/api_service.pb.h>

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
    ReadyEvent_.TrySetFrom(RowsFuture_);
}

IUnversionedRowBatchPtr TRowBatchReader::Read(const TRowBatchReadOptions& options)
{
    StoredRows_.clear();

    if (!ReadyEvent_.IsSet() || !ReadyEvent_.Get().IsOK()) {
        return CreateEmptyUnversionedRowBatch();
    }

    if (!Finished_) {
        ReadyEvent_ = NewPromise<void>();
    }

    std::vector<TUnversionedRow> rows;
    rows.reserve(options.MaxRowsPerRead);
    i64 dataWeight = 0;

    while (RowsFuture_ &&
        RowsFuture_.IsSet() &&
        RowsFuture_.Get().IsOK() &&
        !Finished_ &&
        std::ssize(rows) < options.MaxRowsPerRead &&
        dataWeight < options.MaxDataWeightPerRead)
    {
        const auto& currentRows = RowsFuture_.Get().Value();

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
    return ReadyEvent_;
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
                return ExpectEndOfStream(Underlying_).Apply(BIND([=] {
                    return std::move(rows);
                }));
            }
            return MakeFuture(std::move(rows));
        }));
}

void TRowBatchReader::ApplyStatistics(const NProto::TRowsetStatistics& /*statistics*/)
{ }

////////////////////////////////////////////////////////////////////////////////

IRowBatchReaderPtr CreateRowBatchReader(
    IAsyncZeroCopyInputStreamPtr inputStream,
    bool isStreamWithStatistics)
{
    return New<TRowBatchReader>(std::move(inputStream), isStreamWithStatistics);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
