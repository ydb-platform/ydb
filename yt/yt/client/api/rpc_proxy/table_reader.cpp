#include "table_reader.h"
#include "helpers.h"
#include "row_stream.h"
#include "wire_row_stream.h"

#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/table_reader.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/data_statistics.pb.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/row_batch.h>

#include <yt/yt/core/rpc/stream.h>

namespace NYT::NApi::NRpcProxy {

using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TTableReader
    : public ITableReader
{
public:
    TTableReader(
        IAsyncZeroCopyInputStreamPtr underlying,
        i64 startRowIndex,
        const std::vector<TString>& omittedInaccessibleColumns,
        TTableSchemaPtr schema,
        const NApi::NRpcProxy::NProto::TRowsetStatistics& statistics)
        : Underlying_ (std::move(underlying))
        , StartRowIndex_(startRowIndex)
        , TableSchema_(std::move(schema))
        , OmittedInaccessibleColumns_(omittedInaccessibleColumns)
        , Decoder_(CreateWireRowStreamDecoder(NameTable_))
    {
        YT_VERIFY(Underlying_);

        ApplyReaderStatistics(statistics);

        RowsWithStatisticsFuture_ = GetRowsWithStatistics();
        ReadyEvent_.TrySetFrom(RowsWithStatisticsFuture_);
    }

    i64 GetStartRowIndex() const override
    {
        return StartRowIndex_;
    }

    i64 GetTotalRowCount() const override
    {
        return TotalRowCount_;
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        auto dataStatistics = DataStatistics_;
        dataStatistics.set_row_count(RowCount_);
        dataStatistics.set_data_weight(DataWeight_);

        return dataStatistics;
    }

    TFuture<void> GetReadyEvent() override
    {
        return ReadyEvent_;
    }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
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

        while (RowsWithStatisticsFuture_ &&
               RowsWithStatisticsFuture_.IsSet() &&
               RowsWithStatisticsFuture_.Get().IsOK() &&
               !Finished_ &&
               std::ssize(rows) < options.MaxRowsPerRead &&
               dataWeight < options.MaxDataWeightPerRead)
        {
            const auto& currentRows = RowsWithStatisticsFuture_.Get().Value().Rows;
            const auto& currentStatistics = RowsWithStatisticsFuture_.Get().Value().Statistics;

            if (currentRows.Empty()) {
                ReadyEvent_.Set();
                Finished_ = true;
                ApplyReaderStatistics(currentStatistics);
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
            ApplyReaderStatistics(currentStatistics);

            if (CurrentRowsOffset_ == std::ssize(currentRows)) {
                RowsWithStatisticsFuture_ = GetRowsWithStatistics();
                CurrentRowsOffset_ = 0;
            }
        }

        RowCount_ += rows.size();
        DataWeight_ += dataWeight;

        ReadyEvent_.TrySetFrom(RowsWithStatisticsFuture_);
        return rows.empty()
            ? nullptr
            : CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    const TTableSchemaPtr& GetTableSchema() const override
    {
        return TableSchema_;
    }

    const std::vector<TString>& GetOmittedInaccessibleColumns() const override
    {
        return OmittedInaccessibleColumns_;
    }

private:
    struct TRowsWithStatistics
    {
        TSharedRange<TUnversionedRow> Rows;
        NApi::NRpcProxy::NProto::TRowsetStatistics Statistics;
    };

    const IAsyncZeroCopyInputStreamPtr Underlying_;
    const i64 StartRowIndex_;
    const TTableSchemaPtr TableSchema_;
    const std::vector<TString> OmittedInaccessibleColumns_;

    const TNameTablePtr NameTable_ = New<TNameTable>();
    const IRowStreamDecoderPtr Decoder_;

    NChunkClient::NProto::TDataStatistics DataStatistics_;
    i64 TotalRowCount_;

    i64 RowCount_ = 0;
    i64 DataWeight_ = 0;

    TNameTableToSchemaIdMapping IdMapping_;

    TPromise<void> ReadyEvent_ = NewPromise<void>();

    std::vector<TSharedRange<TUnversionedRow>> StoredRows_;
    TFuture<TRowsWithStatistics> RowsWithStatisticsFuture_;
    i64 CurrentRowsOffset_ = 0;

    bool Finished_ = false;

    void ApplyReaderStatistics(const NApi::NRpcProxy::NProto::TRowsetStatistics& statistics)
    {
        TotalRowCount_ = statistics.total_row_count();
        DataStatistics_ = statistics.data_statistics();
    }

    TFuture<TRowsWithStatistics> GetRowsWithStatistics()
    {
        return Underlying_->Read()
            .Apply(BIND([this, weakThis = MakeWeak(this)] (const TSharedRef& block) {
                auto this_ = weakThis.Lock();
                if (!this_) {
                    THROW_ERROR_EXCEPTION(NYT::EErrorCode::Canceled, "Reader destroyed");
                }

                NApi::NRpcProxy::NProto::TRowsetDescriptor descriptor;
                NApi::NRpcProxy::NProto::TRowsetStatistics statistics;
                auto payloadRef = DeserializeRowStreamBlockEnvelope(block, &descriptor, &statistics);

                ValidateRowsetDescriptor(
                    descriptor,
                    NApi::NRpcProxy::CurrentWireFormatVersion,
                    NApi::NRpcProxy::NProto::RK_UNVERSIONED,
                    NApi::NRpcProxy::NProto::ERowsetFormat::RF_YT_WIRE);

                auto decoder = GetOrCreateDecoder(descriptor.rowset_format());
                auto batch = decoder->Decode(payloadRef, descriptor);
                auto rows = batch->MaterializeRows();
                auto rowsWithStatistics = TRowsWithStatistics{
                    std::move(rows),
                    std::move(statistics)
                };

                if (rowsWithStatistics.Rows.Empty()) {
                    return ExpectEndOfStream(Underlying_).Apply(BIND([=] {
                        return std::move(rowsWithStatistics);
                    }));
                }
                return MakeFuture(std::move(rowsWithStatistics));
            }));
    }

    IRowStreamDecoderPtr GetOrCreateDecoder(NApi::NRpcProxy::NProto::ERowsetFormat format)
    {
        if (format != NApi::NRpcProxy::NProto::RF_YT_WIRE) {
            THROW_ERROR_EXCEPTION("Unsupported rowset format %Qv",
                NApi::NRpcProxy::NProto::ERowsetFormat_Name(format));
        }
        return Decoder_;
    }
};

TFuture<ITableReaderPtr> CreateTableReader(IAsyncZeroCopyInputStreamPtr inputStream)
{
    return inputStream->Read().Apply(BIND([=] (const TSharedRef& metaRef) {
        NApi::NRpcProxy::NProto::TRspReadTableMeta meta;
        if (!TryDeserializeProto(&meta, metaRef)) {
            THROW_ERROR_EXCEPTION("Failed to deserialize table reader meta information");
        }

        i64 startRowIndex = meta.start_row_index();
        auto omittedInaccessibleColumns = FromProto<std::vector<TString>>(
            meta.omitted_inaccessible_columns());
        auto schema = NYT::FromProto<TTableSchemaPtr>(meta.schema());
        return New<TTableReader>(
            inputStream,
            startRowIndex,
            std::move(omittedInaccessibleColumns),
            std::move(schema),
            meta.statistics());
    })).As<ITableReaderPtr>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
