#include "table_reader.h"
#include "row_batch_reader.h"
#include "helpers.h"

#include <yt/yt/client/api/table_reader.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/data_statistics.pb.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/system/rw_spin_lock.h>

namespace NYT::NApi::NRpcProxy {

using namespace NConcurrency;
using namespace NTableClient;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TTableReader
    : public TRowBatchReader
    , public ITableReader
{
public:
    TTableReader(
        IAsyncZeroCopyInputStreamPtr underlying,
        i64 startRowIndex,
        const std::vector<std::string>& omittedInaccessibleColumns,
        TTableSchemaPtr schema,
        const NProto::TRowsetStatistics& statistics,
        const NProfiling::TWallTimer& totalTimer)
        : TRowBatchReader(std::move(underlying), /*isStreamWithStatistics*/ true)
        , StartRowIndex_(startRowIndex)
        , TableSchema_(std::move(schema))
        , OmittedInaccessibleColumns_(omittedInaccessibleColumns)
        , TotalTimer_(totalTimer)
        , InitialWaitTime_(totalTimer.GetElapsedTime())
    {
        ApplyStatistics(statistics);
    }

    i64 GetStartRowIndex() const override
    {
        return StartRowIndex_;
    }

    i64 GetTotalRowCount() const override
    {
        auto guard = ReaderGuard(StatisticsLock_);
        return TotalRowCount_;
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        auto guard = ReaderGuard(StatisticsLock_);
        auto dataStatistics = DataStatistics_;
        dataStatistics.set_row_count(RowCount_);
        dataStatistics.set_data_weight(DataWeight_);
        return dataStatistics;
    }

    TTableReaderTimingStatistics GetTimingStatistics() const override
    {
        // The wait for the meta block happened before this object existed, so it is added here.
        auto waitTime = InitialWaitTime_ + GetWaitTime();
        auto readTime = GetReadTime();
        auto totalTime = TotalTimer_.GetElapsedTime();
        auto busyTime = waitTime + readTime;

        TTableReaderTimingStatistics statistics{
            .DataReadTiming = NChunkClient::TTimingStatistics{
                .WaitTime = waitTime,
                .ReadTime = readTime,
                .IdleTime = totalTime > busyTime ? totalTime - busyTime : TDuration::Zero(),
            },
            .TotalTime = totalTime,
            .DecodeTime = GetDecodeTime(),
        };

        auto guard = ReaderGuard(StatisticsLock_);
        statistics.Remote = RemoteTimingStatistics_;
        return statistics;
    }

    const TTableSchemaPtr& GetTableSchema() const override
    {
        return TableSchema_;
    }

    const std::vector<std::string>& GetOmittedInaccessibleColumns() const override
    {
        return OmittedInaccessibleColumns_;
    }

private:
    const i64 StartRowIndex_;
    const TTableSchemaPtr TableSchema_;
    const std::vector<std::string> OmittedInaccessibleColumns_;
    const NProfiling::TWallTimer TotalTimer_;
    const TDuration InitialWaitTime_;

    // NB: Statistics are updated asynchronously.
    YT_DECLARE_SPIN_LOCK(TReaderWriterSpinLock, StatisticsLock_);
    NChunkClient::NProto::TDataStatistics DataStatistics_;
    i64 TotalRowCount_ = 0;
    std::optional<TRemoteTableReaderTimingStatistics> RemoteTimingStatistics_;

    void ApplyStatistics(const NProto::TRowsetStatistics& statistics) override
    {
        auto guard = WriterGuard(StatisticsLock_);
        TotalRowCount_ = statistics.total_row_count();
        DataStatistics_ = statistics.data_statistics();
        if (statistics.has_timing_statistics()) {
            RemoteTimingStatistics_ = FromProto<TRemoteTableReaderTimingStatistics>(statistics.timing_statistics());
        }
    }
};

TFuture<ITableReaderPtr> CreateTableReader(
    IAsyncZeroCopyInputStreamPtr inputStream,
    const NProfiling::TWallTimer& totalTimer)
{
    return inputStream->Read().Apply(BIND([=] (const TSharedRef& metaRef) {
        NApi::NRpcProxy::NProto::TRspReadTableMeta meta;
        if (!TryDeserializeProto(&meta, metaRef)) {
            THROW_ERROR_EXCEPTION("Failed to deserialize table reader meta information");
        }

        return New<TTableReader>(
            inputStream,
            meta.start_row_index(),
            FromProto<std::vector<std::string>>(meta.omitted_inaccessible_columns()),
            FromProto<TTableSchemaPtr>(meta.schema()),
            meta.statistics(),
            totalTimer);
    })).As<ITableReaderPtr>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
