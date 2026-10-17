#pragma once

#include "public.h"
#include "row_batch_reader.h"

#include <yt/yt/client/chunk_client/timing_statistics.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/data_statistics.pb.h>

#include <yt/yt/core/yson/public.h>

#include <library/cpp/yt/string/format.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

//! Timing statistics of the proxy-side reader as delivered to a remote client.
//! The first three fields mirror #TTableReaderTimingStatistics measured on the proxy.
struct TRemoteTableReaderTimingStatistics
{
    std::optional<TDuration> MasterFetchTime;
    std::optional<NChunkClient::TTimingStatistics> DataReadTiming;
    TDuration TotalTime;
    //! Time spent encoding row batches into the wire format.
    TDuration EncodeTime;
    //! Time the proxy was blocked because the client had not consumed the sent data.
    TDuration WriteStallTime;
    //! Time the proxy had nothing to send, i.e. everything sent was already consumed by the client.
    TDuration WindowDrainedTime;
};

void Serialize(const TRemoteTableReaderTimingStatistics& statistics, NYson::IYsonConsumer* consumer);

void FormatValue(TStringBuilderBase* builder, const TRemoteTableReaderTimingStatistics& statistics, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

//! Timing statistics of a table reader as observed by its caller.
//! Optional fields are absent while the corresponding phase has not happened yet or is not applicable to the reader.
//! #TotalTime approximately equals the sum of #MasterFetchTime and the components of #DataReadTiming.
struct TTableReaderTimingStatistics
{
    //! Time spent fetching table metadata from master before the underlying reader was created.
    std::optional<TDuration> MasterFetchTime;
    //! Wait, read and idle time of the underlying reader.
    std::optional<NChunkClient::TTimingStatistics> DataReadTiming;
    //! Time elapsed since the reader was requested, i.e. since the corresponding #CreateTableReader call.
    TDuration TotalTime;
    //! Time spent decoding blocks in the background; overlaps with the other components.
    //! Reported by remote (RPC proxy) readers only.
    std::optional<TDuration> DecodeTime;
    //! Statistics of the proxy-side reader.
    //! Reported by remote (RPC proxy) readers only.
    std::optional<TRemoteTableReaderTimingStatistics> Remote;
};

void Serialize(const TTableReaderTimingStatistics& statistics, NYson::IYsonConsumer* consumer);

void FormatValue(TStringBuilderBase* builder, const TTableReaderTimingStatistics& statistics, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

struct ITableReader
    : public virtual IRowBatchReader
{
    //! Returns the starting row index within the table.
    virtual i64 GetStartRowIndex() const = 0;

    //! Returns the total (approximate) number of rows readable.
    virtual i64 GetTotalRowCount() const = 0;

    //! Returns various data statistics.
    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const = 0;

    //! Returns timing statistics accumulated so far.
    virtual TTableReaderTimingStatistics GetTimingStatistics() const = 0;

    //! Returns schema of the table.
    virtual const NTableClient::TTableSchemaPtr& GetTableSchema() const = 0;

    //! Returns the names of columns that are not accessible according to columnar ACL
    //! and were omitted. See #TTableReaderOptions::OmitInaccessibleColumns.
    virtual const std::vector<std::string>& GetOmittedInaccessibleColumns() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ITableReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
