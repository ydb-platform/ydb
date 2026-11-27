#pragma once

#include "public.h"

#include <yt/yt/client/api/client_common.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/hive/public.h>

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/misc/strong_typedef.h>

#include <variant>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EQueueExportScheduleType,
    (CronExpression)
    (Period)
);

//! This CRON format supports features beyond the standard ones, allowing from 5 to 7 fields to be specified:
//!  - with 5 fields, it's a standard CRON
//!  - with 6 fields, the first field is interpreted as SECONDS
//!  - with 7 fields, the last field is interpreted as YEARS
//!
//! \note See library/cpp/cron_expression/readme.md.
YT_DEFINE_STRONG_TYPEDEF(TQueueExportCronExpression, std::string);

//! Either export cron schedule or export period.
class TQueueExportSchedule
{
public:
    //! \note Does not validate cron expression correctness.
    explicit TQueueExportSchedule(TQueueExportCronExpression cronExpression);
    explicit TQueueExportSchedule(TDuration period);

    EQueueExportScheduleType GetType() const;

    //! \note Each getter throws unless #GetType matches it.
    TQueueExportCronExpression GetCronExpression() const;
    TDuration GetPeriod() const;

private:
    std::variant<TQueueExportCronExpression, TDuration> Schedule_;
};

void FormatValue(TStringBuilderBase* builder, const TQueueExportSchedule& schedule, TStringBuf spec);

struct TQueueTabletExport
{
    i64 StartOffset = 0;
    i64 FinishOffset = 0;
    //! Number of exported chunks.
    i64 ChunkCount = 0;
    NHiveClient::TTimestamp MaxTimestamp = NHiveClient::NullTimestamp;
    //! Id of the last exported chunk.
    NChunkClient::TChunkId LastChunk;
};

struct TQueueExportStaticTable
{
    NHiveClient::TTimestamp StartTimestamp = NHiveClient::NullTimestamp;
    NHiveClient::TTimestamp FinishTimestamp = NHiveClient::NullTimestamp;
    THashMap<i64, TQueueTabletExport> Tablets;
    std::string TableName;
    TError Error;
};

////////////////////////////////////////////////////////////////////////////////

struct TExportQueueIntoStaticTableOptions
    : public NApi::TTransactionalOptions
{
    //! If set, specifies the maximum number of exported tables.
    std::optional<i64> TableCountLimit;

    //! If set to a value larger than zero, the created output tables will be created with an expiration time
    //! computed as the export unix ts of the table + ttl.
    TDuration ExportTtl;

    //! A format-string supporting the following specifiers:
    //!  - %UNIX_TS: the unix timestamp corresponding to the exported table
    //!  - %PERIOD: the length of the export period in seconds
    //!  - %ISO: unix timestamp formatted as an ISO time string
    //!  - all specifiers supported by the strftime function (e.g. %H, %M, %S, etc.), used to format the table's unix timestamp
    //! NB: It is your responsibility to guarantee that these names will be unique across export iterations (given that the
    //! unix timestamps corresponding to the output tables are guaranteed to be unique by the export algorithm).
    //! An attempt to produce a table which already exists will lead to an error, in which case the data will be exported
    //! on the next iteration.
    std::string OutputTableNamePattern = "%UNIX_TS-%PERIOD";

    //! If true, the unix timestamp used in formatting the output table name will be the upper bound actually used in gathering chunks for this table.
    //! Otherwise, the unix timestamp used in the name formatting will be equal to the upper bound minus one export period.
    //! E.g. if hourly exports are set up, setting this value to true will mean that a table named 17:00 has data from 16:00 to 17:00,
    //! whereas setting it to false would mean that it has data from 17:00 to 18:00.
    //! NB: In any case, it should be understood that the table exported with an upper bound timestamp of T is not guaranteed to contain all
    //! data with timestamp <= T. In case of delays, some of the data can end up in latter tables. You should be especially careful with
    //! commit_ordering=weak queues, since commit timestamp monotonicty within a tablet is not guaranteed for them.
    bool UseUpperBoundForTableNames = false;

    // COMPAT(akozhikhov)
    bool EnableExportFromQueueWithHunks = false;
};

struct TExportQueueIntoStaticTableResult
{
    std::vector<TQueueExportStaticTable> Tables;
    //! Flag specifying whether queue has more data to be exported right away.
    //! Used in case the number of exported tables reaches internal hard limit per call.
    bool HasMore = false;

    //! Number of tables this call was going to create, i.e. #Tables plus the tables that were skipped
    //! because an earlier table failed to be exported.
    i64 PreparedTableCount = 0;

    //! Errors about the tablets whose last exported chunk does not end at the corresponding start offset.
    std::vector<TError> TabletOffsetMismatchErrors;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
