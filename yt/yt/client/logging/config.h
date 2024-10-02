#pragma once

#include "public.h"

#include <yt/yt/client/table_client/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/logging/config.h>

#include <yt/yt/core/misc/backoff_strategy.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

//! This log writer can write to both sorted and ordered tables.
//! Configuration notes:
//!   - This writer *must* be configured with format = yson.
//!   - System events are disabled by default for format = yson logs. If you are enabling them for family = plain_text
//!     logs, do not forget to set system_message_family = plain_text.
//!   - If multiple hosts are writing to the same table, consider setting enable_host_field = true.
class TDynamicTableLogWriterConfig
    : public TLogWriterConfig
{
public:
    static constexpr const TStringBuf WriterType = "dynamic_table";

    //! Log destination.
    NYPath::TYPath TablePath;

    //! Log events will be flushed with at least this frequency.
    TDuration FlushPeriod;

    //! Due to its asynchronous nature, the writer keeps serialized events in its own internal buffer.
    //! The buffer size is controlled by the two limits below. Both of them are specified in *bytes*.
    //! If the buffer size exceeds the high watermark, the writer drops any new events until the buffer
    //! size drops below the low watermark.
    i64 LowBacklogWeightWatermark;
    i64 HighBacklogWeightWatermark;

    //! Controls the maximum number of rows that will be batched within a single transaction.
    //! Increase very carefully, the typical server limit is 100'000 rows per transaction.
    //! You can decrease this limit to make your transactions smaller.
    i64 MaxBatchRowCount;

    //! Controls the maximum number of *bytes* that will be batched within a single transaction.
    //! You can decrease this limit to make your transactions smaller.
    i64 MaxBatchWeight;

    //! Infinite retries are configured by default. In this case, persistent write failures will eventually cause
    //! the buffer to overflow and the drop of all incoming events.
    //! One can configure a small number of invocations, which would allow the logger to progress through persistent write failures
    //! by dropping events from the beginning of the queue.
    TExponentialBackoffOptions WriteBackoff;

    //! YSON-parsing options, similar to the ones available in insert_rows.
    //! They are used to convert YSON-serialized log events to unversioned rows.
    NTableClient::TTypeConversionConfigPtr TypeConversion;
    NTableClient::TInsertRowsFormatConfigPtr InsertRowsFormat;

    REGISTER_YSON_STRUCT(TDynamicTableLogWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicTableLogWriterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
