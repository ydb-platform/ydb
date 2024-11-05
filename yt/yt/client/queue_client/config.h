#pragma once

#include "public.h"

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

class TPartitionReaderConfig
    : public NYTree::TYsonStruct
{
public:
    i64 MaxRowCount;
    i64 MaxDataWeight;

    //! If set, this value is used to compute the number of rows to read considering the given MaxDataWeight.
    std::optional<i64> DataWeightPerRowHint;

    bool UseNativeTabletNodeApi;
    bool UsePullQueueConsumer;

    REGISTER_YSON_STRUCT(TPartitionReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPartitionReaderConfig)

////////////////////////////////////////////////////////////////////////////////

//! Automatic trimming configuration for a single queue.
//!
//! All rows up to the smallest offset among vital consumers are considered trimmable.
//! If no other setting in the config explicitly prohibits trimming these rows,
//! they will be trimmed automatically by the responsible queue agent.
//! This is not applicable if no vital consumers exist for a queue.
// TODO(achulkov2): Add example of how multiple vital/non-vital consumers and the options below interact.
class TQueueAutoTrimConfig
    : public NYTree::TYsonStructLite
{
public:
    //! If set to false, no automatic trimming is performed.
    bool Enable;

    //! If set, this number of rows is guaranteed to be kept in each partition.
    std::optional<i64> RetainedRows;

    //! If set, rows, that were created no more than this time ago, will be kept in each partition.
    std::optional<TDuration> RetainedLifetimeDuration;

    REGISTER_YSON_STRUCT_LITE(TQueueAutoTrimConfig);

    static void Register(TRegistrar registrar);
};

bool operator==(const TQueueAutoTrimConfig& lhs, const TQueueAutoTrimConfig& rhs);

////////////////////////////////////////////////////////////////////////////////

class TQueueStaticExportConfig
    : public NYTree::TYsonStructLite
{
public:
    //! Export will be performed at times that are multiple of this period.
    TDuration ExportPeriod;

    //! Path to directory that will contain resulting static tables with exported data.
    NYPath::TYPath ExportDirectory;

    //! If set to a value larger than zero, the created output tables will be created with an expiration time computed as now + ttl.
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
    TString OutputTableNamePattern;
    //! If true, the unix timestamp used in formatting the output table name will be the upper bound actually used in gathering chunks for this table.
    //! Otherwise, the unix timestamp used in the name formatting will be equal to the upper bound minus one export period.
    //! E.g. if hourly exports are set up, setting this value to true will mean that a table named 17:00 has data from 16:00 to 17:00,
    //! whereas setting it to false would mean that it has data from 17:00 to 18:00.
    //! NB: In any case, it should be understood that the table exported with an upper bound timestamp of T is not guaranteed to contain all
    //! data with timestamp <= T. In case of delays, some of the data can end up in latter tables. You should be especially careful with
    //! commit_ordering=%false queues, since commit timestamp monotonicty within a tablet is not guaranteed for them.
    bool UseUpperBoundForTableNames;

    REGISTER_YSON_STRUCT_LITE(TQueueStaticExportConfig);

    static void Register(TRegistrar registrar);
};

bool operator==(const TQueueStaticExportConfig& lhs, const TQueueStaticExportConfig& rhs);

////////////////////////////////////////////////////////////////////////////////

class TQueueStaticExportDestinationConfig
    : public NYTree::TYsonStructLite
{
public:
    NObjectClient::TObjectId OriginatingQueueId;

    REGISTER_YSON_STRUCT_LITE(TQueueStaticExportDestinationConfig);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
