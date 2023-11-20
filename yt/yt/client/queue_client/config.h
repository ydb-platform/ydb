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
    bool UsePullConsumer;

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
