#pragma once
#include "defs.h"
#include "keyvalue_request_type.h"
#include <ydb/core/tablet/tablet_metrics.h>

namespace NKikimr {
namespace NKeyValue {

struct TRequestStat {
    ui64 ReadBytes;
    ui64 Reads;
    ui64 ReadNodata;
    ui64 RangeReadBytes;
    ui64 RangeReadItems;
    ui64 RangeReadItemsNodata;
    ui64 IndexRangeRead;
    ui64 DeleteBytes;
    ui64 Deletes;
    ui64 Renames;
    ui64 CopyRanges;
    ui64 Concats;
    ui64 WriteBytes;
    ui64 Writes;
    ui64 GetStatuses;
    ui64 EnqueuedAs;
    TDeque<ui64> GetLatencies;
    TDeque<ui64> PutLatencies;
    TDeque<ui64> GetStatusLatencies;
    TInstant KeyvalueStorageRequestSentAt;
    TInstant LocalBaseTxCreatedAt;
    TInstant IntermediateCreatedAt;
    TRequestType::EType RequestType;
    NMetrics::TTabletThroughputRawValue GroupReadBytes;
    NMetrics::TTabletThroughputRawValue GroupWrittenBytes;
    NMetrics::TTabletIopsRawValue GroupReadIops;
    NMetrics::TTabletIopsRawValue GroupWrittenIops;

    TVector<ui32> YellowStopChannels;
    TVector<ui32> YellowMoveChannels;

    void Clear() {
        ReadBytes = 0;
        Reads = 0;
        ReadNodata = 0;
        RangeReadBytes = 0;
        RangeReadItems = 0;
        RangeReadItemsNodata = 0;
        IndexRangeRead = 0;
        DeleteBytes = 0;
        Deletes = 0;
        CopyRanges = 0;
        Concats = 0;
        Renames = 0;
        WriteBytes = 0;
        Writes = 0;
        GetStatuses = 0;
        EnqueuedAs = 0;
        GetLatencies.clear();
        PutLatencies.clear();
        RequestType = TRequestType::ReadOnly;
        YellowMoveChannels.clear();
        YellowStopChannels.clear();
    }

    TRequestStat() {
        Clear();
    }
};

} // NKeyValue
} // NKikimr
