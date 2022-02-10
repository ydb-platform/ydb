#include "schema.h"

namespace NKikimr::NSQS {

TVector<TTable> GetStandardTables(ui64 shards, ui64 partitions, bool enableAutosplit, ui64 sizeToSplit) {
    const TVector<TColumn> AttributesColumns = {
        TColumn("State",                     NScheme::NTypeIds::Uint64, true),
        TColumn("ContentBasedDeduplication", NScheme::NTypeIds::Bool),
        TColumn("DelaySeconds",              NScheme::NTypeIds::Uint64),
        TColumn("FifoQueue",                 NScheme::NTypeIds::Bool),
        TColumn("MaximumMessageSize",        NScheme::NTypeIds::Uint64),
        TColumn("MessageRetentionPeriod",    NScheme::NTypeIds::Uint64),
        TColumn("ReceiveMessageWaitTime",    NScheme::NTypeIds::Uint64),
        TColumn("VisibilityTimeout",         NScheme::NTypeIds::Uint64),
        TColumn("DlqName",                   NScheme::NTypeIds::Utf8),
        TColumn("DlqArn",                    NScheme::NTypeIds::Utf8),
        TColumn("MaxReceiveCount",           NScheme::NTypeIds::Uint64),
        TColumn("ShowDetailedCountersDeadline", NScheme::NTypeIds::Uint64)};

    const TVector<TColumn> StateColumns = {
        TColumn("State",                     NScheme::NTypeIds::Uint64, true),
        TColumn("CleanupTimestamp",          NScheme::NTypeIds::Uint64),
        TColumn("CreatedTimestamp",          NScheme::NTypeIds::Uint64),
        TColumn("LastModifiedTimestamp",     NScheme::NTypeIds::Uint64),
        TColumn("RetentionBoundary",         NScheme::NTypeIds::Uint64),
        TColumn("InflyCount",                NScheme::NTypeIds::Int64),
        TColumn("MessageCount",              NScheme::NTypeIds::Int64),
        TColumn("ReadOffset",                NScheme::NTypeIds::Uint64),
        TColumn("WriteOffset",               NScheme::NTypeIds::Uint64),
        TColumn("CleanupVersion",            NScheme::NTypeIds::Uint64),
        TColumn("InflyVersion",              NScheme::NTypeIds::Uint64)};

    const TVector<TColumn> MessagesDataColumns = {
        TColumn("RandomId",                  NScheme::NTypeIds::Uint64, true, partitions),
        TColumn("Offset",                    NScheme::NTypeIds::Uint64, true),
        TColumn("Attributes",                NScheme::NTypeIds::String),
        TColumn("Data",                      NScheme::NTypeIds::String),
        TColumn("MessageId",                 NScheme::NTypeIds::String),
        TColumn("SenderId",                  NScheme::NTypeIds::String)};

    const TVector<TColumn> MessagesColumns = {
        TColumn("Offset",                    NScheme::NTypeIds::Uint64, true),
        TColumn("RandomId",                  NScheme::NTypeIds::Uint64),
        TColumn("SentTimestamp",             NScheme::NTypeIds::Uint64),
        TColumn("DelayDeadline",             NScheme::NTypeIds::Uint64)};

    const TVector<TColumn> InflyColumns = {
        TColumn("Offset",                    NScheme::NTypeIds::Uint64, true),
        TColumn("RandomId",                  NScheme::NTypeIds::Uint64),
        TColumn("LoadId",                    NScheme::NTypeIds::Uint64),
        TColumn("FirstReceiveTimestamp",     NScheme::NTypeIds::Uint64),
        TColumn("LockTimestamp",             NScheme::NTypeIds::Uint64),
        TColumn("ReceiveCount",              NScheme::NTypeIds::Uint32),
        TColumn("SentTimestamp",             NScheme::NTypeIds::Uint64),
        TColumn("VisibilityDeadline",        NScheme::NTypeIds::Uint64),
        TColumn("DelayDeadline",             NScheme::NTypeIds::Uint64)};

    const TVector<TColumn> SentTimestampIdxColumns = {
        TColumn("SentTimestamp",             NScheme::NTypeIds::Uint64, true),
        TColumn("Offset",                    NScheme::NTypeIds::Uint64, true),
        TColumn("RandomId",                  NScheme::NTypeIds::Uint64),
        TColumn("DelayDeadline",             NScheme::NTypeIds::Uint64)};

    TVector<TTable> list;
    list.reserve(2 + 4 * shards);

    list.push_back(TTable("Attributes")
                    .SetColumns(AttributesColumns)
                    .SetSmall(true)
                    .SetInMemory(true)
                    .SetShard(-1)
                    .SetHasLeaderTablet());
    list.push_back(TTable("State")
                    .SetColumns(StateColumns)
                    .SetSmall(true)
                    .SetOnePartitionPerShard(true)
                    .SetInMemory(true)
                    .SetShard(-1));

    for (ui64 i = 0; i < shards; ++i) {
        list.push_back(TTable("MessageData")
                       .SetColumns(MessagesDataColumns)
                       .SetSequential(true)
                       .SetShard(i)
                       .SetAutosplit(enableAutosplit, sizeToSplit));
        list.push_back(TTable("Messages")
                       .SetColumns(MessagesColumns)
                       .SetSequential(true)
                       .SetShard(i));
        list.push_back(TTable("Infly")
                       .SetColumns(InflyColumns)
                       .SetSmall(true)
                       .SetInMemory(true)
                       .SetShard(i));
        list.push_back(TTable("SentTimestampIdx")
                       .SetColumns(SentTimestampIdxColumns)
                       .SetSequential(true)
                       .SetShard(i));
    }

    return list;
}

TVector<TTable> GetStandardTableNames(ui64 shards) {
    TVector<TTable> list;
    list.reserve(2 + 4 * shards);

    list.push_back(TTable("Attributes").SetShard(-1));
    list.push_back(TTable("State").SetShard(-1));

    for (ui64 i = 0; i < shards; ++i) {
        list.push_back(TTable("MessageData").SetShard(i));
        list.push_back(TTable("Messages").SetShard(i));
        list.push_back(TTable("Infly").SetShard(i));
        list.push_back(TTable("SentTimestampIdx").SetShard(i));
    }

    return list;
}

} // namespace NKikimr::NSQS
