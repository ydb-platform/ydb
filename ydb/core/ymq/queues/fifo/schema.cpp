#include "schema.h"

namespace NKikimr::NSQS {

static const TVector<TColumn> AttributesColumns = {
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

static const TVector<TColumn> StateColumns = {
    TColumn("State",                     NScheme::NTypeIds::Uint64, true),
    TColumn("CleanupTimestamp",          NScheme::NTypeIds::Uint64),
    TColumn("CreatedTimestamp",          NScheme::NTypeIds::Uint64),
    TColumn("LastModifiedTimestamp",     NScheme::NTypeIds::Uint64),
    TColumn("RetentionBoundary",         NScheme::NTypeIds::Uint64),
    TColumn("MessageCount",              NScheme::NTypeIds::Int64),
    TColumn("InflyCount",                NScheme::NTypeIds::Int64),
    TColumn("ReadOffset",                NScheme::NTypeIds::Uint64),
    TColumn("WriteOffset",               NScheme::NTypeIds::Uint64),
    TColumn("CleanupVersion",            NScheme::NTypeIds::Uint64),
    TColumn("InflyVersion",              NScheme::NTypeIds::Uint64)};

static const TVector<TColumn> DataColumns = {
    TColumn("RandomId",                  NScheme::NTypeIds::Uint64, true, 20),
    TColumn("Offset",                    NScheme::NTypeIds::Uint64, true),
    TColumn("DedupId",                   NScheme::NTypeIds::String),
    TColumn("Attributes",                NScheme::NTypeIds::String),
    TColumn("Data",                      NScheme::NTypeIds::String),
    TColumn("MessageId",                 NScheme::NTypeIds::String),
    TColumn("SenderId",                  NScheme::NTypeIds::String),
};

static const TVector<TColumn> MessagesColumns = {
    TColumn("Offset",                    NScheme::NTypeIds::Uint64, true),
    TColumn("RandomId",                  NScheme::NTypeIds::Uint64),
    TColumn("GroupId",                   NScheme::NTypeIds::String),
    TColumn("NextOffset",                NScheme::NTypeIds::Uint64),
    TColumn("NextRandomId",              NScheme::NTypeIds::Uint64),
    TColumn("ReceiveCount",              NScheme::NTypeIds::Uint32),
    TColumn("FirstReceiveTimestamp",     NScheme::NTypeIds::Uint64),
    TColumn("SentTimestamp",             NScheme::NTypeIds::Uint64)};

static const TVector<TColumn> GroupsColumns = {
    TColumn("GroupId",                   NScheme::NTypeIds::String, true),
    TColumn("RandomId",                  NScheme::NTypeIds::Uint64),
    TColumn("Head",                      NScheme::NTypeIds::Uint64),
    TColumn("Tail",                      NScheme::NTypeIds::Uint64),
    TColumn("ReceiveAttemptId",          NScheme::NTypeIds::Utf8),
    TColumn("LockTimestamp",             NScheme::NTypeIds::Uint64),
    TColumn("VisibilityDeadline",        NScheme::NTypeIds::Uint64)};

static const TVector<TColumn> DeduplicationColumns = {
    TColumn("DedupId",                   NScheme::NTypeIds::String, true),
    TColumn("Deadline",                  NScheme::NTypeIds::Uint64),
    TColumn("Offset",                    NScheme::NTypeIds::Uint64),
    TColumn("MessageId",                 NScheme::NTypeIds::String)};

static const TVector<TColumn> ReadsColumns = {
    TColumn("ReceiveAttemptId",          NScheme::NTypeIds::Utf8, true),
    TColumn("Deadline",                  NScheme::NTypeIds::Uint64)};

static const TVector<TColumn> SentTimestampIdxColumns = {
    TColumn("SentTimestamp",             NScheme::NTypeIds::Uint64, true),
    TColumn("Offset",                    NScheme::NTypeIds::Uint64, true),
    TColumn("RandomId",                  NScheme::NTypeIds::Uint64),
    TColumn("DelayDeadline",             NScheme::NTypeIds::Uint64),
    TColumn("GroupId",                   NScheme::NTypeIds::String)};

TVector<TTable> GetFifoTables() {
    TVector<TTable> list;
    list.reserve(8);

    list.push_back(TTable("Attributes")
                    .SetColumns(AttributesColumns)
                    .SetShard(-1)
                    .SetHasLeaderTablet());
    list.push_back(TTable("Data")
                    .SetColumns(DataColumns)
                    .SetShard(-1));
    list.push_back(TTable("Deduplication")
                    .SetColumns(DeduplicationColumns)
                    .SetShard(-1));
    list.push_back(TTable("Groups")
                    .SetColumns(GroupsColumns)
                    .SetShard(-1));
    list.push_back(TTable("Messages")
                    .SetColumns(MessagesColumns)
                    .SetSequential(true)
                    .SetShard(-1));
    list.push_back(TTable("Reads")
                    .SetColumns(ReadsColumns)
                    .SetShard(-1));
    list.push_back(TTable("State")
                    .SetColumns(StateColumns)
                    .SetShard(-1));
    list.push_back(TTable("SentTimestampIdx")
                    .SetColumns(SentTimestampIdxColumns)
                    .SetSequential(true)
                    .SetShard(-1));

    return list;
}

} // namespace NKikimr::NSQS
