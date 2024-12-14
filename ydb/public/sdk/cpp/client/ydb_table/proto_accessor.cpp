#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include "table.h"

namespace NYdb {

const Ydb::TableStats::QueryStats& TProtoAccessor::GetProto(const NTable::TQueryStats& queryStats) {
    return queryStats.GetProto();
}

const Ydb::Table::DescribeTableResult& TProtoAccessor::GetProto(const NTable::TTableDescription& tableDescription) {
    return tableDescription.GetProto();
}

NTable::TQueryStats TProtoAccessor::FromProto(const Ydb::TableStats::QueryStats& queryStats) {
    return NTable::TQueryStats(queryStats);
}

NTable::TTableDescription TProtoAccessor::FromProto(const Ydb::Table::CreateTableRequest& request) {
    return NTable::TTableDescription(request);
}

NTable::TIndexDescription TProtoAccessor::FromProto(const Ydb::Table::TableIndex& tableIndex) {
    return NTable::TIndexDescription(tableIndex);
}

NTable::TIndexDescription TProtoAccessor::FromProto(const Ydb::Table::TableIndexDescription& tableIndexDesc) {
    return NTable::TIndexDescription(tableIndexDesc);
}

NTable::TChangefeedDescription TProtoAccessor::FromProto(const Ydb::Table::Changefeed& changefeed) {
    return NTable::TChangefeedDescription(changefeed);
}

NTable::TChangefeedDescription TProtoAccessor::FromProto(const Ydb::Table::ChangefeedDescription& changefeed) {
    return NTable::TChangefeedDescription(changefeed);
}

Ydb::Table::ValueSinceUnixEpochModeSettings::Unit TProtoAccessor::GetProto(NTable::TValueSinceUnixEpochModeSettings::EUnit value) {
    switch (value) {
    case NTable::TValueSinceUnixEpochModeSettings::EUnit::Seconds:
        return Ydb::Table::ValueSinceUnixEpochModeSettings::UNIT_SECONDS;
    case NTable::TValueSinceUnixEpochModeSettings::EUnit::MilliSeconds:
        return Ydb::Table::ValueSinceUnixEpochModeSettings::UNIT_MILLISECONDS;
    case NTable::TValueSinceUnixEpochModeSettings::EUnit::MicroSeconds:
        return Ydb::Table::ValueSinceUnixEpochModeSettings::UNIT_MICROSECONDS;
    case NTable::TValueSinceUnixEpochModeSettings::EUnit::NanoSeconds:
        return Ydb::Table::ValueSinceUnixEpochModeSettings::UNIT_NANOSECONDS;
    case NTable::TValueSinceUnixEpochModeSettings::EUnit::Unknown:
        return Ydb::Table::ValueSinceUnixEpochModeSettings::UNIT_UNSPECIFIED;
    }
}

NTable::TValueSinceUnixEpochModeSettings::EUnit TProtoAccessor::FromProto(Ydb::Table::ValueSinceUnixEpochModeSettings::Unit value) {
    switch (value) {
    case Ydb::Table::ValueSinceUnixEpochModeSettings::UNIT_SECONDS:
        return NTable::TValueSinceUnixEpochModeSettings::EUnit::Seconds;
    case Ydb::Table::ValueSinceUnixEpochModeSettings::UNIT_MILLISECONDS:
        return NTable::TValueSinceUnixEpochModeSettings::EUnit::MilliSeconds;
    case Ydb::Table::ValueSinceUnixEpochModeSettings::UNIT_MICROSECONDS:
        return NTable::TValueSinceUnixEpochModeSettings::EUnit::MicroSeconds;
    case Ydb::Table::ValueSinceUnixEpochModeSettings::UNIT_NANOSECONDS:
        return NTable::TValueSinceUnixEpochModeSettings::EUnit::NanoSeconds;
    default:
        return NTable::TValueSinceUnixEpochModeSettings::EUnit::Unknown;
    }
}

}
