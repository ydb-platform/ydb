#include <ydb-cpp-sdk/client/proto/accessor.h>

#include <ydb-cpp-sdk/client/table/table.h>

namespace NYdb::inline V3 {

const NYdbProtos::TableStats::QueryStats& TProtoAccessor::GetProto(const NTable::TQueryStats& queryStats) {
    return queryStats.GetProto();
}

const NYdbProtos::Table::DescribeTableResult& TProtoAccessor::GetProto(const NTable::TTableDescription& tableDescription) {
    return tableDescription.GetProto();
}

const NYdbProtos::Table::DescribeExternalDataSourceResult& TProtoAccessor::GetProto(const NTable::TExternalDataSourceDescription& description) {
    return description.GetProto();
}

const NYdbProtos::Table::DescribeExternalTableResult& TProtoAccessor::GetProto(const NTable::TExternalTableDescription& description) {
    return description.GetProto();
}

NTable::TQueryStats TProtoAccessor::FromProto(const NYdbProtos::TableStats::QueryStats& queryStats) {
    return NTable::TQueryStats(queryStats);
}

NTable::TTableDescription TProtoAccessor::FromProto(const NYdbProtos::Table::CreateTableRequest& request) {
    return NTable::TTableDescription(request);
}

NTable::TIndexDescription TProtoAccessor::FromProto(const NYdbProtos::Table::TableIndex& tableIndex) {
    return NTable::TIndexDescription(tableIndex);
}

NTable::TIndexDescription TProtoAccessor::FromProto(const NYdbProtos::Table::TableIndexDescription& tableIndexDesc) {
    return NTable::TIndexDescription(tableIndexDesc);
}

NTable::TChangefeedDescription TProtoAccessor::FromProto(const NYdbProtos::Table::Changefeed& changefeed) {
    return NTable::TChangefeedDescription(changefeed);
}

NTable::TChangefeedDescription TProtoAccessor::FromProto(const NYdbProtos::Table::ChangefeedDescription& changefeed) {
    return NTable::TChangefeedDescription(changefeed);
}

NYdbProtos::Table::ValueSinceUnixEpochModeSettings::Unit TProtoAccessor::GetProto(NTable::TValueSinceUnixEpochModeSettings::EUnit value) {
    switch (value) {
    case NTable::TValueSinceUnixEpochModeSettings::EUnit::Seconds:
        return NYdbProtos::Table::ValueSinceUnixEpochModeSettings::UNIT_SECONDS;
    case NTable::TValueSinceUnixEpochModeSettings::EUnit::MilliSeconds:
        return NYdbProtos::Table::ValueSinceUnixEpochModeSettings::UNIT_MILLISECONDS;
    case NTable::TValueSinceUnixEpochModeSettings::EUnit::MicroSeconds:
        return NYdbProtos::Table::ValueSinceUnixEpochModeSettings::UNIT_MICROSECONDS;
    case NTable::TValueSinceUnixEpochModeSettings::EUnit::NanoSeconds:
        return NYdbProtos::Table::ValueSinceUnixEpochModeSettings::UNIT_NANOSECONDS;
    case NTable::TValueSinceUnixEpochModeSettings::EUnit::Unknown:
        return NYdbProtos::Table::ValueSinceUnixEpochModeSettings::UNIT_UNSPECIFIED;
    }
}

NTable::TValueSinceUnixEpochModeSettings::EUnit TProtoAccessor::FromProto(NYdbProtos::Table::ValueSinceUnixEpochModeSettings::Unit value) {
    switch (value) {
    case NYdbProtos::Table::ValueSinceUnixEpochModeSettings::UNIT_SECONDS:
        return NTable::TValueSinceUnixEpochModeSettings::EUnit::Seconds;
    case NYdbProtos::Table::ValueSinceUnixEpochModeSettings::UNIT_MILLISECONDS:
        return NTable::TValueSinceUnixEpochModeSettings::EUnit::MilliSeconds;
    case NYdbProtos::Table::ValueSinceUnixEpochModeSettings::UNIT_MICROSECONDS:
        return NTable::TValueSinceUnixEpochModeSettings::EUnit::MicroSeconds;
    case NYdbProtos::Table::ValueSinceUnixEpochModeSettings::UNIT_NANOSECONDS:
        return NTable::TValueSinceUnixEpochModeSettings::EUnit::NanoSeconds;
    default:
        return NTable::TValueSinceUnixEpochModeSettings::EUnit::Unknown;
    }
}

}
