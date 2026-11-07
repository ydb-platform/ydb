#pragma once

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <util/generic/strbuf.h>

#include <string>

namespace NKikimr::NBackup {

// Async replication marks the changefeed it installs on a source table with
// this attribute. The controller keeps reading that source feed, so a copy of
// it restored elsewhere is never consumed: its important consumer would retain
// every write to the restored table forever. Such feeds are neither exported
// nor imported; application changefeeds are kept.
constexpr TStringBuf ASYNC_REPLICATION_CHANGEFEED_ATTRIBUTE = "__async_replication";

inline bool IsAsyncReplicationChangefeed(const Ydb::Table::ChangefeedDescription& changefeed) {
    return changefeed.attributes().count(std::string(ASYNC_REPLICATION_CHANGEFEED_ATTRIBUTE)) > 0;
}

} // namespace NKikimr::NBackup
