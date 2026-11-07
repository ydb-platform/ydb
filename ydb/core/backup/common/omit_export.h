#pragma once

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <util/generic/strbuf.h>

#include <string>

namespace NKikimr::NBackup {

// A scheme object carrying this user attribute is left out of exports and
// backups: async replication marks the changefeed it installs on a source
// table this way. The controller keeps reading that source feed, so a copy
// restored elsewhere is never consumed; its important consumer would retain
// every write to the restored table forever.
constexpr TStringBuf OMIT_EXPORT_ATTRIBUTE = "__omit_export";
constexpr TStringBuf OMIT_EXPORT_VALUE = "true";

template <typename TAttributes>
bool IsOmittedFromExport(const TAttributes& attributes) {
    const auto it = attributes.find(typename TAttributes::key_type(OMIT_EXPORT_ATTRIBUTE));
    return it != attributes.end() && it->second == OMIT_EXPORT_VALUE;
}

// Replication feeds installed before the controller started marking them are
// recognized by the replication attribute itself.
constexpr TStringBuf ASYNC_REPLICATION_ATTRIBUTE = "__async_replication";

inline bool IsChangefeedOmittedFromExport(const Ydb::Table::ChangefeedDescription& changefeed) {
    return IsOmittedFromExport(changefeed.attributes())
        || changefeed.attributes().count(std::string(ASYNC_REPLICATION_ATTRIBUTE)) > 0;
}

} // namespace NKikimr::NBackup
