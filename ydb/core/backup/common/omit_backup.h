#pragma once

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <util/generic/strbuf.h>

#include <string>

namespace NKikimr::NBackup {

// Objects carrying this attribute are omitted from backup and export.
// Directory-like objects are not supported: this is an object-local flag, not
// an inherited subtree policy. Removing it clears only this exclusion rule.
constexpr TStringBuf OMIT_BACKUP_ATTRIBUTE = "__omit_backup";
constexpr TStringBuf OMIT_BACKUP_VALUE = "true";

template <typename TAttributes>
bool IsOmittedFromBackup(const TAttributes& attributes) {
    const auto it = attributes.find(typename TAttributes::key_type(OMIT_BACKUP_ATTRIBUTE));
    return it != attributes.end() && it->second == OMIT_BACKUP_VALUE;
}

// Both existing and newly created replication feeds use this marker. New sources
// additionally receive __omit_backup; the controller falls back to this marker
// when an old source rejects that attribute. This exclusion still applies after
// removal of __omit_backup: a restored replication feed has no reader, so its
// important consumer could retain data indefinitely.
constexpr TStringBuf ASYNC_REPLICATION_ATTRIBUTE = "__async_replication";

inline bool IsChangefeedOmittedFromBackup(const Ydb::Table::ChangefeedDescription& changefeed) {
    return IsOmittedFromBackup(changefeed.attributes())
        || changefeed.attributes().count(std::string(ASYNC_REPLICATION_ATTRIBUTE)) > 0;
}

} // namespace NKikimr::NBackup
