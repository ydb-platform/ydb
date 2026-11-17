#pragma once

#include <util/generic/strbuf.h>

namespace Ydb::Table {
class ChangefeedDescription;
}

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

// Both existing and newly created replication feeds use this marker. Sources
// additionally receive __omit_backup when enabled in replication_config.
// This exclusion still applies after removal of __omit_backup: a restored
// replication feed has no reader, so its important consumer could retain data
// indefinitely.
constexpr TStringBuf ASYNC_REPLICATION_ATTRIBUTE = "__async_replication";

bool IsChangefeedOmittedFromBackup(const Ydb::Table::ChangefeedDescription& changefeed);

} // namespace NKikimr::NBackup
