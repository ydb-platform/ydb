#pragma once

#include <util/generic/fwd.h>
#include <util/generic/strbuf.h>

namespace NYdb {

inline namespace Dev {
namespace NScheme {
    struct TSchemeEntry;
}
}

namespace NConsoleClient {

constexpr TStringBuf METADATA_DIR_NAME = ".metadata";
constexpr TStringBuf SYSVIEWS_DIR_NAME = ".sys";
constexpr TStringBuf TMP_DIR_NAME = ".tmp";
constexpr TStringBuf BACKUPS_DIR_NAME = ".backups";
constexpr TStringBuf OLD_SECRETS_DIR_NAME = "secrets";
constexpr TStringBuf TEMPORARY_BACKUP_DIR_PREFIX = "~backup_";

bool IsSystemName(const TStringBuf name);

bool IsSystemDir(const NScheme::TSchemeEntry& entry);

bool IsSystemObject(const NScheme::TSchemeEntry& entry);

bool IsBackupName(const TStringBuf name);

bool IsBackupDir(const NScheme::TSchemeEntry& entry);

// Includes system objects and temporary backup directories with the ~backup_ prefix.
bool IsInternalName(const TStringBuf name);

bool IsInternalDir(const NScheme::TSchemeEntry& entry);

bool IsInternalObject(const NScheme::TSchemeEntry& entry);

} // NConsoleClient
} // NYdb
