#pragma once

#include <util/generic/strbuf.h>

namespace NKikimr::NNaming {

inline constexpr TStringBuf SYSVIEWS_DIR_NAME = ".sys";
inline constexpr TStringBuf METADATA_DIR_NAME = ".metadata";
inline constexpr TStringBuf TMP_DIR_NAME = ".tmp";
inline constexpr TStringBuf BACKUPS_DIR_NAME = ".backups";
inline constexpr TStringBuf TEMPORARY_BACKUP_DIR_PREFIX = "~backup_";
inline constexpr TStringBuf TEMPORARY_EXPORT_DIR_PREFIX = "export-";

// Classify individual scheme path components for traversal, not creation validation.
// System names include suffixes, such as .sys_health and .metadata_custom.
bool IsSystemName(TStringBuf name);

// Temporary directories created by local dump and server-side export.
bool IsTemporaryName(TStringBuf name);

bool IsInternalName(TStringBuf name);

} // namespace NKikimr::NNaming
