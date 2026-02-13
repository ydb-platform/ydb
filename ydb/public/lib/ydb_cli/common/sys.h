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

bool IsSystemName(const TStringBuf name);

bool IsSystemDir(const NScheme::TSchemeEntry& entry);

bool IsSystemObject(const NScheme::TSchemeEntry& entry);

} // NConsoleClient
} // NYdb
