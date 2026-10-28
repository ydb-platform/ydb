#pragma once

#include <ydb/library/naming_conventions/scheme_object_names.h>

#include <util/generic/fwd.h>
#include <util/generic/strbuf.h>

namespace NYdb {

inline namespace Dev {
namespace NScheme {
    struct TSchemeEntry;
}
}

namespace NConsoleClient {

using NKikimr::NNaming::METADATA_DIR_NAME;
using NKikimr::NNaming::SYSVIEWS_DIR_NAME;
using NKikimr::NNaming::TMP_DIR_NAME;
using NKikimr::NNaming::BACKUPS_DIR_NAME;
constexpr TStringBuf OLD_SECRETS_DIR_NAME = "secrets";

using NKikimr::NNaming::IsSystemName;
using NKikimr::NNaming::IsTemporaryName;
using NKikimr::NNaming::IsInternalName;

bool IsSystemDir(const NScheme::TSchemeEntry& entry);

bool IsSystemObject(const NScheme::TSchemeEntry& entry);

bool IsTemporaryDir(const NScheme::TSchemeEntry& entry);

bool IsInternalDir(const NScheme::TSchemeEntry& entry);

bool IsInternalObject(const NScheme::TSchemeEntry& entry);

} // NConsoleClient
} // NYdb
