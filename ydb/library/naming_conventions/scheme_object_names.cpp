#include "scheme_object_names.h"

#include <algorithm>
#include <array>

namespace NKikimr::NNaming {

namespace {

constexpr std::array SystemDirectoryPrefixes = {
    SYSVIEWS_DIR_NAME,
    METADATA_DIR_NAME,
    TMP_DIR_NAME,
    BACKUPS_DIR_NAME,
};

} // namespace

bool IsSystemName(TStringBuf name) {
    return std::any_of(SystemDirectoryPrefixes.begin(), SystemDirectoryPrefixes.end(), [name](TStringBuf prefix) {
        return name.StartsWith(prefix);
    });
}

bool IsTemporaryName(TStringBuf name) {
    return name.StartsWith(TEMPORARY_BACKUP_DIR_PREFIX)
        || name.StartsWith(TEMPORARY_EXPORT_DIR_PREFIX);
}

bool IsInternalName(TStringBuf name) {
    return IsSystemName(name) || IsTemporaryName(name);
}

} // namespace NKikimr::NNaming
