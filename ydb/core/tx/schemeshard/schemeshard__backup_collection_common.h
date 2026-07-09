#pragma once

#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <util/generic/string.h>

#include <optional>

namespace NKikimr::NSchemeShard {
    bool IsSupportedIndex(TPathId pathId, const TOperationContext& context);
    bool IsSupportedIndex(TPathId pathId, const TSchemeShard* ss);
}

namespace NKikimr::NSchemeShard::NBackup {

inline constexpr TStringBuf FullBackupSuffix = "_full";
inline constexpr TStringBuf IncrementalBackupSuffix = "_incremental";

inline TString FullBackupDirName(TStringBuf trimmed) {
    return TStringBuilder() << trimmed << FullBackupSuffix;
}
inline TString IncrementalBackupDirName(TStringBuf trimmed) {
    return TStringBuilder() << trimmed << IncrementalBackupSuffix;
}

struct TBackupCollectionPaths {
    TPath RootPath;
    TPath DstPath;
};

std::optional<TBackupCollectionPaths> ResolveBackupCollectionPaths(
    const TString& rootPathStr,
    const TString& name,
    bool preValidateDst,
    TOperationContext& context,
    THolder<TProposeResponse>& result,
    bool enforceBackupCollectionsDirExists = true);

std::optional<THashMap<TString, THashSet<TString>>> GetBackupRequiredPaths(
    const TTxTransaction& tx,
    const TString& targetDir,
    const TString& targetName,
    const TOperationContext& context);

std::optional<THashMap<TString, THashSet<TString>>> GetRestoreRequiredPaths(
    const TTxTransaction& tx,
    const TString& targetName,
    const TOperationContext& context);

TString ToX509String(const TInstant& datetime);

}  // namespace NKikimr::NSchemeShard::NBackup
