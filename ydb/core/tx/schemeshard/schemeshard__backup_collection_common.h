#pragma once

#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <util/generic/string.h>

#include <optional>

namespace NKikimr::NSchemeShard::NBackup {

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
