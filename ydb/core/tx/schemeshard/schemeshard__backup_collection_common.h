#pragma once

#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <util/generic/string.h>

#include <optional>

namespace NKikimr::NSchemeShard {

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

}  // namespace NKikimr::NSchemeShard
