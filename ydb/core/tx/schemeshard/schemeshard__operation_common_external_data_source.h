#pragma once

#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#include <utility>

#define RETURN_RESULT_UNLESS(x) if (!(x)) return result;

namespace NKikimr::NSchemeShard::NExternalDataSource {

inline TPath::TChecker IsParentPathValid(const TPath& parentPath, const TTxTransaction& tx, const bool isCreate) {
    auto checks = parentPath.Check();
    checks.NotUnderDomainUpgrade()
        .IsAtLocalSchemeShard()
        .IsResolved()
        .NotDeleted()
        .NotUnderDeleting()
        .IsCommonSensePath()
        .IsLikeDirectory();

    if (isCreate) {
        checks.FailOnRestrictedCreateInTempZone();
    }
    Y_UNUSED(tx);

    return checks;
}

inline bool IsParentPathValid(const THolder<TProposeResponse>& result,
                              const TPath& parentPath,
                              const TTxTransaction& tx,
                              const bool isCreate) {
    const auto checks = IsParentPathValid(parentPath, tx, isCreate);

    if (!checks) {
        result->SetError(checks.GetStatus(), checks.GetError());
    }

    return static_cast<bool>(checks);
}

bool Validate(const NKikimrSchemeOp::TExternalDataSourceDescription& desc,
              const NExternalSource::IExternalSourceFactory::TPtr& factory,
              TString& errStr);

TExternalDataSourceInfo::TPtr CreateExternalDataSource(
    const NKikimrSchemeOp::TExternalDataSourceDescription& desc, ui64 alterVersion);

} // namespace NKikimr::NSchemeShard::NExternalDataSource
