#pragma once

#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#include <ydb/core/tablet_flat/test/libs/table/test_iter.h>

#include <utility>

#define LOG_I(stream) LOG_INFO_S  (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
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

    return std::move(checks);
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
