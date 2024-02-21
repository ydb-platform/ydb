#pragma once

#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#include <ydb/core/tablet_flat/test/libs/table/test_iter.h>

#include <utility>

#define LOG_I(stream) LOG_INFO_S  (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define RETURN_RESULT_UNLESS(x) if (!(x)) return result;

namespace NKikimr::NSchemeShard::NExternalTable {

inline TPath::TChecker IsParentPathValid(const TPath& parentPath) {
    return parentPath.Check()
        .NotUnderDomainUpgrade()
        .IsAtLocalSchemeShard()
        .IsResolved()
        .NotDeleted()
        .NotUnderDeleting()
        .IsCommonSensePath()
        .IsLikeDirectory();
}

inline bool IsParentPathValid(const THolder<TProposeResponse>& result,
                              const TPath& parentPath) {
    const auto checks = IsParentPathValid(parentPath);

    if (!checks) {
        result->SetError(checks.GetStatus(), checks.GetError());
    }

    return static_cast<bool>(checks);
}

bool Validate(const TString& sourceType,
              const NKikimrSchemeOp::TExternalTableDescription& desc,
              TString& errStr);

std::pair<TExternalTableInfo::TPtr, TMaybe<TString>> CreateExternalTable(
    const TString& sourceType,
    const NKikimrSchemeOp::TExternalTableDescription& desc,
    const NExternalSource::IExternalSourceFactory::TPtr& factory,
    ui64 alterVersion);


} // namespace NKikimr::NSchemeShard::NExternalDataSource
