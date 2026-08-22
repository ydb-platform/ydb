#pragma once

#include <ydb/core/kqp/runtime/kqp_compute.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>

#include <util/generic/ptr.h>

namespace NKikimrSysView {

class TSysViewDescription;

} // namespace NKikimrSysView

namespace NKikimr::NSysView {

THolder<NActors::IActor> CreateStreamingQueriesScan(const NActors::TActorId& ownerId, ui32 scanId, const TString& database,
    const NKikimrSysView::TSysViewDescription& sysViewInfo, const TTableRange& tableRange,
    const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken, bool reverse);

} // namespace NKikimr::NSysView
