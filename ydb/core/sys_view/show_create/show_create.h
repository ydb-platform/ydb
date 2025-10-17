#pragma once

#include <ydb/core/kqp/runtime/kqp_compute.h>

#include <ydb/core/protos/sys_view_types.pb.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>

namespace NKikimr {
namespace NSysView {

THolder<NActors::IActor> CreateShowCreate(const NActors::TActorId& ownerId, ui32 scanId,
    const TString& database, const NKikimrSysView::TSysViewDescription& sysViewInfo,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken);

} // NSysView
} // NKikimr
