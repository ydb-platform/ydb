#pragma once

#include <ydb/core/kqp/runtime/kqp_compute.h>
#include <ydb/core/protos/sys_view_types.pb.h>

#include <ydb/library/actors/core/actor.h>

namespace NKikimr {
namespace NSysView {

THolder<NActors::IActor> CreateSystemViewScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TString& tablePath, const TMaybe<NKikimrSysView::TSysViewDescription>& sysViewInfo, TVector<TSerializedTableRange> ranges,
    const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns, TIntrusiveConstPtr<NACLib::TUserToken> userToken,
    const TString& database, bool reverse);

THolder<NActors::IActor> CreateSystemViewScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TString& tablePath, const TMaybe<NKikimrSysView::TSysViewDescription>& sysViewInfo, const TTableRange& tableRange,
    const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns, TIntrusiveConstPtr<NACLib::TUserToken> userToken,
    const TString& database, bool reverse);

} // NSysView
} // NKikimr
