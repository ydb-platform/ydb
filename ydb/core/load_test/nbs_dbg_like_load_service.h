#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/protos/test_shard_control.pb.h>

#include <util/datetime/base.h>
#include <util/stream/output.h>
#include <util/generic/string.h>

namespace NKikimr::NNbsDbgLike {

// NBS-DBG-like load tablet HTTP helpers

enum class ENbsLoadTabletOp {
    Create,
    Delete,
};

NActors::IActor* CreateNbsDbgLikeLoadTabletHttpRequest(
    ENbsLoadTabletOp op,
    ui64 ownerIdx,
    TString configText,
    NActors::TActorId origin,
    ui32 subRequestId,
    TString storagePoolsText);

NActors::IActor* CreateNbsLoadTabletListPageActor(
    NActors::TActorId parent,
    ui32 httpRequestId,
    ui32 subRequestId);

// Budget for a Hive listing round-trip when the caller carries no deadline of
// its own. A run startup passes its remaining budget instead.
constexpr TDuration NbsDbgLikeListControlTimeout = TDuration::Seconds(45);

NActors::IActor* CreateNbsDbgLikeLoadTabletControl(
    const NKikimrClient::TNbsDbgLikeLoadControl& request, NActors::TActorId origin, ui64 cookie);
NActors::IActor* CreateNbsDbgLikeLoadTabletListControl(const NKikimrClient::TNbsDbgLikeLoadControl& request,
    NActors::TActorId origin, ui64 cookie, TDuration timeout = NbsDbgLikeListControlTimeout);

NActors::IActor* CreateNbsDbgLikeLoadServiceProbe(const NKikimrClient::TNbsDbgLikeLoadControl& request,
    NActors::TActorId origin, ui64 cookie, TDuration timeout);

void RenderTabletForm(IOutputStream& str, const TString& nbsTabletListHtml = TString());

} // namespace NKikimr::NNbsDbgLike
