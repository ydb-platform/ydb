#pragma once

#include <ydb/library/actors/core/actor.h>

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

void RenderTabletForm(IOutputStream& str, const TString& nbsTabletListHtml = TString());

} // namespace NKikimr::NNbsDbgLike
