#pragma once

// #include "public.h"

#include <ydb/core/nbs/cloud/storage/core/libs/actors/helpers.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/protos/base.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>

#include <util/generic/string.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

NProto::TError MakeKikimrError(NKikimrProto::EReplyStatus status,
                               TString errorReason = {});
NProto::TError MakeSchemeShardError(NKikimrScheme::EStatus status,
                                    TString errorReason = {});

inline void PipeSend(const NActors::TActorContext& ctx,
                     const NActors::TActorId& pipe,
                     std::unique_ptr<NActors::IEventHandle>&& event)
{
    // see pipe client code for details
    event->Rewrite(NKikimr::TEvTabletPipe::EvSend, pipe);
    ctx.Send(event.release());
}

}   // namespace NYdb::NBS
