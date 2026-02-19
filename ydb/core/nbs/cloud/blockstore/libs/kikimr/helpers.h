#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/core/protos/base.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/log.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

inline bool IsUnrecoverable(const NKikimrProto::EReplyStatus status)
{
    switch (status) {
        case NKikimrProto::NODATA:
        case NKikimrProto::ERROR:
            return true;

        default:
            return false;
    }
}

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_IMPLEMENT_REQUEST(name, ns)                     \
    void Handle##name(const ns::TEv##name##Request::TPtr& ev,      \
                      const NActors::TActorContext& ctx);          \
                                                                   \
    void Reject##name(const ns::TEv##name##Request::TPtr& ev,      \
                      const NActors::TActorContext& ctx)           \
    {                                                              \
        auto response = std::make_unique<ns::TEv##name##Response>( \
            MakeError(E_REJECTED, #name " request rejected"));     \
                                                                   \
        NYdb::NBS::Reply(ctx, *ev, std::move(response));           \
    }                                                              \
    // BLOCKSTORE_IMPLEMENT_REQUEST

#define BLOCKSTORE_HANDLE_REQUEST(name, ns)      \
    HFunc(ns::TEv##name##Request, Handle##name); \
    // BLOCKSTORE_HANDLE_REQUEST

#define BLOCKSTORE_REJECT_REQUEST(name, ns)      \
    HFunc(ns::TEv##name##Request, Reject##name); \
    // BLOCKSTORE_REJECT_REQUEST

#define BLOCKSTORE_HANDLE_RESPONSE(name, ns)                \
    HFunc(ns::TEv##name##Response, Handle##name##Response); \
    // BLOCKSTORE_HANDLE_RESPONSE

#define BLOCKSTORE_IGNORE_RESPONSE(name, ns) \
    IgnoreFunc(ns::TEv##name##Response);     \
    // BLOCKSTORE_IGNORE_RESPONSE

}   // namespace NYdb::NBS
