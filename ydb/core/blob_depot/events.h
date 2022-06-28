#pragma once

#include "defs.h"

#include <ydb/core/protos/blob_depot.pb.h>

namespace NKikimr {

    struct TEvBlobDepot {
        enum {
            EvApplyConfig = EventSpaceBegin(TKikimrEvents::ES_BLOB_DEPOT),
            EvApplyConfigResult,
            EvRegisterAgent,
            EvRegisterAgentResult,
            EvAllocateIds,
            EvAllocateIdsResult,
            EvBlock,
            EvBlockResult,
            EvPushNotify,
            EvQueryBlocks,
            EvQueryBlocksResult,
            EvCommitBlobSeq,
            EvCommitBlobSeqResult,
            EvResolve,
            EvResolveResult,
        };

#define BLOBDEPOT_PARAM_ARG(ARG) std::optional<std::decay_t<decltype(Record.Get##ARG())>> param##ARG,

#define BLOBDEPOT_SETTER(ARG)               \
        if (param##ARG) {                   \
            Record.Set##ARG(*param##ARG);   \
        }

#define BLOBDEPOT_EVENT_PB_NO_ARGS(NAME)                                                    \
        struct T##NAME : TEventPB<T##NAME, NKikimrBlobDepot::T##NAME, NAME> {               \
            T##NAME() = default;                                                            \
        }

#define BLOBDEPOT_EVENT_PB(NAME, ...)                                                       \
        struct T##NAME : TEventPB<T##NAME, NKikimrBlobDepot::T##NAME, NAME> {               \
            T##NAME() = default;                                                            \
                                                                                            \
            struct TArgListTerminator {};                                                   \
                                                                                            \
            T##NAME(Y_MAP_ARGS(BLOBDEPOT_PARAM_ARG, __VA_ARGS__) TArgListTerminator = {}) { \
                Y_MAP_ARGS(BLOBDEPOT_SETTER, __VA_ARGS__)                                   \
            }                                                                               \
        }

        BLOBDEPOT_EVENT_PB(EvApplyConfig, TxId);
        BLOBDEPOT_EVENT_PB(EvApplyConfigResult, TabletId, TxId);
        BLOBDEPOT_EVENT_PB(EvRegisterAgent, VirtualGroupId);
        BLOBDEPOT_EVENT_PB_NO_ARGS(EvRegisterAgentResult);
        BLOBDEPOT_EVENT_PB(EvAllocateIds, ChannelKind);
        BLOBDEPOT_EVENT_PB(EvAllocateIdsResult, ChannelKind, Generation);
        BLOBDEPOT_EVENT_PB(EvBlock, TabletId, BlockedGeneration);
        BLOBDEPOT_EVENT_PB(EvBlockResult, Status, ErrorReason);
        BLOBDEPOT_EVENT_PB_NO_ARGS(EvPushNotify);
        BLOBDEPOT_EVENT_PB_NO_ARGS(EvQueryBlocks);
        BLOBDEPOT_EVENT_PB_NO_ARGS(EvQueryBlocksResult);
        BLOBDEPOT_EVENT_PB_NO_ARGS(EvCommitBlobSeq);
        BLOBDEPOT_EVENT_PB_NO_ARGS(EvCommitBlobSeqResult);
        BLOBDEPOT_EVENT_PB_NO_ARGS(EvResolve);
        BLOBDEPOT_EVENT_PB(EvResolveResult, Status, ErrorReason);

        template<typename TEvent>
        struct TResponseFor {};

        template<> struct TResponseFor<TEvApplyConfig> { using Type = TEvApplyConfigResult; };
        template<> struct TResponseFor<TEvRegisterAgent> { using Type = TEvRegisterAgentResult; };
        template<> struct TResponseFor<TEvAllocateIds> { using Type = TEvAllocateIdsResult; };
        template<> struct TResponseFor<TEvBlock> { using Type = TEvBlockResult; };
        template<> struct TResponseFor<TEvQueryBlocks> { using Type = TEvQueryBlocksResult; };
        template<> struct TResponseFor<TEvCommitBlobSeq> { using Type = TEvCommitBlobSeqResult; };
        template<> struct TResponseFor<TEvResolve> { using Type = TEvResolveResult; };

        template<typename TRequestEvent, typename... TArgs>
        static auto MakeResponseFor(TAutoPtr<TEventHandle<TRequestEvent>>& ev, TActorId selfId, TArgs&&... args) {
            auto event = std::make_unique<typename TResponseFor<TRequestEvent>::Type>(std::forward<TArgs>(args)...);
            auto *record = &event->Record;
            auto handle = std::make_unique<IEventHandle>(ev->Sender, selfId, event.release(), 0, ev->Cookie);
            if (ev->InterconnectSession) {
                handle->Rewrite(TEvInterconnect::EvForward, ev->InterconnectSession);
            }
            return std::make_pair(std::move(handle), record);
        }
    };

} // NKikimr
