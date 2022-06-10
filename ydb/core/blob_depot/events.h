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
        BLOBDEPOT_EVENT_PB_NO_ARGS(EvAllocateIds);
        BLOBDEPOT_EVENT_PB_NO_ARGS(EvAllocateIdsResult);
        BLOBDEPOT_EVENT_PB(EvBlock, TabletId, BlockedGeneration);
        BLOBDEPOT_EVENT_PB(EvBlockResult, Status, ErrorReason);
        BLOBDEPOT_EVENT_PB_NO_ARGS(EvPushNotify);
        BLOBDEPOT_EVENT_PB_NO_ARGS(EvQueryBlocks);
        BLOBDEPOT_EVENT_PB_NO_ARGS(EvQueryBlocksResult);
        BLOBDEPOT_EVENT_PB_NO_ARGS(EvCommitBlobSeq);
        BLOBDEPOT_EVENT_PB_NO_ARGS(EvCommitBlobSeqResult);
        BLOBDEPOT_EVENT_PB_NO_ARGS(EvResolve);
        BLOBDEPOT_EVENT_PB(EvResolveResult, Status, ErrorReason);
    };

} // NKikimr
