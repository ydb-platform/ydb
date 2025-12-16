#pragma once

#include "defs.h"

#include <ydb/core/protos/forced_compaction.pb.h>

namespace NKikimr {
namespace NSchemeShard {

struct TEvForcedCompaction {
    enum EEv {
        EvCreateForcedCompactionRequest = EventSpaceBegin(TKikimrEvents::ES_FORCED_COMPACTION),
        EvCreateForcedCompactionResponse,
        EvGetForcedCompactionRequest,
        EvGetForcedCompactionResponse,

        EvEnd
    };

    static_assert(
        EvEnd < EventSpaceEnd(TKikimrEvents::ES_FORCED_COMPACTION),
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_FORCED_COMPACTION)"
    );

#ifdef DECLARE_EVENT_CLASS
#error DECLARE_EVENT_CLASS macro redefinition
#else
#define DECLARE_EVENT_CLASS(NAME) struct T##NAME: public TEventPB<T##NAME, NKikimrForcedCompaction::T##NAME, NAME>
#endif

    DECLARE_EVENT_CLASS(EvCreateForcedCompactionRequest) {
        TEvCreateForcedCompactionRequest() = default;

        explicit TEvCreateForcedCompactionRequest(
            const ui64 txId,
            const TString& dbName,
            const NKikimrForcedCompaction::TCreateForcedCompactionRequest& request
        ) {
            Record.SetTxId(txId);
            Record.SetDatabaseName(dbName);
            Record.MutableRequest()->CopyFrom(request);
        }
    };

    DECLARE_EVENT_CLASS(EvCreateForcedCompactionResponse) {
        TEvCreateForcedCompactionResponse() = default;

        explicit TEvCreateForcedCompactionResponse(const ui64 txId) {
            Record.SetTxId(txId);
        }
    };

    DECLARE_EVENT_CLASS(EvGetForcedCompactionRequest) {
        TEvGetForcedCompactionRequest() = default;

        explicit TEvGetForcedCompactionRequest(const TString& dbName, const NKikimrForcedCompaction::TGetForcedCompactionRequest& request) {
            Record.SetDatabaseName(dbName);
            Record.MutableRequest()->CopyFrom(request);
        }

        explicit TEvGetForcedCompactionRequest(const TString& dbName, const ui64 compactionId) {
            Record.SetDatabaseName(dbName);
            Record.MutableRequest()->SetId(compactionId);
        }
    };

    DECLARE_EVENT_CLASS(EvGetForcedCompactionResponse) {
    };

#undef DECLARE_EVENT_CLASS

}; // TEvForcedCompaction

} // NSchemeShard
} // NKikimr
