#pragma once

#include "defs.h"

#include <ydb/core/protos/forced_compaction.pb.h>

namespace NKikimr::NSchemeShard {

struct TEvForcedCompaction {
    enum EEv {
        EvCreateRequest = EventSpaceBegin(TKikimrEvents::ES_FORCED_COMPACTION),
        EvCreateResponse,
        EvGetRequest,
        EvGetResponse,

        EvEnd
    };

    struct TEvCreateRequest: public TEventPB<TEvCreateRequest, NKikimrForcedCompaction::TEvCreateRequest, EvCreateRequest> {
        TEvCreateRequest() = default;

        explicit TEvCreateRequest(
            const ui64 txId,
            const TString& dbName,
            NKikimrForcedCompaction::TForcedCompactionSettings settings)
        {
            Record.SetTxId(txId);
            Record.SetDatabaseName(dbName);
            *Record.MutableSettings() = std::move(settings);
        }
    };

    struct TEvCreateResponse: public TEventPB<TEvCreateResponse, NKikimrForcedCompaction::TEvCreateResponse, EvCreateResponse> {
        TEvCreateResponse() = default;

        explicit TEvCreateResponse(const ui64 txId) {
            Record.SetTxId(txId);
        }
    };

    struct TEvGetRequest: public TEventPB<TEvGetRequest, NKikimrForcedCompaction::TEvGetRequest, EvGetRequest> {
        TEvGetRequest() = default;

        explicit TEvGetRequest(const TString& dbName, const ui64 forcedCompactionId) {
            Record.SetDatabaseName(dbName);
            Record.SetForcedCompactionId(forcedCompactionId);
        }
    };

    struct TEvGetResponse: public TEventPB<TEvGetResponse, NKikimrForcedCompaction::TEvGetResponse, EvGetResponse> {
        TEvGetResponse() = default;
    };

};

} // namespace NKikimr::NSchemeShard
