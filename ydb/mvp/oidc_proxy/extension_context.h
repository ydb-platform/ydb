#pragma once

#include "cracked_page.h"

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/queue.h>

namespace NMVP::NOIDC {

struct TProxiedResponseParams {
    NHttp::THttpIncomingRequestPtr Request;
    THolder<TCrackedPage> ProtectedPage;
    TString ResponseError;

    TString StatusOverride;
    TString MessageOverride;
    TString BodyOverride;
    THolder<NHttp::THeadersBuilder> HeadersOverride;
};

struct TExtensionContext : public TThrRefBase {
    struct TRoute: public TQueue<NActors::TActorId> {
        TActorId Next() {
            if (empty()) {
                return TActorId();
            }
            TActorId target = front();
            pop();
            return target;
        }
    };

    TActorId Sender;
    TRoute Route;
    THolder<TProxiedResponseParams> Params;
};

} // NMVP::NOIDC
