#pragma once

#include <ydb/core/base/events.h>
#include <ydb/library/actors/core/events.h>

namespace NKikimr::NSasl {

    struct TEvSasl {
        enum EEv {
            EvComputedHashes = EventSpaceBegin(TKikimrEvents::ES_SASL_AUTH),
            EvEnd,
        };

        struct TEvComputedHashes : public TEventLocal<TEvComputedHashes, EvComputedHashes> {
            std::string Error;
            std::string Hashes;
            std::string ArgonHash;
        };
    };

} // namespace NKikimr::NSasl
