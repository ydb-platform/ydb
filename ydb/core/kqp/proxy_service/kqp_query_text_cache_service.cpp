#include "kqp_query_text_cache_service.h"

#include <ydb/core/kqp/common/kqp_data_integrity_trails.h>
#include <ydb/core/kqp/common/kqp_tli.h>
#include <ydb/core/kqp/common/kqp_query_text_cache_events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NKqp {

// Per-node service that handles cross-node query text lookups for TLI logging.
// When a victim's SessionActor needs the breaker's query text and the breaker ran
// on a different node, it sends a TEvLookupQueryText request to this service on
// the breaker's node. The service looks up the text from the local TNodeQueryTextCache
// and responds.
class TKqpQueryTextCacheService : public NActors::TActorBootstrapped<TKqpQueryTextCacheService> {
public:
    void Bootstrap() {
        Become(&TKqpQueryTextCacheService::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvLookupQueryText, HandleLookup)
    )

private:
    void HandleLookup(TEvLookupQueryText::TPtr& ev) {
        auto& record = ev->Get()->Record;
        ui64 querySpanId = record.GetQuerySpanId();

        TString queryText = NDataIntegrity::TNodeQueryTextCache::Instance().Get(querySpanId);

        auto response = MakeHolder<TEvLookupQueryTextResponse>();
        response->Record.SetQuerySpanId(querySpanId);
        if (!queryText.empty()) {
            response->Record.SetQueryText(queryText);
        }
        Send(ev->Sender, response.Release());
    }
};

NActors::IActor* CreateKqpQueryTextCacheService() {
    return new TKqpQueryTextCacheService();
}

} // namespace NKikimr::NKqp
