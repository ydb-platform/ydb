#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/actors/http/http_cache.h>

namespace NMeta {

using namespace NHttp;

struct TCacheOwnership {
    TString ForwardUrl;
    TInstant Deadline;

    TString GetForwardUrlForDebug() const {
        return ForwardUrl.empty() ? "(local)" : ForwardUrl;
    }
};

using TGetCacheOwnershipCallback = std::function<void(TCacheOwnership)>;
using TGetCacheOwnership = std::function<bool (const TString&, TGetCacheOwnershipCallback)>;

NActors::IActor* CreateHttpMetaCache(const NActors::TActorId& httpProxyId, TGetCachePolicy cachePolicy, TGetCacheOwnership cacheOwnership);

}
