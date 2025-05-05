#include <util/generic/fwd.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

namespace NKikimr::NTopicHelpers {
enum class EAuthResult {
    AuthOk,
    AccessDenied,
    TokenRequired
};

EAuthResult CheckAccess(const NKikimr::TAppData& appData, const NKikimr::NSchemeCache::TSchemeCacheNavigate::TEntry& describeEntry,
                        const TString& serializedToken, const TString& entityName, TString& error);

} // namespace
