#include "scope.h"

#include <util/string/split.h>

namespace NYdb {
namespace NFq {

TString TScope::YandexCloudScopeSchema = "yandexcloud";

TString TScope::ParseFolder() const {
    const TVector<TString> path = StringSplitter(Scope).Split('/').SkipEmpty(); // yandexcloud://{folder_id}
    if (path.size() == 2 && path.front().StartsWith(YandexCloudScopeSchema)) {
        return path.back();
    }
    return {};
}

} // namespace NFq
} // namespace Ndb
