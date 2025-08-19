#include "resource.h"

#include <yql/essentials/utils/yql_panic.h>

#include <library/cpp/resource/resource.h>

namespace NYql::NDocs {

    bool IsMatching(const TResourceFilter& filter, TStringBuf key) {
        return key.Contains(filter.BaseDirectorySuffix) &&
               key.EndsWith(filter.CutSuffix);
    }

    TStringBuf RelativePath(const TResourceFilter& filter, TStringBuf key Y_LIFETIME_BOUND) {
        size_t pos = key.find(filter.BaseDirectorySuffix);
        YQL_ENSURE(pos != TString::npos);
        pos += filter.BaseDirectorySuffix.size();

        TStringBuf tail = TStringBuf(key).SubStr(pos);
        tail.remove_suffix(filter.CutSuffix.size());
        return tail;
    }

    TResourcesByRelativePath FindResources(const TResourceFilter& filter) {
        YQL_ENSURE(
            filter.BaseDirectorySuffix.EndsWith('/'),
            "BaseDirectory should end with '/', but got '" << filter.BaseDirectorySuffix << "'");

        TResourcesByRelativePath resources;
        for (TStringBuf key : NResource::ListAllKeys()) {
            if (!key.StartsWith("resfs/file/") || !IsMatching(filter, key)) {
                continue;
            }

            TStringBuf path = RelativePath(filter, key);
            YQL_ENSURE(!resources.contains(path));

            resources[path] = NResource::Find(key);
        }
        return resources;
    }

} // namespace NYql::NDocs
