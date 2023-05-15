#include "udf_helpers.h"

#include <library/cpp/resource/resource.h>

#include <util/generic/singleton.h>
#include <mutex>
#include <unordered_map>

namespace NYql {
namespace NUdf {

namespace {

struct TLoadedResources {
    TString Get(TStringBuf resourceId) {
        const std::unique_lock lock(Sync);
        const auto ins = Resources.emplace(resourceId, "");
        if (ins.second)
            ins.first->second = NResource::Find(resourceId);
        return ins.first->second;
    }

    std::mutex Sync;
    std::unordered_map<TString, TString> Resources;
};

}

TString LoadResourceOnce(TStringBuf resourceId) {
    return Singleton<TLoadedResources>()->Get(resourceId);
}

}
}
