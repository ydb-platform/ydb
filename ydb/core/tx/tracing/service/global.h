#pragma once
#include <util/generic/hash.h>
#include <ydb/core/tx/tracing/usage/tracing.h>
#include <util/system/mutex.h>

namespace NKikimr::NTracing {

class TTracing {
private:
    THashMap<TString, std::shared_ptr<TTraceClient>> Clients;
    std::shared_ptr<TTraceClient> CreateOrGetClient(const TString& clientId, const TString& parentId) {
        auto it = Clients.find(clientId);
        if (it == Clients.end()) {
            it = Clients.emplace(clientId, std::make_shared<TTraceClient>(clientId, parentId)).first;
        }
        return it->second;
    }
    TMutex Mutex;
public:
    TTracing();

    void Clean();

    std::shared_ptr<TTraceClient> GetClient(const TString& type, const TString& clientId, const TString& parentId);
    std::shared_ptr<TTraceClient> GetLocalClient(const TString& type, const TString& clientId);
};

}
