#include "global.h"
#include "actor.h"
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NTracing {

std::shared_ptr<NKikimr::NTracing::TTraceClient> TTracing::GetClient(const TString& type, const TString& clientId, const TString& parentId) {
    TGuard<TMutex> g(Mutex);
    auto parent = CreateOrGetClient(parentId, "");
    auto client = CreateOrGetClient(clientId, parentId);
    AFL_VERIFY(client->GetParentId() == parentId);
    client->SetType(type);
    parent->RegisterChildren(client);
    return client;
}

std::shared_ptr<NKikimr::NTracing::TTraceClient> TTracing::GetLocalClient(const TString& type, const TString& clientId) {
    auto client = std::make_shared<TTraceClient>(clientId, "");
    client->SetType(type);
    return client;
}

TTracing::TTracing() {
    if (NActors::TlsActivationContext) {
        NActors::TActivationContext::Register(new TRegularTracesCleanerActor());
    }
}

void TTracing::Clean() {
    THashMap<TString, std::shared_ptr<TTraceClient>> idsToRemove;
    {
        TGuard<TMutex> g(Mutex);
        for (auto&& i : Clients) {
            AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("name", i.first)("count", i.second.use_count())("children", i.second->CheckChildrenFree());
            if (i.second.use_count() == 1 && i.second->CheckChildrenFree()) {
                idsToRemove.emplace(i.first, i.second);
            }
        }
        for (auto&& i : idsToRemove) {
            Clients.erase(i.first);
        }
    }
    for (auto&& i : idsToRemove) {
        AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "dump")("name", i.first)("parent", i.second->GetParentId());
        i.second->Dump();
    }
}

}
