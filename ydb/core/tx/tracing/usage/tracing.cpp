#include "tracing.h"
#include <ydb/core/tx/tracing/service/global.h>
#include <util/string/join.h>
#include <util/stream/buffered.h>
#include <util/stream/fwd.h>
#include <util/stream/file.h>
#include <util/system/tls.h>

namespace NKikimr::NTracing {

TTraceClientGuard TTraceClient::GetClient(const TString& type, const TString& clientId, const TString& parentId) {
    return Singleton<TTracing>()->GetClient(type, clientId, parentId);
}

namespace {

static TAtomicCounter ClientsCounter = 0;
NTls::TValue<std::vector<TTraceClient*>> GuardedClients;
NTls::TValue<std::vector<TTraceClient::TDurationGuard*>> Guards;
}

TTraceClient::TDurationGuard TTraceClient::MakeContextGuard(const TString& id) {
    if (GuardedClients.Get().size()) {
        return GuardedClients.Get().back()->MakeGuard(id);
    } else {
        return TTraceClient::TDurationGuard(id);
    }
}

TTraceClient::TDurationGuard* TTraceClient::GetContextGuard() {
    if (GuardedClients.Get().size()) {
        return Guards.Get().back();
    } else {
        return nullptr;
    }
}

TTraceClient::TDurationGuard::TDurationGuard(TTraceClient& owner, const TString& id)
    : Owner(&owner)
    , Id(id)
{
    GuardedClients.Get().emplace_back(Owner);
    Guards.Get().emplace_back(this);
    auto it = Owner->Stats.find(Id);
    if (it == Owner->Stats.end()) {
        it = Owner->Stats.emplace(Id, TStatInfo()).first;
    }
    Stat = &it->second;
    Stat->Start(StartInstant);
}

TTraceClient::TDurationGuard::TDurationGuard(const TString& id)
    : Id(id) {
}

TTraceClient::TDurationGuard::~TDurationGuard() {
    if (!Owner) {
        return;
    }
    AFL_VERIFY(GuardedClients.Get().size() && GuardedClients.Get().back()->GetClientId() == Owner->GetClientId());
    GuardedClients.Get().pop_back();
    Guards.Get().pop_back();
    Stat->Finish(TInstant::Now());
}

TTraceClientGuard TTraceClient::GetClientUnique(const TString& type, const TString& clientId, const TString& parentId) {
    return Singleton<TTracing>()->GetClient(type, clientId + "::" + ::ToString(ClientsCounter.Inc()), parentId);
}

TTraceClientGuard TTraceClient::GetLocalClient(const TString& type, const TString& clientId) {
    return Singleton<TTracing>()->GetLocalClient(type, clientId + "::" + ::ToString(ClientsCounter.Inc()));
}

TTraceClientGuard TTraceClient::GetTypeUnique(const TString& type, const TString& parentId) {
    return Singleton<TTracing>()->GetClient(type, type + "::" + ::ToString(ClientsCounter.Inc()), parentId);
}

NJson::TJsonValue TTraceClient::ToJsonImpl(THashSet<TString>& readyIds) const {
    if (!readyIds.emplace(ClientId).second || readyIds.size() > 10) {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("problem", "LOOP_FOUND_OR_LARGE");
        result.InsertValue("ids", JoinSeq(",", readyIds));
        return result;
    }
    const TInstant finishInstant = GetFinishInstant();
    NJson::TJsonValue result = NJson::JSON_MAP;
    result.InsertValue("id", ClientId);
    result.InsertValue("full", ActualStat.ToJson("_full_task", finishInstant));
    if (ParentId) {
        result.InsertValue("parent_id", ParentId);
    }
    if (Stats.size()) {
        const TInstant startInstant = ActualStat.GetFirstVerified();
        std::map<ui64, std::vector<TString>> points;
        for (auto&& i : Stats) {
            points[(i.second.GetFirstVerified() - startInstant).MilliSeconds()].emplace_back("f_" + i.first);
            if (i.second.InProgress()) {
                points[(finishInstant - startInstant).MilliSeconds()].emplace_back("l_" + i.first);
            } else {
                points[(i.second.GetLastVerified() - startInstant).MilliSeconds()].emplace_back("l_" + i.first);
            }
        }
        auto& jsonArrayPoints = result.InsertValue("p", NJson::JSON_ARRAY);
        for (auto&& i : points) {
            auto& jsonMap = jsonArrayPoints.AppendValue(NJson::JSON_MAP);
            jsonMap.InsertValue("t", (double)(0.001 * i.first));
            auto& jsonEvents = jsonMap.InsertValue("events", NJson::JSON_ARRAY);
            for (auto&& e : i.second) {
                jsonEvents.AppendValue(e);
            }
        }
        auto& jsonArrayDurations = result.InsertValue("events", NJson::JSON_ARRAY);
        for (auto&& i : Stats) {
            jsonArrayDurations.AppendValue(i.second.ToJson(i.first, finishInstant));
        }
    }
    if (Children.size()) {
        THashMap<TString, std::vector<std::shared_ptr<TTraceClient>>> clientsByType;
        for (auto&& i : Children) {
            clientsByType[i.second->GetType()].emplace_back(i.second);
        }
        auto& jsonChild = result.InsertValue("child", NJson::JSON_MAP);
        for (auto&& [type, clients] : clientsByType) {
            auto& jsonChildWithType = jsonChild.InsertValue(type, NJson::JSON_ARRAY);
            for (auto&& c : clients) {
                jsonChildWithType.AppendValue(c->ToJsonImpl(readyIds));
            }
        }
    }
    readyIds.erase(ClientId);
    return result;
}

void TTraceClient::Dump() const {
    if (!ParentId) {
        TFileOutput fOutput("/tmp/" + ClientId + ".json.txt");
        fOutput << ToJson();
    }
}

bool TTraceClient::CheckChildrenFree() const {
    for (auto&& i : Children) {
        if (i.second.use_count() > 2) {
            return false;
        }
        if (!i.second->CheckChildrenFree()) {
            return false;
        }
    }
    return true;
}

TInstant TStatInfo::GetFirstVerified() const {
    AFL_VERIFY(First);
    return *First;
}

TInstant TStatInfo::GetLastVerified() const {
    AFL_VERIFY(Last);
    return *Last;
}

}
