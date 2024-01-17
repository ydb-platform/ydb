#pragma once
#include <ydb/library/accessor/accessor.h>
#include <util/datetime/base.h>
#include <memory>
#include <optional>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NTracing {

class TStatInfo {
private:
    YDB_ACCESSOR_DEF(TDuration, Duration);
    YDB_ACCESSOR(ui32, Count, 0);
    YDB_ACCESSOR(ui32, StatCount, 0);
    YDB_ACCESSOR_DEF(std::optional<TInstant>, First);
    YDB_ACCESSOR_DEF(std::optional<TInstant>, Last);
    YDB_ACCESSOR_DEF(std::optional<TInstant>, ActualStart);
public:
    TStatInfo() = default;

    NJson::TJsonValue ToJson(const TString& name, const TInstant finishInstant) const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("name", name);
        result.InsertValue("d_finished", Duration.MicroSeconds());
        if (First) {
            result.InsertValue("f", First->MicroSeconds());
        }
        if (Last) {
            result.InsertValue("l", Last->MicroSeconds());
        } else {
            result.InsertValue("l", finishInstant.MicroSeconds());
        }
        if (ActualStart) {
            result.InsertValue("a", ActualStart->MicroSeconds());
        }
        result.InsertValue("d", (Duration + GetCurrentDuration(finishInstant)).MicroSeconds());
        result.InsertValue("c", Count);
        if (StatCount) {
            result.InsertValue("sc", StatCount);
        }
        return result;
    }
    TInstant GetLastVerified() const;

    TInstant GetFirstVerified() const;

    TDuration GetCurrentDuration(const TInstant now) const {
        return ActualStart ? now - *ActualStart : TDuration::Zero();
    }

    bool InProgress() const {
        return !!ActualStart;
    }

    void Start(const TInstant start) {
        if (!First || *First > start) {
            First = start;
        }
        AFL_VERIFY(!ActualStart);
        ActualStart = start;
    }

    void Finish(const TInstant value) {
        if (!Last || *Last < value) {
            Last = value;
        }
        AFL_VERIFY(ActualStart);
        Duration += value - *ActualStart;
        ++Count;
        ActualStart.reset();
    }
};

class TTraceClientGuard;

class TTraceClient {
private:
    YDB_READONLY_DEF(TString, Type);
    YDB_READONLY_DEF(TString, ClientId);
    YDB_READONLY_DEF(TString, ParentId);
    THashMap<TString, std::shared_ptr<TTraceClient>> Children;
    THashMap<TString, TStatInfo> Stats;
    TStatInfo ActualStat;
    TInstant GetFinishInstant() const {
        return ActualStat.GetLast().value_or(TInstant::Now());
    }

    NJson::TJsonValue ToJsonImpl(THashSet<TString>& readyIds) const;
public:
    void SetType(const TString& value) {
        AFL_VERIFY(!Type || Type == value);
        Type = value;
    }

    bool CheckChildrenFree() const;

    void Finish() {
        ActualStat.Finish(TInstant::Now());
    }

    TTraceClient(const TString& id, const TString& parentId)
        : ClientId(id)
        , ParentId(parentId)
    {
        ActualStat.Start(TInstant::Now());
    }

    void Dump() const;

    NJson::TJsonValue ToJson() const {
        THashSet<TString> readyIds;
        return ToJsonImpl(readyIds);
    }

    void RegisterChildren(const std::shared_ptr<TTraceClient>& client) {
        AFL_VERIFY(client->GetParentId() == ClientId);
        auto it = Children.find(client->GetClientId());
        if (it == Children.end()) {
            Children.emplace(client->GetClientId(), client);
        }
    }

    const std::shared_ptr<TTraceClient>& GetChildrenVerified(const TString& id) const {
        auto it = Children.find(id);
        AFL_VERIFY(it != Children.end());
        return it->second;
    }

    class TDurationGuard: TNonCopyable {
    private:
        const TInstant StartInstant = TInstant::Now();
        TTraceClient* Owner = nullptr;
        const TString Id;
        TStatInfo* Stat;
    public:
        TDurationGuard(TTraceClient& owner, const TString& id);
        TDurationGuard(const TString& id);
        TStatInfo& GetStatInfo() {
            AFL_VERIFY(Stat);
            return *Stat;
        }
        ~TDurationGuard();
    };

    static TTraceClient::TDurationGuard MakeContextGuard(const TString& id);
    static TTraceClient::TDurationGuard* GetContextGuard();

    TDurationGuard MakeGuard(const TString& id) {
        return TDurationGuard(*this, id);
    }

    static TTraceClientGuard GetClient(const TString& type, const TString& clientId, const TString& parentId);
    static TTraceClientGuard GetClientUnique(const TString& type, const TString& clientId, const TString& parentId);
    static TTraceClientGuard GetLocalClient(const TString& type, const TString& clientId);
    static TTraceClientGuard GetTypeUnique(const TString& type, const TString& parentId);
};

class TTraceClientGuard: public TNonCopyable {
private:
    std::shared_ptr<TTraceClient> Client;
public:
    TTraceClientGuard(const std::shared_ptr<TTraceClient>& client)
        : Client(client)
    {

    }

    ~TTraceClientGuard() {
        Client->Finish();
    }

    TTraceClient* operator->() {
        return Client.get();
    }

    const TTraceClient* operator->() const {
        return Client.get();
    }
};

}
