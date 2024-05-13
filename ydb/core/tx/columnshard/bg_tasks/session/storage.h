#pragma once
#include "session.h"

namespace NKikimr::NOlap::NBackground {

class TSessionsContainer {
private:
    THashMap<TString, std::shared_ptr<TSession>> Sessions;
public:
    std::vector<TSessionInfoReport> GetSessionsInfoForReport() const {
        std::vector<TSessionInfoReport> result;
        for (auto&& i : Sessions) {
            result.emplace_back(i.second->GetSessionInfoForReport());
        }
        return result;
    }

    TConclusionStatus AddSession(const std::shared_ptr<TSession>& session) {
        AFL_VERIFY(!!session);
        if (!Sessions.emplace(session->GetIdentifier(), session).second) {
            return TConclusionStatus::Fail("task emplaced already");
        }
        return TConclusionStatus::Success();
    }
    bool RemoveSession(const TString& identifier) {
        AFL_VERIFY(!!identifier);
        auto it = Sessions.find(identifier);
        if (it == Sessions.end()) {
            return false;
        }
        Sessions.erase(it);
        return true;
    }
    void Start(const std::shared_ptr<ITabletAdapter>& adapter) {
        for (auto&& [_, i] : Sessions) {
            if (i->GetLogicContainer()->IsReadyForStart() && !i->GetLogicContainer()->IsFinished()) {
                TStartContext context(i, adapter);
                i->StartActor(context);
            }
        }
    }

    void Finish() {
        for (auto&& [_, i] : Sessions) {
            if (i->IsRunning()) {
                i->FinishActor();
            }
        }
    }

    std::shared_ptr<TSession> GetSession(const TString& identifier) {
        auto it = Sessions.find(identifier);
        if (it == Sessions.end()) {
            return {};
        }
        return it->second;
    }

};

class TSessionsStorage {
private:
    NActors::TActorId ProcessActorId;
    THashMap<TString, std::shared_ptr<TSessionsContainer>> ContainersByClass;

    TSessionsContainer& GetContainerOrCreateNew(const TString& className) {
        auto it = ContainersByClass.find(className);
        if (it == ContainersByClass.end()) {
            it = ContainersByClass.emplace(className, std::make_shared<TSessionsContainer>()).first;
        }
        return *it->second;
    }
public:
    std::vector<TSessionInfoReport> GetSessionsInfoForReport() const {
        std::vector<TSessionInfoReport> result;
        for (auto&& [_, i] : ContainersByClass) {
            std::vector<TSessionInfoReport> resultClass = i->GetSessionsInfoForReport();
            result.insert(result.end(), resultClass.begin(), resultClass.end());
        }
        return result;
    }

    void Start(const std::shared_ptr<ITabletAdapter>& adapter) {
        for (auto&& i : ContainersByClass) {
            i.second->Start(adapter);
        }
    }

    void Finish() {
        for (auto&& i : ContainersByClass) {
            i.second->Finish();
        }
    }

    bool RemoveSession(const TString& className, const TString& identifier) {
        auto container = GetSessionsContainer(className);
        if (!container) {
            return false;
        }
        return container->RemoveSession(identifier);
    }

    TConclusionStatus AddSession(const std::shared_ptr<TSession>& session) {
        AFL_VERIFY(!!session);
        TSessionsContainer& sessions = GetContainerOrCreateNew(session->GetLogicContainer().GetClassName());
        return sessions.AddSession(session);
    }

    std::shared_ptr<TSession> GetSession(const TString& className, const TString& identifier) {
        TSessionsContainer& sessions = GetContainerOrCreateNew(className);
        return sessions.GetSession(identifier);
    }

    std::shared_ptr<TSessionsContainer> GetSessionsContainer(const TString& className) {
        auto it = ContainersByClass.find(className);
        if (it == ContainersByClass.end()) {
            return nullptr;
        }
        return it->second;
    }
};

}