#pragma once
#include "accessor_subscribe.h"
#include "config.h"
#include "scheme_describe.h"
#include "accessor_snapshot_simple.h"

#include <ydb/services/metadata/service.h>
#include <ydb/services/metadata/initializer/common.h>
#include <ydb/services/metadata/initializer/events.h>
#include <ydb/services/metadata/initializer/manager.h>
#include <ydb/services/metadata/initializer/snapshot.h>
#include <ydb/services/metadata/initializer/fetcher.h>
#include <ydb/services/metadata/manager/abstract.h>

#include <library/cpp/actors/core/hfunc.h>

namespace NKikimr::NMetadata::NProvider {

class TEvStartMetadataService: public TEventLocal<TEvStartMetadataService, EEvents::EvStartMetadataService> {
};

class TEvTableDescriptionFailed: public TEventLocal<TEvTableDescriptionFailed, EEvents::EvTableDescriptionFailed> {
private:
    YDB_READONLY_DEF(TString, ErrorMessage);
    YDB_READONLY_DEF(TString, RequestId);
public:
    explicit TEvTableDescriptionFailed(const TString& errorMessage, const TString& reqId)
        : ErrorMessage(errorMessage)
        , RequestId(reqId) {

    }
};

class TEvTableDescriptionSuccess: public TEventLocal<TEvTableDescriptionSuccess, EEvents::EvTableDescriptionSuccess> {
private:
    using TDescription = THashMap<ui32, TSysTables::TTableColumnInfo>;
    YDB_READONLY_DEF(TString, RequestId);
    YDB_READONLY_DEF(TDescription, Description);
public:
    TEvTableDescriptionSuccess(TDescription&& description, const TString& reqId)
        : RequestId(reqId)
        , Description(std::move(description))
    {
    }

    NModifications::TTableSchema GetSchema() const {
        return NModifications::TTableSchema(Description);
    }
};

class TServiceInternalController: public NInitializer::IInitializerOutput,
    public TDSAccessorSimple::TEvController, public ISchemeDescribeController {
private:
    const NActors::TActorIdentity ActorId;
public:
    TServiceInternalController(const NActors::TActorIdentity& actorId)
        : TDSAccessorSimple::TEvController(actorId)
        , ActorId(actorId)
    {

    }

    virtual void InitializationFinished(const TString& id) const override;

    virtual void OnDescriptionFailed(const TString& errorMessage, const TString& requestId) const override;
    virtual void OnDescriptionSuccess(THashMap<ui32, TSysTables::TTableColumnInfo>&& result, const TString& requestId) const override;
};

class TManagersId {
private:
    YDB_READONLY_DEF(std::set<TString>, ManagerIds);
public:
    TManagersId(const std::vector<IClassBehaviour::TPtr>& managers) {
        for (auto&& i : managers) {
            ManagerIds.emplace(i->GetTypeId());
        }
    }

    bool IsEmpty() const {
        return ManagerIds.empty();
    }

    bool RemoveId(const TString& id) {
        auto it = ManagerIds.find(id);
        if (it == ManagerIds.end()) {
            return false;
        }
        ManagerIds.erase(it);
        return true;
    }

    bool operator<(const TManagersId& item) const {
        if (ManagerIds.size() < item.ManagerIds.size()) {
            return true;
        } else if (ManagerIds.size() > item.ManagerIds.size()) {
            return false;
        } else {
            auto itSelf = ManagerIds.begin();
            auto itItem = item.ManagerIds.begin();
            while (itSelf != ManagerIds.end()) {
                if (*itSelf < *itItem) {
                    return true;
                }
                ++itSelf;
                ++itItem;
            }
            return false;
        }
    }
};

class TWaitEvent {
private:
    TAutoPtr<IEventBase> Event;
    const TActorId Sender;
public:
    TWaitEvent(TAutoPtr<IEventBase> ev, const TActorId& sender)
        : Event(ev)
        , Sender(sender)
    {

    }

    void Resend(const TActorIdentity& receiver) {
        TActivationContext::Send(new IEventHandle(receiver, Sender, Event.Release()));
    }
};

class TService: public NActors::TActorBootstrapped<TService> {
private:
    using TBase = NActors::TActor<TService>;
    bool ActiveFlag = false;
    bool PreparationFlag = false;
    std::map<TString, NActors::TActorId> Accessors;
    std::map<TManagersId, std::deque<TWaitEvent>> EventsWaiting;
    std::map<TString, IClassBehaviour::TPtr> ManagersInRegistration;
    std::map<TString, IClassBehaviour::TPtr> RegisteredManagers;

    std::shared_ptr<NInitializer::TFetcher> InitializationFetcher;
    std::shared_ptr<NInitializer::TSnapshot> InitializationSnapshot;
    std::shared_ptr<TServiceInternalController> InternalController;
    const TConfig Config;

    void Handle(TEvStartMetadataService::TPtr& ev);
    void Handle(NInitializer::TEvInitializationFinished::TPtr & ev);
    void Handle(TEvRefreshSubscriberData::TPtr& ev);

    void Handle(TEvAskSnapshot::TPtr& ev);
    void Handle(TEvPrepareManager::TPtr& ev);
    void Handle(TEvSubscribeExternal::TPtr& ev);
    void Handle(TEvUnsubscribeExternal::TPtr& ev);
    void Handle(TEvObjectsOperation::TPtr& ev);

    void Handle(TEvTableDescriptionSuccess::TPtr& ev);
    void Handle(TEvTableDescriptionFailed::TPtr& ev);

    void Handle(TDSAccessorSimple::TEvController::TEvResult::TPtr& ev);
    void Handle(TDSAccessorSimple::TEvController::TEvError::TPtr& ev);
    void Handle(TDSAccessorSimple::TEvController::TEvTableAbsent::TPtr& ev);

    void PrepareManagers(std::vector<IClassBehaviour::TPtr> manager, TAutoPtr<IEventBase> ev, const NActors::TActorId& sender);
    void InitializationFinished(const TString& initId);
    void Activate();

    template <class TAction>
    void ProcessEventWithFetcher(IEventHandle& ev, NFetcher::ISnapshotsFetcher::TPtr fetcher, TAction action) {
        std::vector<IClassBehaviour::TPtr> needManagers;
        for (auto&& i : fetcher->GetManagers()) {
            if (!RegisteredManagers.contains(i->GetTypeId())) {
                needManagers.emplace_back(i);
            }
        }
        if (needManagers.empty()) {
            auto it = Accessors.find(fetcher->GetComponentId());
            if (it == Accessors.end()) {
                THolder<TExternalData> actor = MakeHolder<TExternalData>(Config, fetcher);
                it = Accessors.emplace(fetcher->GetComponentId(), Register(actor.Release())).first;
            }
            action(it->second);
        } else {
            PrepareManagers(needManagers, ev.ReleaseBase(), ev.Sender);
        }
    }

public:

    void Bootstrap(const NActors::TActorContext& ctx);

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TDSAccessorSimple::TEvController::TEvResult, Handle);
            hFunc(TDSAccessorSimple::TEvController::TEvError, Handle);
            hFunc(TDSAccessorSimple::TEvController::TEvTableAbsent, Handle);

            hFunc(TEvObjectsOperation, Handle);
            hFunc(TEvRefreshSubscriberData, Handle);
            hFunc(TEvAskSnapshot, Handle);
            hFunc(TEvPrepareManager, Handle);
            hFunc(TEvSubscribeExternal, Handle);
            hFunc(TEvUnsubscribeExternal, Handle);

            hFunc(TEvTableDescriptionSuccess, Handle);
            hFunc(TEvTableDescriptionFailed, Handle);

            hFunc(TEvStartMetadataService, Handle);
            
            hFunc(NInitializer::TEvInitializationFinished, Handle);
            default:
                Y_VERIFY(false);
        }
    }

    TService(const TConfig& config)
        : Config(config)
    {
        TServiceOperator::Register(Config);
    }
};

NActors::IActor* CreateService(const TConfig& config);

}
