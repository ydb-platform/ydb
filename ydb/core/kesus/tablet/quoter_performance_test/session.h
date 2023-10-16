#pragma once
#include "test_state.h"
#include <ydb/core/kesus/tablet/ut_helpers.h>

#include <util/generic/yexception.h>

#include <limits>

using namespace NKikimr;
using namespace NKikimr::NKesus;

class TSessionActor : public TActorBootstrapped<TSessionActor> {
public:
    TSessionActor(TIntrusivePtr<TTestState> state)
        : State(std::move(state))
        , ResState(State->Options.ResourcesCount)
    {
    }

    void Bootstrap(const NActors::TActorContext&) {
        Become(&TSessionActor::StateFunc);
        TabletPipe = Register(NTabletPipe::CreateClient(SelfId(), State->TabletId));
        Subscribe();
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKesus::TEvSubscribeOnResourcesResult, Handle);
            hFunc(TEvKesus::TEvResourcesAllocated, Handle);
            cFunc(TEvents::TEvWakeup::EventType, EndSessionAndDie);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
        }
    }

    void Subscribe() {
        auto req = MakeHolder<TEvKesus::TEvSubscribeOnResources>();
        ActorIdToProto(SelfId(), req->Record.MutableActorID());
        req->Record.MutableResources()->Reserve(State->Options.ResourcesCount);

        //const double amount = State->Options.MaxUnitsPerSecond * State->Options.TestTime.Seconds() * 2.0;
        const double amount = std::numeric_limits<double>::infinity();
        for (size_t res = 0; res < State->Options.ResourcesCount; ++res) {
            auto* reqRes = req->Record.AddResources();
            reqRes->SetResourcePath(GetResourceName(res));
            reqRes->SetStartConsuming(true);
            reqRes->SetInitialAmount(amount);
        }
        NTabletPipe::SendData(SelfId(), TabletPipe, req.Release());
    }

    void Handle(TEvKesus::TEvSubscribeOnResourcesResult::TPtr& ev) {
        Y_ENSURE(ev->Get()->Record.ResultsSize() == State->Options.ResourcesCount);
        ScheduleStop();
    }

    void ScheduleStop() {
        Schedule(State->Options.TestTime, new TEvents::TEvWakeup());
    }

    void Handle(TEvKesus::TEvResourcesAllocated::TPtr& ev) {
        for (const auto& res : ev->Get()->Record.GetResourcesInfo()) {
            Y_ENSURE(res.GetStateNotification().GetStatus() == Ydb::StatusIds::SUCCESS);

            const auto resIndex = State->ResId2StateIndex.find(res.GetResourceId());
            Y_ENSURE(resIndex != State->ResId2StateIndex.end());
            ResState[resIndex->second].ConsumedAmount += res.GetAmount();
            //Cerr << ResState[resIndex->second].ConsumedAmount << Endl;
        }
    }

    void EndSessionAndDie() {
        with_lock (State->Mutex) {
            for (size_t res = 0; res < State->Options.ResourcesCount; ++res) {
                State->ResourcesState[res].ConsumedAmount += ResState[res].ConsumedAmount;
            }
        }
        Send(State->EdgeActorId, new TEvents::TEvWakeup());
        NTabletPipe::CloseClient(SelfId(), TabletPipe);
        PassAway();
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        Y_ABORT();
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        Y_ABORT_UNLESS(ev->Get()->Status == NKikimrProto::OK);
    }

private:
    TIntrusivePtr<TTestState> State;
    TInstant StartSessionTime;
    TActorId TabletPipe;
    std::vector<TTestState::TResourceState> ResState;
};
