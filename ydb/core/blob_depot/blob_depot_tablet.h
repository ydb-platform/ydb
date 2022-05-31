#pragma once

#include "defs.h"
#include "events.h"

namespace NKikimr::NBlobDepot {

    using NTabletFlatExecutor::TTabletExecutedFlat;

    class TBlobDepot
        : public TActor<TBlobDepot>
        , public TTabletExecutedFlat
    {
    public:
        TBlobDepot(TActorId tablet, TTabletStorageInfo *info)
            : TActor(&TThis::StateInit)
            , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
        {}

        void Handle(TEvBlobDepot::TEvApplyConfig::TPtr ev) {
            auto response = std::make_unique<TEvBlobDepot::TEvApplyConfigResult>(TabletID(), ev->Get()->Record.GetTxId());
            Send(ev->Sender, response.release(), 0, ev->Cookie);
        }

        void Handle(TEvTabletPipe::TEvServerConnected::TPtr ev) {
            Y_UNUSED(ev);
        }

        void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr ev) {
            Y_UNUSED(ev);
        }

        void HandlePoison() {
            Become(&TThis::StateZombie);
            Send(Tablet(), new TEvents::TEvPoison);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        std::deque<std::unique_ptr<IEventHandle>> InitialEventsQ;

        void Enqueue(TAutoPtr<IEventHandle>& ev, const TActorContext&) override {
            InitialEventsQ.emplace_back(ev.Release());
        }

        void OnActivateExecutor(const TActorContext&) override {
            Become(&TThis::StateWork);
            for (auto&& ev : std::exchange(InitialEventsQ, {})) {
                TActivationContext::Send(ev.release());
            }
        }

        void OnDetach(const TActorContext&) override {
            // TODO: what does this callback mean
            PassAway();
        }

        void OnTabletDead(TEvTablet::TEvTabletDead::TPtr& /*ev*/, const TActorContext&) override {
            PassAway();
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        STFUNC(StateInit) {
            if (ev->GetTypeRewrite() == TEvents::TSystem::Poison) {
                HandlePoison();
            } else {
                StateInitImpl(ev, ctx);
            }
        }

        STFUNC(StateZombie) {
            StateInitImpl(ev, ctx);
        }

        STFUNC(StateWork) {
            switch (const ui32 type = ev->GetTypeRewrite()) {
                cFunc(TEvents::TSystem::Poison, HandlePoison);

                hFunc(TEvBlobDepot::TEvApplyConfig, Handle);

                hFunc(TEvTabletPipe::TEvServerConnected, Handle);
                hFunc(TEvTabletPipe::TEvServerDisconnected, Handle);

                default:
                    if (!HandleDefaultEvents(ev, ctx)) {
                        Y_FAIL("unexpected event Type# 0x%08" PRIx32, type);
                    }
                    break;
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        bool ReassignChannelsEnabled() const override {
            return true;
        }
    };

} // NKikimr::NBlobDepot
