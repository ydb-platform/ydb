#pragma once
#include <ydb/core/base/tablet.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/library/aclib/aclib.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/ptr.h>
#include <util/stream/output.h>

namespace NKikimr {

struct TFakeSchemeShardState : public TThrRefBase {
    typedef TIntrusivePtr<TFakeSchemeShardState> TPtr;

    TFakeSchemeShardState()
    {}
    NACLib::TSecurityObject ACL;
};

// The functionality of this class is not full.
// So anyone is welcome to improve it.
class TFakeSchemeShard : public TActor<TFakeSchemeShard>, public NTabletFlatExecutor::TTabletExecutedFlat {
public:
    using TState = TFakeSchemeShardState;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::FLAT_SCHEMESHARD_ACTOR;
    }

    TFakeSchemeShard(const TActorId &tablet, TTabletStorageInfo *info, TState::TPtr state)
        : TActor<TFakeSchemeShard>(&TFakeSchemeShard::StateInit)
        , NTabletFlatExecutor::TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
        , State(state)
    {
    }

    void DefaultSignalTabletActive(const TActorContext &) override {
        // must be empty
    }

    void OnActivateExecutor(const TActorContext &ctx) final {
        Become(&TFakeSchemeShard::StateWork);
        SignalTabletActive(ctx);
    }

    void OnDetach(const TActorContext &ctx) override {
        Die(ctx);
    }

    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx) override {
        Y_UNUSED(ev);
        Die(ctx);
    }

    void StateInit(STFUNC_SIG) {
        StateInitImpl(ev, SelfId());
    }

    void StateWork(STFUNC_SIG) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);
            HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeScheme, Handle);
            HFunc(TEvents::TEvPoisonPill, Handle);
        }
    }

    void BrokenState(STFUNC_SIG) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);
        }
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeScheme::TPtr &ev, const TActorContext &ctx) {
        const auto& record = ev->Get()->Record;
        UNIT_ASSERT(record.GetPathId() == 1);
        TAutoPtr<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResultBuilder> response =
            new NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResultBuilder();
        TString out;
        Y_PROTOBUF_SUPPRESS_NODISCARD State->ACL.GetACL().SerializeToString(&out);
        response->Record.MutablePathDescription()->MutableSelf()->SetACL(out);
        response->Record.MutablePathDescription()->MutableSelf()->SetEffectiveACL(out);
        //Fill response from State
        ctx.Send(ev->Sender, response.Release());
    }

    void Handle(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        Become(&TThis::BrokenState);
        ctx.Send(Tablet(), new TEvents::TEvPoisonPill);
    }

private:
    TState::TPtr State;
};

inline void BootFakeSchemeShard(TTestActorRuntime& runtime, ui64 tabletId, TFakeSchemeShardState::TPtr state) {
    CreateTestBootstrapper(runtime, CreateTestTabletInfo(tabletId, TTabletTypes::SchemeShard), [=](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TFakeSchemeShard(tablet, info, state);
        });

    {
        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 1));
        runtime.DispatchEvents(options);
    }
}

} // namespace NKikimr
