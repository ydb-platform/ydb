#include "tablet_helpers.h"
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tablet_flat/flat_dbase_scheme.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr {

namespace {

class TDummyFlatTablet : public TActor<TDummyFlatTablet>, public NTabletFlatExecutor::TTabletExecutedFlat {

    struct Schema : NIceDb::Schema {
        struct Snaps : Table<1> {
            struct SourceTableId : Column<1, NScheme::NTypeIds::Uint32> {};
            struct DestinationTablet : Column<2, NScheme::NTypeIds::Uint64> {};
            struct Delivered : Column<3, NScheme::NTypeIds::Bool> {};

            using TKey = TableKey<SourceTableId, DestinationTablet>;
            using TColumns = TableColumns<SourceTableId, DestinationTablet, Delivered>;
        };

        struct t_by_ui64 : Table<32> {
            struct key : Column<32, NScheme::NTypeIds::Uint64> {};
            struct v_bytes : Column<33, NScheme::NTypeIds::String4k> {};
            struct v_ui64 : Column<34, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<key>;
            using TColumns = TableColumns<key, v_bytes, v_ui64>;
        };

        struct t_by_bytes : Table<33> {
            struct key : Column<32, NScheme::NTypeIds::String4k> {};
            struct v_bytes : Column<33, NScheme::NTypeIds::String4k> {};
            struct v_ui64 : Column<34, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<key>;
            using TColumns = TableColumns<key, v_bytes, v_ui64>;
        };

        using TTables = SchemaTables<Snaps, t_by_ui64, t_by_bytes>;
    };

    struct TTxSchemeInit : public NTabletFlatExecutor::ITransaction {
        TDummyFlatTablet * const Self;

        TTxSchemeInit(TDummyFlatTablet *self)
            : Self(self)
        {}

        bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
            Y_UNUSED(ctx);
            NIceDb::TNiceDb(txc.DB).Materialize<Schema>();
            return true;
        }

        void Complete(const TActorContext &ctx) override {
            Self->Execute(new TTxInit(Self), ctx);
        }
    };

    struct TTxInit : public NTabletFlatExecutor::ITransaction {
        TDummyFlatTablet * const Self;

        TTxInit(TDummyFlatTablet *self)
            : Self(self)
        {}

        bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
            Y_UNUSED(txc);
            Y_UNUSED(ctx);
            return true;
        }

        void Complete(const TActorContext &ctx) override {
            Self->SignalTabletActive(ctx);
        }
    };

    friend struct TTxSchemeInit;
    friend struct TTxInit;

    void OnActivateExecutor(const TActorContext &ctx) override {
        Become(&TThis::StateWork);
        if (Executor()->GetStats().IsFollower)
            SignalTabletActive(ctx);
        else
            Execute(new TTxSchemeInit(this), ctx);
    }

    void OnDetach(const TActorContext &ctx) override {
        Die(ctx);
    }

    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx) override {
        Y_UNUSED(ev);
        Die(ctx);
    }

    void DefaultSignalTabletActive(const TActorContext &ctx) override {
        Y_UNUSED(ctx);
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TEST_ACTOR_RUNTIME;
    }

    TDummyFlatTablet(const TActorId &tablet, TTabletStorageInfo *info)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
    {}

    STFUNC(StateInit) {
        StateInitImpl(ev, SelfId());
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            default:
                HandleDefaultEvents(ev, SelfId());
                break;
        }
    }
};

} // namespace

IActor* CreateFlatDummyTablet(const TActorId &tablet, TTabletStorageInfo *info) {
    return new TDummyFlatTablet(tablet, info);

}}
