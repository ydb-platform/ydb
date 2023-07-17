#pragma once
#include "txallocator.h"
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/counters_tx_allocator.pb.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr {

using namespace NTabletFlatExecutor;
using NTabletFlatExecutor::ITransaction;

namespace NTxAllocator {

struct TTxAllocatorMonCounters {
    TIntrusivePtr<::NMonitoring::TDynamicCounters> AllocatorCounters;

    ::NMonitoring::TDynamicCounters::TCounterPtr Allocated;
    ::NMonitoring::TDynamicCounters::TCounterPtr AllocationsPresence;
};

class TTxAllocator : public TActor<TTxAllocator>, public TTabletExecutedFlat {
public:
    static constexpr ui64 MaxCapacity = 0xFFFFFFFFFFFFull; //48bit

private:
    struct TTxSchema;
    struct TTxReserve;

    ITransaction* CreateTxSchema();
    ITransaction* CreateTxReserve(TEvTxAllocator::TEvAllocate::TPtr &ev);

    const ui64 PrivateMarker;
    TTxAllocatorMonCounters MonCounters;

    void InitCounters(const TActorContext &ctx);

    void OnActivateExecutor(const TActorContext &ctx) override;
    void OnDetach(const TActorContext &ctx) override;
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx) override;
    void DefaultSignalTabletActive(const TActorContext &ctx)  override;

    ui64 IsAllowedToAllocate(ui64 size) const;

    ui64 ApplyPrivateMarker(const ui64 elem);

    void Reply(const ui64 rangeBegin, const ui64 rangeEnd, const TEvTxAllocator::TEvAllocate::TPtr &ev, const TActorContext &ctx);
    void ReplyImposible(const TEvTxAllocator::TEvAllocate::TPtr &ev, const TActorContext &ctx);

    void Handle(TEvTxAllocator::TEvAllocate::TPtr &ev, const TActorContext &ctx);

public:
    struct Schema : NIceDb::Schema {
        // for compatible use old names config, dummyKey, reservedIds
        struct config : Table<1> {
            enum EKeyType : bool {
                ReservedTo = true,
            };

            // for compatible use old names
            struct dummyKey : Column<1, NScheme::NTypeIds::Bool> { using Type = EKeyType; }; // PK
            struct reservedIds : Column<2, NScheme::NTypeIds::Uint64> {};

            // for compatible do not use colum this collum
            //struct ForCompatible : Column<3, NScheme::NTypeIds::String4k> {};

            using TKey = TableKey<dummyKey>;
            using TColumns = TableColumns<dummyKey, reservedIds>;
        };

        using TTables = SchemaTables<config>;
    };

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_ALLOCATOR_ACTOR;
    }

    TTxAllocator(const TActorId &tablet, TTabletStorageInfo *info);

    STFUNC(StateInit) {
        StateInitImpl(ev, SelfId());
    }

    void Enqueue(STFUNC_SIG) override {
        ALOG_ERROR(NKikimrServices::TX_ALLOCATOR,
                    "tablet# " << TabletID() <<
                    " IGNORING message type# " <<  ev->GetTypeRewrite() <<
                    " from Sender# " << ev->Sender.ToString() <<
                    " at StateInit");
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxAllocator::TEvAllocate, Handle);
            IgnoreFunc(TEvTabletPipe::TEvServerConnected);
            IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);
        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                ALOG_ERROR(NKikimrServices::TX_ALLOCATOR,
                            "tablet# " << TabletID() <<
                            " IGNORING message type# " <<  ev->GetTypeRewrite() <<
                            " from Sender# " << ev->Sender.ToString() <<
                            " at StateWork");
            }
        }
    }
};
}
}
