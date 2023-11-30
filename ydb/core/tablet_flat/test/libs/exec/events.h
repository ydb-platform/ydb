#pragma once

#include "tablet_flat_executor.h"
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/actorsystem.h>

namespace NKikimr {
namespace NFake {

    enum EEv {
        Base_ = EventSpaceBegin(TKikimrEvents::ES_TABLET) + 2015,

        EvTerm      = Base_ + 0,    /* Terminates test in runtime env   */
        EvGone      = Base_ + 1,
        EvFire      = Base_ + 2,    /* Start new actor under root model */

        EvReady     = Base_ + 10,
        EvExecute   = Base_ + 11,
        EvResult    = Base_ + 12,
        EvReturn    = Base_ + 13,
        EvCompacted = Base_ + 14,
        EvCompact   = Base_ + 15,
        EvCall      = Base_ + 16,
    };

    struct TEvTerm : public TEventLocal<TEvTerm, EvTerm> { };

    struct TEvFire : public TEventLocal<TEvFire, EvFire> {
        TEvFire(ui32 level, const TActorId &alias, TActorSetupCmd cmd)
            : Level(level)
            , Alias(alias)
            , Cmd(std::move(cmd))
        {

        }

        const ui32 Level = Max<ui32>();
        const TActorId Alias;
        TActorSetupCmd Cmd;
    };

    struct TEvReady : public TEventLocal<TEvReady, EvReady> {
        TEvReady(ui64 tabletId, const TActorId& tabletActorID)
            : TabletID(tabletId)
            , ActorId(tabletActorID)
        {}
        ui64 TabletID;
        TActorId ActorId;
    };

    struct TEvExecute : public TEventLocal<TEvExecute, EvExecute> {
        using ITransaction = NTabletFlatExecutor::ITransaction;

        TEvExecute(TAutoPtr<ITransaction> func) {
            THolder<ITransaction> h(func.Release());
            Funcs.push_back(std::move(h));
        }

        TEvExecute(TVector<THolder<ITransaction>> funcs)
            : Funcs(std::move(funcs))
        { }

        TVector<THolder<ITransaction>> Funcs;
    };

    struct TEvResult : public TEventLocal<TEvResult, EvResult> {
        ui64 TabletID;
        bool Success;
    };

    struct TEvReturn : public TEventLocal<TEvReturn, EvReturn> { };

    struct TEvCompacted : public TEventLocal<TEvCompacted, EvCompacted> {
        TEvCompacted(ui32 table) : Table(table) { }

        ui64 Table;
    };

    struct TEvCompact : public TEventLocal<TEvCompact, EvCompact> {
        TEvCompact(ui32 table, bool memOnly = false)
            : Table(table)
            , MemOnly(memOnly)
        { }

        ui64 Table;
        bool MemOnly;
    };

    struct TEvCall : public TEventLocal<TEvCall, EvCall> {
        using IExecutor = NTabletFlatExecutor::NFlatExecutorSetup::IExecutor;
        using TCallback = std::function<void(IExecutor*, const TActorContext&)>;

        TEvCall(TCallback callback) : Callback(std::move(callback)) { }

        TCallback Callback;
    };

}
}
