#pragma once
#include "bootstrapper.h"
#include <ydb/core/protos/bootstrapper.pb.h>

namespace NKikimr {

struct TEvBootstrapper::TEvWatch : public TEventPB<TEvWatch, NKikimrBootstrapper::TEvWatch, EvWatch> {
    TEvWatch()
    {}

    TEvWatch(ui64 tabletId, ui64 selfSeed, ui64 round)
    {
        Record.SetTabletID(tabletId);
        Record.SetSelfSeed(selfSeed);
        Record.SetRound(round);
    }
};

struct TEvBootstrapper::TEvWatchResult : public TEventPB<TEvWatchResult, NKikimrBootstrapper::TEvWatchResult, EvWatchResult> {
    TEvWatchResult()
    {}

    TEvWatchResult(ui64 tabletId, NKikimrBootstrapper::TEvWatchResult::EState state, ui64 seed, ui64 round)
    {
        Record.SetTabletID(tabletId);
        Record.SetState(state);
        Record.SetSeed(seed);
        Record.SetRound(round);
    }
};

struct TEvBootstrapper::TEvNotify : public TEventPB<TEvNotify, NKikimrBootstrapper::TEvNotify, EvNotify> {
    TEvNotify()
    {}

    TEvNotify(ui64 tabletId, NKikimrBootstrapper::TEvNotify::EOp op, ui64 round)
    {
        Record.SetTabletID(tabletId);
        Record.SetOp(op);
        Record.SetRound(round);
    }
};

} // namespace NKikimr
