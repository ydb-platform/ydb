#pragma once

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/bridge.h>

namespace NKikimr::NBridge {

    struct TKeepCheckPile {
        ui32 GroupId;
        ui32 Generation;

        TKeepCheckPile(const NKikimrBridge::TGroupState::TPile& pile)
            : GroupId(pile.GetGroupId())
            , Generation(pile.GetGroupGeneration())
        {}
    };

    // Observe the physical write even if the read or syncer that issued it goes away.
    IActor* CreateKeepCheckActor(TActorId parent, TVector<TKeepCheckPile> sources, TKeepCheckPile target,
        TVector<TLogoBlobID> kept, bool waitForCheck);

} // NKikimr::NBridge
