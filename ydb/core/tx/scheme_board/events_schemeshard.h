#pragma once

#include "defs.h"
#include "events.h"

#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/protos/scheme_board.pb.h>

namespace NKikimr::NSchemeBoard {

namespace NSchemeshardEvents {

// populator --> schemeshard events
struct TEvUpdateAck: public TEventPB<TEvUpdateAck, NKikimrSchemeBoard::TEvUpdateAck, TSchemeBoardEvents::EvUpdateAck> {
    TEvUpdateAck() = default;

    explicit TEvUpdateAck(
        const ui64 owner,
        const ui64 generation,
        const TPathId pathId,
        const ui64 version
    ) {
        Record.SetOwner(owner);
        Record.SetGeneration(generation);
        Record.SetLocalPathId(pathId.LocalPathId);
        Record.SetVersion(version);
        Record.SetPathOwnerId(pathId.OwnerId);
    }

    TPathId GetPathId() const {
        return TPathId(
            Record.HasPathOwnerId() ? Record.GetPathOwnerId() : Record.GetOwner(),
            Record.GetLocalPathId()
        );
    }

    TString ToString() const override;
};

}  // NSchemeshardEvents

}  // NKikimr::NSchemeBoard
