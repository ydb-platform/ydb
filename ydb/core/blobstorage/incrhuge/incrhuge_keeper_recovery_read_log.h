#pragma once

#include "defs.h"

#include <library/cpp/actors/core/actor.h>

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>

#include <util/generic/maybe.h>

namespace NKikimr {
    namespace NIncrHuge {

        NActors::IActor *CreateRecoveryReadLogActor(const NActors::TActorId& pdiskActorId, ui8 owner,
                NPDisk::TOwnerRound ownerRound, TMaybe<ui64> chunksEntrypointLsn, TMaybe<ui64> deletesEntrypointLsn);

    } // NIncrHuge
} // NKikimr
