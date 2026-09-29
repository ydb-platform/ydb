#pragma once

#include <ydb/core/base/events.h>

namespace NYdb::NBS::NNbs1CompatApi::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Event id layout of the NBS1 tablet protocol (cloud/blockstore/libs/kikimr/
// components.h): every component of BLOCKSTORE_ACTORS owns 101 consecutive ids
// inside ES_BLOCKSTORE, starting one past the event space begin. Only the
// components whose requests the NBS2 volume tablet answers are listed.
struct TBlockStoreEvents
{
    enum
    {
        START = EventSpaceBegin(NKikimr::TKikimrEvents::ES_BLOCKSTORE),

        // Size of one component's id range including its END marker.
        COMPONENT_SIZE = 101,

        // SERVICE is the second component of BLOCKSTORE_ACTORS.
        SERVICE_START = START + 1 + COMPONENT_SIZE * 1,

        // VOLUME is the fourth component of BLOCKSTORE_ACTORS.
        VOLUME_START = START + 1 + COMPONENT_SIZE * 3,
    };

    static_assert(
        SERVICE_START == START + 102,
        "SERVICE_START expected to be == START + 102");
    static_assert(
        VOLUME_START == START + 304,
        "VOLUME_START expected to be == START + 304");
};

}   // namespace NYdb::NBS::NNbs1CompatApi::NBlockStore
