#pragma once
#include <ydb/core/base/events.h>

namespace NKikimr::NOlap::NBackground {

class TEvents {
public:
    enum EEv {
        EvExecuteGeneralLocalTransaction = EventSpaceBegin(TKikimrEvents::ES_TX_BACKGROUND),
        EvLocalTransactionComplete,
        EvSessionControl,
        EvRemoveSession,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_BACKGROUND), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_BACKGROUND)");
};

}