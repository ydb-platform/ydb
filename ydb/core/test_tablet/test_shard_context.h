#pragma once

#include "defs.h"
#include "state_server_interface.h"
#include "processor.h"

namespace NKikimr::NTestShard {

    class TTestShardContext::TData {
        TProcessor Processor;

    public:
        void Action(TActorIdentity self, TEvStateServerRequest::TPtr ev);
    };

} // NKikimr::NTestShard
