#pragma once
#include "private_events.h"
#include <ydb/core/tx/replication/ut_helpers/test_env.h>

namespace NKikimr::NReplication::NController {

class TDstCreatorEnv: public NTestHelpers::TEnv<> {
public:
    using NTestHelpers::TEnv<>::TEnv;

private:
    TTestActorRuntime::TEventObserverHolder SaveObserver = GetRuntime().AddObserver<TEvPrivate::TEvSaveDstCreateRequest>(
        [this](TEvPrivate::TEvSaveDstCreateRequest::TPtr& ev) {
            if (ev->Recipient == GetSender()) {
                GetRuntime().Send(ev->Sender, GetSender(), new TEvPrivate::TEvDstCreateRequestSaved());
                ev.Reset();
            }
        });
};

}
