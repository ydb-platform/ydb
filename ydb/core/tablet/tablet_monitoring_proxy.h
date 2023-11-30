#pragma once

////////////////////////////////////////////
#include "defs.h"

#include <ydb/library/actors/core/defs.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/tablet_pipe.h>

////////////////////////////////////////////
namespace NKikimr { namespace NTabletMonitoringProxy {

struct TTabletMonitoringProxyConfig {
    bool PreferLocal = true;
    NTabletPipe::TClientRetryPolicy RetryPolicy;

    void SetRetryLimitCount(ui32 retryLimitCount) {
        RetryPolicy = {.RetryLimitCount = retryLimitCount};
    }
};

//
inline TActorId MakeTabletMonitoringProxyID(ui32 node = 0) {
    char x[12] = {'t','a','b','l','m','o','n','p','r','o','x','y'};
    return TActorId(node, TStringBuf(x, 12));
}

//
IActor* CreateTabletMonitoringProxy(TTabletMonitoringProxyConfig config = TTabletMonitoringProxyConfig());

} } // end of the NKikimr::NCompactionService namespace

