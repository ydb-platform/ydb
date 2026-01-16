#include "schemeshard_scheme_describers.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/kesus/tablet/events.h>
#include <ydb/core/tx/schemeshard/schemeshard_export_helpers.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/interconnect/types.h>

namespace NKikimr::NSchemeShard {

using namespace NKesus;

class TKesusResourcesDescriber : public NActors::TActorBootstrapped<TKesusResourcesDescriber> {
    void CreatePipe() {
        NTabletPipe::TClientConfig cfg;
        cfg.RetryPolicy = {
            .RetryLimitCount = 3u
        };
        KesusPipeClient = this->Register(NTabletPipe::CreateClient(this->SelfId(), KesusTabletId, cfg));
    }

    void GetAllResources() {
        THolder<TEvKesus::TEvDescribeQuoterResources> req = MakeHolder<TEvKesus::TEvDescribeQuoterResources>();
        req->Record.SetRecursive(true);
        req->Record.AddResourcePaths(""); // All resources

        NTabletPipe::SendData(SelfId(), KesusPipeClient, req.Release());
    }

public:
    void Bootstrap() {
        CreatePipe();
        GetAllResources();
    }

    void HandleResourcesDescription(TEvKesus::TEvDescribeQuoterResourcesResult::TPtr ev) {
        Send(ReplyTo, ev->Release());
    }

    STATEFN(StateDescribe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKesus::TEvDescribeQuoterResourcesResult, HandleResourcesDescription);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

    ui64 KesusTabletId = 0;
    TActorId KesusPipeClient;
    TActorId ReplyTo;
};

}