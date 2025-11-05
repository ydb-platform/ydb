#pragma once

#include "defs.h"
#include "rpc_deferrable.h"

#include <ydb/public/api/protos/draft/ydb_test_shard.pb.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/base/appdata.h>

namespace NKikimr::NGRpcService {

template <typename TDerived, typename TRequest, typename TResultRecord>
class TTestShardRequestBase : public TRpcOperationRequestActor<TDerived, TRequest> {
    using TBase = TRpcOperationRequestActor<TDerived, TRequest>;

public:
    TTestShardRequestBase(IRequestOpCtx* request)
        : TBase(request) {}

    void Bootstrap() {
        const auto& ctx = TActivationContext::AsActorContext();
        TBase::Bootstrap(ctx);

        auto* self = Self();
        Ydb::StatusIds::StatusCode status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
        NYql::TIssues issues;

        if (!self->ValidateRequest(status, issues)) {
            self->Reply(status, issues, self->ActorContext());
            return;
        }

        self->Become(&TDerived::StateFunc);
        self->SendRequestToHive();
    }

protected:
    void SetupHivePipe(ui64 hiveId) {
        const ui64 hiveTabletId = hiveId > 0 ? hiveId : AppData()->DomainsInfo->GetHive();
        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = {.RetryLimitCount = 3u};
        HivePipeClient = this->RegisterWithSameMailbox(
            NTabletPipe::CreateClient(this->SelfId(), hiveTabletId, pipeConfig));
    }

    void PassAway() override {
        if (HivePipeClient) {
            NTabletPipe::CloseClient(this->SelfId(), HivePipeClient);
            HivePipeClient = {};
        }
        TBase::PassAway();
    }

    const TDerived* Self() const { return static_cast<const TDerived*>(this); }
    TDerived* Self() { return static_cast<TDerived*>(this); }

protected:
    ui32 DomainUid = 1;
    ui64 HiveId = 0;
    TActorId HivePipeClient;
};

} // namespace NKikimr::NGRpcService
