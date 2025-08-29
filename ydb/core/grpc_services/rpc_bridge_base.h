#pragma once

#include "defs.h"
#include "rpc_deferrable.h"

#include <ydb/public/api/protos/draft/ydb_bridge.pb.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/nodewarden/node_warden.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>
#include <ydb/core/protos/bridge.pb.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

namespace NKikimr::NGRpcService {


template <typename TDerived, typename TRequest, typename TResultRecord>
class TBridgeRequestGrpc : public TRpcOperationRequestActor<TDerived, TRequest> {
    using TBase = TRpcOperationRequestActor<TDerived, TRequest>;

public:
    TBridgeRequestGrpc(IRequestOpCtx* request)
        : TBase(request) {}

    void Bootstrap() {
        const auto& ctx = TActivationContext::AsActorContext();
        TBase::Bootstrap(ctx);
        auto *self = Self();

        Ydb::StatusIds::StatusCode status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
        NYql::TIssues issues;
        if (!self->ValidateRequest(status, issues)) {
            self->Reply(status, issues, self->ActorContext());
            return;
        }

        self->Become(&TDerived::StateFunc);
        self->SendRequest(ctx);
    }

protected:
    const TDerived *Self() const { return static_cast<const TDerived*>(this); }
    TDerived *Self() { return static_cast<TDerived*>(this); }

    bool ReplyIfNotBootstrapped(TEvNodeWardenStorageConfig::TPtr& ev) {
        const auto* config = ev->Get()->Config.get();
        if (!config || config->GetGeneration() == 0) {
            auto* self = Self();
            self->Reply(Ydb::StatusIds::PRECONDITION_FAILED, "bridge cluster is not bootstrapped",
                NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
            return true;
        }
        return false;
    }
};

} // namespace NKikimr::NGRpcService
