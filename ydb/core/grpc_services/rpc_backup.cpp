#include "service_backup.h"
#include "grpc_request_proxy.h"
#include "rpc_calls.h"
#include "rpc_operation_request_base.h"

#include <ydb/public/api/protos/draft/ydb_backup.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard_backup.h>

#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/ptr.h>
#include <util/string/builder.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace NKikimrIssues;
using namespace Ydb;

using TEvFetchBackupCollectionsRequest = TGrpcRequestOperationCall<
    Ydb::Backup::FetchBackupCollectionsRequest,
    Ydb::Backup::FetchBackupCollectionsResponse>;

class TFetchBackupCollectionsRPC
    : public TRpcOperationRequestActor<
        TFetchBackupCollectionsRPC,
        TEvFetchBackupCollectionsRequest,
        true
    >
{
public:
    using TRpcOperationRequestActor<
        TFetchBackupCollectionsRPC,
        TEvFetchBackupCollectionsRequest,
        true
    >::TRpcOperationRequestActor;

    TStringBuf GetLogPrefix() const override {
        return "[Backup]";
    }

    IEventBase* MakeRequest() override {
        // TODO copy / transfer event from public api to internal

        auto ev = MakeHolder<NSchemeShard::TEvBackup::TEvFetchBackupCollectionsRequest>();
        ev->Record.SetTxId(this->TxId);
        ev->Record.SetDatabaseName(this->DatabaseName);
        if (this->UserToken) {
            ev->Record.SetUserSID(this->UserToken->GetUserSID());
        }

        return ev.Release();
    }

    void Bootstrap(const TActorContext&) {
        const auto& request = *(this->GetProtoRequest());
        if (request.operation_params().has_forget_after() && request.operation_params().operation_mode() != Ydb::Operations::OperationParams::SYNC) {
            return this->Reply(StatusIds::UNSUPPORTED, TIssuesIds::DEFAULT_ERROR, "forget_after is not supported for this type of operation");
        }

        // TODO some validation

        this->AllocateTxId();
        this->Become(&TFetchBackupCollectionsRPC::StateServe);
    }

    template <class T>
    void Handle(T&) {
        this->Reply(StatusIds::INTERNAL_ERROR, TIssuesIds::DEFAULT_ERROR, "This call isn't supported yet.");
    }

    STATEFN(StateServe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NSchemeShard::TEvBackup::TEvFetchBackupCollectionsResponse, Handle);
        default:
            return this->StateBase(ev);
        }
    }
};

void DoFetchBackupCollectionsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFetchBackupCollectionsRPC(p.release()));
}

void DoListBackupCollectionsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFetchBackupCollectionsRPC(p.release()));
}

void DoCreateBackupCollectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFetchBackupCollectionsRPC(p.release()));
}

void DoReadBackupCollectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFetchBackupCollectionsRPC(p.release()));
}

void DoUpdateBackupCollectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFetchBackupCollectionsRPC(p.release()));
}

void DoDeleteBackupCollectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFetchBackupCollectionsRPC(p.release()));
}

}
}
