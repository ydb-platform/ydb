#include "service_import.h"
#include "grpc_request_proxy.h"
#include "rpc_import_base.h"
#include "rpc_calls.h"
#include "rpc_operation_request_base.h"

#include <ydb/public/api/protos/ydb_import.pb.h>

#include <ydb/core/tx/schemeshard/schemeshard_import.h>

#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/ptr.h>
#include <util/string/builder.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace NSchemeShard;
using namespace NKikimrIssues;
using namespace Ydb;

using TEvImportFromS3Request = TGrpcRequestOperationCall<Ydb::Import::ImportFromS3Request,
    Ydb::Import::ImportFromS3Response>;

template <typename TDerived, typename TEvRequest>
class TImportRPC: public TRpcOperationRequestActor<TDerived, TEvRequest, true>, public TImportConv {
    TStringBuf GetLogPrefix() const override {
        return "[CreateImport]";
    }

    IEventBase* MakeRequest() override {
        const auto& request = *(this->GetProtoRequest());

        auto ev = MakeHolder<TEvImport::TEvCreateImportRequest>();
        ev->Record.SetTxId(this->TxId);
        ev->Record.SetDatabaseName(this->GetDatabaseName());
        if (this->UserToken) {
            ev->Record.SetUserSID(this->UserToken->GetUserSID());
        }

        auto& createImport = *ev->Record.MutableRequest();
        createImport.MutableOperationParams()->CopyFrom(request.operation_params());
        if (std::is_same_v<TEvRequest, TEvImportFromS3Request>) {
            createImport.MutableImportFromS3Settings()->CopyFrom(request.settings());
        }

        return ev.Release();
    }

    void Handle(TEvImport::TEvCreateImportResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record.GetResponse();

        LOG_D("Handle TEvImport::TEvCreateImportResponse"
            << ": record# " << record.ShortDebugString());

        this->Reply(TImportConv::ToOperation(record.GetEntry()));
    }

public:
    using TRpcOperationRequestActor<TDerived, TEvRequest, true>::TRpcOperationRequestActor;

    void Bootstrap(const TActorContext&) {
        const auto& request = *(this->GetProtoRequest());
        if (request.operation_params().has_forget_after() && request.operation_params().operation_mode() != Ydb::Operations::OperationParams::SYNC) {
            return this->Reply(StatusIds::UNSUPPORTED, TIssuesIds::DEFAULT_ERROR, "forget_after is not supported for this type of operation");
        }

        if (request.settings().items().empty()) {
            return this->Reply(StatusIds::BAD_REQUEST, TIssuesIds::DEFAULT_ERROR, "Items are not set");
        }

        this->AllocateTxId();
        this->Become(&TDerived::StateWait);
    }

    STATEFN(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvImport::TEvCreateImportResponse, Handle);
        default:
            return this->StateBase(ev);
        }
    }

}; // TImportRPC

class TImportFromS3RPC: public TImportRPC<TImportFromS3RPC, TEvImportFromS3Request> {
public:
    using TImportRPC::TImportRPC;
};

void DoImportFromS3Request(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TImportFromS3RPC(p.release()));
}

} // namespace NGRpcService
} // namespace NKikimr
