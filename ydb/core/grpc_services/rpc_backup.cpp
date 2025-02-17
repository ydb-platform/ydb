#include "service_backup.h"
#include "grpc_request_proxy.h"
#include "rpc_calls.h"
#include "rpc_operation_request_base.h"

#include <ydb/public/api/protos/draft/ydb_backup.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard_backup.h>

#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/ptr.h>
#include <util/string/builder.h>

namespace NKikimr::NGRpcService {

using namespace NActors;
using namespace NKikimrIssues;
using namespace Ydb;

template <class TIn, class TOut>
class TBackupCollectionsRPC
    : public TRpcOperationRequestActor<
        TBackupCollectionsRPC<TIn, TOut>,
        TGrpcRequestOperationCall<TIn, TOut>,
        true
    >
{
public:
    using TSelf = TBackupCollectionsRPC<TIn, TOut>;
    using TRpcOperationRequestActor<
        TBackupCollectionsRPC<TIn, TOut>,
        TGrpcRequestOperationCall<TIn, TOut>,
        true
    >::TRpcOperationRequestActor;

    TStringBuf GetLogPrefix() const override {
        return "[Backup]";
    }

    IEventBase* MakeRequest() override {
        // TODO copy / transfer event from public api to internal

        auto ev = MakeHolder<typename NSchemeShard::TEvBackup::TEvApiMapping<TIn>::TEv>();
        ev->Record.SetTxId(this->TxId);
        ev->Record.SetDatabaseName(this->GetDatabaseName());
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
        this->Become(&TSelf::StateServe);
    }

    template <class T>
    void Handle(T&) {
        this->Reply(StatusIds::INTERNAL_ERROR, TIssuesIds::DEFAULT_ERROR, "This call isn't supported yet.");
    }

    STATEFN(StateServe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NSchemeShard::TEvBackup::TEvApiMapping<TOut>::TEv, Handle);
        default:
            return this->StateBase(ev);
        }
    }
};

#ifdef DEFINE_REQUEST_HANDLER
#error DEFINE_REQUEST_HANDLER macro redefinition
#else
#define DEFINE_REQUEST_HANDLER(NAME) void Do ## NAME ## Request(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) { \
    f.RegisterActor(new TBackupCollectionsRPC<Ydb::Backup:: NAME ## Request, Ydb::Backup:: NAME ## Response>(p.release())); \
} Y_SEMICOLON_GUARD
#endif

DEFINE_REQUEST_HANDLER(FetchBackupCollections);
DEFINE_REQUEST_HANDLER(ListBackupCollections);
DEFINE_REQUEST_HANDLER(CreateBackupCollection);
DEFINE_REQUEST_HANDLER(ReadBackupCollection);
DEFINE_REQUEST_HANDLER(UpdateBackupCollection);
DEFINE_REQUEST_HANDLER(DeleteBackupCollection);

#undef DEFINE_REQUEST_HANDLER

} // namespace NKikimr::NGrpcService
