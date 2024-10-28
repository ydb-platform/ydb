#include "rpc_change_schema.h"
#include "service_tablet.h"

#include <ydb/core/grpc_services/rpc_request_base.h>
#include <ydb/core/grpc_services/audit_dml_operations.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/protos/scheme_log.pb.h>

#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/protobuf/json/proto2json.h>

namespace NKikimr::NGRpcService {

class TRpcChangeTabletSchema : public TRpcRequestActor<TRpcChangeTabletSchema, TEvChangeTabletSchemaRequest> {
    using TBase = TRpcRequestActor<TRpcChangeTabletSchema, TEvChangeTabletSchemaRequest>;

public:
    using TBase::TBase;

    void Bootstrap() {
        if (!CheckAccess()) {
            auto error = TStringBuilder() << "Access denied";
            if (this->UserToken) {
                error << ": '" << this->UserToken->GetUserSID() << "' is not an admin";
            }

            this->Reply(Ydb::StatusIds::UNAUTHORIZED, NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
            return;
        }

        auto* req = this->GetProtoRequest();
        AuditContextAppend(Request.Get(), *req);

        try {
            TabletId = req->tablet_id();
            TabletReq = std::make_unique<TEvTablet::TEvLocalSchemeTx>();
            if (const auto& changes = req->schema_changes(); !changes.empty()) {
                NProtobufJson::Json2Proto(changes, *TabletReq->Record.MutableSchemeChanges(), {
                    .FieldNameMode = NProtobufJson::TJson2ProtoConfig::FieldNameSnakeCaseDense,
                    .AllowUnknownFields = false,
                    .MapAsObject = true,
                    .EnumValueMode = NProtobufJson::TJson2ProtoConfig::EnumSnakeCaseInsensitive,
                });
            }
            TabletReq->Record.SetDryRun(req->dry_run());
        } catch (const std::exception& e) {
            this->Reply(Ydb::StatusIds::BAD_REQUEST, e.what());
            return;
        }

        PipeClient = RegisterWithSameMailbox(NTabletPipe::CreateClient(SelfId(), TabletId, NTabletPipe::TClientRetryPolicy{
            // We need at least one retry since local resolver cache may be outdated
            .RetryLimitCount = 1,
        }));

        Schedule(TDuration::Seconds(60), new TEvents::TEvWakeup);

        Become(&TThis::StateWork);
    }

private:
    bool CheckAccess() const {
        if (AppData()->AdministrationAllowedSIDs.empty()) {
            return true;
        }

        if (!this->UserToken) {
            return false;
        }

        for (const auto& sid : AppData()->AdministrationAllowedSIDs) {
            if (this->UserToken->IsExist(sid)) {
                return true;
            }
        }

        return false;
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvTablet::TEvLocalSchemeTxResponse, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        auto* msg = ev->Get();
        if (msg->Status != NKikimrProto::OK) {
            this->Reply(Ydb::StatusIds::UNAVAILABLE,
                TStringBuilder() << "Tablet " << TabletId << " is unavailable");
            return;
        }

        NTabletPipe::SendData(SelfId(), PipeClient, TabletReq.release());
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        this->Reply(Ydb::StatusIds::UNDETERMINED,
            TStringBuilder() << "Tablet " << TabletId << " disconnected");
    }

    void Handle(TEvTablet::TEvLocalSchemeTxResponse::TPtr& ev) {
        NTabletPipe::CloseClient(SelfId(), PipeClient);

        auto* msg = ev->Get();
        if (msg->Record.GetStatus() != NKikimrProto::OK) {
            this->Reply(Ydb::StatusIds::GENERIC_ERROR,
                msg->Record.HasErrorReason() ? msg->Record.GetErrorReason() : "Unknown error");
            return;
        }

        auto* response = google::protobuf::Arena::CreateMessage<Ydb::Tablet::ChangeTabletSchemaResponse>(Request->GetArena());
        response->set_status(Ydb::StatusIds::SUCCESS);
        if (msg->Record.HasFullScheme()) {
            try {
                TString text;
                NProtobufJson::Proto2Json(msg->Record.GetFullScheme(), text, {
                    .EnumMode = NProtobufJson::TProto2JsonConfig::EnumName,
                    .FieldNameMode = NProtobufJson::TProto2JsonConfig::FieldNameSnakeCaseDense,
                    .MapAsObject = true,
                });
                response->set_schema(std::move(text));
            } catch (const std::exception& e) {
                response->set_status(Ydb::StatusIds::GENERIC_ERROR);
                auto* issue = response->add_issues();
                issue->set_severity(NYql::TSeverityIds::S_ERROR);
                issue->set_message(e.what());
            }
        }
        Request->Reply(response, response->status());
        PassAway();
    }

    void Handle(TEvents::TEvWakeup::TPtr&) {
        NTabletPipe::CloseClient(SelfId(), PipeClient);
        this->Reply(Ydb::StatusIds::TIMEOUT,
            TStringBuilder() << "Tablet " << TabletId << " is not responding");
    }

private:
    ui64 TabletId;
    std::unique_ptr<TEvTablet::TEvLocalSchemeTx> TabletReq;
    TActorId PipeClient;
};

void DoChangeTabletSchemaRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TRpcChangeTabletSchema(p.release()));
}

template<>
IActor* TEvChangeTabletSchemaRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestNoOpCtx* msg) {
    return new TRpcChangeTabletSchema(msg);
}

} // namespace NKikimr::NGRpcService
