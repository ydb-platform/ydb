#include <ydb/core/grpc_services/service_udf.h>

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/rpc_request_base.h>
#include <ydb/core/udf_api/common.h>
#include <ydb/core/udf_api/events.h>
#include <ydb/core/udf_api/mutation_actor.h>
#include <ydb/core/udf_api/query_actor.h>

#include <ydb/public/api/protos/ydb_udf.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NGRpcService {

using namespace NActors;

using TEvDeleteModuleRequest = TGrpcRequestOperationCall<Ydb::Udf::DeleteModuleRequest,
    Ydb::Udf::DeleteModuleResponse>;
using TEvListModulesRequest = TGrpcRequestOperationCall<Ydb::Udf::ListModulesRequest,
    Ydb::Udf::ListModulesResponse>;
using TEvDescribeModuleRequest = TGrpcRequestOperationCall<Ydb::Udf::DescribeModuleRequest,
    Ydb::Udf::DescribeModuleResponse>;

namespace {

TString AccessDeniedMessage(const NACLib::TUserToken* userToken) {
    TStringBuilder error;
    error << "Access denied";
    if (userToken) {
        error << ": '" << userToken->GetUserSID() << "' administers neither the cluster nor this database";
    }
    return error;
}

//! Shared shape of the unary UdfService calls: check that the caller may change
//! the store of this database, hand the request to an actor of
//! `ydb/core/udf_api` and pack whatever comes back into the operation.
template <class TDerived, class TEvRequest, class TResultEvent>
class TUdfRequestActor: public TRpcRequestActor<TDerived, TEvRequest, true> {
protected:
    using TBase = TRpcRequestActor<TDerived, TEvRequest, true>;

public:
    using TBase::TBase;

    void Bootstrap() {
        this->Become(&TDerived::StateWork);

        const TString database = this->GetDatabaseName();
        TString error;
        if (!NUdfApi::IsDatabaseServedHere(database, error)) {
            this->Reply(Ydb::StatusIds::BAD_REQUEST, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, error);
            return;
        }

        // With no database to resolve there is no owner and so no database
        // administrator; only a cluster administrator can get through.
        if (database.empty() || NUdfApi::CanDecideWithoutDatabaseOwner(this->UserToken.Get())) {
            Authorize(TString());
            return;
        }
        this->Send(MakeSchemeCacheID(), NUdfApi::MakeDatabaseOwnerRequest(database));
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TResultEvent, Handle);
            default:
                break;
        }
    }

private:
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        TString owner;
        if (!NUdfApi::ParseDatabaseOwner(*ev->Get()->Request, owner)) {
            this->Reply(Ydb::StatusIds::SCHEME_ERROR, NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR,
                "Error resolving database");
            return;
        }
        Authorize(owner);
    }

    void Authorize(const TString& databaseOwner) {
        if (!NUdfApi::IsUdfStoreAdministrator(this->UserToken.Get(), databaseOwner)) {
            this->Reply(Ydb::StatusIds::UNAUTHORIZED, NKikimrIssues::TIssuesIds::ACCESS_DENIED,
                AccessDeniedMessage(this->UserToken.Get()));
            return;
        }
        this->Register(static_cast<TDerived*>(this)->CreateWorker());
    }

    void Handle(typename TResultEvent::TPtr& ev) {
        const auto* msg = ev->Get();
        if (msg->Status != Ydb::StatusIds::SUCCESS) {
            this->Reply(msg->Status, msg->ErrorMessage);
            return;
        }
        auto operation = TBase::MakeOperation(Ydb::StatusIds::SUCCESS);
        operation.mutable_result()->PackFrom(msg->Result);
        this->Reply(operation);
    }
};

class TDeleteModuleRPC
    : public TUdfRequestActor<TDeleteModuleRPC, TEvDeleteModuleRequest, NUdfApi::TEvDeleteModuleResult> {
public:
    using TUdfRequestActor::TUdfRequestActor;

    IActor* CreateWorker() {
        return NUdfApi::CreateDeleteModuleActor(SelfId(), *GetProtoRequest(), GetDatabaseName());
    }
};

class TListModulesRPC
    : public TUdfRequestActor<TListModulesRPC, TEvListModulesRequest, NUdfApi::TEvListModulesResult> {
public:
    using TUdfRequestActor::TUdfRequestActor;

    IActor* CreateWorker() {
        return NUdfApi::CreateListModulesActor(SelfId(), *GetProtoRequest());
    }
};

class TDescribeModuleRPC
    : public TUdfRequestActor<TDescribeModuleRPC, TEvDescribeModuleRequest, NUdfApi::TEvDescribeModuleResult> {
public:
    using TUdfRequestActor::TUdfRequestActor;

    IActor* CreateWorker() {
        return NUdfApi::CreateDescribeModuleActor(SelfId(), *GetProtoRequest(), GetDatabaseName());
    }
};

//! The whole module body is buffered before anything is written, so the stream
//! needs a ceiling: without one a single caller could name an arbitrary amount
//! of node memory. Modules are compiled artifacts of a few tens of megabytes,
//! so this is well above anything the store is meant to hold.
constexpr ui64 MaxModuleBodySize = 256ull * 1024 * 1024;

class TUploadModuleStreamActor: public TActorBootstrapped<TUploadModuleStreamActor> {
    using IContext = IUploadModuleStreamContext;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    TUploadModuleStreamActor(TIntrusivePtr<IContext> context, const TActorId& grpcRequestProxyId)
        : Context_(std::move(context))
        , GRpcRequestProxyId_(grpcRequestProxyId)
    {
    }

    void Bootstrap() {
        Become(&TUploadModuleStreamActor::StateWork);
        Context_->Attach(SelfId());

        Database_ = ExtractDatabaseName(Context_->GetPeerMetaValues(NYdb::YDB_DATABASE_HEADER))
            .GetOrElse(TString());
        Send(GRpcRequestProxyId_, new TEvRequestAuthAndCheck(
            Database_,
            ExtractYdbToken(Context_->GetPeerMetaValues(NYdb::YDB_AUTH_TICKET_HEADER)),
            SelfId(),
            TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin),
            Context_->GetPeerName(),
            TString()));
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRequestAuthAndCheckResult, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(IContext::TEvReadFinished, Handle);
            hFunc(IContext::TEvNotifiedWhenDone, Handle);
            hFunc(NUdfApi::TEvUploadModuleResult, Handle);
            default:
                break;
        }
    }

private:
    void Handle(TEvRequestAuthAndCheckResult::TPtr& ev) {
        const auto* msg = ev->Get();
        if (msg->Status != Ydb::StatusIds::SUCCESS) {
            Reply(msg->Status, msg->Issues.ToOneLineString());
            return;
        }

        TString error;
        if (!NUdfApi::IsDatabaseServedHere(Database_, error)) {
            Reply(Ydb::StatusIds::BAD_REQUEST, error);
            return;
        }

        UserToken_ = msg->UserToken;
        // With no database to resolve there is no owner and so no database
        // administrator; only a cluster administrator can get through.
        if (Database_.empty() || NUdfApi::CanDecideWithoutDatabaseOwner(UserToken_.Get())) {
            Authorize(TString());
            return;
        }
        Send(MakeSchemeCacheID(), NUdfApi::MakeDatabaseOwnerRequest(Database_));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        TString owner;
        if (!NUdfApi::ParseDatabaseOwner(*ev->Get()->Request, owner)) {
            Reply(Ydb::StatusIds::SCHEME_ERROR, "Error resolving database");
            return;
        }
        Authorize(owner);
    }

    void Authorize(const TString& databaseOwner) {
        if (!NUdfApi::IsUdfStoreAdministrator(UserToken_.Get(), databaseOwner)) {
            Reply(Ydb::StatusIds::UNAUTHORIZED, AccessDeniedMessage(UserToken_.Get()));
            return;
        }
        Context_->Read();
    }

    void Handle(IContext::TEvReadFinished::TPtr& ev) {
        if (!ev->Get()->Success) {
            // The transport reports the end of the read side the same way
            // whether the client half-closed or the connection broke, so the
            // body is only accepted here because `total_size` says it is whole.
            RunMutation();
            return;
        }

        const auto& chunk = ev->Get()->Record;
        switch (chunk.payload_case()) {
            case Ydb::Udf::UploadModuleChunk::kMetadata:
                if (HasMetadata_) {
                    Reply(Ydb::StatusIds::BAD_REQUEST, "metadata may only be sent once");
                    return;
                }
                if (chunk.metadata().total_size() == 0) {
                    Reply(Ydb::StatusIds::BAD_REQUEST,
                        "metadata must carry total_size: it is the only way to tell a body that arrived whole"
                        " from one cut short mid-stream");
                    return;
                }
                if (chunk.metadata().total_size() > MaxModuleBodySize) {
                    Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder()
                        << "total_size " << chunk.metadata().total_size() << " exceeds the "
                        << MaxModuleBodySize << " byte limit");
                    return;
                }
                HasMetadata_ = true;
                TotalSize_ = chunk.metadata().total_size();
                Params_ = chunk.metadata().params();
                {
                    TString error;
                    const auto status = NUdfApi::ValidateUpload(Params_, error);
                    if (status != Ydb::StatusIds::SUCCESS) {
                        Reply(status, error);
                        return;
                    }
                }
                break;
            case Ydb::Udf::UploadModuleChunk::kData:
                if (!HasMetadata_) {
                    Reply(Ydb::StatusIds::BAD_REQUEST, "the first message of the stream must carry metadata");
                    return;
                }
                if (Body_.size() + chunk.data().size() > TotalSize_) {
                    Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder()
                        << "the stream carries more than the announced total_size of " << TotalSize_ << " bytes");
                    return;
                }
                Body_ += chunk.data();
                break;
            case Ydb::Udf::UploadModuleChunk::PAYLOAD_NOT_SET:
                Reply(Ydb::StatusIds::BAD_REQUEST, "stream message carries neither metadata nor data");
                return;
        }

        Context_->Read();
    }

    void Handle(IContext::TEvNotifiedWhenDone::TPtr&) {
        PassAway();
    }

    void Handle(NUdfApi::TEvUploadModuleResult::TPtr& ev) {
        const auto* msg = ev->Get();
        if (msg->Status != Ydb::StatusIds::SUCCESS) {
            Reply(msg->Status, msg->ErrorMessage);
            return;
        }
        Reply(Ydb::StatusIds::SUCCESS, {}, &msg->Result);
    }

    void RunMutation() {
        if (!HasMetadata_) {
            Reply(Ydb::StatusIds::BAD_REQUEST, "the stream ended before any metadata was sent");
            return;
        }
        // Nothing below this point can tell a truncated upload from a complete
        // one, and a module that is short a few bytes is a module that fails to
        // compile for reasons nobody will connect to the upload.
        if (Body_.size() != TotalSize_) {
            Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder()
                << "the stream ended after " << Body_.size() << " of the announced " << TotalSize_ << " bytes");
            return;
        }
        Register(NUdfApi::CreateUploadModuleActor(SelfId(), Params_, std::move(Body_)));
    }

    //! Errors travel in the operation of the single response message rather
    //! than in the gRPC status, so a client reads them the same way it reads
    //! the unary calls of this service.
    void Reply(
        Ydb::StatusIds::StatusCode status,
        const TString& error,
        const Ydb::Udf::UploadModuleResult* result = nullptr)
    {
        Ydb::Udf::UploadModuleResponse response;
        auto& operation = *response.mutable_operation();
        operation.set_ready(true);
        operation.set_status(status);
        if (result) {
            operation.mutable_result()->PackFrom(*result);
        }
        if (error) {
            NYql::IssueToMessage(NYql::TIssue(error), operation.add_issues());
        }

        Context_->WriteAndFinish(std::move(response), grpc::Status::OK);
        PassAway();
    }

private:
    const TIntrusivePtr<IContext> Context_;
    const TActorId GRpcRequestProxyId_;

    TString Database_;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken_;

    bool HasMetadata_ = false;
    ui64 TotalSize_ = 0;
    Ydb::Udf::UploadModuleParams Params_;
    TString Body_;
};

} // namespace

void DoDeleteModuleRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TDeleteModuleRPC(p.release()));
}

void DoListModulesRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TListModulesRPC(p.release()));
}

void DoDescribeModuleRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TDescribeModuleRPC(p.release()));
}

IActor* CreateUploadModuleStreamActor(
    TIntrusivePtr<IUploadModuleStreamContext> context,
    const TActorId& grpcRequestProxyId)
{
    return new TUploadModuleStreamActor(std::move(context), grpcRequestProxyId);
}

} // namespace NKikimr::NGRpcService
