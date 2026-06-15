#include <ydb/core/ymq/actor/serviceid.h>

#include <ydb/library/ycloud/impl/access_service.h>
#include <ydb/library/folder_service/events.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/stream/file.h>
#include <util/string/builder.h>

namespace NKikimr::NSQS {

static const TString SERVICE_ACCOUNT_PREFIX = "sa_";
static const TString USER_ACCOUNT_PREFIX = "usr_";

class TSqsAccessServiceMock
    : public TActor<TSqsAccessServiceMock>
{
    using TThis = TSqsAccessServiceMock;
    using TBase = TActor<TSqsAccessServiceMock>;

    size_t RequestNumber = 0;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_ACTOR;
    }

    TSqsAccessServiceMock()
        : TBase(&TThis::StateWork)
        {}

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NCloud::TEvAccessService::TEvAuthenticateRequest, Handle);
            hFunc(NCloud::TEvAccessService::TEvAuthorizeRequest, Handle);
            hFunc(NCloud::TEvAccessService::TEvAuthenticateRequestV2, Handle);
            hFunc(NCloud::TEvAccessService::TEvAuthorizeRequestV2, Handle);
            cFunc(TEvPoisonPill::EventType, PassAway);
        }
    }

    void Handle(NCloud::TEvAccessService::TEvAuthenticateRequest::TPtr& ev) {
        auto result = MakeHolder<NCloud::TEvAccessService::TEvAuthenticateResponse>();
        HandleAuthenticateRequest(ev->Get()->Request, result->Status, result->Response);
        Send(ev->Sender, result.Release());
    }

    template <typename TRequest, typename TResponse>
    void HandleAuthenticateRequest(const TRequest& request, NYdbGrpc::TGrpcStatus& status, TResponse& response) {
        if (++RequestNumber % 3 == 0) {
            status = NYdbGrpc::TGrpcStatus("Unavailable", grpc::StatusCode::UNAVAILABLE, false);
        } else {
            if (request.Hasiam_token()) {
                status = NYdbGrpc::TGrpcStatus("Auth error", grpc::StatusCode::UNAUTHENTICATED, false);
            } else {
                TString idStr = request.Getsignature().Getaccess_key_id();

                TStringBuf id = idStr;
                id.SkipPrefix(SERVICE_ACCOUNT_PREFIX);

                if (id == "alkonavt") {
                    status = NYdbGrpc::TGrpcStatus("Auth error", grpc::StatusCode::UNAUTHENTICATED, false);
                } else if (!id) {
                    status = NYdbGrpc::TGrpcStatus("Empty access key id", grpc::StatusCode::INVALID_ARGUMENT, false);
                } else {
                    auto& serviceAccount = *response.mutable_subject()->mutable_service_account();
                    serviceAccount.Setid(TString(id));
                    serviceAccount.Setfolder_id(TString::Join("FOLDER_", id));
                }
            }
        }
    }

    template <typename TRequest, typename TResponse>
    void HandleAuthorizeRequest(const TRequest& request, NYdbGrpc::TGrpcStatus& status, TResponse& response) {
        if (++RequestNumber % 3 == 0) {
            status = NYdbGrpc::TGrpcStatus("Unavailable", grpc::StatusCode::DEADLINE_EXCEEDED, false);
        } else {
            TString idStr;
            if (request.Hasiam_token()) {
                idStr = request.Getiam_token();
            } else {
                idStr = request.Getsignature().Getaccess_key_id();
            }

            TStringBuf id = idStr;
            if (!id) {
               status = NYdbGrpc::TGrpcStatus("Empty access key id", grpc::StatusCode::INVALID_ARGUMENT, false);
            } else if (id.SkipPrefix(USER_ACCOUNT_PREFIX)) {
                if (id == "alkonavt") {
                    status = NYdbGrpc::TGrpcStatus("Auth error", grpc::StatusCode::UNAUTHENTICATED, false);
                } else {
                    response.mutable_subject()->mutable_user_account()->set_id(TString(id));
                    status = NYdbGrpc::TGrpcStatus("OK", grpc::StatusCode::OK, false);
                }
            } else if (id.SkipPrefix(SERVICE_ACCOUNT_PREFIX)) {
                auto& serviceAccount = *response.mutable_subject()->mutable_service_account();
                serviceAccount.Setid(TString(id));
                serviceAccount.Setfolder_id(TString::Join("FOLDER_", id));
                status = NYdbGrpc::TGrpcStatus("OK", grpc::StatusCode::OK, false);
            } else {
                status = NYdbGrpc::TGrpcStatus("Auth error", grpc::StatusCode::UNAUTHENTICATED, false);
            }
        }
    }

    void Handle(NCloud::TEvAccessService::TEvAuthorizeRequest::TPtr& ev) {
        auto result = MakeHolder<NCloud::TEvAccessService::TEvAuthorizeResponse>();
        HandleAuthorizeRequest(ev->Get()->Request, result->Status, result->Response);
        Send(ev->Sender, result.Release());
    }

    void Handle(NCloud::TEvAccessService::TEvAuthenticateRequestV2::TPtr& ev) {
        auto result = MakeHolder<NCloud::TEvAccessService::TEvAuthenticateResponseV2>();
        HandleAuthenticateRequest(ev->Get()->Request, result->Status, result->Response);
        Send(ev->Sender, result.Release());
    }

    void Handle(NCloud::TEvAccessService::TEvAuthorizeRequestV2::TPtr& ev) {
        auto result = MakeHolder<NCloud::TEvAccessService::TEvAuthorizeResponseV2>();
        HandleAuthorizeRequest(ev->Get()->Request, result->Status, result->Response);
        Send(ev->Sender, result.Release());
    }
};

class TSqsFolderServiceMock
    : public TActor<TSqsFolderServiceMock>
{
    using TThis = TSqsFolderServiceMock;
    using TBase = TActor<TSqsFolderServiceMock>;

    using TEvGetCloudByFolderResponse = NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderResponse;
    using TEvGetCloudByFolderRequest = NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderRequest;

    size_t RequestNumber = 0;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_ACTOR;
    }

    TSqsFolderServiceMock()
        : TBase(&TThis::StateWork)
        {}

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvGetCloudByFolderRequest, Handle);
            cFunc(TEvPoisonPill::EventType, PassAway);
        }
    }

    void Handle(TEvGetCloudByFolderRequest::TPtr& ev) {
        const auto folder = ev->Get()->FolderId;
        THolder<TEvGetCloudByFolderResponse> result = MakeHolder<TEvGetCloudByFolderResponse>();

        if (++RequestNumber % 3 == 0) {
            result->Status = NYdbGrpc::TGrpcStatus("Oops", grpc::StatusCode::INTERNAL, false);
        } else {
            if (folder != "FOLDER_alkonavt") {
                result->FolderId = folder;
                result->CloudId = TString("CLOUD_FOR_") + folder;
                result->Status = NYdbGrpc::TGrpcStatus("OK", grpc::StatusCode::OK, false);
            } else {
                result->Status = NYdbGrpc::TGrpcStatus("Auth error", grpc::StatusCode::UNAUTHENTICATED, false);
            }
        }

        Send(ev->Sender, result.Release());
    }
};

IActor* CreateSqsAccessService(const TString& address, const TString& pathToRootCA, bool enableV2Interface) {
    if (!address) {
        return new TSqsAccessServiceMock();
    }

    NCloud::TAccessServiceSettings settings;
    settings.Endpoint = address;
    settings.CertificateRootCA = TUnbufferedFileInput(pathToRootCA).ReadAll();

    return NCloud::CreateAccessServiceWithCache(settings, enableV2Interface);
}

IActor* CreateMockSqsFolderService() {
    return new TSqsFolderServiceMock();
}

} // namespace NKikimr::NSQS
