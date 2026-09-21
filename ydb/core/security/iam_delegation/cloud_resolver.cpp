#include "cloud_resolver.h"
#include "iam_actor_base.h"

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/ycloud/api/folder_service.h>
#include <ydb/library/ycloud/api/service_account_service.h>
#include <ydb/library/ycloud/impl/folder_service.h>
#include <ydb/library/ycloud/impl/service_account_service.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::IAM_DELEGATION

namespace NKikimr::NIamDelegation {

using namespace NActors;

// The calls are authorized with the user's token, which is what the base class obtains from its token
// source: a static source holding the user's token makes CallWithRetry send it as is.
class TCloudResolver : public TIamActorBase<TCloudResolver> {
private:
    using TBase = TIamActorBase<TCloudResolver>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::IAM_CLOUD_RESOLVER_ACTOR;
    }

    TCloudResolver(const TIamDelegationSettings& settings, const TString& userToken, const TString& serviceAccountId,
            const TActorId& replyTo, ui64 cookie)
        : TBase(settings, CreateStaticSystemTokenSource(userToken))
        , ServiceAccountId(serviceAccountId)
        , ReplyTo(replyTo)
        , Cookie(cookie)
        , HasUserToken(!userToken.empty())
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);

        if (!HasUserToken) {
            // nothing to authorize the lookups with: answered at once rather than retried as a missing system token
            auto result = std::make_unique<TEvIamDelegation::TEvResolveCloudResult>();
            result->ServiceAccountId = ServiceAccountId;
            result->Result = TDelegationResult::Error(Ydb::StatusIds::UNAUTHORIZED,
                TStringBuilder() << "no user token to look service account " << ServiceAccountId << " up with");
            Send(ReplyTo, result.release(), 0, Cookie);
            BeginShutdown();
            co_return;
        }

        {
            NCloud::TServiceAccountServiceSettings clientSettings(Settings.ServiceControlEndpoint, "ydb-iam-delegation");
            clientSettings.EnableSsl = Settings.EnableSsl;
            clientSettings.RequestTimeoutMs = Settings.RequestTimeout.MilliSeconds();
            ServiceAccountClient = RegisterWithSameMailbox(NCloud::CreateServiceAccountService(clientSettings));
        }
        {
            NCloud::TFolderServiceSettings clientSettings;
            clientSettings.Endpoint = Settings.ResourceManagerEndpoint;
            clientSettings.EnableSsl = Settings.EnableSsl;
            clientSettings.RequestTimeoutMs = Settings.RequestTimeout.MilliSeconds();
            FolderClient = RegisterWithSameMailbox(NCloud::CreateFolderService(clientSettings));
        }

        auto result = std::make_unique<TEvIamDelegation::TEvResolveCloudResult>();
        result->ServiceAccountId = ServiceAccountId;
        try {
            result->FolderId = co_await GetFolder();
            result->CloudId = co_await ResolveCloud(result->FolderId);
            YDB_LOG_INFO("Cloud of service account resolved",
                {"serviceAccountId", ServiceAccountId},
                {"folderId", result->FolderId},
                {"cloudId", result->CloudId}
            );
        } catch (const TIamCallError& e) {
            result->Result = TDelegationResult::Error(e.Status, e.what());
        } catch (const std::exception& e) {
            result->Result = TDelegationResult::Error(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "unexpected exception: " << e.what());
        }
        if (!result->Result.IsSuccess()) {
            YDB_LOG_WARN("Cloud of service account is unknown",
                {"serviceAccountId", ServiceAccountId},
                {"status", result->Result.Status},
                {"issues", result->Result.Issues.ToOneLineString()}
            );
        }
        Send(ReplyTo, result.release(), 0, Cookie);
        BeginShutdown();
    }

    STRICT_STFUNC(StateWork,
        // late replies after a timeout
        IgnoreFunc(NCloud::TEvServiceAccountService::TEvGetServiceAccountResponse);
        IgnoreFunc(NCloud::TEvFolderService::TEvResolveFoldersResponse);
        IgnoreFunc(TEvIamDelegation::TEvSystemTokenReady);
        IgnoreFunc(TEvents::TEvUndelivered);
        cFunc(TEvents::TEvPoison::EventType, BeginShutdown);
    )

    STFUNC(StateDying) {
        Y_UNUSED(ev); // PassAway may be waiting for coroutine tasks
    }

private:
    void BeginShutdown() {
        Become(&TThis::StateDying);
        Send(ServiceAccountClient, new TEvents::TEvPoison());
        Send(FolderClient, new TEvents::TEvPoison());
        PassAway();
    }

    async<TString> GetFolder() {
        auto response = co_await CallWithRetry<NCloud::TEvServiceAccountService::TEvGetServiceAccountRequest, NCloud::TEvServiceAccountService::TEvGetServiceAccountResponse>(
            ServiceAccountClient, "GetServiceAccount", [this](auto& request) {
                request.set_service_account_id(ServiceAccountId);
            });
        const TString folderId = response->Get()->Response.folder_id();
        if (folderId.empty()) {
            throw TIamCallError(Ydb::StatusIds::INTERNAL_ERROR) << "GetServiceAccount returned no folder for " << ServiceAccountId;
        }
        co_return folderId;
    }

    async<TString> ResolveCloud(TString folderId) {
        auto response = co_await CallWithRetry<NCloud::TEvFolderService::TEvResolveFoldersRequest, NCloud::TEvFolderService::TEvResolveFoldersResponse>(
            FolderClient, "ResolveFolders", [&folderId](auto& request) {
                request.add_folder_ids(folderId);
            });
        for (const auto& folder : response->Get()->Response.resolved_folders()) {
            if (folder.id() == folderId && !folder.cloud_id().empty()) {
                co_return folder.cloud_id();
            }
        }
        throw TIamCallError(Ydb::StatusIds::NOT_FOUND) << "ResolveFolders did not resolve folder " << folderId << " of service account " << ServiceAccountId;
    }

private:
    const TString ServiceAccountId;
    const TActorId ReplyTo;
    const ui64 Cookie;
    const bool HasUserToken;
    TActorId ServiceAccountClient;
    TActorId FolderClient;
};

IActor* CreateCloudResolver(const TIamDelegationSettings& settings, const TString& userToken, const TString& serviceAccountId,
    const TActorId& replyTo, ui64 cookie)
{
    AFL_ENSURE(settings.CanResolveCloud())("serviceControlEndpoint", settings.ServiceControlEndpoint)("resourceManagerEndpoint", settings.ResourceManagerEndpoint);
    return new TCloudResolver(settings, userToken, serviceAccountId, replyTo, cookie);
}

} // namespace NKikimr::NIamDelegation
