#include "cloud_resolver.h"
#include "iam_actor_base.h"

#include <ydb/library/ycloud/api/folder_service.h>
#include <ydb/library/ycloud/api/service_account_service.h>
#include <ydb/library/ycloud/impl/folder_service.h>
#include <ydb/library/ycloud/impl/service_account_service.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::IAM_DELEGATION

namespace NKikimr::NIamDelegation {

using namespace NActors;

namespace {

// A client actor registered for one call: poisoned when the call ends, including when the caller's
// timeout cancels the call or the caller dies with the call in flight. The poison goes through the actor
// system pointer taken at construction: the frame may be destroyed without an activation context.
class TCallClient {
public:
    explicit TCallClient(IActor* client)
        : ActorSystem(TActivationContext::ActorSystem())
        , Id(TActivationContext::AsActorContext().RegisterWithSameMailbox(client))
    {}

    TCallClient(const TCallClient&) = delete;
    TCallClient& operator=(const TCallClient&) = delete;

    ~TCallClient() {
        ActorSystem->Send(new IEventHandle(Id, TActorId(), new TEvents::TEvPoison()));
    }

    TActorSystem* const ActorSystem;
    const TActorId Id;
};

} // namespace

async<TResolvedCloud> ResolveCloud(TIamDelegationSettings settings, TString userToken, TString serviceAccountId) {
    AFL_ENSURE(settings.CanResolveCloud())("serviceControlEndpoint", settings.ServiceControlEndpoint)("resourceManagerEndpoint", settings.ResourceManagerEndpoint);
    if (userToken.empty()) {
        throw TIamCallError(Ydb::StatusIds::UNAUTHORIZED) << "no user token to look service account " << serviceAccountId << " up with";
    }

    NCloud::TServiceAccountServiceSettings serviceAccountSettings(settings.ServiceControlEndpoint, "ydb-iam-delegation");
    serviceAccountSettings.EnableSsl = settings.EnableSsl;
    serviceAccountSettings.RequestTimeoutMs = settings.RequestTimeout.MilliSeconds();
    const TCallClient serviceAccountClient(NCloud::CreateServiceAccountService(serviceAccountSettings));

    NCloud::TFolderServiceSettings folderSettings;
    folderSettings.Endpoint = settings.ResourceManagerEndpoint;
    folderSettings.EnableSsl = settings.EnableSsl;
    folderSettings.RequestTimeoutMs = settings.RequestTimeout.MilliSeconds();
    const TCallClient folderClient(NCloud::CreateFolderService(folderSettings));

    const auto credentials = TIamCallCredentials::Token(userToken);
    TResolvedCloud resolved;

    const auto account = co_await IamCallWithRetry<NCloud::TEvServiceAccountService::TEvGetServiceAccountRequest, NCloud::TEvServiceAccountService::TEvGetServiceAccountResponse>(
        settings, credentials, serviceAccountClient.Id, "GetServiceAccount", [&serviceAccountId](auto& request) {
            request.set_service_account_id(serviceAccountId);
        });
    resolved.FolderId = account->Get()->Response.folder_id();
    if (resolved.FolderId.empty()) {
        throw TIamCallError(Ydb::StatusIds::INTERNAL_ERROR) << "GetServiceAccount returned no folder for " << serviceAccountId;
    }

    const auto folders = co_await IamCallWithRetry<NCloud::TEvFolderService::TEvResolveFoldersRequest, NCloud::TEvFolderService::TEvResolveFoldersResponse>(
        settings, credentials, folderClient.Id, "ResolveFolders", [&resolved](auto& request) {
            request.add_folder_ids(resolved.FolderId);
        });
    for (const auto& folder : folders->Get()->Response.resolved_folders()) {
        if (folder.id() == resolved.FolderId && !folder.cloud_id().empty()) {
            resolved.CloudId = folder.cloud_id();
        }
    }
    if (resolved.CloudId.empty()) {
        throw TIamCallError(Ydb::StatusIds::NOT_FOUND) << "ResolveFolders did not resolve folder " << resolved.FolderId << " of service account " << serviceAccountId;
    }

    YDB_LOG_INFO("Cloud of service account resolved",
        {"serviceAccountId", serviceAccountId},
        {"folderId", resolved.FolderId},
        {"cloudId", resolved.CloudId}
    );
    co_return resolved;
}

} // namespace NKikimr::NIamDelegation
