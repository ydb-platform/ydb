#include <ydb/core/tx/schemeshard/schemeshard_pq_helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/persqueue/public/cloud_events/actor.h>

namespace NKikimr::NSchemeShard {

void FinishWithError(
    TProposeResponse* result,
    const NKikimrSchemeOp::TModifyScheme& operation,
    NKikimrScheme::EStatus status,
    const TString& errStr,
    TOperationContext& context)
{
    result->SetError(status, errStr);
    SendTopicCloudEvent(
        operation,
        status,
        errStr,
        context.SS,
        context.PeerName,
        context.UserToken ? context.UserToken->GetUserSID() : TString(),
        context.UserAgent);
}

void SendTopicCloudEvent(
    const NKikimrSchemeOp::TModifyScheme& operation,
    NKikimrScheme::EStatus status,
    const TString& reason,
    TSchemeShard* ss,
    const TString& peerName,
    const TString& userSID,
    const TString& userAgent)
{
    NPQ::NCloudEvents::TCloudEventInfo info;
    const TString workingDir = operation.GetWorkingDir();
    TString name;
    if (operation.HasCreatePersQueueGroup()) {
        name = operation.GetCreatePersQueueGroup().GetName();
    } else if (operation.HasAlterPersQueueGroup()) {
        name = operation.GetAlterPersQueueGroup().GetName();
    } else if (operation.HasDrop()) {
        name = operation.GetDrop().GetName();
    } else {
        return;
    }

    info.TopicPath = workingDir.empty() ? name : workingDir + "/" + name;

    // Cloud / folder / database
    TPath dbPath = DatabasePathFromModifySchemeOperation(ss, operation);
    if (!dbPath.IsEmpty()) {
        auto [cloudId, folderId, databaseId] = GetDatabaseCloudIds(dbPath);
        info.CloudId = cloudId;
        info.FolderId = folderId;
        info.DatabaseId = databaseId;
    }

    info.RemoteAddress = peerName;
    info.UserSID = userSID;
    info.Issue = reason;
    info.UserAgent = userAgent;
    info.CreatedAt = TInstant::Now();
    info.ModifyScheme = std::move(operation);
    info.OperationStatus = status;

    auto* sys = NActors::TActivationContext::ActorSystem();
    auto actorId = sys->Register(new NPQ::NCloudEvents::TCloudEventsActor());

    sys->Send(actorId, new NPQ::NCloudEvents::TCloudEvent(std::move(info)));
}

} // NKikimr::NSchemeShard