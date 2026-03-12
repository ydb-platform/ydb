#include <ydb/core/tx/schemeshard/schemeshard_pq_helpers.h> 
#include <ydb/core/persqueue/public/cloud_events/actor.h>

namespace NKikimr::NSchemeShard {

void SendTopicCloudEvent(
    const NKikimrSchemeOp::TModifyScheme& operation,
    NKikimrScheme::EStatus status,
    TSchemeShard* ss,
    const TString& peerName,
    const TString& userSID,
    const TString& maskedToken,
    ui64)
{
    NPQ::NCloudEvents::TCloudEventInfo info;
    const TString workingDir = operation.GetWorkingDir();
    TString name;
    if (operation.HasCreatePersQueueGroup()) {
        name = operation.GetCreatePersQueueGroup().GetName();
        info.OperationType = "CreateTopic";
    } else if (operation.HasAlterPersQueueGroup()) {
        name = operation.GetAlterPersQueueGroup().GetName();
        info.OperationType = "AlterTopic";
    } else if (operation.HasDeallocatePersQueueGroup()) {
        name = operation.GetDeallocatePersQueueGroup().GetName();
        info.OperationType = "DeleteTopic";
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
    info.MaskedToken = maskedToken;
    info.CreatedAt = TInstant::Now();
    info.ModifyScheme = std::move(operation);
    info.OperationStatus = status;

    auto* sys = NActors::TActivationContext::ActorSystem();
    auto actorId = sys->Register(new NPQ::NCloudEvents::TCloudEventsActor());

    sys->Send(actorId, new NPQ::NCloudEvents::TCloudEvent(std::move(info)));
}

} // NKikimr::NSchemeShard