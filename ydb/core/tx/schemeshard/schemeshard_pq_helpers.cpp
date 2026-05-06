#include <ydb/core/tx/schemeshard/schemeshard_pq_helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard_audit_log.h>
#include <ydb/core/persqueue/public/cloud_events/cloud_events.h>

namespace NKikimr::NSchemeShard {

TPath DatabasePathFromModifySchemeOperation(
    TSchemeShard* ss,
    const NKikimrSchemeOp::TModifyScheme& operation);

std::tuple<TString, TString, TString> GetDatabaseCloudIds(const TPath& databasePath);

namespace {

bool BuildTopicCloudEventInfo(
    const NKikimrSchemeOp::TModifyScheme& operation,
    TSchemeShard* ss,
    NKikimrScheme::EStatus status,
    const TString& reason,
    const TString& userSID,
    const TString& peerName,
    NPQ::NCloudEvents::TCloudEventInfo& info)
{
    const TString workingDir = operation.GetWorkingDir();
    TString name;
    if (operation.HasCreatePersQueueGroup()) {
        name = operation.GetCreatePersQueueGroup().GetName();
    } else if (operation.HasAlterPersQueueGroup()) {
        name = operation.GetAlterPersQueueGroup().GetName();
    } else if (operation.HasDrop()) {
        name = operation.GetDrop().GetName();
    } else {
        return false;
    }

    info.TopicPath = workingDir.empty() ? name : workingDir + "/" + name;
    const auto parentPath = TPath::Resolve(workingDir, ss);
    if (parentPath.IsResolved() && parentPath.Base()->IsCdcStream()) {
        info.TopicPath = workingDir;
    }

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
    info.CreatedAt = TInstant::Now();
    info.ModifyScheme = operation;
    info.OperationStatus = status;

    return true;
}

bool BuildTopicCloudEventInfo(
    const NKikimrSchemeOp::TModifyScheme& operation,
    TOperationContext& context,
    NKikimrScheme::EStatus status,
    const TString& reason,
    const TString& userSID,
    const TString& peerName,
    NPQ::NCloudEvents::TCloudEventInfo& info)
{
    return BuildTopicCloudEventInfo(
        operation,
        context.SS,
        status,
        reason,
        userSID,
        peerName,
        info);
}

} // anonymous namespace

void FinishWithError(
    TProposeResponse* result,
    const NKikimrSchemeOp::TModifyScheme& operation,
    NKikimrScheme::EStatus status,
    const TString& errStr,
    TOperationContext& context)
{
    result->SetError(status, errStr);

    NPQ::NCloudEvents::TCloudEventInfo info;
    if (!BuildTopicCloudEventInfo(operation, context, status, errStr, TString(), TString(), info)) {
        return;
    }

    // FinishWithError is used for early Propose-time rejects of topic requests.
    // The operation does not proceed to the normal completion path in these cases,
    // so the error cloud event is sent immediately instead of via context.OnComplete.
    auto actorId = context.Ctx.Register(NPQ::NCloudEvents::CreateCloudEventActor());
    context.Ctx.Send(actorId, new NPQ::NCloudEvents::TCloudEvent(std::move(info)));
}

void ScheduleSendTopicCloudEvent(
    const NKikimrSchemeOp::TModifyScheme& operation,
    TOperationContext& context,
    NKikimrScheme::EStatus status,
    const TString& reason,
    const TString& userSID,
    const TString& peerName)
{
    NPQ::NCloudEvents::TCloudEventInfo info;
    if (!BuildTopicCloudEventInfo(operation, context, status, reason, userSID, peerName, info)) {
        LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::PERSQUEUE,
            "Failed to build topic cloud event info for operation: "
                << NKikimrSchemeOp::EOperationType_Name(operation.GetOperationType()));
        return;
    }

    auto actorId = context.Ctx.Register(NPQ::NCloudEvents::CreateCloudEventActor());

    context.OnComplete.Send(
        actorId,
        new NPQ::NCloudEvents::TCloudEvent(std::move(info)));
}

void SendTopicCloudEvent(
    const NKikimrSchemeOp::TModifyScheme& operation,
    TSchemeShard* ss,
    const TActorContext& ctx,
    NKikimrScheme::EStatus status,
    const TString& reason,
    const TString& userSID,
    const TString& peerName)
{
    NPQ::NCloudEvents::TCloudEventInfo info;
    if (!BuildTopicCloudEventInfo(operation, ss, status, reason, userSID, peerName, info)) {
        LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::PERSQUEUE,
            "Failed to build topic cloud event info for operation: "
                << NKikimrSchemeOp::EOperationType_Name(operation.GetOperationType()));
        return;
    }

    auto actorId = ctx.Register(NPQ::NCloudEvents::CreateCloudEventActor());

    ctx.Send(actorId, new NPQ::NCloudEvents::TCloudEvent(std::move(info)));
}

} // NKikimr::NSchemeShard
