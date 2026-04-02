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
    TOperationContext& context,
    NKikimrScheme::EStatus status,
    const TString& reason,
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

    // Cloud / folder / database
    TPath dbPath = DatabasePathFromModifySchemeOperation(context.SS, operation);
    if (!dbPath.IsEmpty()) {
        auto [cloudId, folderId, databaseId] = GetDatabaseCloudIds(dbPath);
        info.CloudId = cloudId;
        info.FolderId = folderId;
        info.DatabaseId = databaseId;
    }

    info.RemoteAddress = context.PeerName;
    info.UserSID = context.UserToken ? context.UserToken->GetUserSID() : TString();
    info.Issue = reason;
    info.CreatedAt = TInstant::Now();
    info.ModifyScheme = operation;
    info.OperationStatus = status;

    return true;
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
    if (!BuildTopicCloudEventInfo(operation, context, status, errStr, info)) {
        return;
    }

    auto* sys = NActors::TActivationContext::ActorSystem();
    auto actorId = sys->Register(NPQ::NCloudEvents::CreateCloudEventActor());
    sys->Send(actorId, new NPQ::NCloudEvents::TCloudEvent(std::move(info)));
}

void ScheduleSendTopicCloudEvent(
    const NKikimrSchemeOp::TModifyScheme& operation,
    TOperationContext& context,
    NKikimrScheme::EStatus status,
    const TString& reason)
{
    NPQ::NCloudEvents::TCloudEventInfo info;
    if (!BuildTopicCloudEventInfo(operation, context, status, reason, info)) {
        return;
    }

    auto* sys = NActors::TActivationContext::ActorSystem();
    auto actorId = sys->Register(NPQ::NCloudEvents::CreateCloudEventActor());

    context.OnComplete.Send(
        actorId,
        new NPQ::NCloudEvents::TCloudEvent(std::move(info)));
}

TPQDoneWithCloudEvents::TPQDoneWithCloudEvents(const TOperationId& id, const TTxTransaction& tx)
    : TDone(id)
    , Transaction(tx)
{
    auto events = AllIncomingEvents();
    events.erase(TEvPrivate::TEvCompleteBarrier::EventType);
    IgnoreMessages(DebugHint(), events);
}

bool TPQDoneWithCloudEvents::ProgressState(TOperationContext& context)
{
    ScheduleSendTopicCloudEvent(
        Transaction,
        context,
        NKikimrScheme::StatusSuccess,
        TString());

    TDone::ProgressState(context);

    return false;
}

} // NKikimr::NSchemeShard
