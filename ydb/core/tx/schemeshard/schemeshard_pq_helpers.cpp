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

} // anonymous namespace

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

void SendTopicCloudEventIfNeeded(
    const NKikimrScheme::TEvModifySchemeTransaction& record,
    const NKikimrScheme::TEvModifySchemeTransactionResult& response,
    TSchemeShard* ss,
    const TString& peerName,
    const TString& userSID)
{
    const auto status = response.GetStatus();
    const bool isSuccess = status == NKikimrScheme::StatusSuccess || status == NKikimrScheme::StatusAccepted;
    const auto eventStatus = isSuccess ? NKikimrScheme::StatusSuccess : status;
    const TString reason = !isSuccess && response.HasReason() ? response.GetReason() : TString();

    const auto& ctx = NActors::TlsActivationContext->AsActorContext();
    const auto sendTopicCloudEvent = [ss, &ctx, eventStatus, &reason, &peerName, &userSID](const auto& transaction) {
        switch (transaction.GetOperationType()) {
            case NKikimrSchemeOp::EOperationType::ESchemeOpCreatePersQueueGroup:
            case NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup:
            case NKikimrSchemeOp::EOperationType::ESchemeOpDropPersQueueGroup:
                break;
            default:
                return;
        }

        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Sending topic cloud event for operation: "
                << NKikimrSchemeOp::EOperationType_Name(transaction.GetOperationType()));

        SendTopicCloudEvent(
            transaction,
            ss,
            ctx,
            eventStatus,
            reason,
            userSID,
            peerName);
    };

    const auto txId = TTxId(record.GetTxId());
    if (ss->Operations.contains(txId)) {
        for (const auto& part : ss->Operations.at(txId)->Parts) {
            sendTopicCloudEvent(part->GetTransaction());
        }
        return;
    }

    for (const auto& transaction : record.GetTransaction()) {
        sendTopicCloudEvent(transaction);
    }
}

} // NKikimr::NSchemeShard
