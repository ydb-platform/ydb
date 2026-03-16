#include <ydb/core/tx/schemeshard/schemeshard_pq_helpers.h>
#include <ydb/core/persqueue/public/cloud_events/actor.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr::NSchemeShard {

NKikimrSchemeOp::TModifyScheme BuildCdcStreamModifyScheme(
    const TString& streamPath,
    NKikimrSchemeOp::EOperationType opType)
{
    NKikimrSchemeOp::TModifyScheme op;
    op.SetOperationType(opType);
    auto pos = streamPath.rfind('/');
    TString tableName;
    TString streamName;
    if (pos != TString::npos && pos > 0) {
        op.SetWorkingDir(streamPath.substr(0, pos));
        streamName = streamPath.substr(pos + 1);
        auto pos2 = streamPath.rfind('/', pos - 1);
        if (pos2 != TString::npos) {
            tableName = streamPath.substr(pos2 + 1, pos - pos2 - 1);
            op.SetWorkingDir(streamPath.substr(0, pos2));
        } else {
            tableName = streamPath.substr(0, pos);
            op.SetWorkingDir(TString());
        }
    } else {
        streamName = streamPath;
    }
    if (opType == NKikimrSchemeOp::ESchemeOpCreateCdcStream) {
        op.MutableCreateCdcStream()->SetTableName(tableName);
        op.MutableCreateCdcStream()->MutableStreamDescription()->SetName(streamName);
    } else if (opType == NKikimrSchemeOp::ESchemeOpAlterCdcStream) {
        op.MutableAlterCdcStream()->SetTableName(tableName);
        op.MutableAlterCdcStream()->SetStreamName(streamName);
    } else if (opType == NKikimrSchemeOp::ESchemeOpDropCdcStream) {
        op.MutableDropCdcStream()->SetTableName(tableName);
        op.MutableDropCdcStream()->AddStreamName(streamName);
    }
    return op;
}

void SendTopicCloudEvent(
    const NKikimrSchemeOp::TModifyScheme& operation,
    NKikimrScheme::EStatus status,
    const TString& reason,
    TSchemeShard* ss,
    const TString& peerName,
    const TString& userSID,
    const TString& maskedToken,
    [[maybe_unused]] ui64 txId)
{
    NPQ::NCloudEvents::TCloudEventInfo info;
    const TString workingDir = operation.GetWorkingDir();
    TString topicPath;
    if (operation.HasCreatePersQueueGroup()) {
        const auto name = operation.GetCreatePersQueueGroup().GetName();
        topicPath = workingDir.empty() ? name : workingDir + "/" + name;
    } else if (operation.HasAlterPersQueueGroup()) {
        const auto name = operation.GetAlterPersQueueGroup().GetName();
        topicPath = workingDir.empty() ? name : workingDir + "/" + name;
    } else if (operation.HasDrop()) {
        const auto name = operation.GetDrop().GetName();
        topicPath = workingDir.empty() ? name : workingDir + "/" + name;
    } else if (operation.HasCreateCdcStream()) {
        const auto& create = operation.GetCreateCdcStream();
        const auto streamName = create.HasStreamDescription() && create.GetStreamDescription().HasName()
            ? create.GetStreamDescription().GetName()
            : "stream";
        topicPath = workingDir.empty()
            ? create.GetTableName() + "/" + streamName
            : workingDir + "/" + create.GetTableName() + "/" + streamName;
    } else if (operation.HasAlterCdcStream()) {
        const auto& alter = operation.GetAlterCdcStream();
        topicPath = workingDir.empty()
            ? alter.GetTableName() + "/" + alter.GetStreamName()
            : workingDir + "/" + alter.GetTableName() + "/" + alter.GetStreamName();
    } else if (operation.HasDropCdcStream()) {
        const auto& drop = operation.GetDropCdcStream();
        const auto streamName = drop.StreamNameSize() > 0 ? drop.GetStreamName(0) : TString();
        topicPath = workingDir.empty()
            ? drop.GetTableName() + "/" + streamName
            : workingDir + "/" + drop.GetTableName() + "/" + streamName;
    } else {
        return;
    }

    info.TopicPath = topicPath;

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
    info.Issue = reason;
    info.CreatedAt = TInstant::Now();
    info.ModifyScheme = operation;
    info.OperationStatus = status;

    auto* sys = NActors::TActivationContext::ActorSystem();
    auto actorId = sys->Register(new NPQ::NCloudEvents::TCloudEventsActor());

    sys->Send(actorId, new NPQ::NCloudEvents::TCloudEvent(std::move(info)));
}

} // NKikimr::NSchemeShard