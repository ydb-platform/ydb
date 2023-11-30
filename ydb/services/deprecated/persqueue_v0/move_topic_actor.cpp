#include "move_topic_actor.h"
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/cms/console/util.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <google/protobuf/util/message_differencer.h>

using namespace NKikimrSchemeOp;
using namespace NKikimr::NSchemeShard;

namespace NKikimr {
namespace NGRpcService {
using namespace NPersQueue;

std::pair<TString, TString> SplitPathToDirAndName (TStringBuf path) {
    TStringBuf fullPath(path);
    TStringBuf dir, name;
    auto res = fullPath.TryRSplit("/", dir, name);
    if (!res || dir.empty() || name.empty()) {
        return {};
    } else {
        return {TString(dir), TString(name)};
    }
}

#define RESET_DOUBLE(name, value)      \
    first.Set##name(value);            \
    second.Set##name(value);           \

#define RESET_DOUBLE_OPT(proto, name, value)   \
    first##proto->Set##name(value);            \
    second##proto->Set##name(value);           \


bool ComparePQDescriptions(NKikimrSchemeOp::TPersQueueGroupDescription first,
                           NKikimrSchemeOp::TPersQueueGroupDescription second) {

    RESET_DOUBLE(Name, "");
    RESET_DOUBLE(PathId, 0);
    RESET_DOUBLE(AlterVersion, 0);

    auto* firstTabletConfig = first.MutablePQTabletConfig();
    auto* secondTabletConfig = second.MutablePQTabletConfig();
    RESET_DOUBLE_OPT(TabletConfig, TopicName, "");
    RESET_DOUBLE_OPT(TabletConfig, Version, 0);
    RESET_DOUBLE_OPT(TabletConfig, YdbDatabaseId, "");
    RESET_DOUBLE_OPT(TabletConfig, YdbDatabasePath, "");

    NKikimrSchemeOp::TPersQueueGroupAllocate emptyAllocate{};
    first.MutableAllocate()->CopyFrom(emptyAllocate);
    second.MutableAllocate()->CopyFrom(emptyAllocate);
    return google::protobuf::util::MessageDifferencer::Equals(first, second);
}

#undef RESET_DOUBLE
#undef RESET_DOUBLE_OPT

TMoveTopicActor::TMoveTopicActor(NYdbGrpc::IRequestContextBase* request)
    : TxPending(E_UNDEFINED)
    , RequestCtx(request)
{
    const auto* req = GetProtoRequest();
    SrcPath = req->source_path();
    DstPath = req->destination_path();
}

const TMoveTopicRequest* TMoveTopicActor::GetProtoRequest() {
    auto request = dynamic_cast<const TMoveTopicRequest*>(RequestCtx->GetRequest());
    Y_ABORT_UNLESS(request != nullptr);
    return request;
}

void TMoveTopicActor::Bootstrap(const NActors::TActorContext& ctx) {
    Become(&TThis::StateWork);
    if (SrcPath == DstPath) {
        Reply(Ydb::StatusIds::BAD_REQUEST,
              TStringBuilder() << "Source and destination path are equal in request: '"
                               << GetProtoRequest()->GetSourcePath() << "' and '"
                               << GetProtoRequest()->GetDestinationPath() << "'");
    }
    SendAclRequest(ctx);
}

void TMoveTopicActor::SendDescribeRequest(const TString& path, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_MOVE_TOPIC, "Describe path: " << path);

    std::unique_ptr<TEvTxUserProxy::TEvNavigate> navigateRequest(new TEvTxUserProxy::TEvNavigate());
    NKikimrSchemeOp::TDescribePath* record = navigateRequest->Record.MutableDescribePath();
    record->SetPath(path);
    ctx.Send(MakeTxProxyID(), navigateRequest.release());
}

void TMoveTopicActor::HandleDescribeResponse(
        TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext& ctx
) {

    auto& record = ev->Get()->GetRecord();
    const auto& path = record.GetPath();

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_MOVE_TOPIC, "Got describe path result for path: " << path);
    bool isSrc;
    if (path == SrcPath) {
        isSrc = true;
    } else if (path == DstPath) {
        isSrc = false;
    } else {
        Verify(false, TStringBuilder() << "Got DescribeResponseNo for unknown path: " << path, ctx);
        return;
    }
    auto& exists = isSrc ? SrcExists : DstExists;
    const auto status = record.GetStatus();
    switch (status) {
        case NKikimrScheme::StatusSuccess:
        {
            exists = true;
            if (isSrc) {
                SrcDescription = ev;
            } else {
                DstDescription = ev;
            }
            if (!record.GetPathDescription().HasPersQueueGroup()) {
                Reply(Ydb::StatusIds::BAD_REQUEST,
                      TStringBuilder() << "Path exists, but it is not a topic: " << path);
                return;
            }
            if (!isSrc) {
                break;
            }
            auto& pqDescr = record.GetPathDescription().GetPersQueueGroup();
            if (!pqDescr.HasAllocate() || !pqDescr.GetAllocate().HasBalancerTabletID()
                                       || pqDescr.GetAllocate().GetBalancerTabletID() == 0) {
                Reply(Ydb::StatusIds::UNSUPPORTED,
                      TStringBuilder() << "Could not get PQAllocate for topic: " << path);
                return;
            }
            if (pqDescr.GetAllocate().GetPQTabletConfig().GetFederationAccount().empty()) {
                Reply(Ydb::StatusIds::PRECONDITION_FAILED,
                      TStringBuilder() << "Cannot move topic with no federation account specified: " << path);

                return;
            }
            if (pqDescr.GetAllocate().GetAlterVersion() != pqDescr.GetAlterVersion()) {
                Reply(Ydb::StatusIds::INTERNAL_ERROR,
                      TStringBuilder() << "Pq allocate alter version mismatch for path: " << path);

            }
            break;
        }
        case NKikimrScheme::StatusPathDoesNotExist:
        case NKikimrScheme::StatusSchemeError:
            exists = false;
            break;
        default:
            Reply(Ydb::StatusIds::GENERIC_ERROR, TStringBuilder() << "Failed to describe path: " << path);
            return;
    }
    if (SrcExists.Defined() && DstExists.Defined()) {
        return ProcessDescriptions(ctx);
    }
}

void TMoveTopicActor::ProcessDescriptions(const TActorContext& ctx) {
    if (*SrcExists) {
        if (!Verify(SrcDescription != nullptr,
                    TStringBuilder() << "No src path description when it exists: " << SrcPath, ctx))
            return;

        auto& srcPqDescr = SrcDescription->Get()->GetRecord().GetPathDescription().GetPersQueueGroup();
        if (*DstExists) {
            if (!Verify(DstDescription != nullptr,
                        TStringBuilder() << "No dst path description when it exists: " << DstPath, ctx))
                return;

            auto& dstPqDescr = DstDescription->Get()->GetRecord().GetPathDescription().GetPersQueueGroup();
            if (!GetProtoRequest()->GetSkipDestinationCheck() && !ComparePQDescriptions(srcPqDescr, dstPqDescr)) {
                Reply(Ydb::StatusIds::BAD_REQUEST,
                      TStringBuilder() << "Both source and destination exist but different: " << DstPath);
                return;
            } else {
                /** Destination already exist, deallocate source */
                return SendDeallocateRequest(ctx);
            }
        } else {
            return SendAllocateRequest(srcPqDescr.GetAllocate(), ctx);
        }
    } else {
        if (*DstExists) {
            /** Source doesn't exist, destination exists. Suppose already moved */
            Reply(Ydb::StatusIds::ALREADY_EXISTS);
            return;
        } else {
            Reply(Ydb::StatusIds::NOT_FOUND, TStringBuilder() << "Source path " << SrcPath << " doesn't exist");
            return;
        }
    }
}

void TMoveTopicActor::SendAllocateRequest(
        const TPersQueueGroupAllocate& pqAllocate, const TActorContext& ctx
) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_MOVE_TOPIC, "Send allocate PQ Group on path: " << DstPath);

    TxPending = E_ALLOCATE;
    std::unique_ptr <TEvTxUserProxy::TEvProposeTransaction> proposal(new TEvTxUserProxy::TEvProposeTransaction());

    auto pathPair = SplitPathToDirAndName(DstPath);
    if (pathPair.first.empty()) {
        Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Bad destination path: " << DstPath);
        return;

    }
    auto* modifyScheme = proposal->Record.MutableTransaction()->MutableModifyScheme();
    modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAllocatePersQueueGroup);
    auto* allocate = modifyScheme->MutableAllocatePersQueueGroup();
    allocate->CopyFrom(pqAllocate);

    modifyScheme->SetWorkingDir(pathPair.first);
    allocate->SetName(pathPair.second);

    ctx.Send(MakeTxProxyID(), proposal.release());
}

void TMoveTopicActor::SendDeallocateRequest(const TActorContext& ctx) {
    if (GetProtoRequest()->GetDoNotDeallocate()) {
        Reply(Ydb::StatusIds::SUCCESS);
        return;
    }
    TxPending = E_DEALLOCATE;
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_MOVE_TOPIC, "Send deallocate PQ Group on path: " << SrcPath);

    auto pathPair = SplitPathToDirAndName(SrcPath);
    if (pathPair.first.empty()) {
        Reply(Ydb::StatusIds::BAD_REQUEST,
              TStringBuilder() << "Bad source path: " << SrcPath);
        return;
    }

    std::unique_ptr <TEvTxUserProxy::TEvProposeTransaction> proposal(new TEvTxUserProxy::TEvProposeTransaction());
    auto* modifyScheme = proposal->Record.MutableTransaction()->MutableModifyScheme();
    modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDeallocatePersQueueGroup);
    modifyScheme->SetWorkingDir(pathPair.first);
    auto* deallocate = modifyScheme->MutableDeallocatePersQueueGroup();
    deallocate->SetName(pathPair.second);
    ctx.Send(MakeTxProxyID(), proposal.release());
}

void TMoveTopicActor::HandleProposeStatus(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev,
                                         const TActorContext& ctx) {
    auto& rec = ev->Get()->Record;
    switch (rec.GetStatus()) {
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete:
            return OnTxComplete(rec.GetTxId(), ctx);
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress:
            TxId = rec.GetTxId();
            SchemeShardTabletId = rec.GetSchemeShardTabletId();
            SendNotifyRequest(ctx);
            break;
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError:
            switch (TxPending) {
                case E_DEALLOCATE:
                // Check if removal finished or in-progress.
                    Reply(Ydb::StatusIds::GENERIC_ERROR,
                          TStringBuilder() << "Source path deallocate failed: " << SrcPath << " with status: "
                                          << ev->Get()->Record.DebugString());
                    return;
                case E_ALLOCATE:
                    Reply(Ydb::StatusIds::BAD_REQUEST,
                          TStringBuilder() << "Cannot allocate path: " << DstPath);
                    return;
                default:
                    Verify(false, "Unknown TxState (HandleProposeStatus)", ctx);
                    return;
            }
        default:
        {
            Reply(Ydb::StatusIds::GENERIC_ERROR,
                  TStringBuilder() << "Scheme shard tx failed");
            return;
        }
    }
}

void TMoveTopicActor::HandleTxComplete(TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_MOVE_TOPIC, "Got TEvNotifyTxCompletionResult: "
            << ev->Get()->Record.ShortDebugString());

    OnTxComplete(ev->Get()->Record.GetTxId(), ctx);
}

void TMoveTopicActor::OnTxComplete(ui64 txId, const TActorContext& ctx) {
    if (!Verify(txId, "Empty tx id on TxCompleteHandler.", ctx))
        return;

    switch (TxPending) {
        case E_ALLOCATE:
            if (!Verify(!AllocateTxId, "Non-empty AllocateTxId on first TxComplete Handler.", ctx)) return;
            AllocateTxId = txId;
            return SendDeallocateRequest(ctx);

        case E_DEALLOCATE:
            if (txId != AllocateTxId) {
                Reply(Ydb::StatusIds::SUCCESS);
            } else {
                LOG_WARN_S(ctx, NKikimrServices::PQ_MOVE_TOPIC, "Duplicate completion for TxId: " << txId << ", ignored");
            }
            return;
        default:
            Verify(false, "Unknown TxState - OnTxComplete", ctx);
            return;
    }
}

void TMoveTopicActor::SendNotifyRequest(const TActorContext &ctx) {
    if (!Pipe)
        OpenPipe(ctx);

    auto request = MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>();
    request->Record.SetTxId(TxId);

    NTabletPipe::SendData(ctx, Pipe, request.Release());
}

void TMoveTopicActor::OpenPipe(const TActorContext &ctx)
{
    if (!Verify(SchemeShardTabletId, "Empty SchemeShardTabletId", ctx)) return;
    NTabletPipe::TClientConfig pipeConfig;
    pipeConfig.RetryPolicy = NConsole::FastConnectRetryPolicy();
    auto pipe = NTabletPipe::CreateClient(ctx.SelfID, SchemeShardTabletId, pipeConfig);
    Pipe = ctx.ExecutorThread.RegisterActor(pipe);
}

void TMoveTopicActor::OnPipeDestroyed(const TActorContext &ctx)
{
    if (Pipe) {
        NTabletPipe::CloseClient(ctx, Pipe);
        Pipe = TActorId();
    }
    SendNotifyRequest(ctx);
}

void TMoveTopicActor::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
    if (ev->Get()->Status != NKikimrProto::OK) {
        OnPipeDestroyed(ctx);
    }
}

void TMoveTopicActor::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&, const TActorContext& ctx) {
    OnPipeDestroyed(ctx);
}

void TMoveTopicActor::Reply(const Ydb::StatusIds::StatusCode status, const TString& error) {
    TMoveTopicResponse response;
    response.set_status(status);
    if (error) {
        auto* issue = response.mutable_issues()->Add();
        //google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> issues;
        //auto& issue = *issues.Add();
        issue->set_severity(NYql::TSeverityIds::S_ERROR);
        issue->set_message(error);
    }
    RequestCtx->Reply(&response);
    PassAway();
    //Reply(status, ErrorToIssues(error));
}

bool TMoveTopicActor::Verify(bool condition, const TString& error, const TActorContext& ctx) {
    if (condition) {
        return true;
    }
    LOG_ALERT_S(ctx, NKikimrServices::PQ_MOVE_TOPIC, error << "(THIS IS A BUG)");
    Reply(Ydb::StatusIds::INTERNAL_ERROR,
          TStringBuilder() << error << " This is a bug");
    return false;

}

// Auth
void TMoveTopicActor::SendAclRequest(const TActorContext& ctx) {
    const auto req = GetProtoRequest();
    TString ticket = req->token();
    ctx.Send(MakeTicketParserID(), new TEvTicketParser::TEvAuthorizeTicket(ticket));
}

void TMoveTopicActor::HandleAclResponse(TEvTicketParser::TEvAuthorizeTicketResult::TPtr& ev, const TActorContext& ctx) {
    TString ticket = ev->Get()->Ticket;
    TString maskedTicket = ticket.size() > 5 ? (ticket.substr(0, 5) + "***" + ticket.substr(ticket.size() - 5)) : "***";
    LOG_INFO_S(ctx, NKikimrServices::PQ_MOVE_TOPIC, "CheckACL ticket " << maskedTicket << " got result from TICKET_PARSER response: error: "
                                                                        << ev->Get()->Error << " user: "
                                                                        << (ev->Get()->Error.empty() ? ev->Get()->Token->GetUserSID() : ""));

    if (!ev->Get()->Error.empty()) {
        Reply(Ydb::StatusIds::UNAUTHORIZED,
              TStringBuilder() << "Ticket parsing error: " << ev->Get()->Error);
        return;
    }
    auto sid = ev->Get()->Token->GetUserSID();
    auto& pqConfig = AppData(ctx)->PQConfig;
    bool authRes = false;
    if (pqConfig.HasMoveTopicActorConfig()) {
        for (auto& allowed : pqConfig.GetMoveTopicActorConfig().GetAllowedUserSIDs()) {
            if (allowed == sid) {
                authRes = true;
                break;
            }
        }
    }
    if (!authRes) {
        Reply(Ydb::StatusIds::UNAUTHORIZED,
              TStringBuilder() << "User: " << sid << " is not authorized to make this request");
        return;
    }
    SendDescribeRequest(SrcPath, ctx);
    SendDescribeRequest(DstPath, ctx);
}


} //namespace NGRpcProxy
} // namespace NKikimr
