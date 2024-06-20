#include "node_broker_impl.h"
#include "node_broker__scheme.h"

#include <ydb/core/base/appdata.h>

namespace NKikimr {
namespace NNodeBroker {

using namespace NKikimrNodeBroker;

class TNodeBroker::TTxRegisterNode : public TTransactionBase<TNodeBroker> {
public:
    TTxRegisterNode(TNodeBroker *self, TEvNodeBroker::TEvRegistrationRequest::TPtr &ev,
                    const NActors::TScopeId& scopeId, const TSubDomainKey& servicedSubDomain)
        : TBase(self)
        , Event(ev)
        , ScopeId(scopeId)
        , ServicedSubDomain(servicedSubDomain)
        , NodeId(0)
        , ExtendLease(false)
        , FixNodeId(false)
    {
    }

    bool Error(TStatus::ECode code,
               const TString &reason,
               const TActorContext &ctx)
    {
        const auto &rec = Event->Get()->Record;
        auto host = rec.GetHost();
        auto port = rec.GetPort();
        LOG_ERROR_S(ctx, NKikimrServices::NODE_BROKER,
                    "Cannot register node " << host << ":" << port << ": " << code << ": " << reason);

        Response->Record.MutableStatus()->SetCode(code);
        Response->Record.MutableStatus()->SetReason(reason);

        return true;
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        auto &rec = Event->Get()->Record;
        auto host = rec.GetHost();
        ui16 port = (ui16)rec.GetPort();
        TString addr = rec.GetAddress();
        auto expire = rec.GetFixedNodeId() ? TInstant::Max() : Self->Epoch.NextEnd;

        LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "TTxRegisterNode Execute");
        LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                    "Registration request from " << host << ":" << port << " "
                    << (rec.GetFixedNodeId() ? "(fixed)" : "(not fixed)") << " "
                    << "tenant: " << (rec.HasPath() ? rec.GetPath() : "<unspecified>"));

        TNodeLocation loc(rec.GetLocation());

        Response = new TEvNodeBroker::TEvRegistrationResponse;

        if (rec.HasPath() && ScopeId == NActors::TScopeId()) {
            return Error(TStatus::ERROR,
                         TStringBuilder() << "Cannot resolve scope id for path " << rec.GetPath(),
                         ctx);
        }

        if (Self->EnableStableNodeNames && rec.HasPath() && ServicedSubDomain == InvalidSubDomainKey) {
            return Error(TStatus::ERROR,
                         TStringBuilder() << "Cannot resolve subdomain key for path " << rec.GetPath(),
                         ctx);
        }

        // Already registered?
        auto it = Self->Hosts.find(std::make_tuple(host, addr, port));
        if (it != Self->Hosts.end()) {
            auto &node = Self->Nodes.find(it->second)->second;
            NodeId = node.NodeId;

            if (node.Address != rec.GetAddress()
                || node.ResolveHost != rec.GetResolveHost())
                return Error(TStatus::WRONG_REQUEST,
                             TStringBuilder() << "Another address is registered for "
                             << host << ":" << port,
                             ctx);

            if (node.Location != loc && node.Location != TNodeLocation()) {
                return Error(TStatus::WRONG_REQUEST,
                             TStringBuilder() << "Another location is registered for "
                             << host << ":" << port,
                             ctx);
            } else if (node.Location != loc) {
                node.Location = loc;
                Self->DbUpdateNodeLocation(node, txc);
            }

            if (!node.IsFixed() && rec.GetFixedNodeId()) {
                Self->DbFixNodeId(node, txc);
                FixNodeId = true;
            } else if (!node.IsFixed() && node.Expire < expire) {
                Self->DbUpdateNodeLease(node, txc);
                ExtendLease = true;
            }
            node.AuthorizedByCertificate = rec.GetAuthorizedByCertificate();
            
            if (Self->EnableStableNodeNames) {
                if (ServicedSubDomain != node.ServicedSubDomain) {
                    if (node.SlotIndex.has_value()) {
                        Self->SlotIndexesPools[node.ServicedSubDomain].Release(node.SlotIndex.value());
                    }
                    node.ServicedSubDomain = ServicedSubDomain;
                    node.SlotIndex = Self->SlotIndexesPools[node.ServicedSubDomain].AcquireLowestFreeIndex();
                    Self->DbAddNode(node, txc);
                } else if (!node.SlotIndex.has_value()) {    
                    node.SlotIndex = Self->SlotIndexesPools[node.ServicedSubDomain].AcquireLowestFreeIndex();
                    Self->DbAddNode(node, txc);
                }
            }
            
            Response->Record.MutableStatus()->SetCode(TStatus::OK);
            Self->FillNodeInfo(node, *Response->Record.MutableNode());

            return true;
        }

        if (Self->FreeIds.Empty())
            return Error(TStatus::ERROR_TEMP, "No free node IDs", ctx);

        NodeId = Self->FreeIds.FirstNonZeroBit();
        Self->FreeIds.Reset(NodeId);

        Node = MakeHolder<TNodeInfo>(NodeId, rec.GetAddress(), host, rec.GetResolveHost(), port, loc);
        Node->AuthorizedByCertificate = rec.GetAuthorizedByCertificate();
        Node->Lease = 1;
        Node->Expire = expire;

        if (Self->EnableStableNodeNames) {
            Node->ServicedSubDomain = ServicedSubDomain;
            Node->SlotIndex = Self->SlotIndexesPools[Node->ServicedSubDomain].AcquireLowestFreeIndex();
        }

        Response->Record.MutableStatus()->SetCode(TStatus::OK);

        Self->DbAddNode(*Node, txc);
        Self->DbUpdateEpochVersion(Self->Epoch.Version + 1, txc);

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "TTxRegisterNode Complete");

        if (Node) {
            Self->AddNode(*Node);
            Self->UpdateEpochVersion();
            Self->AddNodeToEpochCache(*Node);
        } else if (ExtendLease)
            Self->ExtendLease(Self->Nodes.at(NodeId));
        else if (FixNodeId)
            Self->FixNodeId(Self->Nodes.at(NodeId));

        Y_ABORT_UNLESS(Response);
        // With all modifications applied we may fill node info.
        if (Response->Record.GetStatus().GetCode() == TStatus::OK)
            Self->FillNodeInfo(Self->Nodes.at(NodeId), *Response->Record.MutableNode());
        LOG_TRACE_S(ctx, NKikimrServices::NODE_BROKER,
                    "TTxRegisterNode reply with: " << Response->Record.ShortDebugString());

        if (ScopeId != NActors::TScopeId()) {
            auto& record = Response->Record;
            record.SetScopeTabletId(ScopeId.first);
            record.SetScopePathId(ScopeId.second);
        }

        ctx.Send(Event->Sender, Response.Release());

        Self->TxCompleted(this, ctx);
    }

private:
    TEvNodeBroker::TEvRegistrationRequest::TPtr Event;
    const NActors::TScopeId ScopeId;
    const TSubDomainKey ServicedSubDomain;
    TAutoPtr<TEvNodeBroker::TEvRegistrationResponse> Response;
    THolder<TNodeInfo> Node;
    ui32 NodeId;
    bool ExtendLease;
    bool FixNodeId;
};

ITransaction *TNodeBroker::CreateTxRegisterNode(TEvNodeBroker::TEvRegistrationRequest::TPtr &ev,
                                                const NActors::TScopeId& scopeId,
                                                const TSubDomainKey& servicedSubDomain)
{
    return new TTxRegisterNode(this, ev, scopeId, servicedSubDomain);
}

} // NNodeBroker
} // NKikimr
