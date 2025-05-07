#include "node_broker_impl.h"
#include "node_broker__scheme.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/counters_node_broker.pb.h>

namespace NKikimr {
namespace NNodeBroker {

using namespace NKikimrNodeBroker;

class TNodeBroker::TTxRegisterNode : public TTransactionBase<TNodeBroker> {
public:
    TTxRegisterNode(TNodeBroker *self, TEvPrivate::TEvResolvedRegistrationRequest::TPtr &resolvedEv)
        : TBase(self)
        , Event(resolvedEv->Get()->Request)
        , ScopeId(resolvedEv->Get()->ScopeId)
        , ServicedSubDomain(resolvedEv->Get()->ServicedSubDomain)
        , NodeId(0)
        , ExtendLease(false)
        , FixNodeId(false)
        , SetLocation(false)
        , UpdateNodeAuthorizedByCertificate(false)
        , AllocateSlotIndex(false)
        , SlotIndexSubdomainChanged(false)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_REGISTER_NODE; }

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
        auto expire = rec.GetFixedNodeId() ? TInstant::Max() : Self->Dirty.Epoch.NextEnd;

        LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "TTxRegisterNode Execute");
        LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                    "Registration request from " << host << ":" << port << " "
                    << (rec.GetFixedNodeId() ? "(fixed)" : "(not fixed)") << " "
                    << "tenant: " << (rec.HasPath() ? rec.GetPath() : "<unspecified>"));

        TNodeLocation loc(rec.GetLocation());

        Response = new TEvNodeBroker::TEvRegistrationResponse;

        if (rec.HasPath() && ScopeId == NActors::TScopeId()) {
            return Error(TStatus::ERROR,
                         TStringBuilder() << "Failed to resolve the database by its path. Perhaps the database " << rec.GetPath() << " does not exist",
                         ctx);
        }

        if (Self->EnableStableNodeNames && rec.HasPath() && ServicedSubDomain == InvalidSubDomainKey) {
            return Error(TStatus::ERROR,
                         TStringBuilder() << "Cannot resolve subdomain key for path " << rec.GetPath(),
                         ctx);
        }

        // Already registered?
        auto it = Self->Dirty.Hosts.find(std::make_tuple(host, addr, port));
        if (it != Self->Dirty.Hosts.end()) {
            auto &node = Self->Dirty.Nodes.find(it->second)->second;
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
                Self->Dirty.DbUpdateNodeLocation(node, txc);
                SetLocation = true;
            }

            if (!node.IsFixed() && rec.GetFixedNodeId()) {
                Self->Dirty.DbFixNodeId(node, txc);
                Self->Dirty.FixNodeId(node);
                FixNodeId = true;
            } else if (!node.IsFixed() && node.Expire < expire) {
                Self->Dirty.DbUpdateNodeLease(node, txc);
                Self->Dirty.ExtendLease(node);
                ExtendLease = true;
            }
            if (node.AuthorizedByCertificate != rec.GetAuthorizedByCertificate()) {
                node.AuthorizedByCertificate = rec.GetAuthorizedByCertificate();
                Self->Dirty.DbUpdateNodeAuthorizedByCertificate(node, txc);
                UpdateNodeAuthorizedByCertificate = true;
            }

            if (Self->EnableStableNodeNames) {
                if (ServicedSubDomain != node.ServicedSubDomain) {
                    if (node.SlotIndex.has_value()) {
                        Self->Dirty.SlotIndexesPools[node.ServicedSubDomain].Release(node.SlotIndex.value());
                    }
                    node.ServicedSubDomain = ServicedSubDomain;
                    node.SlotIndex = Self->Dirty.SlotIndexesPools[node.ServicedSubDomain].AcquireLowestFreeIndex();
                    Self->Dirty.DbAddNode(node, txc);
                    SlotIndexSubdomainChanged = true;
                } else if (!node.SlotIndex.has_value()) {
                    node.SlotIndex = Self->Dirty.SlotIndexesPools[node.ServicedSubDomain].AcquireLowestFreeIndex();
                    Self->Dirty.DbAddNode(node, txc);
                    AllocateSlotIndex = true;
                }
            }

            Response->Record.MutableStatus()->SetCode(TStatus::OK);
            return true;
        }

        if (Self->Dirty.FreeIds.Empty())
            return Error(TStatus::ERROR_TEMP, "No free node IDs", ctx);

        NodeId = Self->Dirty.FreeIds.FirstNonZeroBit();

        Node = MakeHolder<TNodeInfo>(NodeId, rec.GetAddress(), host, rec.GetResolveHost(), port, loc);
        Node->AuthorizedByCertificate = rec.GetAuthorizedByCertificate();
        Node->Lease = 1;
        Node->Expire = expire;

        if (Self->EnableStableNodeNames) {
            Node->ServicedSubDomain = ServicedSubDomain;
            Node->SlotIndex = Self->Dirty.SlotIndexesPools[Node->ServicedSubDomain].AcquireLowestFreeIndex();
        }

        Response->Record.MutableStatus()->SetCode(TStatus::OK);

        Self->Dirty.DbAddNode(*Node, txc);
        Self->Dirty.AddNode(*Node);
        Self->Dirty.DbUpdateEpochVersion(Self->Dirty.Epoch.Version + 1, txc);
        Self->Dirty.UpdateEpochVersion();

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "TTxRegisterNode Complete");

        if (Node) {
            Self->Committed.AddNode(*Node);
            Self->Committed.UpdateEpochVersion();
            Self->AddNodeToEpochCache(*Node);
        } else if (ExtendLease)
            Self->Committed.ExtendLease(Self->Committed.Nodes.at(NodeId));
        else if (FixNodeId)
            Self->Committed.FixNodeId(Self->Committed.Nodes.at(NodeId));

        if (SetLocation) {
            Self->Committed.Nodes.at(NodeId).Location = TNodeLocation(Event->Get()->Record.GetLocation());
        }

        if (UpdateNodeAuthorizedByCertificate) {
            Self->Committed.Nodes.at(NodeId).AuthorizedByCertificate = Event->Get()->Record.GetAuthorizedByCertificate();
        }

        if (AllocateSlotIndex) {
            Self->Committed.Nodes.at(NodeId).SlotIndex = Self->Committed.SlotIndexesPools[ServicedSubDomain].AcquireLowestFreeIndex();
        } else if (SlotIndexSubdomainChanged) {
            auto& node = Self->Committed.Nodes.at(NodeId);
            if (node.SlotIndex.has_value()) {
                Self->Committed.SlotIndexesPools[node.ServicedSubDomain].Release(node.SlotIndex.value());
            }
            node.ServicedSubDomain = ServicedSubDomain;
            node.SlotIndex = Self->Committed.SlotIndexesPools[ServicedSubDomain].AcquireLowestFreeIndex();
        }

        Y_ABORT_UNLESS(Response);
        // With all modifications applied we may fill node info.
        if (Response->Record.GetStatus().GetCode() == TStatus::OK)
            Self->FillNodeInfo(Self->Committed.Nodes.at(NodeId), *Response->Record.MutableNode());
        LOG_TRACE_S(ctx, NKikimrServices::NODE_BROKER,
                    "TTxRegisterNode reply with: " << Response->Record.ShortDebugString());

        if (ScopeId != NActors::TScopeId()) {
            auto& record = Response->Record;
            record.SetScopeTabletId(ScopeId.first);
            record.SetScopePathId(ScopeId.second);
        }

        ctx.Send(Event->Sender, Response.Release());
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
    bool SetLocation;
    bool UpdateNodeAuthorizedByCertificate;
    bool AllocateSlotIndex;
    bool SlotIndexSubdomainChanged;
};

ITransaction *TNodeBroker::CreateTxRegisterNode(TEvPrivate::TEvResolvedRegistrationRequest::TPtr &ev)
{
    return new TTxRegisterNode(this, ev);
}

} // NNodeBroker
} // NKikimr
