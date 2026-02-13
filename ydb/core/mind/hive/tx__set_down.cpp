#include "tx__set_down.h"

namespace NKikimr::NHive {


TTxSetDown::TTxSetDown(TNodeId nodeId, bool down, TSelf* hive, TActorId source, ui64 cookie, bool forward)
    : TBase(hive)
    , NodeId(nodeId)
    , Down(down)
    , Forward(forward)
    , Source(source)
    , Cookie(cookie)
{}

TTxType TTxSetDown::GetTxType() const { return NHive::TXTYPE_MON_SET_DOWN; }

bool TTxSetDown::SetDown(NIceDb::TNiceDb& db) {
    TNodeInfo* node = Self->FindNode(NodeId);
    if (node != nullptr) {
        node->SetDown(Down);
        db.Table<Schema::Node>().Key(NodeId).Update<Schema::Node::Down, Schema::Node::BecomeUpOnRestart>(Down, false);
        if (Forward) {
            auto tenantHive = Self->GetPipeToTenantHive(node);
            if (tenantHive) {
                SideEffects.Callback([source = Source, tablet = *tenantHive, node = NodeId] {
                    NTabletPipe::SendData(source, tablet, new TEvHive::TEvSetDown(node));
                });
            } else {
                SideEffects.Send(Source, new TEvHive::TEvSetDownReply(), 0, Cookie);
            }
        }
        return true;
    }
    return false;
}

bool TTxSetDown::Execute(TTransactionContext& txc, const TActorContext&) {
    SideEffects.Reset(Self->SelfId());
    NIceDb::TNiceDb db(txc.DB);
    if (!SetDown(db)) {
        auto reply = std::make_unique<TEvHive::TEvSetDownReply>();
        reply->Record.SetStatus(NKikimrProto::ERROR);
        SideEffects.Send(Source, reply.release());
    }
    return true;
}

void TTxSetDown::Complete(const TActorContext& ctx) {
    BLOG_D("THive::TTxSetDown(" << NodeId << ")::Complete");
    SideEffects.Complete(ctx);
}

ITransaction* THive::CreateSetDown(TEvHive::TEvSetDown::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    return new TTxSetDown(record.GetNodeId(), record.GetDown(), this, ev->Sender, ev->Cookie, true);
}

} // NKikimr::NHive
