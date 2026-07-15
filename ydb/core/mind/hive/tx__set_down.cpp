#include "tx__set_down.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::HIVE

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
                SideEffects.Callback([source = Source, tablet = *tenantHive, node = NodeId, down = Down] {
                    NTabletPipe::SendData(source, tablet, new TEvHive::TEvSetDown(node, down));
                });
            } else {
                SideEffects.Send(Source, new TEvHive::TEvSetDownReply(), 0, Cookie);
            }
        }
        return true;
    }
    // setting Down = false on a non-existent node is considered a no-op and thus successful
    if (Down) {
        return false;
    } else {
        if (Forward) {
            SideEffects.Send(Source, new TEvHive::TEvSetDownReply(), 0, Cookie);
        }
        return true;
    }
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
<<<<<<< HEAD
    YDB_LOG_DEBUG("THive::TTxSetDown::Complete setting node down state",
        {"logPrefix", GetLogPrefix()},
        {"nodeId", NodeId},
        {"down", Down},
        {"sideEffects", SideEffects});
=======
        YDB_LOG_DEBUG("THive::TTxSetDown::Complete setting node down state",
            {"logPrefix", GetLogPrefix()},
            {"nodeId", NodeId},
            {"down", Down});
>>>>>>> b02c94bde0d ([YDB_LOG] Migrate ydb/core/mind/hive (#43618))
    SideEffects.Complete(ctx);
}

ITransaction* THive::CreateSetDown(TEvHive::TEvSetDown::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    return new TTxSetDown(record.GetNodeId(), record.GetDown(), this, ev->Sender, ev->Cookie, true);
}

} // NKikimr::NHive
