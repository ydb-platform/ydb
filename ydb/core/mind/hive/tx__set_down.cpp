#include "hive_impl.h"

namespace NKikimr::NHive {

class TTxSetDown : public TTransactionBase<THive> {
public:
    const TNodeId NodeId;
    const bool Down;
    TSideEffects SideEffects;
    bool Forward;
    const TActorId Source;
    const ui64 Cookie;

    TTxSetDown(TNodeId nodeId, bool down, TSelf* hive, TActorId source, ui64 cookie = 0, bool forward = false)
        : TBase(hive)
        , NodeId(nodeId)
        , Down(down)
        , Forward(forward)
        , Source(source)
        , Cookie(cookie)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_SET_DOWN; }

    bool SetDown(NIceDb::TNiceDb& db) {
        TNodeInfo* node = Self->FindNode(NodeId);
        if (node != nullptr) {
            node->SetDown(Down);
            db.Table<Schema::Node>().Key(NodeId).Update<Schema::Node::Down, Schema::Node::BecomeUpOnRestart>(Down, false);
            if (Forward) {
                auto tenantHive = Self->GetPipeToTenantHive(node);
                if (tenantHive) {
                    SideEffects.Callback([self = Self->SelfId(), tablet = *tenantHive, node = NodeId] {
                        NTabletPipe::SendData(self, tablet, new TEvHive::TEvSetDown(node));
                    });
                } else {
                    SideEffects.Send(Source, new TEvHive::TEvSetDownReply(), 0, Cookie);
                }
            }
            return true;
        }
        return false;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SideEffects.Reset(Self->SelfId());
        NIceDb::TNiceDb db(txc.DB);
        if (!SetDown(db)) {
            auto reply = std::make_unique<TEvHive::TEvSetDownReply>();
            reply->Record.SetStatus(NKikimrProto::ERROR);
            SideEffects.Send(Source, reply.release());
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        BLOG_D("THive::TTxSetDown(" << NodeId << ")::Complete");
        SideEffects.Complete(ctx);
    }
};

ITransaction* THive::CreateSetDown(TEvHive::TEvSetDown::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    return new TTxSetDown(record.GetNodeId(), record.GetDown(), this, ev->Sender, ev->Cookie, true);
}

} // NKikimr::NHive
