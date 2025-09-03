#include "hive_impl.h"

namespace NKikimr::NHive {

class TTxSetDown : public TTransactionBase<THive> {
public:
    const TNodeId NodeId;
    const bool Down;

    TTxSetDown(TNodeId nodeId, bool down, TSelf* hive)
        : TBase(hive)
        , NodeId(nodeId)
        , Down(down)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_SET_DOWN; }

    bool SetDown(NIceDb::TNiceDb& db) {
        TNodeInfo* node = Self->FindNode(NodeId);
        if (node != nullptr) {
            node->SetDown(Down);
            db.Table<Schema::Node>().Key(NodeId).Update<Schema::Node::Down, Schema::Node::BecomeUpOnRestart>(Down, false);
            return true;
        }
        return false;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        SetDown(db);
        return true;
    }

    void Complete(const TActorContext&) override {
        BLOG_D("THive::TTxSetDown(" << NodeId << ")::Complete");
    }
};

ITransaction* THive::CreateSetDown(TEvHive::TEvSetDown::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    return new TTxSetDown(record.GetNodeId(), record.GetDown(), this);
}

} // NKikimr::NHive
