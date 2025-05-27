#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxDeleteNode : public TTransactionBase<THive> {
protected:
    TNodeId NodeId;
public:
    TTxDeleteNode(TNodeId nodeId, THive *hive)
        : TBase(hive)
        , NodeId(nodeId)
    {}

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Node>().Key(NodeId).Delete();
        auto restrictionsRowset = db.Table<Schema::TabletAvailabilityRestrictions>().Range(NodeId).Select();
        while (!restrictionsRowset.EndOfSet()) {
            db.Table<Schema::TabletAvailabilityRestrictions>().Key(restrictionsRowset.GetKey()).Delete();
            if (!restrictionsRowset.Next()) {
                return false;
            }
        }
        return true;
    }

    void Complete(const TActorContext&) override {
    }
 };

 ITransaction* THive::CreateDeleteNode(TNodeId nodeId) {
     return new TTxDeleteNode(nodeId, this);
 }

} // NHive
} // NKikimr
