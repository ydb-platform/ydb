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
    TTxSetDown(TNodeId nodeId, bool down, TSelf* hive, TActorId source, ui64 cookie = 0, bool forward = false);
    TTxType GetTxType() const override;
    bool SetDown(NIceDb::TNiceDb& db);
    bool Execute(TTransactionContext& txc, const TActorContext&) override;
    void Complete(const TActorContext& ctx) override;
};

} // NKikimr::NHive
