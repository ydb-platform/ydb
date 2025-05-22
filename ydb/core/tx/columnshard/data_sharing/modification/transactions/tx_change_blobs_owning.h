#pragma once
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>
#include <ydb/core/tx/columnshard/tablet/ext_tx_base.h>

namespace NKikimr::NOlap::NDataSharing {

class TTaskForTablet;

class TTxApplyLinksModification: public NColumnShard::TExtendedTransactionBase {
private:
    using TBase = NColumnShard::TExtendedTransactionBase;
    std::shared_ptr<TTaskForTablet> Task;
    const TTabletId InitiatorTabletId;
    const TString SessionId;
    const ui64 PackIdx;
    bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override;
    void DoComplete(const TActorContext& ctx) override;
public:
    TTxApplyLinksModification(NColumnShard::TColumnShard* self, const std::shared_ptr<TTaskForTablet>& task, const TString& sessionId, const TTabletId initiatorTabletId, const ui64 packIdx)
        : TBase(self)
        , Task(task)
        , InitiatorTabletId(initiatorTabletId)
        , SessionId(sessionId)
        , PackIdx(packIdx)
    {
        AFL_VERIFY(!!Task);
    }

    TTxType GetTxType() const override { return NColumnShard::TXTYPE_DATA_SHARING_APPLY_LINKS_MODIFICATION; }
};


}
