#pragma once
#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NOlap::NExport {
class TSession;
class TTxSaveCursor: public NColumnShard::TTransactionBase<NColumnShard::TColumnShard> {
private:
    using TBase = NColumnShard::TTransactionBase<NColumnShard::TColumnShard>;
    const TCursor Cursor;
    const TActorId ExportActorId;
    std::shared_ptr<TSession> Session;
public:
    TTxSaveCursor(NColumnShard::TColumnShard* self, const std::shared_ptr<TSession>& session, TCursor&& cursor, const TActorId& exportActorId)
        : TBase(self)
        , Cursor(std::move(cursor))
        , ExportActorId(exportActorId)
        , Session(session)
    {
    }

    bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return NColumnShard::TXTYPE_EXPORT_SAVE_CURSOR; }
};

}
