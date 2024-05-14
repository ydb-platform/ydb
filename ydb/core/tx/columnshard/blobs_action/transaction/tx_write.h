#pragma once
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>

namespace NKikimr::NColumnShard {

class TTxWrite : public NTabletFlatExecutor::TTransactionBase<TColumnShard> {
public:
    TTxWrite(TColumnShard* self, const TEvPrivate::TEvWriteBlobsResult::TPtr& putBlobResult)
        : NTabletFlatExecutor::TTransactionBase<TColumnShard>(self)
        , PutBlobResult(putBlobResult)
        , TabletTxNo(++Self->TabletTxCounter)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_WRITE; }


private:
    TEvPrivate::TEvWriteBlobsResult::TPtr PutBlobResult;
    const ui32 TabletTxNo;
    std::vector<std::unique_ptr<NActors::IEventBase>> Results;


    bool InsertOneBlob(TTransactionContext& txc, const NOlap::TWideSerializedBatch& batch, const TWriteId writeId);

    TStringBuilder TxPrefix() const {
        return TStringBuilder() << "TxWrite[" << ToString(TabletTxNo) << "] ";
    }

    TString TxSuffix() const {
        return TStringBuilder() << " at tablet " << Self->TabletID();
    }
};


}
