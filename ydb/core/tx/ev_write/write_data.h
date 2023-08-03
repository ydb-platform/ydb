#pragma once

#include <ydb/core/tx/long_tx_service/public/types.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>



namespace NKikimr::NEvWrite {

class IDataContainer {
public:
    using TPtr = std::shared_ptr<IDataContainer>;
    virtual ~IDataContainer() {}
    virtual std::shared_ptr<arrow::RecordBatch> GetArrowBatch() const = 0;
    virtual const TString& GetData() const = 0;
};

class TWriteMeta {
    YDB_ACCESSOR(ui64, WriteId, 0);
    YDB_READONLY(ui64, TableId, 0);
    YDB_ACCESSOR_DEF(NActors::TActorId, Source);

    // Long Tx logic
    YDB_OPT(NLongTxService::TLongTxId, LongTxId);
    YDB_ACCESSOR(ui64, WritePartId, 0);
    YDB_ACCESSOR_DEF(TString, DedupId);

public:
    TWriteMeta(const ui64 writeId, const ui64 tableId, const NActors::TActorId& source)
        : WriteId(writeId)
        , TableId(tableId)
        , Source(source)
    {}
};

class TWriteData {
    TWriteMeta WriteMeta;
    IDataContainer::TPtr Data;
public:
    TWriteData(const TWriteMeta& writeMeta, IDataContainer::TPtr data);

    const IDataContainer& GetData() const {
        return *Data;
    }

    const IDataContainer::TPtr GetDataPtr() const {
        return Data;
    }

    const TWriteMeta& GetWriteMeta() const {
        return WriteMeta;
    }

    ui64 GetSize() const {
        return Data->GetData().size();
    }
};

}
