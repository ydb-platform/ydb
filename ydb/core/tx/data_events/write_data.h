#pragma once

#include <ydb/core/tx/long_tx_service/public/types.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/library/accessor/accessor.h>

#include <ydb/library/actors/core/monotonic.h>
#include <util/generic/guid.h>

namespace NKikimr::NOlap {
class IBlobsWritingAction;
}

namespace NKikimr::NEvWrite {

class IDataContainer {
public:
    using TPtr = std::shared_ptr<IDataContainer>;
    virtual ~IDataContainer() {}
    virtual std::shared_ptr<arrow::RecordBatch> ExtractBatch() = 0;
    virtual ui64 GetSchemaVersion() const = 0;
    virtual ui64 GetSize() const = 0;
};

class TWriteMeta {
    YDB_ACCESSOR(ui64, WriteId, 0);
    YDB_READONLY(ui64, TableId, 0);
    YDB_ACCESSOR_DEF(NActors::TActorId, Source);
    YDB_ACCESSOR_DEF(std::optional<ui32>, GranuleShardingVersion);

    // Long Tx logic
    YDB_OPT(NLongTxService::TLongTxId, LongTxId);
    YDB_ACCESSOR(ui64, WritePartId, 0);
    YDB_ACCESSOR_DEF(TString, DedupId);

    YDB_READONLY(TString, Id, TGUID::CreateTimebased().AsUuidString());
    YDB_READONLY(TMonotonic, WriteStartInstant, TMonotonic::Now());
    YDB_ACCESSOR(TMonotonic, WriteMiddle1StartInstant, TMonotonic::Now());
    YDB_ACCESSOR(TMonotonic, WriteMiddle2StartInstant, TMonotonic::Now());
    YDB_ACCESSOR(TMonotonic, WriteMiddle3StartInstant, TMonotonic::Now());
    YDB_ACCESSOR(TMonotonic, WriteMiddle4StartInstant, TMonotonic::Now());
    YDB_ACCESSOR(TMonotonic, WriteMiddle5StartInstant, TMonotonic::Now());
    YDB_ACCESSOR(TMonotonic, WriteMiddle6StartInstant, TMonotonic::Now());
public:
    TWriteMeta(const ui64 writeId, const ui64 tableId, const NActors::TActorId& source, const std::optional<ui32> granuleShardingVersion)
        : WriteId(writeId)
        , TableId(tableId)
        , Source(source)
        , GranuleShardingVersion(granuleShardingVersion)
    {}
};

class TWriteData {
private:
    TWriteMeta WriteMeta;
    YDB_READONLY_DEF(IDataContainer::TPtr, Data);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Schema>, PrimaryKeySchema);
    YDB_READONLY_DEF(std::shared_ptr<NOlap::IBlobsWritingAction>, BlobsAction);
public:
    TWriteData(const TWriteMeta& writeMeta, IDataContainer::TPtr data, const std::shared_ptr<arrow::Schema>& primaryKeySchema, const std::shared_ptr<NOlap::IBlobsWritingAction>& blobsAction);

    const IDataContainer& GetDataVerified() const;

    const TWriteMeta& GetWriteMeta() const {
        return WriteMeta;
    }

    TWriteMeta& MutableWriteMeta() {
        return WriteMeta;
    }

    ui64 GetSize() const {
        return Data->GetSize();
    }
};

}
