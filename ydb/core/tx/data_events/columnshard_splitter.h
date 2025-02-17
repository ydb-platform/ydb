#pragma once

#include "events.h"
#include "shards_splitter.h"
#include "payload_helper.h"

#include <ydb/core/tx/sharding/sharding.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/scheme/scheme_types_proto.h>

namespace NKikimr::NEvWrite {

class TColumnShardShardsSplitter: public IShardsSplitter {
    class TShardInfo: public IShardInfo {
    private:
        const TString SchemaData;
        const TString Data;
        const ui32 RowsCount;
        const ui32 GranuleShardingVersion;
    public:
        TShardInfo(const TString& schemaData, const TString& data, const ui32 rowsCount, const ui32 granuleShardingVersion)
            : SchemaData(schemaData)
            , Data(data)
            , RowsCount(rowsCount)
            , GranuleShardingVersion(granuleShardingVersion) {
        }

        virtual ui64 GetBytes() const override {
            return Data.size();
        }

        virtual ui32 GetRowsCount() const override {
            return RowsCount;
        }

        virtual const TString& GetData() const override {
            return Data;
        }

        virtual void Serialize(TEvColumnShard::TEvWrite& evWrite) const override {
            evWrite.SetArrowData(SchemaData, Data);
            evWrite.Record.SetGranuleShardingVersion(GranuleShardingVersion);
        }
        virtual void Serialize(NEvents::TDataEvents::TEvWrite& evWrite, const ui64 tableId, const ui64 schemaVersion) const override {
            TPayloadWriter<NEvents::TDataEvents::TEvWrite> writer(evWrite);
            TString data = Data;
            writer.AddDataToPayload(std::move(data));

            auto* operation = evWrite.Record.AddOperations();
            operation->SetPayloadSchema(SchemaData);
            operation->SetType(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE);
            operation->SetPayloadFormat(NKikimrDataEvents::FORMAT_ARROW);
            operation->SetPayloadIndex(0);
            operation->MutableTableId()->SetTableId(tableId);
            operation->MutableTableId()->SetSchemaVersion(schemaVersion);
        }
    };

private:
    TYdbConclusionStatus DoSplitData(const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry, const IEvWriteDataAccessor& data) override;

private:
    TYdbConclusionStatus SplitImpl(const std::shared_ptr<arrow::RecordBatch>& batch, const std::shared_ptr<NSharding::IShardingBase>& sharding);

    std::shared_ptr<arrow::Schema> ExtractArrowSchema(const NKikimrSchemeOp::TColumnTableSchema& schema);
};
}
