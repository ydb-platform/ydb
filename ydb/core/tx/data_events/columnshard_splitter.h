#pragma once

#include "shards_splitter.h"

#include <ydb/core/tx/sharding/sharding.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/scheme/scheme_types_proto.h>


namespace NKikimr::NEvWrite {

class TColumnShardShardsSplitter : public IShardsSplitter {
    class TShardInfo : public IShardInfo {
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
            , GranuleShardingVersion(granuleShardingVersion)
        {}

        ui64 GetBytes() const override {
            return Data.size();
        }

        ui32 GetRowsCount() const override {
            return RowsCount;
        }

        const TString& GetData() const override {
            return Data;
        }

        void Serialize(TEvWrite& evWrite) const override {
            evWrite.SetArrowData(SchemaData, Data);
            evWrite.Record.SetGranuleShardingVersion(GranuleShardingVersion);
        }
    };

private:
    TYdbConclusionStatus DoSplitData(const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry, const IEvWriteDataAccessor& data) override;

private:
    TYdbConclusionStatus SplitImpl(const std::shared_ptr<arrow::RecordBatch>& batch, const std::shared_ptr<NSharding::IShardingBase>& sharding);

    std::shared_ptr<arrow::Schema> ExtractArrowSchema(const NKikimrSchemeOp::TColumnTableSchema& schema);
};
}
