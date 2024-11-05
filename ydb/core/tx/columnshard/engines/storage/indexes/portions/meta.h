#pragma once
#include <ydb/core/tx/columnshard/splitter/abstract/chunks.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/meta.h>

namespace NKikimr::NOlap::NIndexes {

class TIndexByColumns: public IIndexMeta {
private:
    using TBase = IIndexMeta;
    std::shared_ptr<NArrow::NSerialization::ISerializer> Serializer;

protected:
    std::set<ui32> ColumnIds;

    virtual TString DoBuildIndexImpl(TChunkedBatchReader& reader) const = 0;

    virtual std::shared_ptr<IPortionDataChunk> DoBuildIndex(const THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, const TIndexInfo& indexInfo) const override final;
    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) override;

    TConclusionStatus CheckSameColumnsForModification(const IIndexMeta& newMeta) const;

public:
    TIndexByColumns() = default;
    TIndexByColumns(const ui32 indexId, const TString& indexName, const std::set<ui32>& columnIds, const TString& storageId);
};

}   // namespace NKikimr::NOlap::NIndexes
