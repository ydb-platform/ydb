#pragma once
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>
#include <ydb/core/formats/arrow/replace_key.h>
#include <ydb/core/tx/columnshard/engines/reader/common/stats.h>
#include <ydb/core/formats/arrow/reader/position.h>

namespace NKikimr::NOlap::NReader::NPlain {

// Holds all metadata that is needed to perform read/scan
struct TReadMetadata : public TReadMetadataBase {
    using TBase = TReadMetadataBase;
public:
    using TConstPtr = std::shared_ptr<const TReadMetadata>;

    NArrow::NMerger::TSortableBatchPosition BuildSortedPosition(const NArrow::TReplaceKey& key) const;
    std::shared_ptr<IDataReader> BuildReader(const std::shared_ptr<TReadContext>& context) const;

    bool HasProcessingColumnIds() const {
        return GetProgram().HasProcessingColumnIds();
    }

    std::shared_ptr<TSelectInfo> SelectInfo;
    NYql::NDqProto::EDqStatsMode StatsMode = NYql::NDqProto::EDqStatsMode::DQ_STATS_MODE_NONE;
    std::vector<TCommittedBlob> CommittedBlobs;
    std::shared_ptr<TReadStats> ReadStats;

    TReadMetadata(const std::shared_ptr<TVersionedIndex> info, const TSnapshot& snapshot, const ESorting sorting, const TProgramContainer& ssaProgram)
        : TBase(info, sorting, ssaProgram, info->GetSchema(snapshot), snapshot)
        , ReadStats(std::make_shared<TReadStats>())
    {
    }

    virtual std::vector<TNameTypeInfo> GetKeyYqlSchema() const override {
        return GetResultSchema()->GetIndexInfo().GetPrimaryKeyColumns();
    }

    TConclusionStatus Init(const TReadDescription& readDescription, const TDataStorageAccessor& dataAccessor);

    std::vector<std::string> GetColumnsOrder() const {
        auto schema = GetResultSchema();
        std::vector<std::string> result;
        for (auto&& i : schema->GetSchema()->fields()) {
            result.emplace_back(i->name());
        }
        return result;
    }

    std::set<ui32> GetEarlyFilterColumnIds() const;
    std::set<ui32> GetPKColumnIds() const;

    bool Empty() const {
        Y_ABORT_UNLESS(SelectInfo);
        return SelectInfo->PortionsOrderedPK.empty() && CommittedBlobs.empty();
    }

    size_t NumIndexedChunks() const {
        Y_ABORT_UNLESS(SelectInfo);
        return SelectInfo->NumChunks();
    }

    size_t NumIndexedBlobs() const {
        Y_ABORT_UNLESS(SelectInfo);
        return SelectInfo->Stats().Blobs;
    }

    std::unique_ptr<TScanIteratorBase> StartScan(const std::shared_ptr<TReadContext>& readContext) const override;

    void Dump(IOutputStream& out) const override {
        out << " index chunks: " << NumIndexedChunks()
            << " index blobs: " << NumIndexedBlobs()
            << " committed blobs: " << CommittedBlobs.size()
      //      << " with program steps: " << (Program ? Program->Steps.size() : 0)
            << " at snapshot: " << GetRequestSnapshot().DebugString();
        TBase::Dump(out);
        if (SelectInfo) {
            out << ", ";
            SelectInfo->DebugStream(out);
        }
    }

    friend IOutputStream& operator << (IOutputStream& out, const TReadMetadata& meta) {
        meta.Dump(out);
        return out;
    }
};

}
