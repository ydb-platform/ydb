#include "copy_blob_ids_to_v2.h"
#include "normalizer.h"

#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/tables_manager.h>

namespace NKikimr::NOlap::NCopyBlobIdsToV2 {

class TBlobsWriting: public INormalizerChanges {
private:
    THashMap<TPortionAddress, std::vector<TString>> BlobsToWrite;

public:
    TBlobsWriting(THashMap<TPortionAddress, std::vector<TString>>&& tasks)
        : BlobsToWrite(std::move(tasks)) {
    }

    virtual bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController&) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);
        using IndexColumnsV2 = NColumnShard::Schema::IndexColumnsV2;
        for (auto&& i : BlobsToWrite) {
            NKikimrTxColumnShard::TIndexPortionBlobsInfo protoBlobs;
            for (auto&& blobId : i.second) {
                *protoBlobs.AddBlobIds() = blobId;
            }
            db.Table<IndexColumnsV2>()
                .Key(i.first.GetPathId().GetRawValue(), i.first.GetPortionId())
                .Update(NIceDb::TUpdate<IndexColumnsV2::BlobIds>(protoBlobs.SerializeAsString()));
        }

        return true;
    }

    virtual ui64 GetSize() const override {
        return BlobsToWrite.size();
    }
};

TConclusion<std::vector<INormalizerTask::TPtr>> TNormalizer::DoInit(
    const TNormalizationController& /*controller*/, NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);

    bool ready = true;
    ready = ready & Schema::Precharge<Schema::IndexPortions>(db, txc.DB.GetScheme());
    ready = ready & Schema::Precharge<Schema::IndexColumnsV2>(db, txc.DB.GetScheme());
    if (!ready) {
        return TConclusionStatus::Fail("Not ready");
    }
    THashMap<ui64, std::vector<TString>> portions0;

    {
        auto rowset = db.Table<Schema::IndexPortions>().Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("Not ready");
        }

        while (!rowset.EndOfSet()) {
            TPortionLoadContext portion(rowset);
            AFL_VERIFY(portion.GetMetaProto().BlobIdsSize());
            std::vector<TString> blobIds;
            for (auto&& i : portion.GetMetaProto().GetBlobIds()) {
                blobIds.emplace_back(i);
            }
            AFL_VERIFY(portions0.emplace(portion.GetPortionId(), std::move(blobIds)).second);

            if (!rowset.Next()) {
                return TConclusionStatus::Fail("Not ready");
            }
        }
    }

    std::vector<INormalizerTask::TPtr> tasks;

    {
        auto rowset = db.Table<Schema::IndexColumnsV2>().Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("Not ready");
        }
        THashMap<TPortionAddress, std::vector<TString>> blobsByPortion;
        while (!rowset.EndOfSet()) {
            TColumnChunkLoadContextV2 chunk(rowset, DsGroupSelector);
            auto it = portions0.find(chunk.GetPortionId());
            AFL_VERIFY(it != portions0.end());
            if (chunk.GetBlobIds().empty()) {
                blobsByPortion.emplace(chunk.GetPortionAddress(), std::move(it->second));
                if (blobsByPortion.size() == 10000) {
                    tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TBlobsWriting>(std::move(blobsByPortion))));
                    blobsByPortion.clear();
                }
            } else {
                AFL_VERIFY(it->second.size() == chunk.GetBlobIds().size());
                for (ui32 idx = 0; idx < it->second.size(); ++idx) {
                    AFL_VERIFY(it->second[idx] == chunk.GetBlobIds()[idx].GetLogoBlobId().AsBinaryString());
                }
            }
            portions0.erase(it);

            if (!rowset.Next()) {
                return TConclusionStatus::Fail("Not ready");
            }
        }
        if (blobsByPortion.size()) {
            tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TBlobsWriting>(std::move(blobsByPortion))));
        }
        AFL_VERIFY(portions0.empty());
    }

    return tasks;
}

}   // namespace NKikimr::NOlap::NCopyBlobIdsToV2
